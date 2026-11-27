package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	searchmetrics "go.miloapis.net/search/internal/metrics"
	"k8s.io/klog/v2"
)

const (
	// nakDelay is how long a failed batch waits before JetStream redelivers it.
	// An immediate Nak would hot-loop while Meilisearch is unavailable; 10s keeps
	// redelivery prompt and visible without that.
	nakDelay = 10 * time.Second

	// flushSlotWaitWarnThreshold is how long a launch helper may block on a flush
	// slot before it reports that the indexer is applying backpressure.
	flushSlotWaitWarnThreshold = 5 * time.Second

	// DefaultMaxInFlightFlushes caps the number of concurrent flush goroutines,
	// and with it the number of message batches held in memory at once. It is the
	// primary knob that bounds the indexer's memory during a Meilisearch stall.
	//
	// It is a tuned value, not a derived one. Measured in production at a healthy
	// 2.8k-deep task queue, a delete task takes a median of 47s and up to 79s from
	// enqueue to finish, and a flush waits for the slowest of its per-index tasks.
	// 16 was chosen so that completions clear the stream's fill rate at that
	// latency: with each flush holding a slot for ~47-79s, 16 slots give enough
	// throughput to keep the backlog (and therefore memory) flat against the
	// incoming audit-event rate. If Meilisearch latency or the event rate changes,
	// re-measure against the search_indexer_flush_duration_seconds histogram
	// rather than assuming 16 still holds.
	DefaultMaxInFlightFlushes = 16

	// DefaultFlushTimeout is the longest one flush may run before it is
	// abandoned and its messages are nacked.
	DefaultFlushTimeout = 11 * time.Minute

	// DefaultAckProgressInterval is how often an in-progress flush heartbeats its
	// messages to keep JetStream from redelivering them.
	DefaultAckProgressInterval = 60 * time.Second

	flushTypeUpsert = "upsert"
	flushTypeDelete = "delete"
	flushTypeMixed  = "mixed"

	flushStatusSuccess = "success"
	flushStatusFailure = "failure"
)

// BatchConfig holds configuration for batching operations.
type BatchConfig struct {
	// BatchSize is the maximum number of items to buffer before flushing.
	BatchSize int
	// FlushInterval is the maximum time to wait before flushing.
	FlushInterval time.Duration
	// MaxConcurrentUploads is the maximum number of concurrent uploads to Meilisearch.
	MaxConcurrentUploads int
	// MaxInFlightFlushes is the maximum number of flushes running at once. Each
	// in-flight flush pins its batch of NATS messages until Meilisearch finishes,
	// so this is what bounds the indexer's memory use.
	MaxInFlightFlushes int
	// AckProgressInterval is how often a running flush tells JetStream that its
	// messages are still being worked on, extending ackWait for as long as the
	// flush is genuinely progressing.
	AckProgressInterval time.Duration
	// FlushTimeout caps one whole flush, enqueue and task wait together. A flush
	// that exceeds it frees its slot and nacks its messages for redelivery, so a
	// slow Meilisearch cannot hold every slot indefinitely. Zero selects
	// DefaultFlushTimeout.
	FlushTimeout time.Duration
	// ChunkSize is the most documents one Meilisearch call may carry. It must
	// not exceed the search client's own chunk size, so that each call the
	// batcher makes is enqueued as exactly one task and every document is
	// recorded against the task that carries it. A call the client splits
	// into several tasks fails its flush (see groupTaskUID). Zero leaves
	// groups whole, which is only safe with a client that never splits.
	ChunkSize int
	// DocState is the store that orders writes to one document across every
	// batcher and replica that shares it. Nil uses a store private to this
	// process, which is only suitable for tests and single-replica setups.
	DocState DocStateStore
}

// SearchClient abstracts the search backend interactions.
type SearchClient interface {
	AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error)
	DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error)
	WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error)
}

// UpsertOp is a single document upsert destined for one index. The document's
// metadata.resourceVersion orders it against other writes to the same object.
type UpsertOp struct {
	IndexUID string
	Doc      any
}

// DeleteOp is a single document deletion destined for one index.
type DeleteOp struct {
	IndexUID string
	DocID    string
	// ResourceVersion orders the removal against other writes to the same
	// object. It is zero when the event carried no object, as a Status
	// response does.
	ResourceVersion uint64
	// ObjectDeleted marks the object itself as gone. Such a removal wins over
	// any write that flushes after it, whatever its resourceVersion, because
	// nothing written for a deleted uid can be current. Without it the
	// removal only means the object no longer belongs in this index, and a
	// newer write brings it back.
	ObjectDeleted bool
}

// pendingOp is the latest operation buffered for one document in one index.
type pendingOp struct {
	indexUID string
	docID    string
	rv       uint64
	state    docState
	doc      any
}

func (op pendingOp) record() docRecord {
	return docRecord{RV: op.rv, State: op.state}
}

func (op pendingOp) isUpsert() bool {
	return op.state == docIndexed
}

// batch is one flush's worth of operations and the messages they came from.
type batch struct {
	ops  []pendingOp
	msgs []jetstream.Msg
	turn enqueueTurn
}

func (bt *batch) counts() (upserts, deletes int) {
	for _, op := range bt.ops {
		if op.isUpsert() {
			upserts++
		} else {
			deletes++
		}
	}
	return upserts, deletes
}

// flushType labels the batch's metrics and logs by the operations it carries.
func (bt *batch) flushType() string {
	upserts, deletes := bt.counts()
	switch {
	case upserts > 0 && deletes > 0:
		return flushTypeMixed
	case deletes > 0:
		return flushTypeDelete
	default:
		return flushTypeUpsert
	}
}

// enqueueTurn orders a batch's Meilisearch enqueue calls after those of every
// batch taken before it. prev closes once the previous batch has enqueued its
// tasks (or given up), and done must be closed once this batch has.
type enqueueTurn struct {
	prev <-chan struct{}
	done chan struct{}
}

// Batcher manages batching of upsert and delete operations.
type Batcher struct {
	client      SearchClient
	batchConfig BatchConfig
	docState    DocStateStore

	// pending holds the newest operation per index/docID, so an upsert and a
	// delete for the same document can never both be buffered: whichever is
	// newest by resourceVersion, or latest to arrive at the same version, is
	// the one flushed. pendingUpserts counts the upserts among them.
	pending        map[string]pendingOp
	pendingUpserts int

	// Track NATS messages for acknowledgement, with the stream sequences already
	// buffered so deduplication stays O(1).
	msgs    []jetstream.Msg
	msgSeqs map[uint64]struct{}

	// lastEnqueue is the done channel of the most recently taken batch.
	// Meilisearch applies an index's tasks in the order they were enqueued, so
	// chaining every batch's enqueue behind the one taken before it keeps an
	// older operation from landing after a newer one for the same document when
	// two batches are in flight. Only the enqueue calls are serialized; waiting
	// for the tasks to finish still runs concurrently across flushes.
	lastEnqueue chan struct{}

	ackProgressInterval time.Duration

	wait func(ctx context.Context, wg *sync.WaitGroup) bool

	mu       sync.Mutex
	sem      chan struct{} // Global semaphore to limit concurrent Meilisearch requests
	flushSem chan struct{} // Bounds the number of flushes (and their message batches) in flight

	// ctx is the lifetime handed to Start. It unblocks waiters on flushSem at
	// shutdown; nil until Start is called.
	ctxMu sync.RWMutex
	ctx   context.Context

	warnMu       sync.Mutex
	lastSlotWarn time.Time

	progressMu   sync.Mutex
	lastProgress time.Time
}

// NewBatcher creates a new Batcher instance.
func NewBatcher(client SearchClient, batchConfig BatchConfig) *Batcher {
	maxConcurrent := batchConfig.MaxConcurrentUploads
	if maxConcurrent <= 0 {
		maxConcurrent = 100
	}

	maxInFlight := batchConfig.MaxInFlightFlushes
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxInFlightFlushes
	}

	ackProgressInterval := batchConfig.AckProgressInterval
	if ackProgressInterval <= 0 {
		ackProgressInterval = DefaultAckProgressInterval
	}

	if batchConfig.FlushTimeout <= 0 {
		batchConfig.FlushTimeout = DefaultFlushTimeout
	}

	docState := batchConfig.DocState
	if docState == nil {
		docState = NewMemoryDocStateStore()
	}

	lastEnqueue := make(chan struct{})
	close(lastEnqueue)

	return &Batcher{
		client:              client,
		batchConfig:         batchConfig,
		docState:            docState,
		ackProgressInterval: ackProgressInterval,
		lastEnqueue:         lastEnqueue,
		wait:                waitOrDone,
		pending:             make(map[string]pendingOp),
		msgs:                make([]jetstream.Msg, 0, batchConfig.BatchSize),
		msgSeqs:             make(map[uint64]struct{}),
		sem:                 make(chan struct{}, maxConcurrent),
		flushSem:            make(chan struct{}, maxInFlight),
	}
}

// Start starts the batch flusher loop.
func (b *Batcher) Start(ctx context.Context) {
	b.ctxMu.Lock()
	b.ctx = ctx
	b.ctxMu.Unlock()

	go b.runBatcher(ctx)
}

// doneChan returns the shutdown channel of the context passed to Start, or nil
// if Start was never called. A nil channel blocks forever in a select, so an
// unstarted batcher keeps its pre-shutdown behaviour.
func (b *Batcher) doneChan() <-chan struct{} {
	b.ctxMu.RLock()
	defer b.ctxMu.RUnlock()

	if b.ctx == nil {
		return nil
	}
	return b.ctx.Done()
}

// Submit hands every operation derived from one source message to the batcher
// atomically, and is the only entry point that is safe when a message fans out
// into more than one operation.
//
// A message typically produces one operation per matching policy. Queueing them
// one at a time let a batch trigger fire partway through that fan-out: the first
// batch took the message and acked it once flushed, while the message's
// remaining operations sat in the next batch, where a failure would lose them
// and leave ghost documents behind. Applying all of a message's operations under
// one lock hold, and evaluating the triggers only afterwards, makes that split
// impossible.
func (b *Batcher) Submit(msg *jetstream.Msg, upserts []UpsertOp, deletes []DeleteOp) {
	if len(upserts) == 0 && len(deletes) == 0 {
		return
	}

	// Deletes are buffered before upserts so that, within one message, a policy
	// that matches wins over one that does not for the same index.
	ops := make([]pendingOp, 0, len(deletes)+len(upserts))
	for _, op := range deletes {
		state := docUnindexed
		if op.ObjectDeleted {
			state = docDeleted
		}
		ops = append(ops, pendingOp{
			indexUID: op.IndexUID,
			docID:    op.DocID,
			rv:       op.ResourceVersion,
			state:    state,
		})
	}
	for _, op := range upserts {
		ops = append(ops, pendingOp{
			indexUID: op.IndexUID,
			docID:    upsertDocID(op.Doc),
			rv:       resourceVersionOf(op.Doc),
			state:    docIndexed,
			doc:      op.Doc,
		})
	}

	b.mu.Lock()

	// An operation without a document id cannot be ordered. The id is the
	// document's top-level uid, so a resource whose own top-level uid is not
	// a string, or a transform that sets one, leaves it empty, and every such
	// document in an index would share one document state key and be ordered
	// against unrelated objects. It is dropped here. Its message is still
	// tracked, so it is acked with the batch rather than redelivered forever.
	for _, op := range ops {
		if op.docID == "" {
			klog.Warningf("Skipping %s for index %s: it has no document id; a document's uid must be a non-empty string", opVerb(op), op.indexUID)
			continue
		}
		b.setPending(op)
	}

	if msg != nil {
		b.trackMessage(msg)
	}

	// The trigger is evaluated once, here, after every operation belonging to
	// this message is in the buffer, so a message is never split across two
	// batches.
	//
	// A batch flushes on the message count or the upsert key count: a source
	// message contributes at most one upsert key per policy but the keys
	// collapse by document, so both clauses trip at roughly the same point.
	//
	// Delete keys are not counted. A single source message can fan out into one
	// delete key per policy, so a delete key-count trigger would fire after a
	// fraction of the messages and produce one tiny Meilisearch task per index.
	// Pending delete keys are bounded by policy count times BatchSize and each
	// key is two short strings, so it is the message slice, holding full
	// payloads, that the message count protects.
	var (
		bt  batch
		due bool
	)
	if len(b.msgs) >= b.batchConfig.BatchSize || b.pendingUpserts >= b.batchConfig.BatchSize {
		bt = b.takeBatch()
		due = true
	}

	b.setPendingMetrics()
	b.mu.Unlock()

	if due {
		upserts, deletes := bt.counts()
		klog.Infof("Batch size reached, flushing %d upserts and %d deletes from %d unique messages", upserts, deletes, len(bt.msgs))
		b.launchFlush(bt)
	}
}

// setPending records op as the buffered operation for its document unless
// something at least as new is already buffered, in which case op is dropped:
// a redelivered or late-arriving older event must not displace a newer one
// that is waiting in the same window. MUST hold lock.
func (b *Batcher) setPending(op pendingOp) {
	key := pendingKey(op.indexUID, op.docID)
	if prev, ok := b.pending[key]; ok {
		if !op.record().supersedes(prev.record()) {
			return
		}
		if prev.isUpsert() {
			b.pendingUpserts--
		}
	}
	if op.isUpsert() {
		b.pendingUpserts++
	}
	b.pending[key] = op
}

// upsertDocID is the id of a document to upsert: its uid field, which is the
// index's primary key. It is empty when the field is missing or not a string,
// as when the resource has a top-level uid of its own that is not a string.
func upsertDocID(doc any) string {
	m, ok := doc.(map[string]any)
	if !ok {
		return ""
	}
	uid, _ := m["uid"].(string)
	return uid
}

// opVerb names what op does to its document, for logs.
func opVerb(op pendingOp) string {
	if op.isUpsert() {
		return flushTypeUpsert
	}
	return flushTypeDelete
}

func pendingKey(indexUID, docID string) string {
	return indexUID + "/" + docID
}

// setPendingMetrics publishes the buffered operation counts. MUST hold lock.
func (b *Batcher) setPendingMetrics() {
	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeUpsert).Set(float64(b.pendingUpserts))
	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeDelete).Set(float64(len(b.pending) - b.pendingUpserts))
}

// QueueUpsert adds a document to the pending map and triggers an asynchronous
// flush if the batch size is reached. It is shorthand for a single-operation
// Submit; callers that derive several operations from one message must call
// Submit directly so those operations cannot be split across batches.
func (b *Batcher) QueueUpsert(indexUID string, doc any, msg *jetstream.Msg) {
	b.Submit(msg, []UpsertOp{{IndexUID: indexUID, Doc: doc}}, nil)
}

// QueueDelete removes a document from an index without ordering information,
// as for a document that no longer belongs there. It is shorthand for a
// single-operation Submit; see QueueUpsert.
func (b *Batcher) QueueDelete(indexUID string, docID string, msg *jetstream.Msg) {
	b.Submit(msg, nil, []DeleteOp{{IndexUID: indexUID, DocID: docID}})
}

// trackMessage adds the message to the batch if it hasn't been seen yet.
// must be called with lock held.
func (b *Batcher) trackMessage(msg *jetstream.Msg) {
	meta, err := (*msg).Metadata()
	if err != nil {
		klog.Warningf("Failed to get message metadata, message will be treated as unique: %v", err)
		b.msgs = append(b.msgs, *msg)
		return
	}

	seq := meta.Sequence.Stream
	if _, ok := b.msgSeqs[seq]; ok {
		return // Already have this message
	}
	b.msgSeqs[seq] = struct{}{}
	b.msgs = append(b.msgs, *msg)
}

func (b *Batcher) runBatcher(ctx context.Context) {
	ticker := time.NewTicker(b.batchConfig.FlushInterval)
	defer ticker.Stop()

	for {
		b.markProgress()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flush()
		}
	}
}

func (b *Batcher) flush() {
	b.mu.Lock()
	if len(b.msgs) == 0 && len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	bt := b.takeBatch()
	b.setPendingMetrics()
	b.mu.Unlock()

	b.launchFlush(bt)
}

// launchFlush waits for the previous batch to finish enqueueing and for a free
// in-flight flush slot, then runs the flush asynchronously. Blocking the caller
// is intentional: the JetStream handler queues messages sequentially, so
// blocking here makes the consumer's prefetch window the intake bound instead
// of letting flush goroutines (and the message payloads they pin) pile up
// without limit.
//
// The turn is awaited before the slot so that a slot is only ever held by a
// batch whose predecessors have all enqueued. Holding a slot while waiting for
// the turn would let later batches fill every slot and starve the batch they
// are waiting on.
func (b *Batcher) launchFlush(bt batch) {
	flushType := bt.flushType()
	if !b.awaitTurn(bt.turn, flushType) || !b.acquireFlushSlot(flushType) {
		// Shutting down. The batch is abandoned without being acked or nacked, so
		// JetStream redelivers it to another replica once ackWait expires.
		close(bt.turn.done)
		return
	}
	go func() {
		defer func() { <-b.flushSem }()
		b.performFlush(bt)
	}()
}

// awaitTurn blocks until every batch taken before this one has enqueued its
// tasks. It returns false if the batcher's context is cancelled first. Never
// call it while holding b.mu.
func (b *Batcher) awaitTurn(turn enqueueTurn, flushType string) bool {
	select {
	case <-turn.prev:
		return true
	case <-b.doneChan():
		klog.Infof("Shutting down, abandoning a %s flush that was waiting for an earlier batch to enqueue", flushType)
		return false
	}
}

// acquireFlushSlot blocks until an in-flight flush slot frees up, reporting the
// stall if it takes long enough to matter. It returns false if the batcher's
// context is cancelled first, which is what unwinds a blocked queue call on
// SIGTERM. Never call it while holding b.mu.
//
// Shutdown only skips flushes still waiting for a slot: a flush that already
// holds one is neither cancelled nor awaited and runs to its own timeout, and
// its messages are redelivered after ackWait just like the skipped ones.
func (b *Batcher) acquireFlushSlot(flushType string) bool {
	start := time.Now()
	done := b.doneChan()

	timer := time.NewTimer(flushSlotWaitWarnThreshold)
	defer timer.Stop()

	for {
		select {
		case b.flushSem <- struct{}{}:
			searchmetrics.IndexerFlushSlotWait.WithLabelValues(flushType).Observe(time.Since(start).Seconds())
			return true
		case <-done:
			klog.Infof("Shutting down, abandoning a %s flush that waited %s for a flush slot", flushType, time.Since(start).Truncate(time.Second))
			return false
		case <-timer.C:
			b.warnFlushBackpressure(flushType, time.Since(start))
			timer.Reset(flushSlotWaitWarnThreshold)
		}
	}
}

// warnFlushBackpressure logs at most once per threshold window across all
// waiters so a sustained stall does not flood the log.
func (b *Batcher) warnFlushBackpressure(flushType string, blocked time.Duration) {
	b.warnMu.Lock()
	defer b.warnMu.Unlock()

	if time.Since(b.lastSlotWarn) < flushSlotWaitWarnThreshold {
		return
	}
	b.lastSlotWarn = time.Now()

	klog.Warningf("Indexer is applying backpressure: %s flush has waited %s for a flush slot, all %d in-flight flush slots are busy",
		flushType, blocked.Truncate(time.Second), cap(b.flushSem))
}

// takeBatch captures and resets the current buffer and reserves the batch's
// place in the enqueue order. MUST hold lock.
func (b *Batcher) takeBatch() batch {
	bt := batch{
		ops:  make([]pendingOp, 0, len(b.pending)),
		msgs: b.msgs,
		turn: enqueueTurn{prev: b.lastEnqueue, done: make(chan struct{})},
	}
	for _, op := range b.pending {
		bt.ops = append(bt.ops, op)
	}
	b.lastEnqueue = bt.turn.done

	// Reset
	b.pending = make(map[string]pendingOp)
	b.pendingUpserts = 0
	b.msgs = make([]jetstream.Msg, 0, b.batchConfig.BatchSize)
	b.msgSeqs = make(map[uint64]struct{})

	return bt
}

// startAckProgress heartbeats a batch's messages for as long as its flush is
// running, and returns a stop function that joins the heartbeat goroutine.
//
// A healthy Meilisearch can take longer to finish a batch's tasks than the
// consumer's ackWait, and a flush waits for the slowest of its per-index tasks.
// Without a heartbeat JetStream would redeliver messages that are being indexed
// right now. Calling InProgress on every tick extends the window for as long as
// the flush is progressing, so the wait timeout no longer has to fit inside
// ackWait. The stop function must be called before the ack/nak loop so a
// heartbeat can never race an Ack on the same message.
func (b *Batcher) startAckProgress(msgs []jetstream.Msg, flushType string) (stop func()) {
	if len(msgs) == 0 {
		return func() {}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(b.ackProgressInterval)
		defer ticker.Stop()

		warned := false

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var firstErr error
				for _, msg := range msgs {
					if err := msg.InProgress(); err != nil && firstErr == nil {
						firstErr = err
					}
				}
				searchmetrics.IndexerAckProgress.WithLabelValues(flushType).Add(float64(len(msgs)))

				// Once per flush, not once per message or per tick: a failure here
				// is the same connection problem for every message in the batch.
				if firstErr != nil && !warned {
					warned = true
					klog.Warningf("Failed to extend ackWait for a %s batch of %d messages: %v", flushType, len(msgs), firstErr)
				}
			}
		}
	}()

	return func() {
		close(done)
		wg.Wait()
	}
}

// flushErrors collects the failures of one flush from its goroutines.
type flushErrors struct {
	mu   sync.Mutex
	errs []error
}

func (f *flushErrors) add(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, err)
}

func (f *flushErrors) join() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.Join(f.errs...)
}

// performFlush writes a batch to Meilisearch in three phases. First every
// operation is claimed in the document state store, which replaces any that
// an earlier flush anywhere has already superseded with the newer state. Then
// the resulting states are enqueued. Finally each is recorded against the task
// that carries it; where the store shows a newer state was enqueued before
// this task, the newer state is enqueued again so it lands last. See
// ordering.go for why each phase is needed.
//
// FlushTimeout bounds all three phases together. A flush that exceeds it
// stops where it is and nacks its messages; the redelivery runs the protocol
// again from the newest recorded state.
func (b *Batcher) performFlush(bt batch) {
	flushType := bt.flushType()
	upserts, deletes := bt.counts()
	klog.Infof("Flushing batch of %d upserts and %d deletes to Meilisearch...", upserts, deletes)

	searchmetrics.IndexerInFlightFlushes.Inc()
	defer searchmetrics.IndexerInFlightFlushes.Dec()
	start := time.Now()
	defer func() {
		searchmetrics.IndexerFlushDuration.WithLabelValues(flushType).Observe(time.Since(start).Seconds())
	}()

	// Flushes outlive a shutdown of the batcher on purpose (see
	// acquireFlushSlot), so the flush gets its own lifetime rather than the
	// batcher's, capped by FlushTimeout. The store calls share it.
	ctx, cancel := context.WithTimeout(context.Background(), b.batchConfig.FlushTimeout)
	defer cancel()

	// Heartbeat the batch for the whole time its tasks are outstanding.
	stopAckProgress := b.startAckProgress(bt.msgs, flushType)

	var wg sync.WaitGroup
	var errs flushErrors

	claims, err := b.claimBatch(ctx, bt.ops)
	if err != nil {
		errs.add(fmt.Errorf("claiming document state: %w", err))
	}

	var groups []*flushGroup
	enqueued := false
	if err == nil {
		groups = groupClaims(claims, b.batchConfig.ChunkSize)
		if enqueued = b.enqueueGroups(ctx, groups, &wg, &errs); !enqueued {
			errs.add(fmt.Errorf("flush exceeded %s while enqueueing", b.batchConfig.FlushTimeout))
		}
	}
	close(bt.turn.done)

	// A group whose enqueue was abandoned may still set its task uid, so the
	// record step only runs once every enqueue of the round has returned.
	for round := 0; enqueued && len(groups) > 0; round++ {
		repairs, recordErr := b.recordBatch(ctx, groups)
		if recordErr != nil {
			errs.add(fmt.Errorf("recording document state: %w", recordErr))
			break
		}
		if len(repairs) == 0 {
			break
		}
		if round >= maxRepairRounds {
			errs.add(fmt.Errorf("%d documents still carry a superseded state after %d repair rounds", len(repairs), maxRepairRounds))
			break
		}
		searchmetrics.IndexerOrderRepairs.Add(float64(len(repairs)))
		klog.Infof("Re-enqueueing %d documents whose newer state was enqueued before this batch's", len(repairs))
		groups = groupClaims(repairs, b.batchConfig.ChunkSize)
		if enqueued = b.enqueueGroups(ctx, groups, &wg, &errs); !enqueued {
			errs.add(fmt.Errorf("flush exceeded %s while re-enqueueing", b.batchConfig.FlushTimeout))
		}
	}

	if !b.wait(ctx, &wg) {
		errs.add(fmt.Errorf("flush exceeded %s", b.batchConfig.FlushTimeout))
	}
	stopAckProgress()

	if err := ctx.Err(); err != nil {
		errs.add(err)
	}

	if err := errs.join(); err != nil {
		searchmetrics.IndexerFlushTotal.WithLabelValues(flushType, flushStatusFailure).Inc()
		klog.Errorf("Failed to flush %d upserts and %d deletes from %d messages, nacking for redelivery in %s: %v",
			upserts, deletes, len(bt.msgs), nakDelay, err)
		b.nakBatch(bt.msgs, flushType)
		return
	}

	searchmetrics.IndexerFlushTotal.WithLabelValues(flushType, flushStatusSuccess).Inc()
	klog.Infof("Successfully flushed %d upserts and %d deletes", upserts, deletes)
	for _, msg := range bt.msgs {
		msg.Ack()
	}
}

// enqueueGroups sends each group to Meilisearch and reports whether every
// group was enqueued, or gave up, before ctx ended; when it returns true the
// task uids are recorded on the groups. Waiting for the tasks to finish
// continues in the background under wg. No key appears in two groups of one
// round, so the order of the groups' tasks relative to each other does not
// matter; only the enqueue barrier against the next batch does.
func (b *Batcher) enqueueGroups(ctx context.Context, groups []*flushGroup, wg *sync.WaitGroup, errs *flushErrors) bool {
	var enqueueWG sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		enqueueWG.Add(1)
		go func(g *flushGroup) {
			defer wg.Done()

			// 1. Enqueue - Limited by semaphore
			select {
			case b.sem <- struct{}{}:
			case <-ctx.Done():
				errs.add(ctx.Err())
				enqueueWG.Done()
				return
			}
			tasks, err := g.enqueue(b.client)
			<-b.sem
			if err == nil {
				g.task, err = groupTaskUID(g, tasks)
			}
			enqueueWG.Done()

			if err != nil {
				errs.add(err)
				return
			}

			// 2. Wait (Polling) - NOT limited by semaphore
			if _, err := b.client.WaitForTasks(tasks); err != nil {
				errs.add(err)
			}
		}(g)
	}
	return b.wait(ctx, &enqueueWG)
}

// groupTaskUID is the uid to record against every document in g, which is
// the uid of the one task the group was enqueued as.
//
// A group enqueued as several tasks cannot be recorded at all. Each document
// lands with whichever chunk carried it, and the returned tasks do not say
// which. No single uid is safe for all of them: the lowest makes a document in
// a later chunk look older than a state another flush enqueued between the
// chunks, so the repair is left to a flush that may have finished; the highest
// makes a document in an earlier chunk look newer than such a state, so the
// record reads as applied and the other flush skips the repair it owes. The
// group is reported as an error instead, which fails the flush; its batch is
// nacked and the redelivery claims and enqueues again.
//
// The batcher sizes groups to BatchConfig.ChunkSize, so this only happens when
// that exceeds the search client's own chunk size.
func groupTaskUID(g *flushGroup, tasks []*meilisearch.Task) (int64, error) {
	var uids []int64
	for _, t := range tasks {
		if t != nil {
			uids = append(uids, t.TaskUID)
		}
	}
	switch len(uids) {
	case 0:
		return 0, nil
	case 1:
		return uids[0], nil
	default:
		return 0, fmt.Errorf("index %s: a group of %d documents was enqueued as %d tasks %v, so the task carrying each document is unknown; the batcher's chunk size must not exceed the search client's chunk size",
			g.indexUID, len(g.claims), len(uids), uids)
	}
}

// waitOrDone waits for wg and reports whether it finished before ctx ended.
// The goroutines still running when ctx ends are left to finish on their own.
func waitOrDone(ctx context.Context, wg *sync.WaitGroup) bool {
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-ctx.Done():
		return false
	}
}

// nakBatch returns a failed batch to JetStream for redelivery. Without it the
// batch would sit unacknowledged until ackWait expires.
func (b *Batcher) nakBatch(msgs []jetstream.Msg, flushType string) {
	for _, msg := range msgs {
		if err := msg.NakWithDelay(nakDelay); err != nil {
			klog.Errorf("Failed to nak message for redelivery: %v", err)
		}
	}
	searchmetrics.IndexerMessagesNacked.WithLabelValues(flushType).Add(float64(len(msgs)))
}
