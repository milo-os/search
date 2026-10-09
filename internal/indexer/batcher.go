package indexer

import (
	"context"
	"errors"
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
}

// SearchClient abstracts the search backend interactions.
type SearchClient interface {
	AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error)
	DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error)
	WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error)
}

// UpsertOp is a single document upsert destined for one index.
type UpsertOp struct {
	IndexUID string
	Doc      any
}

// DeleteOp is a single document deletion destined for one index.
type DeleteOp struct {
	IndexUID string
	DocID    string
}

type upsertItem struct {
	indexUID string
	doc      any
}

type deleteItem struct {
	indexUID string
	docID    string
}

// pendingOp is the latest operation buffered for one document in one index.
type pendingOp struct {
	isDelete bool
	upsert   upsertItem
	delete   deleteItem
}

// batch is one flush's worth of operations and the messages they came from.
type batch struct {
	upserts []upsertItem
	deletes []deleteItem
	msgs    []jetstream.Msg
	turn    enqueueTurn
}

// flushType labels the batch's metrics and logs by the operations it carries.
func (bt *batch) flushType() string {
	switch {
	case len(bt.upserts) > 0 && len(bt.deletes) > 0:
		return flushTypeMixed
	case len(bt.deletes) > 0:
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

	// pending holds the latest operation per index/docID, so an upsert and a
	// delete for the same document can never both be buffered: whichever came
	// last is the one flushed. pendingUpserts counts the upserts among them.
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

	mu       sync.Mutex
	sem      chan struct{} // Global semaphore to limit concurrent Meilisearch requests
	flushSem chan struct{} // Bounds the number of flushes (and their message batches) in flight

	// ctx is the lifetime handed to Start. It unblocks waiters on flushSem at
	// shutdown; nil until Start is called.
	ctxMu sync.RWMutex
	ctx   context.Context

	warnMu       sync.Mutex
	lastSlotWarn time.Time
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

	lastEnqueue := make(chan struct{})
	close(lastEnqueue)

	return &Batcher{
		client:              client,
		batchConfig:         batchConfig,
		ackProgressInterval: ackProgressInterval,
		lastEnqueue:         lastEnqueue,
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

	b.mu.Lock()

	// Deletes are buffered before upserts so that, within one message, a policy
	// that matches wins over one that does not for the same index.
	for _, op := range deletes {
		b.setPending(op.IndexUID, op.DocID, pendingOp{
			isDelete: true,
			delete:   deleteItem{indexUID: op.IndexUID, docID: op.DocID},
		})
	}

	for _, op := range upserts {
		// Extract UID for deduplication key
		var docID string
		if m, ok := op.Doc.(map[string]any); ok {
			if uid, ok := m["uid"].(string); ok {
				docID = uid
			}
		}

		b.setPending(op.IndexUID, docID, pendingOp{
			upsert: upsertItem{indexUID: op.IndexUID, doc: op.Doc},
		})
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
		klog.Infof("Batch size reached, flushing %d upserts and %d deletes from %d unique messages", len(bt.upserts), len(bt.deletes), len(bt.msgs))
		b.launchFlush(bt)
	}
}

// setPending records op as the latest operation for docID in indexUID,
// replacing whatever was buffered for it. MUST hold lock.
func (b *Batcher) setPending(indexUID, docID string, op pendingOp) {
	key := indexUID + "/" + docID
	if prev, ok := b.pending[key]; ok && !prev.isDelete {
		b.pendingUpserts--
	}
	if !op.isDelete {
		b.pendingUpserts++
	}
	b.pending[key] = op
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

// QueueDelete adds a document ID to the pending map and triggers an asynchronous
// flush if the batch size is reached. It is shorthand for a single-operation
// Submit; see QueueUpsert.
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
		upserts: make([]upsertItem, 0, b.pendingUpserts),
		deletes: make([]deleteItem, 0, len(b.pending)-b.pendingUpserts),
		msgs:    b.msgs,
		turn:    enqueueTurn{prev: b.lastEnqueue, done: make(chan struct{})},
	}
	for _, op := range b.pending {
		if op.isDelete {
			bt.deletes = append(bt.deletes, op.delete)
		} else {
			bt.upserts = append(bt.upserts, op.upsert)
		}
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

func (b *Batcher) performFlush(bt batch) {
	flushType := bt.flushType()
	klog.Infof("Flushing batch of %d upserts and %d deletes to Meilisearch...", len(bt.upserts), len(bt.deletes))

	searchmetrics.IndexerInFlightFlushes.Inc()
	defer searchmetrics.IndexerInFlightFlushes.Dec()
	start := time.Now()
	defer func() {
		searchmetrics.IndexerFlushDuration.WithLabelValues(flushType).Observe(time.Since(start).Seconds())
	}()

	upsertGroups := make(map[string][]any)
	for _, item := range bt.upserts {
		upsertGroups[item.indexUID] = append(upsertGroups[item.indexUID], item.doc)
	}
	deleteGroups := make(map[string][]string)
	for _, item := range bt.deletes {
		deleteGroups[item.indexUID] = append(deleteGroups[item.indexUID], item.docID)
	}

	var wg, enqueueWG sync.WaitGroup
	var errs []error
	var errMu sync.Mutex

	// Heartbeat the batch for the whole time its tasks are outstanding.
	stopAckProgress := b.startAckProgress(bt.msgs, flushType)

	// run enqueues one Meilisearch task and then waits for it. The keys in a
	// batch are unique, so the order of its tasks relative to each other does
	// not matter; only the enqueue barrier against the next batch does.
	run := func(enqueue func() ([]*meilisearch.Task, error)) {
		wg.Add(1)
		enqueueWG.Add(1)
		go func() {
			defer wg.Done()

			// 1. Enqueue - Limited by semaphore
			b.sem <- struct{}{}
			tasks, err := enqueue()
			<-b.sem
			enqueueWG.Done()

			if err != nil {
				errMu.Lock()
				errs = append(errs, err)
				errMu.Unlock()
				return
			}

			// 2. Wait (Polling) - NOT limited by semaphore
			_, err = b.client.WaitForTasks(tasks)
			if err != nil {
				errMu.Lock()
				errs = append(errs, err)
				errMu.Unlock()
			}
		}()
	}

	for indexUID, docs := range upsertGroups {
		run(func() ([]*meilisearch.Task, error) { return b.client.AddDocumentsAsync(indexUID, docs) })
	}
	for indexUID, docIDs := range deleteGroups {
		run(func() ([]*meilisearch.Task, error) { return b.client.DeleteDocumentsAsync(indexUID, docIDs) })
	}
	enqueueWG.Wait()
	close(bt.turn.done)
	wg.Wait()
	stopAckProgress()

	if len(errs) > 0 {
		searchmetrics.IndexerFlushTotal.WithLabelValues(flushType, flushStatusFailure).Inc()
		klog.Errorf("Failed to flush %d upserts and %d deletes from %d messages, nacking for redelivery in %s: %v",
			len(bt.upserts), len(bt.deletes), len(bt.msgs), nakDelay, errors.Join(errs...))
		b.nakBatch(bt.msgs, flushType)
		return
	}

	searchmetrics.IndexerFlushTotal.WithLabelValues(flushType, flushStatusSuccess).Inc()
	klog.Infof("Successfully flushed %d upserts and %d deletes", len(bt.upserts), len(bt.deletes))
	for _, msg := range bt.msgs {
		msg.Ack()
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
