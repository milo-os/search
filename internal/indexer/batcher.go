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

	// defaultMaxInFlightFlushes caps the number of concurrent flush goroutines,
	// and with it the number of message batches held in memory at once.
	defaultMaxInFlightFlushes = 8

	flushTypeUpsert = "upsert"
	flushTypeDelete = "delete"

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

// Batcher manages batching of upsert and delete operations.
type Batcher struct {
	client      SearchClient
	batchConfig BatchConfig

	// Track unique operations in the current batch
	pendingUpserts map[string]upsertItem
	pendingDeletes map[string]deleteItem

	// Track NATS messages for acknowledgement, with the stream sequences already
	// buffered so deduplication stays O(1).
	upsertMsgs    []jetstream.Msg
	deleteMsgs    []jetstream.Msg
	upsertMsgSeqs map[uint64]struct{}
	deleteMsgSeqs map[uint64]struct{}

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
		maxInFlight = defaultMaxInFlightFlushes
	}

	return &Batcher{
		client:         client,
		batchConfig:    batchConfig,
		pendingUpserts: make(map[string]upsertItem),
		pendingDeletes: make(map[string]deleteItem),
		upsertMsgs:     make([]jetstream.Msg, 0, batchConfig.BatchSize),
		deleteMsgs:     make([]jetstream.Msg, 0, batchConfig.BatchSize),
		upsertMsgSeqs:  make(map[uint64]struct{}),
		deleteMsgSeqs:  make(map[uint64]struct{}),
		sem:            make(chan struct{}, maxConcurrent),
		flushSem:       make(chan struct{}, maxInFlight),
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

	for _, op := range upserts {
		// Extract UID for deduplication key
		var docID string
		if m, ok := op.Doc.(map[string]any); ok {
			if uid, ok := m["uid"].(string); ok {
				docID = uid
			}
		}

		// Add/Update the pending item (last write wins for same docID)
		b.pendingUpserts[op.IndexUID+"/"+docID] = upsertItem{
			indexUID: op.IndexUID,
			doc:      op.Doc,
		}
	}

	for _, op := range deletes {
		b.pendingDeletes[op.IndexUID+"/"+op.DocID] = deleteItem{
			indexUID: op.IndexUID,
			docID:    op.DocID,
		}
	}

	if msg != nil {
		if len(upserts) > 0 {
			b.trackMessage(msg, true)
		}
		if len(deletes) > 0 {
			b.trackMessage(msg, false)
		}
	}

	// Both triggers are evaluated once, here, after every operation belonging to
	// this message is in the buffers, so a message is never split across two
	// batches of the same kind.
	//
	// Upserts flush on the message count or the key count: a source message
	// contributes at most one upsert key per policy but the keys collapse by
	// document, so both clauses trip at roughly the same point.
	//
	// Deletes flush on the message count only. A single source message fans out
	// into one delete key per matching policy, so a key-count trigger would fire
	// after a fraction of the messages and produce one tiny Meilisearch task per
	// index. Counting messages keeps each flush at roughly BatchSize IDs per
	// index, which Meilisearch batches far more efficiently. Dropping the
	// key-count clause does not risk unbounded growth: pending delete keys are
	// bounded by policy count times BatchSize and each key is two short strings,
	// so it is the message slice, holding full payloads, that this bound
	// protects.
	var (
		upsertQueue []upsertItem
		upsertMsgs  []jetstream.Msg
		upsertDue   bool
		deleteQueue []deleteItem
		deleteMsgs  []jetstream.Msg
		deleteDue   bool
	)
	if len(b.upsertMsgs) >= b.batchConfig.BatchSize || len(b.pendingUpserts) >= b.batchConfig.BatchSize {
		upsertQueue, upsertMsgs = b.takeUpsertBatch()
		upsertDue = true
	}
	if len(b.deleteMsgs) >= b.batchConfig.BatchSize {
		deleteQueue, deleteMsgs = b.takeDeleteBatch()
		deleteDue = true
	}

	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeUpsert).Set(float64(len(b.pendingUpserts)))
	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeDelete).Set(float64(len(b.pendingDeletes)))
	b.mu.Unlock()

	if upsertDue {
		klog.Infof("Batch size reached, flushing %d upserts from %d unique messages", len(upsertQueue), len(upsertMsgs))
		b.launchUpsertFlush(upsertQueue, upsertMsgs)
	}
	if deleteDue {
		klog.Infof("Batch size reached, flushing %d deletes from %d unique messages", len(deleteQueue), len(deleteMsgs))
		b.launchDeleteFlush(deleteQueue, deleteMsgs)
	}
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

// trackMessage adds the message to the appropriate list if it hasn't been seen yet.
// must be called with lock held.
func (b *Batcher) trackMessage(msg *jetstream.Msg, isUpsert bool) {
	meta, err := (*msg).Metadata()
	if err != nil {
		klog.Warningf("Failed to get message metadata, message will be treated as unique: %v", err)
		if isUpsert {
			b.upsertMsgs = append(b.upsertMsgs, *msg)
		} else {
			b.deleteMsgs = append(b.deleteMsgs, *msg)
		}
		return
	}

	seq := meta.Sequence.Stream

	if isUpsert {
		if _, ok := b.upsertMsgSeqs[seq]; ok {
			return // Already have this message
		}
		b.upsertMsgSeqs[seq] = struct{}{}
		b.upsertMsgs = append(b.upsertMsgs, *msg)
		return
	}

	if _, ok := b.deleteMsgSeqs[seq]; ok {
		return // Already have this message
	}
	b.deleteMsgSeqs[seq] = struct{}{}
	b.deleteMsgs = append(b.deleteMsgs, *msg)
}

func (b *Batcher) runBatcher(ctx context.Context) {
	ticker := time.NewTicker(b.batchConfig.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushUpserts()
			b.flushDeletes()
		}
	}
}

func (b *Batcher) flushUpserts() {
	b.mu.Lock()
	if len(b.upsertMsgs) == 0 && len(b.pendingUpserts) == 0 {
		b.mu.Unlock()
		return
	}
	queue, msgs := b.takeUpsertBatch()
	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeUpsert).Set(0)
	b.mu.Unlock()

	b.launchUpsertFlush(queue, msgs)
}

func (b *Batcher) flushDeletes() {
	b.mu.Lock()
	if len(b.deleteMsgs) == 0 && len(b.pendingDeletes) == 0 {
		b.mu.Unlock()
		return
	}
	queue, msgs := b.takeDeleteBatch()
	searchmetrics.IndexerPendingOperations.WithLabelValues(flushTypeDelete).Set(0)
	b.mu.Unlock()

	b.launchDeleteFlush(queue, msgs)
}

// launchUpsertFlush waits for a free in-flight flush slot and then runs the
// flush asynchronously. Blocking the caller is intentional: the JetStream
// handler queues messages sequentially, so blocking here makes the consumer's
// prefetch window the intake bound instead of letting flush goroutines (and the
// message payloads they pin) pile up without limit.
func (b *Batcher) launchUpsertFlush(queue []upsertItem, msgs []jetstream.Msg) {
	if !b.acquireFlushSlot(flushTypeUpsert) {
		// Shutting down. The batch is abandoned without being acked or nacked, so
		// JetStream redelivers it to another replica once ackWait expires.
		return
	}
	go func() {
		defer func() { <-b.flushSem }()
		b.performUpsertFlush(queue, msgs)
	}()
}

// launchDeleteFlush is the delete counterpart of launchUpsertFlush.
func (b *Batcher) launchDeleteFlush(queue []deleteItem, msgs []jetstream.Msg) {
	if !b.acquireFlushSlot(flushTypeDelete) {
		// Shutting down. The batch is abandoned without being acked or nacked, so
		// JetStream redelivers it to another replica once ackWait expires.
		return
	}
	go func() {
		defer func() { <-b.flushSem }()
		b.performDeleteFlush(queue, msgs)
	}()
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

// takeUpsertBatch captures and resets the current upsert buffer. MUST hold lock.
func (b *Batcher) takeUpsertBatch() ([]upsertItem, []jetstream.Msg) {
	queue := make([]upsertItem, 0, len(b.pendingUpserts))
	for _, item := range b.pendingUpserts {
		queue = append(queue, item)
	}

	msgs := b.upsertMsgs

	// Reset
	b.pendingUpserts = make(map[string]upsertItem)
	b.upsertMsgs = make([]jetstream.Msg, 0, b.batchConfig.BatchSize)
	b.upsertMsgSeqs = make(map[uint64]struct{})

	return queue, msgs
}

// takeDeleteBatch captures and resets the current delete buffer. MUST hold lock.
func (b *Batcher) takeDeleteBatch() ([]deleteItem, []jetstream.Msg) {
	// Convert map to slice for processing
	queue := make([]deleteItem, 0, len(b.pendingDeletes))
	for _, item := range b.pendingDeletes {
		queue = append(queue, item)
	}

	msgs := b.deleteMsgs

	// Reset
	b.pendingDeletes = make(map[string]deleteItem)
	b.deleteMsgs = make([]jetstream.Msg, 0, b.batchConfig.BatchSize)
	b.deleteMsgSeqs = make(map[uint64]struct{})

	return queue, msgs
}

func (b *Batcher) performUpsertFlush(queue []upsertItem, msgs []jetstream.Msg) {
	klog.Infof("Flushing batch of %d upserts to Meilisearch...", len(queue))

	searchmetrics.IndexerInFlightFlushes.Inc()
	defer searchmetrics.IndexerInFlightFlushes.Dec()
	start := time.Now()
	defer func() {
		searchmetrics.IndexerFlushDuration.WithLabelValues(flushTypeUpsert).Observe(time.Since(start).Seconds())
	}()

	groups := make(map[string][]any)
	for _, item := range queue {
		groups[item.indexUID] = append(groups[item.indexUID], item.doc)
	}

	var wg sync.WaitGroup
	var errs []error
	var errMu sync.Mutex

	for indexUID, docs := range groups {
		wg.Add(1)
		go func(uid string, d []any) {
			defer wg.Done()

			// 1. Enqueue (POST) - Limited by semaphore
			b.sem <- struct{}{}
			tasks, err := b.client.AddDocumentsAsync(uid, d)
			<-b.sem

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
		}(indexUID, docs)
	}
	wg.Wait()

	if len(errs) > 0 {
		searchmetrics.IndexerFlushTotal.WithLabelValues(flushTypeUpsert, flushStatusFailure).Inc()
		klog.Errorf("Failed to flush %d upserts from %d messages, nacking for redelivery in %s: %v",
			len(queue), len(msgs), nakDelay, errors.Join(errs...))
		b.nakBatch(msgs, flushTypeUpsert)
		return
	}

	searchmetrics.IndexerFlushTotal.WithLabelValues(flushTypeUpsert, flushStatusSuccess).Inc()
	klog.Infof("Successfully flushed %d upserts", len(queue))
	for _, msg := range msgs {
		msg.Ack()
	}
}

func (b *Batcher) performDeleteFlush(queue []deleteItem, msgs []jetstream.Msg) {
	klog.Infof("Flushing batch of %d deletes to Meilisearch...", len(queue))

	searchmetrics.IndexerInFlightFlushes.Inc()
	defer searchmetrics.IndexerInFlightFlushes.Dec()
	start := time.Now()
	defer func() {
		searchmetrics.IndexerFlushDuration.WithLabelValues(flushTypeDelete).Observe(time.Since(start).Seconds())
	}()

	groups := make(map[string][]string)
	for _, item := range queue {
		groups[item.indexUID] = append(groups[item.indexUID], item.docID)
	}

	var wg sync.WaitGroup
	var errs []error
	var errMu sync.Mutex

	for indexUID, docIDs := range groups {
		wg.Add(1)
		go func(uid string, ids []string) {
			defer wg.Done()

			// 1. Enqueue (DELETE) - Limited by semaphore
			b.sem <- struct{}{}
			tasks, err := b.client.DeleteDocumentsAsync(uid, ids)
			<-b.sem

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
		}(indexUID, docIDs)
	}
	wg.Wait()

	if len(errs) > 0 {
		searchmetrics.IndexerFlushTotal.WithLabelValues(flushTypeDelete, flushStatusFailure).Inc()
		klog.Errorf("Failed to flush %d deletes from %d messages, nacking for redelivery in %s: %v",
			len(queue), len(msgs), nakDelay, errors.Join(errs...))
		b.nakBatch(msgs, flushTypeDelete)
		return
	}

	searchmetrics.IndexerFlushTotal.WithLabelValues(flushTypeDelete, flushStatusSuccess).Inc()
	klog.Infof("Successfully flushed %d deletes", len(queue))
	for _, msg := range msgs {
		msg.Ack()
	}
}

// nakBatch returns a failed batch to JetStream for redelivery. Without it the
// batch would sit unacknowledged until ackWait expires.
//
// A message buffered in both the upsert and delete buffers (one event matching
// some policies and not others) can be nacked here after the other flush already
// acked it; the redelivery just re-applies idempotent operations, and the
// double-buffering itself is tracked separately.
func (b *Batcher) nakBatch(msgs []jetstream.Msg, flushType string) {
	for _, msg := range msgs {
		if err := msg.NakWithDelay(nakDelay); err != nil {
			klog.Errorf("Failed to nak message for redelivery: %v", err)
		}
	}
	searchmetrics.IndexerMessagesNacked.WithLabelValues(flushType).Add(float64(len(msgs)))
}
