package indexer

import (
	"context"
	"sync"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/klog/v2"
)

// BatchConfig holds configuration for batching operations.
type BatchConfig struct {
	// BatchSize is the maximum number of items to buffer before flushing.
	BatchSize int
	// FlushInterval is the maximum time to wait before flushing.
	FlushInterval time.Duration
	// MaxConcurrentUploads is the maximum number of concurrent uploads to Meilisearch.
	MaxConcurrentUploads int
}

// SearchClient abstracts the search backend interactions.
type SearchClient interface {
	AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error)
	DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error)
	WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error)
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

	// Track NATS messages for acknowledgement
	upsertMsgs []jetstream.Msg
	deleteMsgs []jetstream.Msg

	mu  sync.Mutex
	sem chan struct{} // Global semaphore to limit concurrent Meilisearch requests
}

// NewBatcher creates a new Batcher instance.
func NewBatcher(client SearchClient, batchConfig BatchConfig) *Batcher {
	maxConcurrent := batchConfig.MaxConcurrentUploads
	if maxConcurrent <= 0 {
		maxConcurrent = 100
	}

	return &Batcher{
		client:         client,
		batchConfig:    batchConfig,
		pendingUpserts: make(map[string]upsertItem),
		pendingDeletes: make(map[string]deleteItem),
		upsertMsgs:     make([]jetstream.Msg, 0, batchConfig.BatchSize),
		deleteMsgs:     make([]jetstream.Msg, 0, batchConfig.BatchSize),
		sem:            make(chan struct{}, maxConcurrent),
	}
}

// Start starts the batch flusher loop.
func (b *Batcher) Start(ctx context.Context) {
	go b.runBatcher(ctx)
}

// QueueUpsert adds a document to the pending map and triggers an asynchronous flush if the batch size is reached.
func (b *Batcher) QueueUpsert(indexUID string, doc any, msg *jetstream.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Extract UID for deduplication key
	var docID string
	if m, ok := doc.(map[string]any); ok {
		if uid, ok := m["uid"].(string); ok {
			docID = uid
		}
	}

	key := indexUID + "/" + docID

	// Add/Update the pending item (last write wins for same docID)
	b.pendingUpserts[key] = upsertItem{
		indexUID: indexUID,
		doc:      doc,
	}

	if msg != nil {
		b.trackMessage(msg, true)
	}

	// Flush if we reached the batch size of unique messages or unique documents
	if len(b.upsertMsgs) >= b.batchConfig.BatchSize || len(b.pendingUpserts) >= b.batchConfig.BatchSize {
		queue, msgs := b.takeUpsertBatch()
		klog.Infof("Batch size reached, flushing %d upserts from %d unique messages", len(queue), len(msgs))
		go b.performUpsertFlush(queue, msgs)
	}
}

// QueueDelete adds a document ID to the pending map and triggers an asynchronous flush if the batch size is reached.
func (b *Batcher) QueueDelete(indexUID string, docID string, msg *jetstream.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := indexUID + "/" + docID
	b.pendingDeletes[key] = deleteItem{
		indexUID: indexUID,
		docID:    docID,
	}

	if msg != nil {
		b.trackMessage(msg, false)
	}

	// Flush if we reached the batch size of unique messages or unique documents
	if len(b.deleteMsgs) >= b.batchConfig.BatchSize || len(b.pendingDeletes) >= b.batchConfig.BatchSize {
		queue, msgs := b.takeDeleteBatch()
		klog.Infof("Batch size reached, flushing %d deletes from %d unique messages", len(queue), len(msgs))
		go b.performDeleteFlush(queue, msgs)
	}
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

	id := meta.Sequence.Stream

	// Check for duplicates in the existing slice.
	var msgs []jetstream.Msg
	if isUpsert {
		msgs = b.upsertMsgs
	} else {
		msgs = b.deleteMsgs
	}

	for _, existing := range msgs {
		if m, err := existing.Metadata(); err == nil {
			if m.Sequence.Stream == id {
				return // Already have this message
			}
		}
	}

	if isUpsert {
		b.upsertMsgs = append(b.upsertMsgs, *msg)
	} else {
		b.deleteMsgs = append(b.deleteMsgs, *msg)
	}
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
	b.mu.Unlock()

	b.performUpsertFlush(queue, msgs)
}

func (b *Batcher) flushDeletes() {
	b.mu.Lock()
	if len(b.deleteMsgs) == 0 && len(b.pendingDeletes) == 0 {
		b.mu.Unlock()
		return
	}
	queue, msgs := b.takeDeleteBatch()
	b.mu.Unlock()

	b.performDeleteFlush(queue, msgs)
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

	return queue, msgs
}

func (b *Batcher) performUpsertFlush(queue []upsertItem, msgs []jetstream.Msg) {
	klog.Infof("Flushing batch of %d upserts to Meilisearch...", len(queue))

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
		klog.Errorf("Failed to flush %d upserts: %v. Messages will not be Acked.", len(queue), errs)
	} else {
		klog.Infof("Successfully flushed %d upserts", len(queue))
		for _, msg := range msgs {
			msg.Ack()
		}
	}
}

func (b *Batcher) performDeleteFlush(queue []deleteItem, msgs []jetstream.Msg) {
	klog.Infof("Flushing batch of %d deletes to Meilisearch...", len(queue))

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
		klog.Errorf("Failed to flush %d deletes: %v. Messages will not be Acked.", len(queue), errs)
	} else {
		klog.Infof("Successfully flushed %d deletes", len(queue))
		for _, msg := range msgs {
			msg.Ack()
		}
	}
}
