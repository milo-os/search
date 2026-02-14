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

	upsertQueue []upsertItem
	deleteQueue []deleteItem
	upsertMsgs  []jetstream.Msg
	deleteMsgs  []jetstream.Msg

	// Track seen messages to trigger batching based on unique message count
	upsertMsgIDs map[uint64]struct{}
	deleteMsgIDs map[uint64]struct{}

	mu  sync.Mutex
	sem chan struct{} // Global semaphore to limit concurrent Meilisearch requests
}

// NewBatcher creates a new Batcher instance.
func NewBatcher(client SearchClient, batchConfig BatchConfig) *Batcher {
	return &Batcher{
		client:       client,
		batchConfig:  batchConfig,
		upsertQueue:  make([]upsertItem, 0, batchConfig.BatchSize),
		deleteQueue:  make([]deleteItem, 0, batchConfig.BatchSize),
		upsertMsgs:   make([]jetstream.Msg, 0, batchConfig.BatchSize),
		deleteMsgs:   make([]jetstream.Msg, 0, batchConfig.BatchSize),
		upsertMsgIDs: make(map[uint64]struct{}),
		deleteMsgIDs: make(map[uint64]struct{}),
		sem:          make(chan struct{}, 100), // 100 concurrent uploads
	}
}

// Start starts the batch flusher loop.
func (b *Batcher) Start(ctx context.Context) {
	go b.runBatcher(ctx)
}

// QueueUpsert adds a document to the upsert queue and triggers an asynchronous flush if the batch size is reached.
func (b *Batcher) QueueUpsert(indexUID string, doc any, msg *jetstream.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.upsertQueue = append(b.upsertQueue, upsertItem{
		indexUID: indexUID,
		doc:      doc,
	})

	if msg != nil {
		if meta, err := (*msg).Metadata(); err == nil {
			id := meta.Sequence.Stream
			// If we already have items from this message delivery attempt in the current batch, ignore redeliveries
			if _, ok := b.upsertMsgIDs[id]; ok {
				return
			}
			b.upsertMsgIDs[id] = struct{}{}
			b.upsertMsgs = append(b.upsertMsgs, *msg)
		}
	}

	// Flush if we reached the batch size of unique messages
	if len(b.upsertMsgs) >= b.batchConfig.BatchSize {
		queue, msgs := b.takeUpsertBatch()
		klog.Infof("Batch size reached, flushing %d upserts from %d unique messages", len(queue), len(msgs))
		go b.performUpsertFlush(queue, msgs)
	}
}

// QueueDelete adds a document ID to the delete queue and triggers an asynchronous flush if the batch size is reached.
func (b *Batcher) QueueDelete(indexUID string, docID string, msg *jetstream.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.deleteQueue = append(b.deleteQueue, deleteItem{
		indexUID: indexUID,
		docID:    docID,
	})

	if msg != nil {
		if meta, err := (*msg).Metadata(); err == nil {
			id := meta.Sequence.Stream
			// If we already have items from this message delivery attempt in the current batch, ignore redeliveries
			if _, ok := b.deleteMsgIDs[id]; ok {
				return
			}
			b.deleteMsgIDs[id] = struct{}{}
			b.deleteMsgs = append(b.deleteMsgs, *msg)
		}
	}

	// Flush if we reached the batch size of unique messages
	if len(b.deleteMsgs) >= b.batchConfig.BatchSize {
		queue, msgs := b.takeDeleteBatch()
		klog.Infof("Batch size reached, flushing %d deletes from %d unique messages", len(queue), len(msgs))
		go b.performDeleteFlush(queue, msgs)
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
	if len(b.upsertMsgs) == 0 && len(b.upsertQueue) == 0 {
		b.mu.Unlock()
		return
	}
	queue, msgs := b.takeUpsertBatch()
	b.mu.Unlock()

	b.performUpsertFlush(queue, msgs)
}

func (b *Batcher) flushDeletes() {
	b.mu.Lock()
	if len(b.deleteMsgs) == 0 && len(b.deleteQueue) == 0 {
		b.mu.Unlock()
		return
	}
	queue, msgs := b.takeDeleteBatch()
	b.mu.Unlock()

	b.performDeleteFlush(queue, msgs)
}

// takeUpsertBatch captures and resets the current upsert buffer. MUST hold lock.
func (b *Batcher) takeUpsertBatch() ([]upsertItem, []jetstream.Msg) {
	queue := b.upsertQueue
	msgs := b.upsertMsgs
	b.upsertQueue = make([]upsertItem, 0, b.batchConfig.BatchSize)
	b.upsertMsgs = make([]jetstream.Msg, 0, b.batchConfig.BatchSize)
	b.upsertMsgIDs = make(map[uint64]struct{})
	return queue, msgs
}

// takeDeleteBatch captures and resets the current delete buffer. MUST hold lock.
func (b *Batcher) takeDeleteBatch() ([]deleteItem, []jetstream.Msg) {
	queue := b.deleteQueue
	msgs := b.deleteMsgs
	b.deleteQueue = make([]deleteItem, 0, b.batchConfig.BatchSize)
	b.deleteMsgs = make([]jetstream.Msg, 0, b.batchConfig.BatchSize)
	b.deleteMsgIDs = make(map[uint64]struct{})
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
