package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderedSearchClient models Meilisearch applying an index's tasks in the order
// they were enqueued. It records every enqueue call and keeps the resulting set
// of documents per index. When deleteGate is set, DeleteDocumentsAsync blocks
// on it before enqueueing, standing in for a slow or queued POST.
type orderedSearchClient struct {
	deleteGate chan struct{}

	mu   sync.Mutex
	log  []string
	docs map[string]map[string]struct{}
}

func newOrderedSearchClient() *orderedSearchClient {
	return &orderedSearchClient{docs: make(map[string]map[string]struct{})}
}

func (c *orderedSearchClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.docs[indexUID] == nil {
		c.docs[indexUID] = make(map[string]struct{})
	}
	for _, d := range documents {
		uid := d.(map[string]any)["uid"].(string)
		c.docs[indexUID][uid] = struct{}{}
		c.log = append(c.log, "add "+indexUID+"/"+uid)
	}
	return []*meilisearch.Task{{TaskUID: 1, IndexUID: indexUID}}, nil
}

func (c *orderedSearchClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	if c.deleteGate != nil {
		<-c.deleteGate
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range documentIDs {
		delete(c.docs[indexUID], id)
		c.log = append(c.log, "delete "+indexUID+"/"+id)
	}
	return []*meilisearch.Task{{TaskUID: 2, IndexUID: indexUID}}, nil
}

func (c *orderedSearchClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	return &meilisearch.Task{Status: "succeeded"}, nil
}

func (c *orderedSearchClient) Log() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.log...)
}

func (c *orderedSearchClient) Has(indexUID, docID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.docs[indexUID][docID]
	return ok
}

func requireAcked(t *testing.T, msgs ...*boundMsg) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, m := range msgs {
			if atomic.LoadInt32(&m.acked) != 1 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "expected every message to be acked exactly once")
	for _, m := range msgs {
		assert.Equal(t, int32(0), atomic.LoadInt32(&m.naked), "message %d was nacked", m.seq)
	}
}

// TestBatcher_DeleteThenUpsert_SameWindow_KeepsDocument covers a resource that
// first fails a policy's conditions (queueing a delete) and then matches it
// (queueing an upsert) before the next flush. Flushing both would let the
// stale delete land after the upsert and remove a document that should exist.
// The superseded message is acked with the batch carrying the upsert.
func TestBatcher_DeleteThenUpsert_SameWindow_KeepsDocument(t *testing.T) {
	client := newOrderedSearchClient()
	batcher := NewBatcher(client, BatchConfig{BatchSize: 100, FlushInterval: time.Hour})

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.QueueDelete("projects", "p1", &jm1)
	batcher.QueueUpsert("projects", map[string]any{"uid": "p1"}, &jm2)

	batcher.flush()

	requireAcked(t, m1, m2)
	assert.Equal(t, []string{"add projects/p1"}, client.Log(), "the superseded delete must not reach Meilisearch")
	assert.True(t, client.Has("projects", "p1"))
}

// TestBatcher_UpsertThenDelete_SameWindow_RemovesDocument is the reverse: the
// newer delete must win over the older pending upsert.
func TestBatcher_UpsertThenDelete_SameWindow_RemovesDocument(t *testing.T) {
	client := newOrderedSearchClient()
	client.docs["projects"] = map[string]struct{}{"p1": {}}
	batcher := NewBatcher(client, BatchConfig{BatchSize: 100, FlushInterval: time.Hour})

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.QueueUpsert("projects", map[string]any{"uid": "p1"}, &jm1)
	batcher.QueueDelete("projects", "p1", &jm2)

	batcher.flush()

	requireAcked(t, m1, m2)
	assert.Equal(t, []string{"delete projects/p1"}, client.Log(), "the superseded upsert must not reach Meilisearch")
	assert.False(t, client.Has("projects", "p1"))
}

// TestBatcher_SameMessage_MatchingPolicyWins covers one message that matches
// one policy and fails another that share an index: the document stays.
func TestBatcher_SameMessage_MatchingPolicyWins(t *testing.T) {
	client := newOrderedSearchClient()
	batcher := NewBatcher(client, BatchConfig{BatchSize: 100, FlushInterval: time.Hour})

	m := &boundMsg{seq: 1}
	var jm jetstream.Msg = m
	batcher.Submit(&jm,
		[]UpsertOp{{IndexUID: "projects", Doc: map[string]any{"uid": "p1"}}},
		[]DeleteOp{{IndexUID: "projects", DocID: "p1"}},
	)

	batcher.flush()

	requireAcked(t, m)
	assert.True(t, client.Has("projects", "p1"))
}

// TestBatcher_EarlierDeleteBatch_EnqueuesBeforeLaterUpsertBatch covers the
// cross-batch case: a delete batch taken first but still enqueueing must reach
// Meilisearch before an upsert batch taken after it, even though the two run
// as separate, concurrent flushes.
func TestBatcher_EarlierDeleteBatch_EnqueuesBeforeLaterUpsertBatch(t *testing.T) {
	client := newOrderedSearchClient()
	client.deleteGate = make(chan struct{})
	batcher := NewBatcher(client, BatchConfig{BatchSize: 1, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2

	// BatchSize 1 takes and launches the delete batch here; its flush blocks
	// in DeleteDocumentsAsync on the gate.
	batcher.QueueDelete("projects", "p1", &jm1)

	upsertDone := make(chan struct{})
	go func() {
		defer close(upsertDone)
		batcher.QueueUpsert("projects", map[string]any{"uid": "p1"}, &jm2)
	}()

	select {
	case <-upsertDone:
		t.Fatal("the later upsert batch launched before the earlier delete batch finished enqueueing")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Empty(t, client.Log(), "nothing may be enqueued while the earlier delete batch is still enqueueing")

	close(client.deleteGate)

	select {
	case <-upsertDone:
	case <-time.After(time.Second):
		t.Fatal("the upsert batch did not launch after the delete batch enqueued")
	}

	requireAcked(t, m1, m2)
	assert.Equal(t, []string{"delete projects/p1", "add projects/p1"}, client.Log())
	assert.True(t, client.Has("projects", "p1"))
}

// TestBatcher_AwaitTurn_UnblocksOnShutdown verifies that a batch waiting for an
// earlier batch to enqueue is abandoned at shutdown instead of hanging, and is
// left neither acked nor nacked so JetStream redelivers it.
func TestBatcher_AwaitTurn_UnblocksOnShutdown(t *testing.T) {
	client := newOrderedSearchClient()
	client.deleteGate = make(chan struct{})
	defer close(client.deleteGate)
	batcher := NewBatcher(client, BatchConfig{BatchSize: 1, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.QueueDelete("projects", "p1", &jm1)

	upsertDone := make(chan struct{})
	go func() {
		defer close(upsertDone)
		batcher.QueueUpsert("projects", map[string]any{"uid": "p2"}, &jm2)
	}()

	select {
	case <-upsertDone:
		t.Fatal("the upsert batch launched before the earlier delete batch finished enqueueing")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case <-upsertDone:
	case <-time.After(time.Second):
		t.Fatal("QueueUpsert did not unblock after the batcher's Start context was cancelled")
	}

	assert.Equal(t, int32(0), atomic.LoadInt32(&m2.acked))
	assert.Equal(t, int32(0), atomic.LoadInt32(&m2.naked))
}
