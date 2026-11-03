package indexer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rvSearchClient models Meilisearch's task queue closely enough to test write
// ordering: every enqueue gets the next task uid, and an index's documents
// take on the state of its tasks in enqueue order. Each document is kept as
// its resourceVersion, so a test can read which write landed last. onAdd, when
// set, runs before an AddDocuments call is given its task uid, which lets a
// test hold one flush between claiming and enqueueing.
type rvSearchClient struct {
	onAdd func(indexUID string, docs []any)

	mu       sync.Mutex
	nextTask int64
	log      []string
	docs     map[string]map[string]map[string]any
}

func newRVSearchClient() *rvSearchClient {
	return &rvSearchClient{docs: make(map[string]map[string]map[string]any)}
}

func (c *rvSearchClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	if c.onAdd != nil {
		c.onAdd(indexUID, documents)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.docs[indexUID] == nil {
		c.docs[indexUID] = make(map[string]map[string]any)
	}
	for _, d := range documents {
		doc := d.(map[string]any)
		uid := doc["uid"].(string)
		c.docs[indexUID][uid] = doc
		c.log = append(c.log, fmt.Sprintf("add %s/%s@%d", indexUID, uid, resourceVersionOf(doc)))
	}
	c.nextTask++
	return []*meilisearch.Task{{TaskUID: c.nextTask, IndexUID: indexUID}}, nil
}

func (c *rvSearchClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range documentIDs {
		delete(c.docs[indexUID], id)
		c.log = append(c.log, "delete "+indexUID+"/"+id)
	}
	c.nextTask++
	return []*meilisearch.Task{{TaskUID: c.nextTask, IndexUID: indexUID}}, nil
}

func (c *rvSearchClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	return &meilisearch.Task{Status: "succeeded"}, nil
}

func (c *rvSearchClient) Log() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.log...)
}

// Doc returns the document indexed for uid, or nil if there is none.
func (c *rvSearchClient) Doc(indexUID, uid string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.docs[indexUID][uid]
}

// rvDoc builds a document for uid at the given resourceVersion carrying one
// field whose value tells which write produced it.
func rvDoc(uid string, rv uint64, hostname string) map[string]any {
	return map[string]any{
		"uid": uid,
		"metadata": map[string]any{
			"uid":             uid,
			"resourceVersion": fmt.Sprint(rv),
		},
		"status": map[string]any{"hostname": hostname},
	}
}

func hostnameOf(doc map[string]any) string {
	if doc == nil {
		return ""
	}
	return doc["status"].(map[string]any)["hostname"].(string)
}

func newOrderingBatcher(client SearchClient, store DocStateStore) *Batcher {
	return NewBatcher(client, BatchConfig{BatchSize: 100, FlushInterval: time.Hour, DocState: store})
}

// TestBatcher_OlderUpdate_SameWindow_DoesNotDisplaceNewer covers a redelivered
// or late event for an object arriving after a newer one in the same window:
// the buffer keeps the newer state.
func TestBatcher_OlderUpdate_SameWindow_DoesNotDisplaceNewer(t *testing.T) {
	client := newRVSearchClient()
	batcher := newOrderingBatcher(client, nil)

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.QueueUpsert("proxies", rvDoc("p1", 7, "poet-jump"), &jm1)
	batcher.QueueUpsert("proxies", rvDoc("p1", 5, ""), &jm2)

	batcher.flush()

	requireAcked(t, m1, m2)
	assert.Equal(t, []string{"add proxies/p1@7"}, client.Log())
	assert.Equal(t, "poet-jump", hostnameOf(client.Doc("proxies", "p1")))
}

// TestBatcher_OlderUpdate_LaterBatch_KeepsNewerState covers the ordering bug
// across flush windows: an object's create flushed after its status update
// used to replace the document with the create response. The older write is
// recognised from the recorded state and the newer state is what lands.
func TestBatcher_OlderUpdate_LaterBatch_KeepsNewerState(t *testing.T) {
	client := newRVSearchClient()
	batcher := newOrderingBatcher(client, nil)

	m1 := &boundMsg{seq: 1}
	var jm1 jetstream.Msg = m1
	batcher.QueueUpsert("proxies", rvDoc("p1", 7, "poet-jump"), &jm1)
	batcher.flush()
	requireAcked(t, m1)

	m2 := &boundMsg{seq: 2}
	var jm2 jetstream.Msg = m2
	batcher.QueueUpsert("proxies", rvDoc("p1", 5, ""), &jm2)
	batcher.flush()
	requireAcked(t, m2)

	assert.Equal(t, uint64(7), resourceVersionOf(client.Doc("proxies", "p1")))
	assert.Equal(t, "poet-jump", hostnameOf(client.Doc("proxies", "p1")))
	assert.Equal(t, []string{"add proxies/p1@7", "add proxies/p1@7"}, client.Log(),
		"the later batch must re-apply the recorded newer state, never its own older one")
}

// TestBatcher_Delete_ThenLateWrites_StaysDeleted covers an object deleted
// before its create or update flushes: the document must not come back,
// whether the late write shares the window with the delete or follows it.
func TestBatcher_Delete_ThenLateWrites_StaysDeleted(t *testing.T) {
	client := newRVSearchClient()
	batcher := newOrderingBatcher(client, nil)

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.Submit(&jm1, nil, []DeleteOp{{IndexUID: "proxies", DocID: "p1", ObjectDeleted: true}})
	batcher.QueueUpsert("proxies", rvDoc("p1", 9, "late-update"), &jm2)
	batcher.flush()
	requireAcked(t, m1, m2)
	assert.Nil(t, client.Doc("proxies", "p1"), "an update in the delete's window must not resurrect the document")

	m3 := &boundMsg{seq: 3}
	var jm3 jetstream.Msg = m3
	batcher.QueueUpsert("proxies", rvDoc("p1", 3, "late-create"), &jm3)
	batcher.flush()
	requireAcked(t, m3)

	assert.Nil(t, client.Doc("proxies", "p1"), "a create flushed after the delete must not resurrect the document")
	assert.Equal(t, []string{"delete proxies/p1", "delete proxies/p1"}, client.Log())
}

// TestBatcher_PolicyRemoval_DoesNotOutliveNewerWrite covers the other kind of
// delete: an object that stopped matching a policy is removed from that index,
// but it still exists, so a newer write that matches again must bring it back.
func TestBatcher_PolicyRemoval_DoesNotOutliveNewerWrite(t *testing.T) {
	client := newRVSearchClient()
	batcher := newOrderingBatcher(client, nil)

	m1 := &boundMsg{seq: 1}
	var jm1 jetstream.Msg = m1
	batcher.Submit(&jm1, nil, []DeleteOp{{IndexUID: "proxies", DocID: "p1", ResourceVersion: 4}})
	batcher.flush()
	requireAcked(t, m1)

	m2 := &boundMsg{seq: 2}
	var jm2 jetstream.Msg = m2
	batcher.QueueUpsert("proxies", rvDoc("p1", 5, "matches-again"), &jm2)
	batcher.flush()
	requireAcked(t, m2)

	assert.Equal(t, "matches-again", hostnameOf(client.Doc("proxies", "p1")))
}

// TestBatcher_SameObject_SplitAcrossBatchers_NewestWins covers the production
// failure: one object's events land on different replicas, and the replica
// holding the older event flushes last. Two batchers share one state store
// and one backend, as replicas share a KV bucket and Meilisearch.
func TestBatcher_SameObject_SplitAcrossBatchers_NewestWins(t *testing.T) {
	client := newRVSearchClient()
	store := NewMemoryDocStateStore()
	replicaA := newOrderingBatcher(client, store)
	replicaB := newOrderingBatcher(client, store)

	mA := &boundMsg{seq: 1}
	mB := &boundMsg{seq: 2}
	var jmA, jmB jetstream.Msg = mA, mB
	replicaA.QueueUpsert("proxies", rvDoc("p1", 10, ""), &jmA)
	replicaB.QueueUpsert("proxies", rvDoc("p1", 20, "poet-jump"), &jmB)

	replicaB.flush()
	requireAcked(t, mB)
	replicaA.flush()
	requireAcked(t, mA)

	assert.Equal(t, uint64(20), resourceVersionOf(client.Doc("proxies", "p1")))
	assert.Equal(t, "poet-jump", hostnameOf(client.Doc("proxies", "p1")))
}

// TestBatcher_SameObject_ConcurrentFlushes_RepairsOverwrite covers the race
// the recorded state alone cannot settle: both replicas claim their write
// before either enqueues, and the older write is enqueued second. Meilisearch
// would apply it last, so the replica that enqueued it must enqueue the newer
// state again behind its own task.
func TestBatcher_SameObject_ConcurrentFlushes_RepairsOverwrite(t *testing.T) {
	client := newRVSearchClient()
	store := NewMemoryDocStateStore()
	replicaA := newOrderingBatcher(client, store)
	replicaB := newOrderingBatcher(client, store)

	// Hold replica A between claiming its older write and enqueueing it.
	olderClaimed := make(chan struct{})
	releaseOlder := make(chan struct{})
	var holdOnce sync.Once
	client.onAdd = func(_ string, docs []any) {
		if resourceVersionOf(docs[0]) == 10 {
			holdOnce.Do(func() {
				close(olderClaimed)
				<-releaseOlder
			})
		}
	}

	mA := &boundMsg{seq: 1}
	mB := &boundMsg{seq: 2}
	var jmA, jmB jetstream.Msg = mA, mB
	replicaA.QueueUpsert("proxies", rvDoc("p1", 10, ""), &jmA)
	replicaB.QueueUpsert("proxies", rvDoc("p1", 20, "poet-jump"), &jmB)

	flushA := make(chan struct{})
	go func() {
		defer close(flushA)
		replicaA.flush()
	}()

	select {
	case <-olderClaimed:
	case <-time.After(time.Second):
		t.Fatal("replica A never reached its enqueue")
	}

	// Replica B claims and enqueues the newer write while A is held.
	replicaB.flush()
	requireAcked(t, mB)
	require.Equal(t, []string{"add proxies/p1@20"}, client.Log())

	close(releaseOlder)
	select {
	case <-flushA:
	case <-time.After(time.Second):
		t.Fatal("replica A's flush did not finish")
	}
	requireAcked(t, mA)

	assert.Equal(t, []string{"add proxies/p1@20", "add proxies/p1@10", "add proxies/p1@20"}, client.Log(),
		"the older write's flush must enqueue the newer state again behind its own task")
	assert.Equal(t, "poet-jump", hostnameOf(client.Doc("proxies", "p1")))
}

func TestDocRecord_Supersedes(t *testing.T) {
	indexed := func(rv uint64) docRecord { return docRecord{RV: rv, State: docIndexed} }
	unindexed := func(rv uint64) docRecord { return docRecord{RV: rv, State: docUnindexed} }
	deleted := func(rv uint64) docRecord { return docRecord{RV: rv, State: docDeleted} }

	assert.True(t, indexed(2).supersedes(indexed(1)))
	assert.False(t, indexed(1).supersedes(indexed(2)))
	assert.True(t, indexed(2).supersedes(indexed(2)), "a repeat of the same version replaces it")
	assert.True(t, unindexed(2).supersedes(indexed(1)))
	assert.True(t, indexed(3).supersedes(unindexed(2)), "a policy removal is ordered like any write")
	assert.True(t, deleted(0).supersedes(indexed(100)), "a deleted object wins regardless of version")
	assert.False(t, indexed(100).supersedes(deleted(0)))
	assert.False(t, deleted(5).supersedes(deleted(1)), "a repeat delete changes nothing")
}

func TestResourceVersionOf(t *testing.T) {
	assert.Equal(t, uint64(1769506541), resourceVersionOf(rvDoc("p1", 1769506541, "")))
	assert.Equal(t, uint64(0), resourceVersionOf(map[string]any{"uid": "p1"}))
	assert.Equal(t, uint64(0), resourceVersionOf(map[string]any{"metadata": map[string]any{"resourceVersion": "abc"}}))
	assert.Equal(t, uint64(0), resourceVersionOf("not a document"))
}
