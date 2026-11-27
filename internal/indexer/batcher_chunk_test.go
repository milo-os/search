package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flushNow runs one flush of ops synchronously, outside the batcher's buffer
// and enqueue chain, so a test can run a whole flush at an exact point of
// another one and choose the order of its operations. It returns the flush's
// message, which is acked on success and nacked on failure.
func flushNow(b *Batcher, seq uint64, ops ...pendingOp) *boundMsg {
	prev := make(chan struct{})
	close(prev)
	m := &boundMsg{seq: seq}
	b.performFlush(batch{
		ops:  ops,
		msgs: []jetstream.Msg{m},
		turn: enqueueTurn{prev: prev, done: make(chan struct{})},
	})
	return m
}

func upsertOp(indexUID string, doc map[string]any) pendingOp {
	return pendingOp{
		indexUID: indexUID,
		docID:    doc["uid"].(string),
		rv:       resourceVersionOf(doc),
		state:    docIndexed,
		doc:      doc,
	}
}

// TestBatcher_MultiChunkGroup_RecordsTheTaskThatCarriedEachDocument covers a
// batch larger than the Meilisearch chunk size. Replica A enqueues p1@5 in the
// second of two chunks; between the chunks, replica B enqueues and records
// p1@6. Meilisearch applies A's second chunk last. If A recorded p1 against
// its first chunk's task, older than B's, it would leave the repair to B,
// which has already finished, and search would keep p1@5 while the store says
// p1@6 is applied.
//
// With the batcher sizing groups to the chunk, each chunk is its own group and
// task, and A records p1 against the task that carried it. When the client
// splits a group instead, no uid is safe to record, so the flush fails and its
// batch is nacked for redelivery without recording anything.
func TestBatcher_MultiChunkGroup_RecordsTheTaskThatCarriedEachDocument(t *testing.T) {
	cases := []struct {
		name string
		// batcherChunk is BatchConfig.ChunkSize. Zero leaves the splitting to
		// the client, which then returns one task per chunk for one group.
		batcherChunk int
		wantNack     bool
	}{
		{name: "batcher sizes groups to the chunk", batcherChunk: 1},
		{name: "client splits the group", batcherChunk: 0, wantNack: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newRVSearchClient()
			client.chunkSize = 1
			store := NewMemoryDocStateStore()
			cfg := BatchConfig{BatchSize: 100, FlushInterval: time.Hour, ChunkSize: tc.batcherChunk, DocState: store}
			replicaA := NewBatcher(client, cfg)
			replicaB := NewBatcher(client, cfg)

			var interleaved atomic.Bool
			var mB *boundMsg
			var bTask atomic.Int64
			client.onTask = func(_ string, docs []any) {
				for _, d := range docs {
					doc := d.(map[string]any)
					if doc["uid"] == "p1" && resourceVersionOf(doc) == 5 && interleaved.CompareAndSwap(false, true) {
						mB = flushNow(replicaB, 2, upsertOp("proxies", rvDoc("p1", 6, "newer")))
					}
				}
			}
			client.afterTask = func(_ string, docs []any, task int64) {
				if resourceVersionOf(docs[0]) == 6 {
					bTask.Store(task)
				}
			}

			// The filler comes first, so p1 is in the second chunk.
			mA := flushNow(replicaA, 1,
				upsertOp("proxies", rvDoc("f1", 5, "filler")),
				upsertOp("proxies", rvDoc("p1", 5, "older")),
			)

			require.True(t, interleaved.Load(), "replica B's flush never ran between replica A's chunks")
			requireAcked(t, mB)
			rec, _, found, err := store.Get(context.Background(), docStateKey("proxies", "p1"))
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, uint64(6), rec.RV)

			if tc.wantNack {
				assert.Equal(t, int32(1), atomic.LoadInt32(&mA.naked), "a group enqueued as several tasks must fail its flush")
				assert.Equal(t, int32(0), atomic.LoadInt32(&mA.acked))
				assert.Equal(t, bTask.Load(), rec.Task, "replica A must record nothing for a group it cannot attribute: %+v", rec)
				f1, _, found, err := store.Get(context.Background(), docStateKey("proxies", "f1"))
				require.NoError(t, err)
				require.True(t, found)
				assert.Zero(t, f1.Task, "no task may be recorded for any document of the split group")
				return
			}

			requireAcked(t, mA)
			assert.Equal(t, "newer", hostnameOf(client.Doc("proxies", "p1")),
				"the newer state must land after the chunk that carried the older one; log: %v", client.Log())
			assert.True(t, rec.applied(), "the store must not report a state as applied that a later task overwrote: %+v", rec)
		})
	}
}

// TestBatcher_MultiChunkBatch_FirstChunkDocument_IsRepaired is the mirror of
// the case above, and pins the batcher's own chunking. Replica B claims p1@5,
// then replica A claims p1@6 and enqueues it in the first of two chunks.
// B's task lands between A's chunks and carries p1@5 over p1@6. A records
// before B does.
//
// With each chunk its own group, A records p1 against the task that carried
// it, earlier than B's, so B sees its own task is the latest and enqueues
// p1@6 again. Were the group left whole for the client to split, A would
// have no single task to record and its flush would fail; before that was an
// error, recording the highest uid made p1 read as applied at A's second
// chunk, and B skipped the repair it owed, leaving p1@5 in search.
func TestBatcher_MultiChunkBatch_FirstChunkDocument_IsRepaired(t *testing.T) {
	client := newRVSearchClient()
	client.chunkSize = 1
	store := NewMemoryDocStateStore()
	cfg := BatchConfig{BatchSize: 100, FlushInterval: time.Hour, ChunkSize: 1, DocState: store}
	replicaA := NewBatcher(client, cfg)
	replicaB := NewBatcher(client, cfg)

	bClaimed := make(chan struct{})
	releaseB := make(chan struct{})
	bEnqueued := make(chan struct{})
	aDone := make(chan struct{})
	var releaseOnce sync.Once
	waitFor := func(ch <-chan struct{}, what string) {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			panic("timed out waiting for " + what)
		}
	}

	client.onTask = func(_ string, docs []any) {
		if resourceVersionOf(docs[0]) == 5 {
			// B has claimed p1@5; hold its enqueue until A's p1@6 has a task.
			close(bClaimed)
			waitFor(releaseB, "replica A to enqueue p1@6")
		}
	}
	client.afterTask = func(_ string, docs []any, _ int64) {
		doc := docs[0].(map[string]any)
		switch {
		case doc["uid"] == "p1" && resourceVersionOf(doc) == 6:
			// A's p1 chunk has its task: let B's task follow it, before A's
			// other chunk.
			releaseOnce.Do(func() { close(releaseB) })
			waitFor(bEnqueued, "replica B to enqueue p1@5")
		case doc["uid"] == "p1" && resourceVersionOf(doc) == 5:
			// B records only once A has finished, so A records first.
			close(bEnqueued)
			waitFor(aDone, "replica A to finish")
		}
	}

	bFlushed := make(chan *boundMsg, 1)
	go func() {
		bFlushed <- flushNow(replicaB, 2, upsertOp("proxies", rvDoc("p1", 5, "older")))
	}()
	waitFor(bClaimed, "replica B to claim p1@5")

	// p1 comes first, so it is in the first chunk.
	mA := flushNow(replicaA, 1,
		upsertOp("proxies", rvDoc("p1", 6, "newer")),
		upsertOp("proxies", rvDoc("f1", 6, "filler")),
	)
	close(aDone)

	var mB *boundMsg
	select {
	case mB = <-bFlushed:
	case <-time.After(5 * time.Second):
		t.Fatal("replica B's flush did not finish")
	}

	requireAcked(t, mA, mB)
	assert.Equal(t, "newer", hostnameOf(client.Doc("proxies", "p1")),
		"the newer state must land after the task that carried the older one; log: %v", client.Log())

	rec, _, found, err := store.Get(context.Background(), docStateKey("proxies", "p1"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint64(6), rec.RV)
	assert.True(t, rec.applied(), "the store must not report a state as applied that a later task overwrote: %+v", rec)
}
