package indexer

import (
	"context"
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
// group large enough to be enqueued as several Meilisearch tasks. Replica A
// enqueues p1@5 in the second of two chunks; between the chunks, replica B
// enqueues and records p1@6. Meilisearch applies A's second chunk last. If A
// recorded p1 against its first chunk's task, older than B's, it would leave
// the repair to B, which has already finished, and search would keep p1@5
// while the store says p1@6 is applied.
func TestBatcher_MultiChunkGroup_RecordsTheTaskThatCarriedEachDocument(t *testing.T) {
	cases := []struct {
		name string
		// batcherChunk is BatchConfig.ChunkSize. Zero leaves the splitting to
		// the client, which then returns one task per chunk for one group.
		batcherChunk int
	}{
		{name: "batcher sizes groups to the chunk", batcherChunk: 1},
		{name: "client splits the group", batcherChunk: 0},
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
			client.onTask = func(_ string, docs []any) {
				for _, d := range docs {
					doc := d.(map[string]any)
					if doc["uid"] == "p1" && resourceVersionOf(doc) == 5 && interleaved.CompareAndSwap(false, true) {
						mB = flushNow(replicaB, 2, upsertOp("proxies", rvDoc("p1", 6, "newer")))
					}
				}
			}

			// The filler comes first, so p1 is in the second chunk.
			mA := flushNow(replicaA, 1,
				upsertOp("proxies", rvDoc("f1", 5, "filler")),
				upsertOp("proxies", rvDoc("p1", 5, "older")),
			)

			require.True(t, interleaved.Load(), "replica B's flush never ran between replica A's chunks")
			requireAcked(t, mA, mB)
			assert.Equal(t, "newer", hostnameOf(client.Doc("proxies", "p1")),
				"the newer state must land after the chunk that carried the older one; log: %v", client.Log())

			rec, _, found, err := store.Get(context.Background(), pendingKey("proxies", "p1"))
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, uint64(6), rec.RV)
			assert.True(t, rec.applied(), "the store must not report a state as applied that a later task overwrote: %+v", rec)
		})
	}
}
