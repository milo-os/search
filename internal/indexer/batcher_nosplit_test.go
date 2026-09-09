package indexer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBatcher_NoSplit_PureDeletes is a regression test for issue #113's
// Submit fix: 250 messages, each fanning out into ten deletes (one per
// index) submitted together via a single Submit call, must never have a
// message's fan-out split across two delete flushes, and every message must
// be acked exactly once. BatchSize is 100, so this exercises two full
// size-triggered flushes plus a final manual flush of the 50-message
// remainder.
func TestBatcher_NoSplit_PureDeletes(t *testing.T) {
	const (
		numIndices = 10
		numMsgs    = 250
		batchSize  = 100
	)

	mockClient := new(MockSearchClient)

	var mu sync.Mutex
	// seenByIndex[indexUID][docID] counts how many DeleteDocumentsAsync calls
	// for that index included docID. A fan-out split across two batches (or a
	// message re-tracked after a mid-fan-out buffer reset) would show up as a
	// count > 1 for at least one docID; a docID never reaching a flush would
	// show up as a missing entry.
	seenByIndex := make(map[string]map[string]int)

	mockClient.On("DeleteDocumentsAsync", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			idx := args.Get(0).(string)
			ids := args.Get(1).([]string)
			mu.Lock()
			if seenByIndex[idx] == nil {
				seenByIndex[idx] = make(map[string]int)
			}
			for _, id := range ids {
				seenByIndex[idx][id]++
			}
			mu.Unlock()
		}).
		Return([]*meilisearch.Task{{TaskUID: 1}}, nil)
	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil)

	batcher := NewBatcher(mockClient, BatchConfig{BatchSize: batchSize, FlushInterval: time.Hour})

	var totalAcked int32
	for m := 1; m <= numMsgs; m++ {
		msg := &MockJetStreamMsg{seq: uint64(m)}
		msg.On("Ack").Run(func(mock.Arguments) { atomic.AddInt32(&totalAcked, 1) }).Return(nil)
		var jm jetstream.Msg = msg

		deletes := make([]DeleteOp, numIndices)
		for p := 0; p < numIndices; p++ {
			deletes[p] = DeleteOp{IndexUID: fmt.Sprintf("index-%d", p), DocID: fmt.Sprintf("doc-%d", m)}
		}
		batcher.Submit(&jm, nil, deletes)
	}

	// Flush the 50-message remainder that never reached BatchSize on its own.
	batcher.flushDeletes()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&totalAcked) == numMsgs
	}, 2*time.Second, 5*time.Millisecond, "expected every message to be acked")

	assert.Equal(t, int32(numMsgs), atomic.LoadInt32(&totalAcked),
		"every message must be acked exactly once, not zero times or more than once")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seenByIndex, numIndices)
	for p := 0; p < numIndices; p++ {
		idx := fmt.Sprintf("index-%d", p)
		counts := seenByIndex[idx]
		require.Len(t, counts, numMsgs, "index %s: expected all %d doc IDs to have been flushed", idx, numMsgs)
		for m := 1; m <= numMsgs; m++ {
			docID := fmt.Sprintf("doc-%d", m)
			assert.Equal(t, 1, counts[docID],
				"index %s: doc %s must appear in exactly one DeleteDocumentsAsync call, not split across two", idx, docID)
		}
	}
}

// TestBatcher_NoSplit_MixedUpsertsAndDeletes is the mixed-operation variant:
// 250 messages, each submitting three upserts and two deletes together in one
// Submit call. A message with both operation types is tracked in both the
// upsert and delete buffers (see the nakBatch comment in batcher.go), so it
// is acked once per buffer it populated - twice here, not once - but neither
// its three upserts nor its two deletes may be split across two flushes of
// their own kind.
func TestBatcher_NoSplit_MixedUpsertsAndDeletes(t *testing.T) {
	const (
		numUpsertIndices = 3
		numDeleteIndices = 2
		numMsgs          = 250
		batchSize        = 100
	)

	mockClient := new(MockSearchClient)

	var mu sync.Mutex
	upsertSeenByIndex := make(map[string]map[string]int)
	deleteSeenByIndex := make(map[string]map[string]int)

	mockClient.On("AddDocumentsAsync", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			idx := args.Get(0).(string)
			docs := args.Get(1).([]any)
			mu.Lock()
			if upsertSeenByIndex[idx] == nil {
				upsertSeenByIndex[idx] = make(map[string]int)
			}
			for _, d := range docs {
				doc := d.(map[string]any)
				upsertSeenByIndex[idx][doc["uid"].(string)]++
			}
			mu.Unlock()
		}).
		Return([]*meilisearch.Task{{TaskUID: 1}}, nil)
	mockClient.On("DeleteDocumentsAsync", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			idx := args.Get(0).(string)
			ids := args.Get(1).([]string)
			mu.Lock()
			if deleteSeenByIndex[idx] == nil {
				deleteSeenByIndex[idx] = make(map[string]int)
			}
			for _, id := range ids {
				deleteSeenByIndex[idx][id]++
			}
			mu.Unlock()
		}).
		Return([]*meilisearch.Task{{TaskUID: 1}}, nil)
	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil)

	batcher := NewBatcher(mockClient, BatchConfig{BatchSize: batchSize, FlushInterval: time.Hour})

	ackCounts := make([]int32, numMsgs+1) // 1-indexed by message number
	for m := 1; m <= numMsgs; m++ {
		mm := m
		msg := &MockJetStreamMsg{seq: uint64(m)}
		msg.On("Ack").Run(func(mock.Arguments) { atomic.AddInt32(&ackCounts[mm], 1) }).Return(nil)
		var jm jetstream.Msg = msg

		docID := fmt.Sprintf("doc-%d", m)
		upserts := make([]UpsertOp, numUpsertIndices)
		for p := 0; p < numUpsertIndices; p++ {
			upserts[p] = UpsertOp{IndexUID: fmt.Sprintf("up-%d", p), Doc: map[string]any{"uid": docID}}
		}
		deletes := make([]DeleteOp, numDeleteIndices)
		for p := 0; p < numDeleteIndices; p++ {
			deletes[p] = DeleteOp{IndexUID: fmt.Sprintf("del-%d", p), DocID: docID}
		}
		batcher.Submit(&jm, upserts, deletes)
	}

	// Flush whatever remainder didn't reach BatchSize in either buffer.
	batcher.flushUpserts()
	batcher.flushDeletes()

	require.Eventually(t, func() bool {
		for m := 1; m <= numMsgs; m++ {
			if atomic.LoadInt32(&ackCounts[m]) != 2 {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "expected every message to be acked exactly twice (once per buffer it populated)")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, upsertSeenByIndex, numUpsertIndices)
	for p := 0; p < numUpsertIndices; p++ {
		idx := fmt.Sprintf("up-%d", p)
		counts := upsertSeenByIndex[idx]
		require.Len(t, counts, numMsgs, "upsert index %s: expected all %d docs to have been flushed", idx, numMsgs)
		for m := 1; m <= numMsgs; m++ {
			assert.Equal(t, 1, counts[fmt.Sprintf("doc-%d", m)],
				"upsert index %s: doc-%d must appear in exactly one AddDocumentsAsync call", idx, m)
		}
	}

	require.Len(t, deleteSeenByIndex, numDeleteIndices)
	for p := 0; p < numDeleteIndices; p++ {
		idx := fmt.Sprintf("del-%d", p)
		counts := deleteSeenByIndex[idx]
		require.Len(t, counts, numMsgs, "delete index %s: expected all %d docs to have been flushed", idx, numMsgs)
		for m := 1; m <= numMsgs; m++ {
			assert.Equal(t, 1, counts[fmt.Sprintf("doc-%d", m)],
				"delete index %s: doc-%d must appear in exactly one DeleteDocumentsAsync call", idx, m)
		}
	}
}
