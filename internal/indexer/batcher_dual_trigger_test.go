package indexer

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatcher_MixedSubmit_FlushesAsOneBatch_RespectsInFlightBound covers a
// Submit call that makes a flush due while its buffer holds both upserts and
// deletes. They travel as one batch, so the call waits for a single flush
// slot rather than one per operation type, never spawns past
// MaxInFlightFlushes, and acks each message exactly once.
func TestBatcher_MixedSubmit_FlushesAsOneBatch_RespectsInFlightBound(t *testing.T) {
	const (
		batchSize   = 3
		maxInFlight = 1
	)

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          batchSize,
		FlushInterval:      time.Hour, // only size-based triggers matter here
		MaxInFlightFlushes: maxInFlight,
	})

	// Occupy the sole flush slot with an unrelated holder flush before the
	// mixed batch becomes due, so every slot is held when it does.
	holderMsgs := make([]*boundMsg, batchSize)
	for i := range holderMsgs {
		holderMsgs[i] = &boundMsg{seq: uint64(i + 1)}
		var jm jetstream.Msg = holderMsgs[i]
		batcher.QueueUpsert("index-1", map[string]any{"uid": fmt.Sprintf("holder-%d", i)}, &jm)
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.inFlight) == maxInFlight
	}, time.Second, time.Millisecond, "expected the holder flush to occupy the only slot")

	// Prime the buffer with two messages, each contributing one upsert and
	// one delete, which together stay under BatchSize.
	primeMsgs := make([]*boundMsg, 0, batchSize-1)
	for i := 0; i < batchSize-1; i++ {
		m := &boundMsg{seq: uint64(batchSize + 1 + i)}
		primeMsgs = append(primeMsgs, m)
		var jm jetstream.Msg = m
		batcher.Submit(&jm,
			[]UpsertOp{{IndexUID: "index-1", Doc: map[string]any{"uid": fmt.Sprintf("u-%d", i)}}},
			[]DeleteOp{{IndexUID: "index-1", DocID: fmt.Sprintf("d-%d", i)}},
		)
	}

	// dualMsg brings the message count to BatchSize, so one batch of upserts
	// and deletes becomes due in this call.
	dualMsg := &boundMsg{seq: uint64(2*batchSize + 1)}
	var jmDual jetstream.Msg = dualMsg

	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		batcher.Submit(&jmDual,
			[]UpsertOp{{IndexUID: "index-1", Doc: map[string]any{"uid": fmt.Sprintf("u-%d", batchSize-1)}}},
			[]DeleteOp{{IndexUID: "index-1", DocID: fmt.Sprintf("d-%d", batchSize-1)}},
		)
	}()

	// Submit blocks inside launchFlush waiting for the only slot, held by the
	// holder flush, so it must not return yet.
	select {
	case <-submitDone:
		t.Fatal("Submit returned while the only flush slot was still held")
	case <-time.After(50 * time.Millisecond):
	}
	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"observed more concurrent flushes than MaxInFlightFlushes allows")

	// Free the holder flush. The mixed batch takes the slot and Submit returns
	// without waiting for a second slot.
	client.release <- struct{}{}

	select {
	case <-submitDone:
	case <-time.After(time.Second):
		t.Fatal("Submit did not return after the mixed batch acquired the slot")
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.addCalls) == 2 && atomic.LoadInt32(&client.deleteCalls) == 1
	}, time.Second, time.Millisecond, "expected the mixed batch to enqueue its upserts and deletes in one flush")
	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight+1,
		"a single flush waits on one task per index and operation type")

	// The mixed batch waits on two tasks (one add, one delete).
	client.release <- struct{}{}
	client.release <- struct{}{}

	require.Eventually(t, func() bool {
		for _, m := range holderMsgs {
			if atomic.LoadInt32(&m.acked) == 0 {
				return false
			}
		}
		for _, m := range primeMsgs {
			if atomic.LoadInt32(&m.acked) == 0 {
				return false
			}
		}
		return atomic.LoadInt32(&dualMsg.acked) > 0
	}, time.Second, time.Millisecond, "expected every message to be acked once every flush completed")

	for _, m := range append(append(holderMsgs, primeMsgs...), dualMsg) {
		assert.Equal(t, int32(1), atomic.LoadInt32(&m.acked), "message %d must be acked exactly once", m.seq)
	}
}
