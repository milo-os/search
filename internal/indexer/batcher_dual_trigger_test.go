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

// TestBatcher_SimultaneousUpsertAndDeleteFlush_RespectsInFlightBound covers
// the residual case documented in the Validate() comment in
// cmd/search/indexer/command.go: one Submit call can make a due upsert flush
// and a due delete flush fire back to back, so a message can wait for two
// flush slots instead of one. With the sole flush slot already held, the
// second launch must wait its turn rather than spawning past
// MaxInFlightFlushes, and both flushes must still run once slots free up.
func TestBatcher_SimultaneousUpsertAndDeleteFlush_RespectsInFlightBound(t *testing.T) {
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
	// dual-trigger message arrives, so every slot is held when it does.
	holderMsgs := make([]*boundMsg, batchSize)
	for i := range holderMsgs {
		holderMsgs[i] = &boundMsg{seq: uint64(i + 1)}
		var jm jetstream.Msg = holderMsgs[i]
		batcher.QueueUpsert("index-1", map[string]any{"uid": fmt.Sprintf("holder-%d", i)}, &jm)
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.inFlight) == maxInFlight
	}, time.Second, time.Millisecond, "expected the holder flush to occupy the only slot")

	// Deletes flush on unique message count, not delete-key count, so tripping
	// both triggers from a single message is not possible: prime the buffers
	// with two messages, each contributing one upsert and one delete, neither
	// of which reaches BatchSize on its own.
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

	// dualMsg is the message whose Submit call pushes both the pending upsert
	// count and the tracked delete-message count to BatchSize at once, so an
	// upsert flush and a delete flush are due together in the same call, the
	// case the Validate() comment documents as a residual double-slot wait.
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

	// Submit blocks inside launchUpsertFlush waiting for the only slot, held
	// by the holder flush, so it must not return yet.
	select {
	case <-submitDone:
		t.Fatal("Submit returned while the only flush slot was still held")
	case <-time.After(50 * time.Millisecond):
	}
	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"observed more concurrent flushes than MaxInFlightFlushes allows")

	// Free the holder flush. dualMsg's upsert flush should take the slot next.
	client.release <- struct{}{}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.addCalls) == 2 // holder batch + dualMsg's upsert batch
	}, time.Second, time.Millisecond, "expected the dual message's upsert flush to start once the slot freed")

	// The delete flush still needs its own turn at the sole slot, which the
	// dual message's upsert flush now holds, so Submit must still be blocked
	// in launchDeleteFlush rather than having spawned past the cap.
	select {
	case <-submitDone:
		t.Fatal("Submit returned before the delete flush acquired a slot; the second launch did not wait its turn")
	case <-time.After(50 * time.Millisecond):
	}
	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"observed more concurrent flushes than MaxInFlightFlushes allows")

	// Free dualMsg's upsert flush. Its delete flush should take the slot next.
	client.release <- struct{}{}

	select {
	case <-submitDone:
	case <-time.After(time.Second):
		t.Fatal("Submit did not return after the delete flush acquired a slot")
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.deleteCalls) == 1
	}, time.Second, time.Millisecond, "expected the dual message's delete flush to start")

	// Free the delete flush so both it and the test complete cleanly.
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

	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"observed more concurrent flushes than MaxInFlightFlushes allows across the whole test")
}
