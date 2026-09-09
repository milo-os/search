package indexer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBatcher_AckProgress_HeartbeatsWhileFlushIsBlocked covers issue #113
// round 2: startAckProgress heartbeats every message in a batch via
// InProgress for as long as the flush's per-index goroutines are running,
// stops the heartbeat before the ack/nak loop runs, and never heartbeats
// again afterward. Covers both upsert and delete.
func TestBatcher_AckProgress_HeartbeatsWhileFlushIsBlocked(t *testing.T) {
	t.Run("upsert", func(t *testing.T) {
		runAckProgressTest(t, true)
	})
	t.Run("delete", func(t *testing.T) {
		runAckProgressTest(t, false)
	})
}

func runAckProgressTest(t *testing.T, isUpsert bool) {
	const (
		batchSize   = 5
		maxInFlight = 4
		interval    = 20 * time.Millisecond
	)

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:           batchSize,
		FlushInterval:       time.Hour, // only the size trigger matters here
		MaxInFlightFlushes:  maxInFlight,
		AckProgressInterval: interval,
	})

	msgs := make([]*MockJetStreamMsg, batchSize)
	var ackedMu sync.Mutex
	ackedAt := make([]time.Time, batchSize)
	setAckedAt := func(i int, at time.Time) {
		ackedMu.Lock()
		defer ackedMu.Unlock()
		ackedAt[i] = at
	}
	getAckedAt := func(i int) time.Time {
		ackedMu.Lock()
		defer ackedMu.Unlock()
		return ackedAt[i]
	}
	for i := range msgs {
		i := i
		msgs[i] = &MockJetStreamMsg{seq: uint64(i + 1)}
		msgs[i].On("Ack").Run(func(mock.Arguments) { setAckedAt(i, time.Now()) }).Return(nil)
	}

	for i, m := range msgs {
		var jm jetstream.Msg = m
		if isUpsert {
			batcher.QueueUpsert("index-1", map[string]any{"uid": fmt.Sprintf("doc-%d", i)}, &jm)
		} else {
			batcher.QueueDelete("index-1", fmt.Sprintf("doc-%d", i), &jm)
		}
	}
	// The last message queued above pushes the batch to BatchSize and
	// launches the flush synchronously; the client blocks it in WaitForTasks.

	// Every message must receive at least two heartbeat ticks before we
	// release the client.
	require.Eventually(t, func() bool {
		for _, m := range msgs {
			if len(m.InProgressCalls()) < 2 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "expected every message to receive at least two InProgress calls before release")

	for _, m := range msgs {
		m.AssertNotCalled(t, "Ack")
	}

	close(client.release)

	require.Eventually(t, func() bool {
		for i := range msgs {
			if getAckedAt(i).IsZero() {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "expected every message to be acked after release")

	for _, m := range msgs {
		m.AssertNumberOfCalls(t, "Ack", 1)
	}

	// The heartbeat must have stopped before Ack, not merely happened to miss
	// a tick: snapshot the InProgress count now, wait several more heartbeat
	// intervals, and confirm it did not grow.
	countsAtAck := make([]int, batchSize)
	for i, m := range msgs {
		countsAtAck[i] = len(m.InProgressCalls())
	}
	time.Sleep(5 * interval)
	for i, m := range msgs {
		assert.Equal(t, countsAtAck[i], len(m.InProgressCalls()),
			"message %d: InProgress must not be called again after Ack (heartbeat did not stop first)", i)
	}

	// Also check chronologically: every recorded InProgress call for a
	// message happened strictly before that message's Ack.
	for i, m := range msgs {
		ackTime := getAckedAt(i)
		for _, ts := range m.InProgressCalls() {
			assert.True(t, ts.Before(ackTime),
				"message %d: InProgress at %s was not before its Ack at %s", i, ts, ackTime)
		}
	}
}

// TestBatcher_AckProgress_ContinuesOnInProgressError verifies that a message
// whose InProgress call errors does not stop the heartbeat or the flush: the
// batch still acks once Meilisearch finishes. startAckProgress logs a
// warning in this case; that log output is not asserted on here since
// capturing klog output is awkward and the behaviour that matters is that the
// flush completes.
func TestBatcher_AckProgress_ContinuesOnInProgressError(t *testing.T) {
	const (
		batchSize = 3
		interval  = 20 * time.Millisecond
	)

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:           batchSize,
		FlushInterval:       time.Hour,
		MaxInFlightFlushes:  4,
		AckProgressInterval: interval,
	})

	var acked int32
	msgs := make([]*MockJetStreamMsg, batchSize)
	for i := range msgs {
		msgs[i] = &MockJetStreamMsg{seq: uint64(i + 1), inProgressErr: fmt.Errorf("stream not found")}
		msgs[i].On("Ack").Run(func(mock.Arguments) { atomic.AddInt32(&acked, 1) }).Return(nil)
	}

	for i, m := range msgs {
		var jm jetstream.Msg = m
		batcher.QueueUpsert("index-1", map[string]any{"uid": fmt.Sprintf("doc-%d", i)}, &jm)
	}

	// The heartbeat must still tick (and record the call) despite every
	// InProgress call returning an error.
	require.Eventually(t, func() bool {
		for _, m := range msgs {
			if len(m.InProgressCalls()) < 1 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "expected InProgress to still be called despite it returning an error")

	close(client.release)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&acked) == int32(batchSize)
	}, time.Second, time.Millisecond, "expected the flush to still ack every message despite InProgress errors")

	for _, m := range msgs {
		m.AssertNumberOfCalls(t, "Ack", 1)
	}
}
