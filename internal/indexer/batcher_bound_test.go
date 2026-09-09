package indexer

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingSearchClient is a SearchClient whose WaitForTasks call blocks until
// release is closed, with an atomic high-water mark of how many calls were
// blocked concurrently. AddDocumentsAsync/DeleteDocumentsAsync return
// immediately, so all observed concurrency comes from flushSem contention in
// launchUpsertFlush/launchDeleteFlush, not from the upload semaphore.
type blockingSearchClient struct {
	release chan struct{}

	inFlight    int32
	maxInFlight int32

	addCalls    int32
	deleteCalls int32
}

func newBlockingSearchClient() *blockingSearchClient {
	return &blockingSearchClient{release: make(chan struct{})}
}

func (c *blockingSearchClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	atomic.AddInt32(&c.addCalls, 1)
	return []*meilisearch.Task{{TaskUID: 1, IndexUID: indexUID}}, nil
}

func (c *blockingSearchClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	atomic.AddInt32(&c.deleteCalls, 1)
	return []*meilisearch.Task{{TaskUID: 1, IndexUID: indexUID}}, nil
}

func (c *blockingSearchClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	n := atomic.AddInt32(&c.inFlight, 1)
	for {
		old := atomic.LoadInt32(&c.maxInFlight)
		if n <= old {
			break
		}
		if atomic.CompareAndSwapInt32(&c.maxInFlight, old, n) {
			break
		}
	}
	<-c.release
	atomic.AddInt32(&c.inFlight, -1)
	return &meilisearch.Task{Status: "succeeded"}, nil
}

// boundMsg is a lightweight jetstream.Msg fake used to observe Ack counts
// under concurrency. It intentionally does not use testify's mock so that
// dozens of instances can be created without per-message expectation setup.
type boundMsg struct {
	jetstream.Msg
	seq   uint64
	acked int32
	naked int32
}

func (m *boundMsg) Ack() error {
	atomic.AddInt32(&m.acked, 1)
	return nil
}

func (m *boundMsg) NakWithDelay(time.Duration) error {
	atomic.AddInt32(&m.naked, 1)
	return nil
}

func (m *boundMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}

// TestBatcher_InFlightFlushBound covers issue #113: with MaxInFlightFlushes
// capping concurrent flushes, a fast producer must be backpressured rather
// than accumulating unbounded flush goroutines and message batches in memory.
func TestBatcher_InFlightFlushBound(t *testing.T) {
	t.Run("upsert", func(t *testing.T) {
		runInFlightBoundTest(t, true)
	})
	t.Run("delete", func(t *testing.T) {
		runInFlightBoundTest(t, false)
	})
}

func runInFlightBoundTest(t *testing.T, isUpsert bool) {
	const (
		batchSize   = 10
		maxInFlight = 2
		numBatches  = 5
		numMsgs     = batchSize * numBatches
	)

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          batchSize,
		FlushInterval:      time.Hour, // only size-based triggers matter here
		MaxInFlightFlushes: maxInFlight,
	})

	msgs := make([]*boundMsg, numMsgs)
	for i := range msgs {
		msgs[i] = &boundMsg{seq: uint64(i + 1)}
	}

	var queuedSoFar int32
	queueDone := make(chan struct{})
	go func() {
		defer close(queueDone)
		for i, m := range msgs {
			var jm jetstream.Msg = m
			if isUpsert {
				batcher.QueueUpsert("index-1", map[string]any{"uid": fmt.Sprintf("doc-%d", i)}, &jm)
			} else {
				batcher.QueueDelete("index-1", fmt.Sprintf("doc-%d", i), &jm)
			}
			// Recorded only after QueueUpsert/QueueDelete returns, so it never
			// counts a message the producer hasn't handed to the batcher yet.
			atomic.StoreInt32(&queuedSoFar, int32(i+1))
		}
	}()

	// Wait for both flush slots to fill and block in WaitForTasks.
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.inFlight) == maxInFlight
	}, time.Second, time.Millisecond, "expected %d concurrent flushes to build up", maxInFlight)

	// The queueing goroutine should now be blocked trying to launch the third
	// batch's flush (both slots are held), not finished queueing all 50
	// messages.
	select {
	case <-queueDone:
		t.Fatal("queueing goroutine finished while both flush slots were still held; the in-flight bound applied no backpressure")
	case <-time.After(50 * time.Millisecond):
	}

	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"observed more concurrent flushes than MaxInFlightFlushes allows")

	// unacked counts only messages the producer has actually handed to the
	// batcher so far (queuedSoFar), not the full backing array: messages the
	// producer hasn't reached yet were never in memory via the batcher and
	// don't count against its bound.
	queued := int(atomic.LoadInt32(&queuedSoFar))
	acked := 0
	for _, m := range msgs {
		if atomic.LoadInt32(&m.acked) > 0 {
			acked++
		}
	}
	unacked := queued - acked
	// A single queueing goroutine can have at most maxInFlight batches
	// actively flushing, plus the one further batch it has already pulled off
	// the pending buffer while blocked waiting for the next free slot (see
	// launchUpsertFlush/launchDeleteFlush: takeUpsertBatch/takeDeleteBatch run
	// before acquireFlushSlot). That bounds memory to a small constant
	// multiple of BatchSize regardless of how many messages are queued in
	// total (numMsgs here), which is what issue #113 was about.
	assert.LessOrEqual(t, unacked, (maxInFlight+1)*batchSize,
		"more messages held un-acked than the in-flight bound should allow (queued=%d acked=%d)", queued, acked)
	assert.Greater(t, unacked, 0)

	close(client.release)

	select {
	case <-queueDone:
	case <-time.After(time.Second):
		t.Fatal("queueing goroutine did not complete after flush slots were released")
	}

	require.Eventually(t, func() bool {
		for _, m := range msgs {
			if atomic.LoadInt32(&m.acked) == 0 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "expected every message to be acked after slots were released")

	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight)
	if isUpsert {
		assert.Equal(t, int32(numBatches), atomic.LoadInt32(&client.addCalls))
	} else {
		assert.Equal(t, int32(numBatches), atomic.LoadInt32(&client.deleteCalls))
	}
}

// TestBatcher_TickerFlush_RespectsInFlightBound covers the FlushInterval path:
// runBatcher's own goroutine drives flushUpserts/flushDeletes, so a stalled
// flush must not let the ticker spawn additional concurrent flushes beyond
// MaxInFlightFlushes.
func TestBatcher_TickerFlush_RespectsInFlightBound(t *testing.T) {
	const maxInFlight = 1

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          1000, // large enough that QueueUpsert itself never triggers a flush
		FlushInterval:      20 * time.Millisecond,
		MaxInFlightFlushes: maxInFlight,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	msg1 := &boundMsg{seq: 1}
	var jm1 jetstream.Msg = msg1
	batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.inFlight) == maxInFlight
	}, time.Second, time.Millisecond, "expected the first tick to occupy the only flush slot")

	// Queue a second message while the slot is held. A later tick will try to
	// flush it and must block waiting for the slot rather than running
	// concurrently with the first flush.
	msg2 := &boundMsg{seq: 2}
	var jm2 jetstream.Msg = msg2
	batcher.QueueUpsert("index-1", map[string]any{"uid": "2"}, &jm2)

	time.Sleep(150 * time.Millisecond) // several FlushInterval ticks elapse

	assert.LessOrEqual(t, int(atomic.LoadInt32(&client.maxInFlight)), maxInFlight,
		"the ticker spawned more concurrent flushes than MaxInFlightFlushes allows")
	assert.Equal(t, int32(1), atomic.LoadInt32(&client.addCalls),
		"only the first batch should have reached the client while the sole flush slot was held")

	close(client.release)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&msg1.acked) == 1 && atomic.LoadInt32(&msg2.acked) == 1
	}, time.Second, time.Millisecond, "expected both messages to be acked once the flush slot was released")

	// Queueing must keep working once the backlog clears.
	msg3 := &boundMsg{seq: 3}
	var jm3 jetstream.Msg = msg3
	batcher.QueueUpsert("index-1", map[string]any{"uid": "3"}, &jm3)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&msg3.acked) == 1
	}, time.Second, time.Millisecond, "expected queueing to keep working after flush slots freed up")
}

// TestBatcher_AcquireFlushSlot_UnblocksOnShutdown verifies that a QueueUpsert
// call blocked waiting for a flush slot unblocks when the context passed to
// Start is cancelled, instead of hanging forever. The abandoned batch must be
// left neither acked nor nacked, since JetStream redelivers it once ackWait
// expires.
func TestBatcher_AcquireFlushSlot_UnblocksOnShutdown(t *testing.T) {
	const maxInFlight = 1

	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          1, // flush on every QueueUpsert call
		FlushInterval:      time.Hour,
		MaxInFlightFlushes: maxInFlight,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	msg1 := &boundMsg{seq: 1}
	var jm1 jetstream.Msg = msg1
	batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&client.inFlight) == maxInFlight
	}, time.Second, time.Millisecond, "expected the first flush to occupy the only slot")

	msg2 := &boundMsg{seq: 2}
	queueDone := make(chan struct{})
	go func() {
		defer close(queueDone)
		var jm2 jetstream.Msg = msg2
		batcher.QueueUpsert("index-1", map[string]any{"uid": "2"}, &jm2)
	}()

	// msg2's QueueUpsert call must block waiting for a flush slot: the only
	// slot is held by msg1's flush, which the client never releases.
	select {
	case <-queueDone:
		t.Fatal("QueueUpsert for msg2 returned before a flush slot was available")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case <-queueDone:
	case <-time.After(time.Second):
		t.Fatal("QueueUpsert did not unblock after the batcher's Start context was cancelled")
	}

	// The batch was abandoned, not flushed: msg2 must be left neither acked
	// nor nacked so JetStream redelivers it once ackWait expires.
	assert.Equal(t, int32(0), atomic.LoadInt32(&msg2.acked))
	assert.Equal(t, int32(0), atomic.LoadInt32(&msg2.naked))

	close(client.release) // let msg1's flush finish so its goroutine doesn't leak past the test
}
