package indexer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestBatcher_ExpiredDeadlineWithoutSlot_NacksAndNeverAcks(t *testing.T) {
	client := &stuckEnqueueClient{release: make(chan struct{})}
	t.Cleanup(func() { close(client.release) })

	for i := 0; i < 500; i++ {
		batcher := NewBatcher(client, BatchConfig{
			BatchSize:     1000,
			FlushInterval: time.Hour,
			FlushTimeout:  time.Nanosecond,
		})
		for j := 0; j < cap(batcher.sem); j++ {
			batcher.sem <- struct{}{}
		}

		msg := &boundMsg{seq: 1}
		var jm jetstream.Msg = msg
		batcher.performFlush(batch{
			upserts: []upsertItem{{indexUID: "index-1", doc: map[string]any{"uid": "a"}}},
			msgs:    []jetstream.Msg{jm},
			turn:    enqueueTurn{prev: batcher.lastEnqueue, done: make(chan struct{})},
		})

		require.Zero(t, atomic.LoadInt32(&msg.acked), "a batch that was never enqueued must not be acked")
		require.Equal(t, int32(1), atomic.LoadInt32(&msg.naked))
	}
	require.Zero(t, atomic.LoadInt32(&client.calls), "no slot was free, so nothing was enqueued")
}
