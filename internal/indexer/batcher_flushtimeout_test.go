package indexer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type stuckEnqueueClient struct {
	release chan struct{}
	calls   int32
}

func (c *stuckEnqueueClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	atomic.AddInt32(&c.calls, 1)
	<-c.release
	return nil, context.DeadlineExceeded
}

func (c *stuckEnqueueClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	return nil, context.DeadlineExceeded
}

func (c *stuckEnqueueClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	return &meilisearch.Task{Status: "succeeded"}, nil
}

func TestBatcher_SlowEnqueueIsCappedAndKeepsLoopTurning(t *testing.T) {
	client := &stuckEnqueueClient{release: make(chan struct{})}
	t.Cleanup(func() { close(client.release) })

	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          1000,
		FlushInterval:      20 * time.Millisecond,
		MaxInFlightFlushes: 1,
		FlushTimeout:       200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batcher.Start(ctx)

	first := &boundMsg{seq: 1}
	second := &boundMsg{seq: 2}
	var m1, m2 jetstream.Msg = first, second

	batcher.QueueUpsert("index-1", map[string]any{"uid": "a"}, &m1)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&client.calls) == 1 }, time.Second, 5*time.Millisecond)

	batcher.QueueUpsert("index-1", map[string]any{"uid": "b"}, &m2)

	require.Eventually(t, func() bool { return atomic.LoadInt32(&first.naked) == 1 }, 2*time.Second, 10*time.Millisecond,
		"the capped flush must nack its batch for redelivery")
	require.Zero(t, atomic.LoadInt32(&first.acked), "a capped flush must not ack")

	require.Eventually(t, func() bool { return atomic.LoadInt32(&client.calls) == 2 }, 2*time.Second, 10*time.Millisecond,
		"the freed slot must let the next flush start")

	require.Eventually(t, func() bool { return time.Since(batcher.LastProgress()) < 100*time.Millisecond },
		time.Second, 10*time.Millisecond, "the flush loop must keep turning")
}
