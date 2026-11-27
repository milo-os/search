package indexer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBatcher_LastProgress_AdvancesOnAQuietBus(t *testing.T) {
	batcher := NewBatcher(new(MockSearchClient), BatchConfig{BatchSize: 1000, FlushInterval: 10 * time.Millisecond})
	assert.True(t, batcher.LastProgress().IsZero(), "an unstarted batcher reports no progress")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	require.Eventually(t, func() bool { return !batcher.LastProgress().IsZero() }, time.Second, time.Millisecond)
	first := batcher.LastProgress()

	require.Eventually(t, func() bool { return batcher.LastProgress().After(first) }, time.Second, time.Millisecond,
		"the flush loop must keep reporting progress with no messages to index")
}

func TestBatcher_LastProgress_StopsWhileTheLoopIsBlocked(t *testing.T) {
	client := newBlockingSearchClient()
	batcher := NewBatcher(client, BatchConfig{
		BatchSize:          1000,
		FlushInterval:      10 * time.Millisecond,
		MaxInFlightFlushes: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	var jm1 jetstream.Msg = &boundMsg{seq: 1}
	batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&client.inFlight) == 1 }, time.Second, time.Millisecond)

	var jm2 jetstream.Msg = &boundMsg{seq: 2}
	batcher.QueueUpsert("index-1", map[string]any{"uid": "2"}, &jm2)

	require.Eventually(t, func() bool {
		before := batcher.LastProgress()
		time.Sleep(30 * time.Millisecond)
		return before.Equal(batcher.LastProgress())
	}, 2*time.Second, time.Millisecond, "the loop should block once the only flush slot is held")
	stalled := batcher.LastProgress()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, stalled, batcher.LastProgress(), "progress must stop while the loop waits on a flush slot")

	close(client.release)
	require.Eventually(t, func() bool { return batcher.LastProgress().After(stalled) }, time.Second, time.Millisecond,
		"progress must resume once the slot frees")
}

func TestIndexer_Attached_ClosesOnlyOnceConsuming(t *testing.T) {
	newIndexer := func(consumeErr error) (*Indexer, *MockConsumer) {
		mockConsumer := new(MockConsumer)
		mockContext := new(MockConsumeContext)
		mockContext.On("Stop").Return()
		if consumeErr != nil {
			mockConsumer.On("Consume", mock.Anything).Return(nil, consumeErr)
		} else {
			mockConsumer.On("Consume", mock.Anything).Return(mockContext, nil)
		}
		batcher := NewBatcher(new(MockSearchClient), BatchConfig{BatchSize: 10, FlushInterval: time.Minute})
		return NewIndexer(mockConsumer, &PolicyCache{}, batcher, false), mockConsumer
	}

	t.Run("attached", func(t *testing.T) {
		idx, _ := newIndexer(nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = idx.Start(ctx) }()

		select {
		case <-idx.Attached():
		case <-time.After(time.Second):
			t.Fatal("Attached did not close after Consume succeeded")
		}
	})

	t.Run("consume fails", func(t *testing.T) {
		idx, _ := newIndexer(errors.New("consumer not found"))
		require.Error(t, idx.Start(context.Background()))

		select {
		case <-idx.Attached():
			t.Fatal("Attached closed although Consume failed")
		default:
		}
	})
}

func TestReindexConsumer_Attached_ClosesOnlyOnceConsuming(t *testing.T) {
	t.Run("attached", func(t *testing.T) {
		mockConsumer := new(MockConsumer)
		mockContext := new(MockConsumeContext)
		mockContext.On("Stop").Return()
		mockConsumer.On("Consume", mock.Anything).Return(mockContext, nil)
		r := NewReindexConsumer(mockConsumer, &PolicyCache{}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = r.Start(ctx) }()

		select {
		case <-r.Attached():
		case <-time.After(time.Second):
			t.Fatal("Attached did not close after Consume succeeded")
		}
	})

	t.Run("consume fails", func(t *testing.T) {
		mockConsumer := new(MockConsumer)
		mockConsumer.On("Consume", mock.Anything).Return(nil, errors.New("consumer not found"))
		r := NewReindexConsumer(mockConsumer, &PolicyCache{}, nil)
		require.Error(t, r.Start(context.Background()))

		select {
		case <-r.Attached():
			t.Fatal("Attached closed although Consume failed")
		default:
		}
	})
}
