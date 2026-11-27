package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type stoppableConsumeContext struct {
	once    sync.Once
	closed  chan struct{}
	stopped atomic.Bool
}

func newStoppableConsumeContext() *stoppableConsumeContext {
	return &stoppableConsumeContext{closed: make(chan struct{})}
}

func (c *stoppableConsumeContext) Stop() {
	c.stopped.Store(true)
	c.once.Do(func() { close(c.closed) })
}
func (c *stoppableConsumeContext) Drain()                  { c.Stop() }
func (c *stoppableConsumeContext) Closed() <-chan struct{} { return c.closed }

type infoConsumer struct {
	*MockConsumer
	mu      sync.Mutex
	infoErr error
}

func (c *infoConsumer) setInfoErr(err error) {
	c.mu.Lock()
	c.infoErr = err
	c.mu.Unlock()
}

func (c *infoConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nil, c.infoErr
}

type consumerUnderTest interface {
	Start(context.Context) error
	Attached() <-chan struct{}
	DetachedSince() time.Time
}

func TestConsumers_ReattachAfterTheConsumerStops(t *testing.T) {
	previous := reattachInterval
	reattachInterval = 10 * time.Millisecond
	t.Cleanup(func() { reattachInterval = previous })

	for name, build := range map[string]func(jetstream.Consumer) consumerUnderTest{
		"audit": func(c jetstream.Consumer) consumerUnderTest {
			return NewIndexer(c, &PolicyCache{}, nil, false)
		},
		"reindex": func(c jetstream.Consumer) consumerUnderTest {
			return NewReindexConsumer(c, &PolicyCache{}, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			first, second := newStoppableConsumeContext(), newStoppableConsumeContext()
			consumer := &infoConsumer{MockConsumer: new(MockConsumer)}
			consumer.On("Consume", mock.Anything).Return(first, nil).Once()
			consumer.On("Consume", mock.Anything).Return(second, nil).Once()
			c := build(consumer)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Start(ctx) }()

			select {
			case <-c.Attached():
			case <-time.After(time.Second):
				t.Fatal("consumer did not attach")
			}
			assert.True(t, c.DetachedSince().IsZero(), "an attached consumer must not report itself detached")

			consumer.setInfoErr(jetstream.ErrConsumerNotFound)
			first.Stop()
			require.Eventually(t, func() bool { return !c.DetachedSince().IsZero() }, time.Second, time.Millisecond,
				"a consumer whose consume context closed must report itself detached")
			time.Sleep(5 * reattachInterval)
			consumer.AssertNumberOfCalls(t, "Consume", 1)
			assert.False(t, c.DetachedSince().IsZero(), "the consumer must stay detached while it is missing")

			consumer.setInfoErr(nil)
			require.Eventually(t, func() bool { return c.DetachedSince().IsZero() }, time.Second, time.Millisecond,
				"the consumer must re-attach once it exists again")
			consumer.AssertNumberOfCalls(t, "Consume", 2)

			cancel()
			require.NoError(t, <-done)
			assert.True(t, second.stopped.Load(), "shutdown must stop the re-attached consume context")
		})
	}
}

func TestAttachment_OnConsumeError_StopsOnlyWhenTheConsumerIsGone(t *testing.T) {
	cases := map[string]struct {
		err      error
		infoErr  error
		wantStop bool
	}{
		"missed heartbeat during a NATS blip":          {err: jetstream.ErrNoHeartbeat, infoErr: jetstream.ErrConsumerNotFound},
		"no responders while JetStream is unreachable": {err: nats.ErrNoResponders, infoErr: context.DeadlineExceeded},
		"no responders while the consumer exists":      {err: nats.ErrNoResponders},
		"no responders for a deleted consumer":         {err: nats.ErrNoResponders, infoErr: jetstream.ErrConsumerNotFound, wantStop: true},
		"no responders for a deleted stream":           {err: nats.ErrNoResponders, infoErr: jetstream.ErrStreamNotFound, wantStop: true},
		"consumer not found":                           {err: jetstream.ErrConsumerNotFound, infoErr: jetstream.ErrConsumerNotFound, wantStop: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			consumer := &infoConsumer{MockConsumer: new(MockConsumer), infoErr: tc.infoErr}
			cc := newStoppableConsumeContext()
			a := &attachment{name: "audit"}

			a.onConsumeError(context.Background(), consumer, cc, tc.err)

			if tc.wantStop {
				require.Eventually(t, cc.stopped.Load, time.Second, time.Millisecond)
				return
			}
			time.Sleep(20 * time.Millisecond)
			assert.False(t, cc.stopped.Load(), "consuming must continue after %v", tc.err)
		})
	}
}
