package indexer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	searchmetrics "go.miloapis.net/search/internal/metrics"
	"k8s.io/klog/v2"
)

var reattachInterval = 5 * time.Second

type attachment struct {
	startSignal
	name string

	mu            sync.Mutex
	detachedSince time.Time
}

// DetachedSince is when the consumer stopped after having attached, or the
// zero time while it is consuming.
func (a *attachment) DetachedSince() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.detachedSince
}

func (a *attachment) setDetached(at time.Time) {
	a.mu.Lock()
	a.detachedSince = at
	a.mu.Unlock()
}

func (a *attachment) consume(ctx context.Context, consumer jetstream.Consumer, handler jetstream.MessageHandler) (jetstream.ConsumeContext, error) {
	return consumer.Consume(handler, jetstream.ConsumeErrHandler(func(cc jetstream.ConsumeContext, err error) {
		a.onConsumeError(ctx, consumer, cc, err)
	}))
}

func (a *attachment) onConsumeError(ctx context.Context, consumer jetstream.Consumer, cc jetstream.ConsumeContext, err error) {
	klog.Warningf("%s consumer: %v", a.name, err)
	if !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return
	}
	go func() {
		if _, infoErr := consumer.Info(ctx); errors.Is(infoErr, jetstream.ErrConsumerNotFound) || errors.Is(infoErr, jetstream.ErrStreamNotFound) {
			cc.Stop()
		}
	}()
}

func (a *attachment) run(ctx context.Context, name string, consumer jetstream.Consumer, handler jetstream.MessageHandler) error {
	a.name = name
	cc, err := a.consume(ctx, consumer, handler)
	if err != nil {
		return err
	}
	a.fire()
	klog.Infof("%s consumer attached", a.name)

	for {
		select {
		case <-ctx.Done():
			cc.Stop()
			return nil
		case <-cc.Closed():
		}
		if ctx.Err() != nil {
			return nil
		}

		klog.Errorf("%s consumer stopped; re-attaching every %s", a.name, reattachInterval)
		searchmetrics.IndexerConsumerStops.WithLabelValues(a.name).Inc()
		a.setDetached(time.Now())

		if cc = a.reattach(ctx, consumer, handler); cc == nil {
			return nil
		}
		klog.Infof("%s consumer re-attached", a.name)
		a.setDetached(time.Time{})
	}
}

func (a *attachment) reattach(ctx context.Context, consumer jetstream.Consumer, handler jetstream.MessageHandler) jetstream.ConsumeContext {
	ticker := time.NewTicker(reattachInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if _, err := consumer.Info(ctx); err != nil {
			klog.Warningf("%s consumer is not available: %v", a.name, err)
			continue
		}
		cc, err := a.consume(ctx, consumer, handler)
		if err != nil {
			klog.Warningf("Failed to re-attach the %s consumer: %v", a.name, err)
			continue
		}
		return cc
	}
}

// DetachedSince is when the indexer's consumer stopped after attaching, or the
// zero time while it is consuming.
func (i *Indexer) DetachedSince() time.Time { return i.attached.DetachedSince() }

// DetachedSince is when the re-index consumer stopped after attaching, or the
// zero time while it is consuming.
func (r *ReindexConsumer) DetachedSince() time.Time { return r.attached.DetachedSince() }
