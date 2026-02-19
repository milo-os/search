package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

// ReindexConsumer consumes ReindexEvents and processes them.
type ReindexConsumer struct {
	consumer    jetstream.Consumer
	policyCache *PolicyCache
	batcher     *Batcher
}

// NewReindexConsumer creates a new ReindexConsumer instance.
func NewReindexConsumer(consumer jetstream.Consumer, policyCache *PolicyCache, batcher *Batcher) *ReindexConsumer {
	return &ReindexConsumer{
		consumer:    consumer,
		policyCache: policyCache,
		batcher:     batcher,
	}
}

// Start starts the re-index consumer loop.
// Note: the Batcher must be started separately by the caller.
func (r *ReindexConsumer) Start(ctx context.Context) error {
	klog.Info("Starting ReindexConsumer...")

	cons, err := r.consumer.Consume(func(msg jetstream.Msg) {
		var event ReindexEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			klog.Errorf("ReindexConsumer: failed to unmarshal event: %v", err)
			msg.Ack()
			return
		}

		klog.Infof("Received event id: %s", event.ID)

		if event.Resource == nil {
			klog.Warningf("ReindexConsumer: received event with nil resource (id=%s)", event.ID)
			msg.Ack()
			return
		}

		// Build unstructured from resource
		obj := &unstructured.Unstructured{Object: event.Resource}

		resourceUID := string(obj.GetUID())
		if resourceUID == "" {
			klog.Warningf("ReindexConsumer: resource has no UID (id=%s, name=%s, namespace=%s)",
				event.ID, obj.GetName(), obj.GetNamespace())
			msg.Ack()
			return
		}

		queued := false
		policies := r.policyCache.GetPolicies()

		for _, cp := range policies {
			// Skip if index name is not set yet
			if cp.Policy.Status.IndexName == "" {
				continue
			}

			evalResult, err := cp.Evaluate(obj)
			if err != nil {
				klog.Errorf("ReindexConsumer: policy %s evaluation error: %v", cp.Policy.Name, err)
				continue
			}

			if evalResult.Matched {
				klog.V(4).Infof("ReindexConsumer: match policy=%s resource=%s/%s (id=%s)",
					cp.Policy.Name, obj.GetNamespace(), obj.GetName(), event.ID)

				// Transform into indexable document
				doc := evalResult.Transform()

				// Ensure UID is set as primary key
				ensureUID(doc, resourceUID)

				r.batcher.QueueUpsert(cp.Policy.Status.IndexName, doc, &msg)
				queued = true
			} else {
				// If it doesn't match this policy, we should ensure it's removed from the index
				// in case it was previously indexed there.
				r.batcher.QueueDelete(cp.Policy.Status.IndexName, resourceUID, &msg)
				queued = true
			}
		}

		// If the message wasn't queued for any operation (e.g. no policies), acknowledge it
		if !queued {
			klog.Warningf("ReindexConsumer: event (id=%s) matched no active policies in cache, skipping", event.ID)
			msg.Ack()
		}
	})

	if err != nil {
		return fmt.Errorf("failed to start re-index consumer: %w", err)
	}
	defer cons.Stop()

	// Wait for context cancellation
	<-ctx.Done()
	klog.Info("Shutting down ReindexConsumer...")
	return nil
}
