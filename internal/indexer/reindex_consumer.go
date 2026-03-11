package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"go.miloapis.net/search/internal/utils"
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

		r.processTargetedEvent(msg, event, obj, resourceUID)
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

// processTargetedEvent evaluates a resource against the specific policy identified
// in the event. It verifies the cached policy's spec hash matches the event's
// spec hash to ensure evaluation uses the correct (updated) conditions.
func (r *ReindexConsumer) processTargetedEvent(msg jetstream.Msg, event ReindexEvent, obj *unstructured.Unstructured, resourceUID string) {
	if event.PolicyName == "" || event.IndexName == "" {
		klog.Warningf("ReindexConsumer: event (id=%s) missing policyName or indexName, dropping", event.ID)
		msg.Ack()
		return
	}

	cp := r.policyCache.GetPolicy(event.PolicyName)
	if cp == nil {
		// Policy not in cache yet — NAK so the message is redelivered
		// after the informer propagates the policy to the cache.
		klog.V(2).Infof("ReindexConsumer: policy %s not in cache yet, NAK for redelivery (id=%s)",
			event.PolicyName, event.ID)
		msg.Nak()
		return
	}

	// Verify the cached policy spec matches the version that triggered re-indexing.
	if event.SpecHash != "" {
		cachedHash := utils.ComputeSpecHash(&cp.Policy.Spec)
		if cachedHash != event.SpecHash {
			// Cache is stale — NAK so the message is redelivered after
			// the informer propagates the updated policy spec.
			klog.V(2).Infof("ReindexConsumer: policy %s cache stale (cached=%s, event=%s), NAK for redelivery (id=%s)",
				event.PolicyName, cachedHash[:8], event.SpecHash[:8], event.ID)
			msg.Nak()
			return
		}
	}

	evalResult, err := cp.Evaluate(obj)
	if err != nil {
		klog.Errorf("ReindexConsumer: policy %s evaluation error: %v", event.PolicyName, err)
		msg.Ack()
		return
	}

	if evalResult.Matched {
		klog.V(4).Infof("ReindexConsumer: match policy=%s resource=%s/%s (id=%s)",
			event.PolicyName, obj.GetNamespace(), obj.GetName(), event.ID)

		doc := evalResult.Transform()
		ensureUID(doc, resourceUID)
		r.batcher.QueueUpsert(event.IndexName, doc, &msg)
	} else {
		klog.V(4).Infof("ReindexConsumer: policy %s did not match resource %s/%s (id=%s), deleting from index",
			event.PolicyName, obj.GetNamespace(), obj.GetName(), event.ID)

		r.batcher.QueueDelete(event.IndexName, resourceUID, &msg)
	}
}
