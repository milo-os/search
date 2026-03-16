package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

// Indexer is the component responsible for indexing resources.
type Indexer struct {
	consumer    jetstream.Consumer
	policyCache *PolicyCache
	batcher     *Batcher
	mu          sync.Mutex
}

type auditEvent struct {
	AuditID   string `json:"auditID"`
	Verb      string `json:"verb"`
	ObjectRef struct {
		APIGroup   string `json:"apiGroup"`
		APIVersion string `json:"apiVersion"`
		Resource   string `json:"resource"`
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		UID        string `json:"uid"`
	} `json:"objectRef"`
	ResponseObject map[string]any `json:"responseObject"`
	// User carries authenticated user information including tenant context in extra fields.
	User struct {
		Extra map[string][]string `json:"extra,omitempty"`
	} `json:"user,omitempty"`
}

// extractTenantFromAuditEvent extracts tenant identity from audit event user extra fields.
// Mirrors processor.ExtractTenant() in the Activity repo.
// Falls back to "platform"/"platform" when fields are absent.
func extractTenantFromAuditEvent(event *auditEvent) (tenantName string, tenantType string) {
	tenantName = "platform"
	tenantType = "platform"

	if event.User.Extra == nil {
		return
	}

	if values, ok := event.User.Extra["iam.miloapis.com/parent-type"]; ok && len(values) > 0 {
		tenantType = values[0]
	}
	if values, ok := event.User.Extra["iam.miloapis.com/parent-name"]; ok && len(values) > 0 {
		tenantName = values[0]
	}

	return
}

// NewIndexer creates a new Indexer instance.
func NewIndexer(consumer jetstream.Consumer, policyCache *PolicyCache, batcher *Batcher) *Indexer {
	return &Indexer{
		consumer:    consumer,
		policyCache: policyCache,
		batcher:     batcher,
	}
}

var upsertVerbs = map[string]bool{"create": true, "update": true, "patch": true}

const deleteVerb = "delete"

// Start starts the indexer consumer loop.
// Note: the Batcher must be started separately by the caller (via batcher.Start)
// before calling this method. This allows multiple Indexer instances to share a
// single Batcher without starting its flush goroutine more than once.
func (i *Indexer) Start(ctx context.Context) error {

	// Consume messages
	cons, err := i.consumer.Consume(func(msg jetstream.Msg) {
		// Parse the audit event
		var event auditEvent

		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			klog.Errorf("Failed to unmarshal audit event: %v", err)
			msg.Ack()
			return
		}

		// Handle deletions separately
		if event.Verb == deleteVerb {
			docID := resolveUID(&event)
			if docID == "" {
				logMissingUIDDetails(&event)
				msg.Ack()
				return
			}

			// Queue delete for all policies since we don't know which one it matched
			for _, cp := range i.policyCache.GetPolicies() {
				// Skip if index name is not set yet
				if cp.Policy.Status.IndexName == "" {
					continue
				}

				i.batcher.QueueDelete(cp.Policy.Status.IndexName, docID, &msg)
			}
			return
		}

		// If NO response object OR NOT an upsert verb, skip it.
		if event.ResponseObject == nil || !upsertVerbs[event.Verb] {
			msg.Ack()
			return
		}

		// Build unstructured from responseObject if present
		obj := &unstructured.Unstructured{Object: event.ResponseObject}

		// Attempt to resolve UID using helper
		resourceUID := resolveUID(&event)
		if resourceUID == "" {
			logMissingUIDDetails(&event)
			msg.Ack()
			return
		}

		queued := false

		policies := i.policyCache.GetPolicies()

		for _, cp := range policies {
			evalResult, err := cp.Evaluate(obj)
			if err != nil {
				klog.Errorf("Policy %s evaluation error: %v", cp.Policy.Name, err)
				continue
			}

			if evalResult.Matched {
				klog.Infof("Policy %s matched %s resource %s (auditID: %s)",
					cp.Policy.Name, event.Verb, obj.GetName(), event.AuditID)

				// Skip if index name is not set yet
				if cp.Policy.Status.IndexName == "" {
					klog.Warningf("Policy %s matched but has no IndexName in status, skipping index", cp.Policy.Name)
					continue
				}

				// Inject tenant context extracted from the audit event's user extra fields.
				evalResult.Tenant, evalResult.TenantType = extractTenantFromAuditEvent(&event)

				// Transform the matching resource into an indexable document
				doc := evalResult.Transform()

				// Ensure UID is set as primary key if not present in the map under "uid"
				ensureUID(doc, resourceUID)

				i.batcher.QueueUpsert(cp.Policy.Status.IndexName, doc, &msg)
				queued = true
			} else {
				// "Update and patch events that don't match should still queue a delete operation"
				if event.Verb == "update" || event.Verb == "patch" {
					if cp.Policy.Status.IndexName != "" {
						i.batcher.QueueDelete(cp.Policy.Status.IndexName, resourceUID, &msg)
						queued = true
					}
				}
			}
		}

		// If the message wasn't queued for any operation, acknowledge it immediately
		if !queued {
			msg.Ack()
		}

	})
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	defer cons.Stop()

	klog.Info("Indexer started successfully")

	// Wait for context cancellation
	<-ctx.Done()
	klog.Info("Shutting down indexer...")
	return nil
}
