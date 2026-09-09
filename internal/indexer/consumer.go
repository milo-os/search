package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

// Indexer is the component responsible for indexing resources.
type Indexer struct {
	consumer           jetstream.Consumer
	policyCache        *PolicyCache
	batcher            *Batcher
	enableMultiTenancy bool
	mu                 sync.Mutex
}

type auditEvent struct {
	AuditID     string            `json:"auditID"`
	Verb        string            `json:"verb"`
	Annotations map[string]string `json:"annotations"`
	User        struct {
		Extra map[string][]string `json:"extra"`
	} `json:"user"`
	ObjectRef struct {
		APIGroup   string `json:"apiGroup"`
		APIVersion string `json:"apiVersion"`
		Resource   string `json:"resource"`
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		UID        string `json:"uid"`
	} `json:"objectRef"`
	ResponseObject map[string]any `json:"responseObject"`
}

// extractTenantFromAuditEvent extracts tenant identity from the audit event.
// It reads exclusively from the top-level audit event annotations:
//   - ScopeTypeAnnotationKey ("platform.miloapis.com/scope.type") for the tenant type
//   - ScopeNameAnnotationKey ("platform.miloapis.com/scope.name") for the tenant name
//
// Falls back to "platform"/"platform" when the annotations are absent or not set.
func extractTenantFromAuditEvent(event *auditEvent) (tenantName string, tenantType string) {
	tenantName = tenantTypePlatform
	tenantType = tenantTypePlatform

	if event.Annotations == nil {
		return
	}

	caser := cases.Title(language.Und)

	if v, ok := event.Annotations[ScopeTypeAnnotationKey]; ok && v != "" {
		// Normalize to title-case to match Milo's scope annotation conventions
		// (e.g. the annotation value "project" becomes "Project").
		// Exception: "platform" is a fallback default and stays lowercase.
		if v != tenantTypePlatform {
			tenantType = caser.String(v)
		} else {
			tenantType = v
		}
	}

	if v, ok := event.Annotations[ScopeNameAnnotationKey]; ok && v != "" {
		tenantName = v
	}

	return
}

// NewIndexer creates a new Indexer instance.
func NewIndexer(consumer jetstream.Consumer, policyCache *PolicyCache, batcher *Batcher, multiTenant bool) *Indexer {
	return &Indexer{
		consumer:           consumer,
		policyCache:        policyCache,
		batcher:            batcher,
		enableMultiTenancy: multiTenant,
	}
}

var upsertVerbs = map[string]bool{"create": true, "update": true, "patch": true}

const (
	deleteVerb = "delete"
	// tenantTypePlatform mirrors tenant.TenantTypePlatform. A local copy is used
	// to avoid an import cycle: internal/tenant/project_watcher.go already
	// imports internal/indexer, so internal/indexer cannot import internal/tenant.
	tenantTypePlatform = "platform"

	// Scope annotation keys from resource metadata. Tenant identity is derived
	// exclusively from these annotations on the ResponseObject.
	ScopeTypeAnnotationKey = "platform.miloapis.com/scope.type"
	ScopeNameAnnotationKey = "platform.miloapis.com/scope.name"
)

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
			i.handleDelete(msg, &event)
			return
		}

		// If NO response object OR NOT an upsert verb, skip it.
		if event.ResponseObject == nil || !upsertVerbs[event.Verb] {
			msg.Ack()
			return
		}

		// Build unstructured from responseObject if present
		obj := &unstructured.Unstructured{Object: event.ResponseObject}

		// Treat create/update/patch events on terminating resources as deletes.
		// For resources with finalizers, the terminal audit event is the update
		// that removes the last finalizer (deletionTimestamp still set) — the
		// physical etcd purge emits no delete-verb event. Upserting here would
		// resurrect the document the earlier delete event just removed.
		if obj.GetDeletionTimestamp() != nil {
			i.handleDelete(msg, &event)
			return
		}

		// Attempt to resolve UID using helper
		resourceUID := resolveUID(&event)
		if resourceUID == "" {
			logMissingUIDDetails(&event)
			msg.Ack()
			return
		}

		// In single-tenant mode, skip non-platform
		// events entirely so that no policy can accidentally queue them.
		tenantName, tenantType := extractTenantFromAuditEvent(&event)
		if !i.enableMultiTenancy && tenantType != tenantTypePlatform {
			msg.Ack()
			return
		}

		// Collect every operation this message produces before handing any of
		// them to the batcher, so they all land in the same batch as the ack.
		var (
			upserts []UpsertOp
			deletes []DeleteOp
		)

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

				// Attach the already-extracted tenant context to the eval result.
				evalResult.Tenant = tenantName
				evalResult.TenantType = tenantType

				// Transform the matching resource into an indexable document
				doc := evalResult.Transform()

				// Ensure UID is set as primary key if not present in the map under "uid"
				ensureUID(doc, resourceUID)

				upserts = append(upserts, UpsertOp{IndexUID: cp.Policy.Status.IndexName, Doc: doc})
			} else {
				// "Update and patch events that don't match should still queue a delete operation"
				if event.Verb == "update" || event.Verb == "patch" {
					if cp.Policy.Status.IndexName != "" {
						deletes = append(deletes, DeleteOp{IndexUID: cp.Policy.Status.IndexName, DocID: resourceUID})
					}
				}
			}
		}

		// If the message wasn't queued for any operation, acknowledge it immediately
		if len(upserts) == 0 && len(deletes) == 0 {
			msg.Ack()
			return
		}

		i.batcher.Submit(&msg, upserts, deletes)

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

// handleDelete queues a delete for the event's resource across all policies
// with an index. It is used for delete-verb events and for create/update/patch
// events on terminating resources (deletionTimestamp set).
func (i *Indexer) handleDelete(msg jetstream.Msg, event *auditEvent) {
	docID := resolveUID(event)
	if docID == "" {
		logMissingUIDDetails(event)
		msg.Ack()
		return
	}

	// Queue delete for all policies since we don't know which one it matched.
	// They are submitted together so the batch that acks this message also owns
	// every delete derived from it.
	var deletes []DeleteOp
	for _, cp := range i.policyCache.GetPolicies() {
		// Skip if index name is not set yet
		if cp.Policy.Status.IndexName == "" {
			continue
		}

		deletes = append(deletes, DeleteOp{IndexUID: cp.Policy.Status.IndexName, DocID: docID})
	}

	// No policy has an index yet, so there is nothing to delete from.
	if len(deletes) == 0 {
		msg.Ack()
		return
	}

	i.batcher.Submit(&msg, nil, deletes)
}
