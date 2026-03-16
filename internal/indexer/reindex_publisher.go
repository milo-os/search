package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/klog/v2"
)

// ReindexEvent is the message shape published to the REINDEX_EVENTS stream.
type ReindexEvent struct {
	// ID is the unique identifier for this re-index request (for deduplication).
	ID string `json:"id"`
	// Resource is the full Kubernetes resource object to be re-indexed.
	Resource map[string]any `json:"resource"`
	// PolicyName identifies the policy that triggered this re-index.
	PolicyName string `json:"policyName"`
	// IndexName is the Meilisearch index name from the policy status.
	IndexName string `json:"indexName"`
	// SpecHash is the SHA-256 hash of the policy spec at the time of publishing.
	// The consumer uses this to ensure it evaluates against the correct policy version.
	SpecHash string `json:"specHash"`

	// Tenant is the name of the tenant this resource belongs to.
	// When empty, the consumer falls back to "platform".
	// +optional
	Tenant string `json:"tenant,omitempty"`

	// TenantType is the type of the tenant ("platform" or "project").
	// When empty, the consumer falls back to "platform".
	// +optional
	TenantType string `json:"tenantType,omitempty"`
}

// ReindexPublisher publishes ReindexEvents to the REINDEX_EVENTS JetStream stream.
type ReindexPublisher struct {
	js      jetstream.JetStream
	subject string
}

// NewReindexPublisher creates a ReindexPublisher.
func NewReindexPublisher(js jetstream.JetStream, subject string) *ReindexPublisher {
	return &ReindexPublisher{js: js, subject: subject}
}

// PublishResource publishes a single Kubernetes resource for re-indexing.
// policyName, indexName, and specHash identify the policy version that triggered the re-index;
// tenant and tenantType identify which tenant the resource belongs to.
// Pass "platform" and "platform" for single-tenant deployments.
func (p *ReindexPublisher) PublishResource(ctx context.Context, resource map[string]any, id, policyName, indexName, specHash, tenant, tenantType string) error {
	evt := ReindexEvent{
		ID:         id,
		Resource:   resource,
		PolicyName: policyName,
		IndexName:  indexName,
		SpecHash:   specHash,
		Tenant:     tenant,
		TenantType: tenantType,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("reindex publisher: failed to marshal event: %w", err)
	}

	if _, err := p.js.Publish(ctx, p.subject, data); err != nil {
		return fmt.Errorf("reindex publisher: failed to publish event: %w", err)
	}

	klog.V(4).Infof("ReindexPublisher: published resource (id=%s, policy=%s, tenant=%s)", id, policyName, tenant)
	return nil
}
