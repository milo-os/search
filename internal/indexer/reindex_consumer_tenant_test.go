package indexer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
)

func reindexStoredTenant(t *testing.T, multiTenant bool, tenantName, tenantType string) (string, string) {
	t.Helper()

	policyCache := newReindexTestPolicyCache(t)
	mockSearch := new(MockSearchClient)
	batcher := NewBatcher(mockSearch, BatchConfig{BatchSize: 1, FlushInterval: time.Minute})

	mockConsumer := new(MockConsumer)
	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()

	event := ReindexEvent{
		ID:         "reindex-tenant",
		PolicyName: "pod-policy",
		IndexName:  "pod-index",
		Tenant:     tenantName,
		TenantType: tenantType,
		Resource: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata":   map[string]any{"name": "mypod", "uid": "pod-uid-tenant"},
		},
	}
	eventBytes, _ := json.Marshal(event)

	msg := &MockJetStreamMsg{seq: 300}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil)
	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	stored := make(chan map[string]any, 1)
	mockSearch.On("AddDocumentsAsync", "pod-index", mock.MatchedBy(func(docs []any) bool {
		if len(docs) == 1 {
			if doc, ok := docs[0].(map[string]any); ok {
				select {
				case stored <- doc:
				default:
				}
			}
		}
		return true
	})).Return(nil, nil).Once()
	mockSearch.On("WaitForTasks", mock.Anything).Return(nil, nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = NewReindexConsumer(mockConsumer, policyCache, batcher, multiTenant).Start(ctx) }()

	select {
	case doc := <-stored:
		name, _ := doc["_tenant"].(string)
		typ, _ := doc["_tenant_type"].(string)
		return name, typ
	case <-time.After(2 * time.Second):
		t.Fatal("no document was stored")
		return "", ""
	}
}

func TestReindexConsumer_EmptyTenant_MultiTenant_StoresUnscoped(t *testing.T) {
	name, typ := reindexStoredTenant(t, true, "", "")
	if name != "unscoped" || typ != "unscoped" {
		t.Errorf("stored %q/%q, want unscoped/unscoped", typ, name)
	}
}

func TestReindexConsumer_TypeWithoutName_MultiTenant_StoresUnscoped(t *testing.T) {
	name, typ := reindexStoredTenant(t, true, "", "Organization")
	if name != "unscoped" || typ != "unscoped" {
		t.Errorf("stored %q/%q, want unscoped/unscoped", typ, name)
	}
}

func TestReindexConsumer_EmptyTenant_SingleTenant_StoresPlatform(t *testing.T) {
	name, typ := reindexStoredTenant(t, false, "", "")
	if name != "platform" || typ != "platform" {
		t.Errorf("stored %q/%q, want platform/platform", typ, name)
	}
}

func TestReindexConsumer_Owner_StoredAsPublished(t *testing.T) {
	name, typ := reindexStoredTenant(t, true, "acme", "Organization")
	if name != "acme" || typ != "Organization" {
		t.Errorf("stored %q/%q, want Organization/acme", typ, name)
	}
}
