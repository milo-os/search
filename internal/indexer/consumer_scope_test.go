package indexer

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// scopePolicies are three indexed kinds: two sharing an API group and one in
// another group. None of their conditions pass, so every update event below is
// a non-match.
var scopePolicies = []v1alpha1.ResourceIndexPolicy{
	scopePolicy("projects", "resourcemanager.miloapis.com", "v1alpha1", "Project"),
	scopePolicy("organizations", "resourcemanager.miloapis.com", "v1alpha1", "Organization"),
	scopePolicy("httpproxies", "networking.datumapis.com", "v1alpha", "HTTPProxy"),
}

func scopePolicy(name, group, version, kind string) v1alpha1.ResourceIndexPolicy {
	return v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: group, Version: version, Kind: kind},
			Conditions: []v1alpha1.PolicyCondition{
				{Name: "never", Expression: "false"},
			},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			IndexName:  name + "-index",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
}

// consumeForPending runs one audit event through the indexer and returns the
// index/docID keys it buffered as deletes, plus whether the message was acked
// straight away because nothing was queued.
func consumeForPending(t *testing.T, event map[string]any) (deleteKeys []string, ackedImmediately bool) {
	t.Helper()

	env, err := internalcel.NewEnv()
	require.NoError(t, err)
	policyCache := &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env,
	}
	for i := range scopePolicies {
		policyCache.upsertPolicy(&scopePolicies[i])
	}

	batcher := NewBatcher(new(MockSearchClient), BatchConfig{BatchSize: 1000, FlushInterval: time.Hour})

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &MockJetStreamMsg{seq: 1}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil).Maybe()

	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()
	mockConsumer := new(MockConsumer)
	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	// Consume runs the handler synchronously, so with the context already
	// cancelled Start processes the message and returns.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, NewIndexer(mockConsumer, policyCache, batcher, true).Start(ctx))

	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	for key, op := range batcher.pending {
		require.True(t, op.isDelete, "no event here should queue an upsert, got one for %s", key)
		deleteKeys = append(deleteKeys, key)
	}
	sort.Strings(deleteKeys)

	for _, call := range msg.Calls {
		if call.Method == "Ack" {
			ackedImmediately = true
		}
	}
	return deleteKeys, ackedImmediately
}

func updateEvent(apiVersion, kind, uid string, extraMeta map[string]any) map[string]any {
	meta := map[string]any{"name": "obj", "uid": uid}
	for k, v := range extraMeta {
		meta[k] = v
	}
	return map[string]any{
		"verb":    "update",
		"auditID": "scope",
		"objectRef": map[string]string{
			"resource": "things",
			"name":     "obj",
		},
		"responseObject": map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata":   meta,
		},
	}
}

// An update to a kind that no policy targets must not touch any index. Before
// scoping, every update anywhere fanned a delete out to every index.
func TestIndexer_NonMatchingUpdate_UnrelatedKind_QueuesNoDelete(t *testing.T) {
	keys, acked := consumeForPending(t, updateEvent("compute.datumapis.com/v1alpha", "Workload", "w-1", nil))

	assert.Empty(t, keys)
	assert.True(t, acked, "a message that queues nothing must be acked at once")
}

// An update to a policy's kind that fails its conditions deletes from that
// policy's index only, not from another kind's index in the same group.
func TestIndexer_NonMatchingUpdate_SameKind_DeletesFromThatIndexOnly(t *testing.T) {
	keys, acked := consumeForPending(t, updateEvent("resourcemanager.miloapis.com/v1alpha1", "Project", "p-1", nil))

	assert.Equal(t, []string{"projects-index/p-1"}, keys)
	assert.False(t, acked, "the message must be acked by the batch carrying its delete")
}

// The same object can be written through another served version, so the
// version is ignored when scoping the delete.
func TestIndexer_NonMatchingUpdate_OtherVersion_StillDeletes(t *testing.T) {
	keys, _ := consumeForPending(t, updateEvent("resourcemanager.miloapis.com/v1beta1", "Project", "p-1", nil))

	assert.Equal(t, []string{"projects-index/p-1"}, keys)
}

// A terminating resource carries its kind in the response object, so the
// delete is scoped to that kind.
func TestIndexer_TerminatingResource_DeletesFromItsKindOnly(t *testing.T) {
	keys, _ := consumeForPending(t, updateEvent("resourcemanager.miloapis.com/v1alpha1", "Project", "p-1",
		map[string]any{"deletionTimestamp": "2026-10-09T00:00:00Z"}))

	assert.Equal(t, []string{"projects-index/p-1"}, keys)
}

// A delete-verb event's response is usually a Status and its objectRef names
// only the plural resource, so the kind is unknown: the delete goes to every
// policy in the resource's API group and to none outside it.
func TestIndexer_DeleteVerb_ScopedToAPIGroup(t *testing.T) {
	event := map[string]any{
		"verb":    "delete",
		"auditID": "scope",
		"objectRef": map[string]string{
			"apiGroup": "resourcemanager.miloapis.com",
			"resource": "projects",
			"name":     "obj",
			"uid":      "p-1",
		},
		"responseObject": map[string]any{
			"apiVersion": "v1",
			"kind":       "Status",
			"status":     "Success",
		},
	}

	keys, _ := consumeForPending(t, event)

	assert.Equal(t, []string{"organizations-index/p-1", "projects-index/p-1"}, keys)
}

// When a delete-verb response is the deleted object itself, its kind scopes
// the delete like any other.
func TestIndexer_DeleteVerb_ObjectResponse_ScopedToKind(t *testing.T) {
	event := updateEvent("resourcemanager.miloapis.com/v1alpha1", "Project", "p-1", nil)
	event["verb"] = "delete"

	keys, _ := consumeForPending(t, event)

	assert.Equal(t, []string{"projects-index/p-1"}, keys)
}
