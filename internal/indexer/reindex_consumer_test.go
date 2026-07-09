package indexer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newReindexTestPolicyCache builds a PolicyCache holding a single match-all
// Pod policy named "pod-policy" with index "pod-index".
func newReindexTestPolicyCache(t *testing.T) *PolicyCache {
	t.Helper()

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Kind: "Pod", Version: "v1"},
			Conditions: []v1alpha1.PolicyCondition{
				{Name: "all", Expression: "true"},
			},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			IndexName:  "pod-index",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}

	env, _ := internalcel.NewEnv()
	policyCache := &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env,
	}
	policyCache.upsertPolicy(policy)
	return policyCache
}

func TestReindexConsumer_TerminatingResource_QueuesDelete(t *testing.T) {
	// A reindex event for a terminating resource (deletionTimestamp set) must
	// queue a delete instead of an upsert, even when the policy matches, so a
	// re-index cannot resurrect a document the audit path evicted.
	policyCache := newReindexTestPolicyCache(t)

	mockSearch := new(MockSearchClient)
	batcher := NewBatcher(mockSearch, BatchConfig{BatchSize: 1, FlushInterval: 1 * time.Minute})

	mockConsumer := new(MockConsumer)
	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()

	event := ReindexEvent{
		ID:         "reindex-1",
		PolicyName: "pod-policy",
		IndexName:  "pod-index",
		Resource: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":              "mypod",
				"uid":               "pod-uid-terminating",
				"deletionTimestamp": "2026-07-08T00:00:00Z",
			},
		},
	}
	eventBytes, _ := json.Marshal(event)

	msg := &MockJetStreamMsg{seq: 200}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil)

	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	// Expect Delete on Search Client, and no upsert.
	mockSearch.On("DeleteDocumentsAsync", "pod-index", mock.MatchedBy(func(ids []string) bool {
		return len(ids) == 1 && ids[0] == "pod-uid-terminating"
	})).Return(nil, nil).Once()
	mockSearch.On("WaitForTasks", mock.Anything).Return(nil, nil).Once()

	consumer := NewReindexConsumer(mockConsumer, policyCache, batcher)
	ctx, cancel := context.WithCancel(context.Background())

	go consumer.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()

	mockSearch.AssertExpectations(t)
	mockSearch.AssertNotCalled(t, "AddDocumentsAsync", mock.Anything, mock.Anything)
}

func TestReindexConsumer_MatchedResource_Upserts(t *testing.T) {
	// A reindex event for a live (non-terminating) resource that matches the
	// policy must upsert as before.
	policyCache := newReindexTestPolicyCache(t)

	mockSearch := new(MockSearchClient)
	batcher := NewBatcher(mockSearch, BatchConfig{BatchSize: 1, FlushInterval: 1 * time.Minute})

	mockConsumer := new(MockConsumer)
	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()

	event := ReindexEvent{
		ID:         "reindex-2",
		PolicyName: "pod-policy",
		IndexName:  "pod-index",
		Resource: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name": "mypod",
				"uid":  "pod-uid-live",
			},
		},
	}
	eventBytes, _ := json.Marshal(event)

	msg := &MockJetStreamMsg{seq: 201}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil)

	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	// Expect Upsert on Search Client, and no delete.
	mockSearch.On("AddDocumentsAsync", "pod-index", mock.Anything).Return(nil, nil).Once()
	mockSearch.On("WaitForTasks", mock.Anything).Return(nil, nil).Once()

	consumer := NewReindexConsumer(mockConsumer, policyCache, batcher)
	ctx, cancel := context.WithCancel(context.Background())

	go consumer.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()

	mockSearch.AssertExpectations(t)
	mockSearch.AssertNotCalled(t, "DeleteDocumentsAsync", mock.Anything, mock.Anything)
}
