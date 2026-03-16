package indexer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MockConsumer is a mock for jetstream.Consumer
type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	args := m.Called(handler)

	var err error

	if msgs, ok := args.Get(1).([]jetstream.Msg); ok {
		for _, msg := range msgs {
			handler(msg)
		}
		err = args.Error(2)
	} else {
		err = args.Error(1)
	}

	if args.Get(0) == nil {
		return nil, err
	}
	return args.Get(0).(jetstream.ConsumeContext), err
}

func (m *MockConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return nil
}
func (m *MockConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return nil, nil
}
func (m *MockConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, nil
}
func (m *MockConsumer) FetchBytes(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, nil
}
func (m *MockConsumer) FetchNoWait(int) (jetstream.MessageBatch, error) {
	return nil, nil
}
func (m *MockConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return nil, nil
}
func (m *MockConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) {
	return nil, nil
}
func (m *MockConsumer) Name() string { return "mock" }

// MockConsumeContext
type MockConsumeContext struct {
	mock.Mock
}

func (m *MockConsumeContext) Stop()                   { m.Called() }
func (m *MockConsumeContext) Drain()                  { m.Called() }
func (m *MockConsumeContext) Closed() <-chan struct{} { return nil }

func TestIndexer_Start_ConsumeFlow(t *testing.T) {
	// 1. Setup PolicyCache with a policy
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

	// 2. Setup Batcher with Mock Search Client
	mockSearch := new(MockSearchClient)
	batcher := NewBatcher(mockSearch, BatchConfig{BatchSize: 10, FlushInterval: 1 * time.Minute})

	// 3. Setup Mock Consumer and Messages
	mockConsumer := new(MockConsumer)
	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()

	// Create test audit event
	event := map[string]interface{}{
		"verb":    "create",
		"auditID": "123",
		"objectRef": map[string]string{
			"resource": "pods",
			"name":     "mypod",
		},
		"responseObject": map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name": "mypod",
				"uid":  "pod-uid-1",
			},
		},
	}
	eventBytes, _ := json.Marshal(event)

	msg := &MockJetStreamMsg{seq: 100}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil)

	// Expectation: Consume is called, we immediately fire the handler with our msg
	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	// Expectation: Batcher should receive an Upsert
	batcher.batchConfig.BatchSize = 1
	mockSearch.On("AddDocumentsAsync", "pod-index", mock.Anything).Return(nil, nil).Once()
	mockSearch.On("WaitForTasks", mock.Anything).Return(nil, nil).Once()

	// 4. Run Indexer
	indexer := NewIndexer(mockConsumer, policyCache, batcher, false)
	ctx, cancel := context.WithCancel(context.Background())

	// Run Start in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		indexer.Start(ctx)
	}()

	// Wait a bit for processing
	time.Sleep(100 * time.Millisecond)

	cancel() // Stop the loop
	wg.Wait()

	mockSearch.AssertExpectations(t)
	msg.AssertExpectations(t)
}

func TestExtractTenantFromAuditEvent_WithUserExtra(t *testing.T) {
	event := &auditEvent{}
	event.User.Extra = map[string][]string{
		"iam.miloapis.com/parent-type": {"project"},
		"iam.miloapis.com/parent-name": {"my-project"},
	}

	name, typ := extractTenantFromAuditEvent(event)

	if name != "my-project" {
		t.Errorf("tenantName: got %q, want %q", name, "my-project")
	}
	if typ != "project" {
		t.Errorf("tenantType: got %q, want %q", typ, "project")
	}
}

func TestExtractTenantFromAuditEvent_NoUserExtra(t *testing.T) {
	event := &auditEvent{}
	// User.Extra is nil by default.

	name, typ := extractTenantFromAuditEvent(event)

	if name != "platform" {
		t.Errorf("tenantName: got %q, want %q", name, "platform")
	}
	if typ != "platform" {
		t.Errorf("tenantType: got %q, want %q", typ, "platform")
	}
}

func TestExtractTenantFromAuditEvent_PartialUserExtra_TypeOnlyNoName(t *testing.T) {
	// Only parent-type is set; parent-name is absent.
	// Expect: tenantType reflects the extra field, tenantName falls back to "platform".
	event := &auditEvent{}
	event.User.Extra = map[string][]string{
		"iam.miloapis.com/parent-type": {"project"},
	}

	name, typ := extractTenantFromAuditEvent(event)

	if name != "platform" {
		t.Errorf("tenantName: got %q, want %q (expected fallback)", name, "platform")
	}
	if typ != "project" {
		t.Errorf("tenantType: got %q, want %q", typ, "project")
	}
}

func TestExtractTenantFromAuditEvent_EmptySliceValues(t *testing.T) {
	// Keys present but with empty slices should not override the defaults.
	event := &auditEvent{}
	event.User.Extra = map[string][]string{
		"iam.miloapis.com/parent-type": {},
		"iam.miloapis.com/parent-name": {},
	}

	name, typ := extractTenantFromAuditEvent(event)

	if name != "platform" {
		t.Errorf("tenantName: got %q, want %q", name, "platform")
	}
	if typ != "platform" {
		t.Errorf("tenantType: got %q, want %q", typ, "platform")
	}
}

func TestIndexer_Consume_Delete(t *testing.T) {
	// Setup similar to above but for DELETE event
	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "", Kind: "Pod"},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			IndexName:  "pod-index",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}

	env2, _ := internalcel.NewEnv()
	policyCache := &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env2,
	}
	policyCache.upsertPolicy(policy)

	mockSearch := new(MockSearchClient)
	batcher := NewBatcher(mockSearch, BatchConfig{BatchSize: 1, FlushInterval: 1 * time.Minute})

	mockConsumer := new(MockConsumer)
	mockContext := new(MockConsumeContext)
	mockContext.On("Stop").Return()

	// DELETE Event
	event := map[string]interface{}{
		"verb":    "delete",
		"auditID": "456",
		"objectRef": map[string]string{
			"resource": "pods", // matches Kind=Pod via heuristic
			"name":     "mypod",
			"uid":      "pod-uid-deleted",
		},
	}
	eventBytes, _ := json.Marshal(event)

	msg := &MockJetStreamMsg{seq: 101}
	msg.On("Data").Return(eventBytes)
	msg.On("Ack").Return(nil)

	mockConsumer.On("Consume", mock.Anything).Return(mockContext, []jetstream.Msg{msg}, nil)

	// Expect Delete on Search Client
	mockSearch.On("DeleteDocumentsAsync", "pod-index", mock.MatchedBy(func(ids []string) bool {
		return len(ids) > 0 && ids[0] == "pod-uid-deleted"
	})).Return(nil, nil).Once()
	mockSearch.On("WaitForTasks", mock.Anything).Return(nil, nil).Once()

	indexer := NewIndexer(mockConsumer, policyCache, batcher, false)
	ctx, cancel := context.WithCancel(context.Background())

	go indexer.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()

	mockSearch.AssertExpectations(t)
}
