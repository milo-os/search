package indexer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// consumeOne runs a single audit event through an indexer whose one policy
// indexes every HTTPProxy, and returns the batcher's buffered operations and
// whether the message was acked without being buffered.
func consumeOne(t *testing.T, event map[string]any) (pending map[string]pendingOp, ackedImmediately bool) {
	t.Helper()

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "proxies"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "networking.datumapis.com", Kind: "HTTPProxy", Version: "v1alpha"},
			Conditions:     []v1alpha1.PolicyCondition{{Name: "all", Expression: "true"}},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{IndexName: "proxies-index"},
	}
	env, err := internalcel.NewEnv()
	require.NoError(t, err)
	policyCache := &PolicyCache{policies: make(map[string]*policyevaluation.CachedPolicy), celEnv: env}
	policyCache.upsertPolicy(policy)

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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, NewIndexer(mockConsumer, policyCache, batcher, false).Start(ctx))

	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	pending = make(map[string]pendingOp, len(batcher.pending))
	for k, v := range batcher.pending {
		pending[k] = v
	}
	for _, call := range msg.Calls {
		if call.Method == "Ack" {
			ackedImmediately = true
		}
	}
	return pending, ackedImmediately
}

func proxyEvent(verb, requestURI string, meta map[string]any) map[string]any {
	return map[string]any{
		"verb":       verb,
		"auditID":    "dry",
		"requestURI": requestURI,
		"objectRef": map[string]string{
			"apiGroup":  "networking.datumapis.com",
			"resource":  "httpproxies",
			"namespace": "default",
			"name":      "alb-validate",
		},
		"responseObject": map[string]any{
			"apiVersion": "networking.datumapis.com/v1alpha",
			"kind":       "HTTPProxy",
			"metadata":   meta,
		},
	}
}

// A dry-run create is audited with a response that carries a uid but no
// resourceVersion, because nothing was persisted. It must not produce a
// document for an object that never existed.
func TestIndexer_DryRunCreate_IsIgnored(t *testing.T) {
	pending, acked := consumeOne(t, proxyEvent("create",
		"/apis/networking.datumapis.com/v1alpha/namespaces/default/httpproxies?dryRun=All&fieldManager=kubectl-create",
		map[string]any{"name": "alb-validate", "uid": "proxy-dry"},
	))

	require.Empty(t, pending)
	require.True(t, acked)
}

// A dry-run delete returns the object as if it had been removed; acting on it
// would drop a live object from search.
func TestIndexer_DryRunDelete_IsIgnored(t *testing.T) {
	pending, acked := consumeOne(t, proxyEvent("delete",
		"/apis/networking.datumapis.com/v1alpha/namespaces/default/httpproxies/alb-validate?dryRun=All",
		map[string]any{"name": "alb-validate", "uid": "proxy-live", "resourceVersion": "5"},
	))

	require.Empty(t, pending)
	require.True(t, acked)
}

// A write whose response lacks a resourceVersion cannot be ordered against the
// object's other writes and describes nothing persisted, so it is skipped
// even without a dryRun marker.
func TestIndexer_WriteWithoutResourceVersion_IsIgnored(t *testing.T) {
	pending, acked := consumeOne(t, proxyEvent("create",
		"/apis/networking.datumapis.com/v1alpha/namespaces/default/httpproxies",
		map[string]any{"name": "alb-validate", "uid": "proxy-norv"},
	))

	require.Empty(t, pending)
	require.True(t, acked)
}

// The same create with a resourceVersion is indexed, and the buffered
// operation carries that version for ordering.
func TestIndexer_Create_IsOrderedByResourceVersion(t *testing.T) {
	pending, acked := consumeOne(t, proxyEvent("create",
		"/apis/networking.datumapis.com/v1alpha/namespaces/default/httpproxies?fieldManager=kubectl-create",
		map[string]any{"name": "alb-validate", "uid": "proxy-real", "resourceVersion": "1769505307"},
	))

	require.False(t, acked)
	op, ok := pending["proxies-index/proxy-real"]
	require.True(t, ok)
	require.Equal(t, docIndexed, op.state)
	require.Equal(t, uint64(1769505307), op.rv)
}

// A real delete marks the object gone so that a create or update flushed
// after it cannot bring the document back.
func TestIndexer_Delete_MarksObjectGone(t *testing.T) {
	pending, acked := consumeOne(t, proxyEvent("delete",
		"/apis/networking.datumapis.com/v1alpha/namespaces/default/httpproxies/alb-validate",
		map[string]any{"name": "alb-validate", "uid": "proxy-gone", "resourceVersion": "9"},
	))

	require.False(t, acked)
	op, ok := pending["proxies-index/proxy-gone"]
	require.True(t, ok)
	require.Equal(t, docDeleted, op.state)
	require.Equal(t, uint64(9), op.rv)
}
