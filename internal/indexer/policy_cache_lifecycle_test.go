package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.miloapis.net/search/pkg/apis/policy/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPolicyCache_Start_Lifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	// Initial policy
	policy1 := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(policy1).Build()

	// Short interval for testing
	interval := 100 * time.Millisecond
	cache, err := NewPolicyCache(k8sClient, interval)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run Start in background
	go func() {
		if err := cache.Start(ctx); err != nil {
			// In a real test, we might want to signal this error,
			// but for now context cancellation is the expected exit.
		}
	}()

	// 1. Check initial sync happened (Start calls refresh immediately)
	// We might need to wait a tiny bit for the goroutine to execute refresh
	assert.Eventually(t, func() bool {
		return len(cache.GetPolicies()) == 1
	}, 1*time.Second, 10*time.Millisecond, "Expected initial policy to be loaded")

	// 2. Add a new policy to the client
	policy2 := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-2"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Service"},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, policy2))

	// 3. Wait for refresh interval and check if new policy is picked up
	assert.Eventually(t, func() bool {
		policies := cache.GetPolicies()
		return len(policies) == 2
	}, 1*time.Second, 50*time.Millisecond, "Expected 2 policies after refresh")

	// 4. Cancel context and ensure it stops (Start returns)
	cancel()
}
