package indexer

import (
	"testing"

	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestPolicyCache creates a PolicyCache with a real CEL env but no backing
// informer cache, suitable for unit-testing upsertPolicy / deletePolicy directly.
func newTestPolicyCache(t *testing.T, requireReady bool) *PolicyCache {
	t.Helper()
	env, err := internalcel.NewEnv()
	require.NoError(t, err)
	return &PolicyCache{
		policies:              make(map[string]*policyevaluation.CachedPolicy),
		celEnv:                env,
		requireReadyCondition: requireReady,
	}
}

// TestPolicyCache_UpsertPolicy mirrors the original TestPolicyCache_Refresh table
// tests, adapted to call upsertPolicy directly (the informer event handler path)
// instead of the removed polling refresh() method.
func TestPolicyCache_UpsertPolicy(t *testing.T) {
	tests := []struct {
		name           string
		policies       []v1alpha1.ResourceIndexPolicy
		expectedCount  int
		expectedPolicy string
	}{
		{
			name:           "No policies",
			policies:       []v1alpha1.ResourceIndexPolicy{},
			expectedCount:  0,
			expectedPolicy: "",
		},
		{
			name: "One ready policy",
			policies: []v1alpha1.ResourceIndexPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "policy-1",
					},
					Spec: v1alpha1.ResourceIndexPolicySpec{
						TargetResource: v1alpha1.TargetResource{
							Group:   "",
							Version: "v1",
							Kind:    "ConfigMap",
						},
						Conditions: []v1alpha1.PolicyCondition{
							{
								Name:       "is-test",
								Expression: "metadata.name.startsWith('test')",
							},
						},
					},
					Status: v1alpha1.ResourceIndexPolicyStatus{
						Conditions: []metav1.Condition{
							{
								Type:   "Ready",
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			expectedCount:  1,
			expectedPolicy: "policy-1",
		},
		{
			name: "One unready policy",
			policies: []v1alpha1.ResourceIndexPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "policy-not-ready",
					},
					Spec: v1alpha1.ResourceIndexPolicySpec{
						Conditions: []v1alpha1.PolicyCondition{
							{
								Name:       "true",
								Expression: "true",
							},
						},
					},
					Status: v1alpha1.ResourceIndexPolicyStatus{
						Conditions: []metav1.Condition{
							{
								Type:   "Ready",
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			expectedCount:  0,
			expectedPolicy: "",
		},
		{
			name: "Invalid CEL expression",
			policies: []v1alpha1.ResourceIndexPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "policy-invalid-cel",
					},
					Spec: v1alpha1.ResourceIndexPolicySpec{
						TargetResource: v1alpha1.TargetResource{
							Group:   "",
							Version: "v1",
							Kind:    "ConfigMap",
						},
						Conditions: []v1alpha1.PolicyCondition{
							{
								Name:       "invalid",
								Expression: "this is not cel code", // Correct syntax error
							},
						},
					},
					Status: v1alpha1.ResourceIndexPolicyStatus{
						Conditions: []metav1.Condition{
							{
								Type:   "Ready",
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			expectedCount:  1,
			expectedPolicy: "policy-invalid-cel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Tests by default use strict ready checking as that's the standard behavior.
			cache := newTestPolicyCache(t, true)

			// Simulate the informer Add event for each policy.
			for i := range tt.policies {
				cache.upsertPolicy(&tt.policies[i])
			}

			policies := cache.GetPolicies()
			assert.Equal(t, tt.expectedCount, len(policies))

			if tt.expectedPolicy != "" {
				found := false
				for _, p := range policies {
					if p.Policy.Name == tt.expectedPolicy {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected policy %s not found in cache", tt.expectedPolicy)
			}
		})
	}
}

func TestPolicyCache_DeletePolicy(t *testing.T) {
	c := newTestPolicyCache(t, true)

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
			Conditions: []v1alpha1.PolicyCondition{
				{Name: "all", Expression: "true"},
			},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
		},
	}

	c.upsertPolicy(policy)
	assert.Len(t, c.GetPolicies(), 1)

	c.deletePolicy("policy-1")
	assert.Empty(t, c.GetPolicies())
}

func TestPolicyCache_UpsertPolicy_NotReady_RemovesExisting(t *testing.T) {
	c := newTestPolicyCache(t, true)

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
		Spec: v1alpha1.ResourceIndexPolicySpec{
			TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
			Conditions: []v1alpha1.PolicyCondition{
				{Name: "all", Expression: "true"},
			},
		},
		Status: v1alpha1.ResourceIndexPolicyStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
		},
	}

	// First upsert as Ready
	c.upsertPolicy(policy)
	assert.Len(t, c.GetPolicies(), 1)

	// Now mark as not-Ready — should be evicted from the cache
	policy.Status.Conditions[0].Status = metav1.ConditionFalse
	c.upsertPolicy(policy)
	assert.Empty(t, c.GetPolicies())
}
