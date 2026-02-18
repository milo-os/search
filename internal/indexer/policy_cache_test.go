package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPolicyCache_Refresh(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

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
			// Create a fake client with the test policies
			objs := []client.Object{}
			for i := range tt.policies {
				objs = append(objs, &tt.policies[i])
			}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

			cache, err := NewPolicyCache(k8sClient, 1*time.Minute)
			require.NoError(t, err)

			// Manually trigger refresh
			err = cache.refresh(context.Background())
			assert.NoError(t, err)

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
