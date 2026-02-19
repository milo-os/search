package indexer

import (
	"testing"

	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyCache_EventHandlers verifies that the Add, Update, and Delete
// event handler logic (wired up in Start) correctly mutates the in-memory
// policy map. We exercise the handlers directly to avoid needing a live
// API server or envtest setup.
func TestPolicyCache_EventHandlers(t *testing.T) {
	env, err := internalcel.NewEnv()
	require.NoError(t, err)

	c := &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env,
	}

	readyPolicy := func(name string) *v1alpha1.ResourceIndexPolicy {
		return &v1alpha1.ResourceIndexPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name},
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
	}

	handler := toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if p, ok := obj.(*v1alpha1.ResourceIndexPolicy); ok {
				c.upsertPolicy(p)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if p, ok := newObj.(*v1alpha1.ResourceIndexPolicy); ok {
				c.upsertPolicy(p)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if p, ok := obj.(*v1alpha1.ResourceIndexPolicy); ok {
				c.deletePolicy(p.Name)
			}
		},
	}

	t.Run("Add ready policy", func(t *testing.T) {
		handler.OnAdd(readyPolicy("policy-1"), false)
		assert.Len(t, c.GetPolicies(), 1)
	})

	t.Run("Update policy to not-ready removes it", func(t *testing.T) {
		notReady := readyPolicy("policy-1")
		notReady.Status.Conditions[0].Status = metav1.ConditionFalse
		handler.OnUpdate(readyPolicy("policy-1"), notReady)
		assert.Empty(t, c.GetPolicies())
	})

	t.Run("Add second policy", func(t *testing.T) {
		handler.OnAdd(readyPolicy("policy-2"), false)
		assert.Len(t, c.GetPolicies(), 1)
	})

	t.Run("Delete via tombstone", func(t *testing.T) {
		tombstone := toolscache.DeletedFinalStateUnknown{
			Key: "policy-2",
			Obj: readyPolicy("policy-2"),
		}
		handler.OnDelete(tombstone)
		assert.Empty(t, c.GetPolicies())
	})
}
