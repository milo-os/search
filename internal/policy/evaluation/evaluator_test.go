package evaluation

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalcel "go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/pkg/apis/policy/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple dot notation",
			input:    ".spec.firstName",
			expected: []string{"spec", "firstName"},
		},
		{
			name:     "bracket notation with single quotes",
			input:    ".metadata.labels['department']",
			expected: []string{"metadata", "labels", "department"},
		},
		{
			name:     "bracket notation with double quotes",
			input:    `.metadata.annotations["kubernetes.io/name"]`,
			expected: []string{"metadata", "annotations", "kubernetes.io/name"},
		},
		{
			name:     "mixed notation",
			input:    ".spec.containers[0].image",
			expected: []string{"spec", "containers", "0", "image"},
		},
		{
			name:     "no leading dot",
			input:    "spec.name",
			expected: []string{"spec", "name"},
		},
		{
			name:     "empty path",
			input:    "",
			expected: nil,
		},
		{
			name:     "single field",
			input:    ".kind",
			expected: []string{"kind"},
		},
		{
			name:     "complex path with nested brackets",
			input:    `.status.conditions[0].lastTransitionTime`,
			expected: []string{"status", "conditions", "0", "lastTransitionTime"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluate(t *testing.T) {
	// Setup CEL environment once
	env, err := internalcel.NewEnv()
	require.NoError(t, err)

	type testCase struct {
		name           string
		policy         *v1alpha1.ResourceIndexPolicy
		resource       *unstructured.Unstructured
		expectedMatch  bool
		expectedFields map[string]any
	}

	// Helper to compile conditions
	compilePolicy := func(p *v1alpha1.ResourceIndexPolicy) *CachedPolicy {
		conditions := make(map[string]cel.Program)
		for _, cond := range p.Spec.Conditions {
			ast, issues := env.Compile(cond.Expression)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("failed to compile condition %s: %v", cond.Expression, issues.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("failed to build program: %v", err)
			}
			conditions[cond.Name] = prg
		}
		return &CachedPolicy{
			Policy:     p,
			Conditions: conditions,
		}
	}

	tests := []testCase{
		{
			name: "GVK mismatch",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "apps",
						Version: "v1",
						Kind:    "Deployment",
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Service",
				},
			},
			expectedMatch:  false,
			expectedFields: map[string]any{},
		},
		{
			name: "Match with simple condition",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "",
						Version: "v1",
						Kind:    "ConfigMap",
					},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "is-prod", Expression: "metadata.labels['env'] == 'prod'"},
					},
					Fields: []v1alpha1.FieldPolicy{
						{Path: ".metadata.name"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":   "my-config",
						"labels": map[string]any{"env": "prod"},
					},
				},
			},
			expectedMatch: true,
			expectedFields: map[string]any{
				".metadata.name": "my-config",
			},
		},
		{
			name: "No match with simple condition",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "",
						Version: "v1",
						Kind:    "ConfigMap",
					},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "is-prod", Expression: "metadata.labels['env'] == 'prod'"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":   "my-config",
						"labels": map[string]any{"env": "dev"},
					},
				},
			},
			expectedMatch:  false,
			expectedFields: map[string]any{},
		},
		{
			name: "Match with multiple conditions (OR semantics) - first matches",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "",
						Version: "v1",
						Kind:    "ConfigMap",
					},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "is-prod", Expression: "metadata.labels['env'] == 'prod'"},
						{Name: "is-staging", Expression: "metadata.labels['env'] == 'staging'"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"labels": map[string]any{"env": "prod"},
					},
				},
			},
			expectedMatch:  true,
			expectedFields: map[string]any{},
		},
		{
			name: "Match with multiple conditions (OR semantics) - second matches",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "",
						Version: "v1",
						Kind:    "ConfigMap",
					},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "is-prod", Expression: "metadata.labels['env'] == 'prod'"},
						{Name: "is-staging", Expression: "metadata.labels['env'] == 'staging'"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"labels": map[string]any{"env": "staging"},
					},
				},
			},
			expectedMatch:  true,
			expectedFields: map[string]any{},
		},
		{
			name: "Field extraction with nested and missing fields",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "apps",
						Version: "v1",
						Kind:    "Deployment",
					},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "all", Expression: "true"},
					},
					Fields: []v1alpha1.FieldPolicy{
						{Path: ".spec.replicas"},
						{Path: ".spec.template.spec.containers[0].image"},
						{Path: ".status.availableReplicas"}, // Missing in resource
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"spec": map[string]any{
						"replicas": int64(3),
						"template": map[string]any{
							"spec": map[string]any{
								"containers": []any{
									map[string]any{
										"image": "nginx:latest",
									},
								},
							},
						},
					},
				},
			},
			expectedMatch: true,
			expectedFields: map[string]any{
				".spec.replicas": int64(3),
				".spec.template.spec.containers[0].image": "nginx:latest",
			},
		},
		{
			name: "Condition evaluation error (should be treated as false/skip)",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					Conditions: []v1alpha1.PolicyCondition{
						// accessing non-existent map key without check might error in some cel configs,
						// but standard cel usually returns error for missing top-level var if strict?
						// Here we try to access a field on 'spec' but spec is missing in resource.
						{Name: "has-restart-policy", Expression: "spec.restartPolicy == 'Always'"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata":   map[string]any{"name": "pod-no-spec"},
					// spec is missing
				},
			},
			// If spec is missing, activation won't have "spec".
			// Expr "spec.restartPolicy" refers to variable "spec".
			// Since "spec" is missing from activation, Eval will return error: "undeclared reference to 'spec'".
			expectedMatch:  false,
			expectedFields: map[string]any{},
		},
		{
			name: "Multiple conditions (OR), none match",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "c1", Expression: "false"},
						{Name: "c2", Expression: "metadata.name == 'other'"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata":   map[string]any{"name": "my-pod"},
				},
			},
			expectedMatch:  false,
			expectedFields: map[string]any{},
		},
		{
			name: "Complex nested array and map extraction",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
					Conditions: []v1alpha1.PolicyCondition{
						{Name: "all", Expression: "true"},
					},
					Fields: []v1alpha1.FieldPolicy{
						{Path: ".spec.containers[1].ports[0].containerPort"},
						{Path: ".spec.volumes[0].name"},
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"spec": map[string]any{
						"containers": []any{
							map[string]any{"name": "c1"}, // index 0
							map[string]any{ // index 1
								"name": "c2",
								"ports": []any{
									map[string]any{"containerPort": int64(8080)},
								},
							},
						},
						"volumes": []any{
							map[string]any{"name": "vol1"},
						},
					},
				},
			},
			expectedMatch: true,
			expectedFields: map[string]any{
				".spec.containers[1].ports[0].containerPort": int64(8080),
				".spec.volumes[0].name":                      "vol1",
			},
		},
		{
			name: "Field extraction edge cases: index out of bounds, type mismatch",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
					Conditions:     []v1alpha1.PolicyCondition{{Name: "all", Expression: "true"}},
					Fields: []v1alpha1.FieldPolicy{
						{Path: ".spec.containers[99].name"}, // Index out of bounds
						{Path: ".spec.containers.name"},     // Treating array as map
						{Path: ".metadata.name[0]"},         // Treating string as array
					},
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata":   map[string]any{"name": "my-pod"},
					"spec": map[string]any{
						"containers": []any{
							map[string]any{"name": "c1"},
						},
					},
				},
			},
			expectedMatch:  true,
			expectedFields: map[string]any{}, // None should be extracted
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cached := compilePolicy(tt.policy)
			result, err := cached.Evaluate(tt.resource)
			require.NoError(t, err) // Evaluate currently returns nil error always

			assert.Equal(t, tt.expectedMatch, result.Matched, "Matched status mismatch")
			if tt.expectedMatch {
				assert.Equal(t, tt.expectedFields, result.Fields, "Fields mismatch")
			}
		})
	}
}
