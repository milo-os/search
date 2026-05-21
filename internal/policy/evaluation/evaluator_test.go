package evaluation

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalcel "go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
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
		{
			name:     "fully qualified annotation key with single quotes",
			input:    ".metadata.annotations['kubernetes.io/display-name']",
			expected: []string{"metadata", "annotations", "kubernetes.io/display-name"},
		},
		{
			name:     "fully qualified label key with single quotes",
			input:    ".metadata.labels['app.kubernetes.io/name']",
			expected: []string{"metadata", "labels", "app.kubernetes.io/name"},
		},
		// Wildcard tests
		{
			name:     "wildcard single level",
			input:    ".spec.ports[*].name",
			expected: []string{"spec", "ports", "*", "name"},
		},
		{
			name:     "wildcard nested double",
			input:    ".spec.containers[*].ports[*].name",
			expected: []string{"spec", "containers", "*", "ports", "*", "name"},
		},
		{
			name:     "wildcard at end of path",
			input:    ".spec.items[*]",
			expected: []string{"spec", "items", "*"},
		},
		{
			name:     "mixed wildcard and numeric index",
			input:    ".spec.ports[*].name",
			expected: []string{"spec", "ports", "*", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParsePath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMeilisearchAttributeName verifies that converting a field path to a
// Meilisearch searchable attribute name (ParsePath + strings.Join with ".") produces
// the correct dot-notation string for fully qualified Kubernetes label/annotation keys.
// This mirrors the logic in the policy controller's desiredAttributes calculation.
func TestMeilisearchAttributeName(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "simple dot notation",
			path:     ".metadata.name",
			expected: "metadata.name",
		},
		{
			name:     "fully qualified annotation key",
			path:     ".metadata.annotations['kubernetes.io/display-name']",
			expected: "metadata.annotations.kubernetes.io/display-name",
		},
		{
			name:     "fully qualified label key",
			path:     ".metadata.labels['app.kubernetes.io/name']",
			expected: "metadata.labels.app.kubernetes.io/name",
		},
		{
			name:     "spec field with bracket selector",
			path:     ".spec.selector['app']",
			expected: "spec.selector.app",
		},
		{
			name:     "data field with dotted key",
			path:     ".data['config.yaml']",
			expected: "data.config.yaml",
		},
		// Wildcard attribute names — callers filter "*" before Join.
		{
			name:     "wildcard single level — raw segments include star",
			path:     ".spec.ports[*].name",
			expected: "spec.ports.*.name", // raw join; controller filters "*" before this
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segments := ParsePath(tt.path)
			result := strings.Join(segments, ".")
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestWildcardMeilisearchAttribute mirrors the controller's path-to-attribute translation.
func TestWildcardMeilisearchAttribute(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "single wildcard",
			path:     ".spec.ports[*].name",
			expected: "spec.ports.name",
		},
		{
			name:     "double nested wildcard",
			path:     ".spec.containers[*].ports[*].name",
			expected: "spec.containers.ports.name",
		},
		{
			name:     "wildcard mixed with bracket label",
			path:     ".spec.ports[*]['name']",
			expected: "spec.ports.name",
		},
		{
			name:     "numeric index translation unchanged",
			path:     ".spec.ports[0].port",
			expected: "spec.ports.0.port",
		},
		{
			name:     "no wildcards unchanged",
			path:     ".metadata.name",
			expected: "metadata.name",
		},
	}

	filterAndJoin := func(path string) string {
		raw := ParsePath(path)
		filtered := make([]string, 0, len(raw))
		for _, seg := range raw {
			if seg != "*" {
				filtered = append(filtered, seg)
			}
		}
		return strings.Join(filtered, ".")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterAndJoin(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluate(t *testing.T) {
	// Setup CEL environment once
	env, err := internalcel.NewEnv()
	require.NoError(t, err)

	type testCase struct {
		name          string
		policy        *v1alpha1.ResourceIndexPolicy
		resource      *unstructured.Unstructured
		expectedMatch bool
		// expectedObject is only checked when expectedMatch is true. nil means
		// "don't check the object contents" (e.g. conditions-only tests).
		expectedObject map[string]any
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
			expectedMatch: false,
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
			expectedObject: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":   "my-config",
					"labels": map[string]any{"env": "prod"},
				},
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
			expectedMatch: false,
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
			expectedMatch: true,
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
			expectedMatch: true,
		},
		{
			name: "Full object stored on match (including non-policy fields)",
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
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]any{"name": "my-deploy", "namespace": "default"},
					"spec": map[string]any{
						"replicas": int64(3),
						"template": map[string]any{
							"spec": map[string]any{
								"containers": []any{
									map[string]any{"image": "nginx:latest"},
								},
							},
						},
					},
				},
			},
			expectedMatch: true,
			// No Fields declared; full source object is stored regardless of declared field surface.
			expectedObject: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "my-deploy", "namespace": "default"},
				"spec": map[string]any{
					"replicas": int64(3),
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"image": "nginx:latest"},
							},
						},
					},
				},
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
			expectedMatch: false,
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
			expectedMatch: false,
		},
		{
			name: "No conditions — unconditional match stores full object",
			policy: &v1alpha1.ResourceIndexPolicy{
				Spec: v1alpha1.ResourceIndexPolicySpec{
					TargetResource: v1alpha1.TargetResource{Group: "", Version: "v1", Kind: "Pod"},
					// No Conditions
				},
			},
			resource: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata":   map[string]any{"name": "my-pod"},
					"spec":       map[string]any{"nodeName": "node-1"},
				},
			},
			expectedMatch: true,
			expectedObject: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata":   map[string]any{"name": "my-pod"},
				"spec":       map[string]any{"nodeName": "node-1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cached := compilePolicy(tt.policy)
			result, err := cached.Evaluate(tt.resource)
			require.NoError(t, err) // Evaluate currently returns nil error always

			assert.Equal(t, tt.expectedMatch, result.Matched, "Matched status mismatch")

			if !tt.expectedMatch {
				assert.Nil(t, result.Object, "Object should be nil when not matched")
				return
			}

			// When matched: Object must be set
			assert.NotNil(t, result.Object, "Object must be set when matched")

			// When the test provides an expected object shape, verify it exactly
			if tt.expectedObject != nil {
				assert.Equal(t, tt.expectedObject, result.Object, "Object mismatch")
			}
		})
	}
}
