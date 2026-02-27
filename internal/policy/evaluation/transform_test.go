package evaluation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvalResult_Transform(t *testing.T) {
	tests := []struct {
		name     string
		fields   map[string]any
		group    string
		version  string
		kind     string
		expected map[string]any
	}{
		{
			name:    "empty fields",
			fields:  map[string]any{},
			group:   "search.miloapis.com",
			version: "v1alpha1",
			kind:    "ResourceIndexPolicy",
			expected: map[string]any{
				"apiVersion": "search.miloapis.com/v1alpha1",
				"kind":       "ResourceIndexPolicy",
			},
		},
		{
			name:    "single field",
			fields:  map[string]any{".metadata.name": "test-cm"},
			group:   "",
			version: "v1",
			kind:    "ConfigMap",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "test-cm"},
			},
		},
		{
			name: "multiple fields",
			fields: map[string]any{
				".metadata.name":                 "test-cm",
				".spec.replicas":                 int64(3),
				".spec.selector.matchLabels.app": "foo",
			},
			group:   "apps",
			version: "v1",
			kind:    "Deployment",
			expected: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "test-cm"},
				"spec": map[string]any{
					"replicas": int64(3),
					"selector": map[string]any{
						"matchLabels": map[string]any{"app": "foo"},
					},
				},
			},
		},
		{
			name:    "nested brackets",
			fields:  map[string]any{".data['config.yaml']": "content"},
			group:   "",
			version: "v1",
			kind:    "ConfigMap",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"data":       map[string]any{"config.yaml": "content"},
			},
		},
		{
			name: "mixed paths sharing prefix (merging)",
			fields: map[string]any{
				".metadata.name":      "test",
				".metadata.namespace": "default",
			},
			group:   "",
			version: "v1",
			kind:    "Pod",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"name":      "test",
					"namespace": "default",
				},
			},
		},
		{
			name: "mixed dot and bracket notation merging",
			fields: map[string]any{
				".spec.selector['app']":    "backend",
				".spec.selector.component": "core",
			},
			group:   "apps",
			version: "v1",
			kind:    "Deployment",
			expected: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"spec": map[string]any{
					"selector": map[string]any{
						"app":       "backend",
						"component": "core",
					},
				},
			},
		},
		{
			name: "array indices as map keys (multiple items)",
			fields: map[string]any{
				".spec.ports[0].port":       int64(80),
				".spec.ports[0].targetPort": int64(8080),
				".spec.ports[0].name":       "http",
				".spec.ports[1].port":       int64(443),
				".spec.ports[1].targetPort": int64(8443),
				".spec.ports[1].name":       "https",
			},
			group:   "apps",
			version: "v1",
			kind:    "StatefulSet",
			expected: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "StatefulSet",
				"spec": map[string]any{
					"ports": map[string]any{
						"0": map[string]any{
							"port":       int64(80),
							"targetPort": int64(8080),
							"name":       "http",
						},
						"1": map[string]any{
							"port":       int64(443),
							"targetPort": int64(8443),
							"name":       "https",
						},
					},
				},
			},
		},
		{
			name: "deeply nested mix",
			fields: map[string]any{
				".status.conditions[0].type":        "Ready",
				".status.conditions[0].status":      "True",
				".status.containerStatuses[0].name": "main",
				".status.hostIP":                    "10.0.0.1",
			},
			group:   "",
			version: "v1",
			kind:    "Pod",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"status": map[string]any{
					"conditions": map[string]any{
						"0": map[string]any{
							"type":   "Ready",
							"status": "True",
						},
					},
					"containerStatuses": map[string]any{
						"0": map[string]any{
							"name": "main",
						},
					},
					"hostIP": "10.0.0.1",
				},
			},
		},
		{
			name: "deep nesting level 5",
			fields: map[string]any{
				".a.b.c.d.e": "deep",
				".a.b.c.f":   "shallow",
			},
			group:   "example.com",
			version: "v1",
			kind:    "CustomResource",
			expected: map[string]any{
				"apiVersion": "example.com/v1",
				"kind":       "CustomResource",
				"a": map[string]any{
					"b": map[string]any{
						"c": map[string]any{
							"d": map[string]any{
								"e": "deep",
							},
							"f": "shallow",
						},
					},
				},
			},
		},
		{
			name: "special characters in keys",
			fields: map[string]any{
				".metadata.annotations['example.com/managed-by']": "controller",
				".metadata.labels['app.kubernetes.io/name']":      "myapp",
				".data['config.json']":                            "{}",
			},
			group:   "",
			version: "v1",
			kind:    "ConfigMap",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"annotations": map[string]any{
						"example.com/managed-by": "controller",
					},
					"labels": map[string]any{
						"app.kubernetes.io/name": "myapp",
					},
				},
				"data": map[string]any{
					"config.json": "{}",
				},
			},
		},
		{
			name: "root level keys without dot",
			fields: map[string]any{
				"kind":       "Service",
				"apiVersion": "v1",
			},
			group:   "",
			version: "v2", // Policy version should overwrite the one from fields
			kind:    "ServiceOverride",
			expected: map[string]any{
				"kind":       "ServiceOverride",
				"apiVersion": "v2",
			},
		},
		{
			name: "multiple bracket segments",
			fields: map[string]any{
				".spec['selector']['app']": "foo",
				".data['key']['subkey']":   "bar",
			},
			group:   "",
			version: "v1",
			kind:    "ConfigMap",
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"spec": map[string]any{
					"selector": map[string]any{
						"app": "foo",
					},
				},
				"data": map[string]any{
					"key": map[string]any{
						"subkey": "bar",
					},
				},
			},
		},
		{
			name: "conflict: scalar vs map (last write wins structurally)",
			fields: map[string]any{
				".a":   "scalar-value", // This sets "a" = "scalar-value"
				".a.b": "nested-value", // This requires "a" to be a map. Logic should overwrite "a" with map {"b": "nested-value"}
			},
			group:    "x",
			version:  "v1",
			kind:     "Y",
			expected: nil, // skip
		},
	}

	for _, tt := range tests {
		if tt.expected == nil {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			r := &EvalResult{
				Matched: true,
				Fields:  tt.fields,
				Group:   tt.group,
				Version: tt.version,
				Kind:    tt.kind,
			}
			doc := r.Transform()

			// Use assert.Equal which compares map structure values deeply
			assert.Equal(t, tt.expected, doc)

			// Debug output for visual verification
			b, _ := json.MarshalIndent(doc, "", "  ")
			t.Logf("Result: %s", string(b))
		})
	}
}
