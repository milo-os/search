package evaluation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvalResult_Transform(t *testing.T) {
	tests := []struct {
		name     string
		result   *EvalResult
		expected map[string]any
	}{
		{
			name: "full object is included in output",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "my-config",
						"namespace": "default",
					},
					// data is a non-policy field — must still appear in the doc
					"data": map[string]any{
						"key": "value",
					},
				},
				Group:   "",
				Version: "v1",
				Kind:    "ConfigMap",
			},
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      "my-config",
					"namespace": "default",
				},
				"data":         map[string]any{"key": "value"},
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "managedFields is stripped from metadata",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]any{
						"name": "my-deploy",
						"managedFields": []any{
							map[string]any{"manager": "kubectl", "operation": "Apply"},
						},
					},
					"spec": map[string]any{"replicas": int64(1)},
				},
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			},
			expected: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "my-deploy"},
				"spec":       map[string]any{"replicas": int64(1)},
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "apiVersion overlay: group + version",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "old/v0", // will be overwritten
					"kind":       "OldKind",
				},
				Group:   "search.miloapis.com",
				Version: "v1alpha1",
				Kind:    "ResourceIndexPolicy",
			},
			expected: map[string]any{
				"apiVersion":   "search.miloapis.com/v1alpha1",
				"kind":         "ResourceIndexPolicy",
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "apiVersion overlay: core group (empty group)",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "old/v0",
					"kind":       "OldKind",
				},
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			},
			expected: map[string]any{
				"apiVersion":   "v1",
				"kind":         "Pod",
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "tenant and tenant_type defaults to platform when empty",
			result: &EvalResult{
				Matched: true,
				Object:  map[string]any{"apiVersion": "v1", "kind": "Service"},
				Group:   "",
				Version: "v1",
				Kind:    "Service",
				// Tenant and TenantType intentionally empty
			},
			expected: map[string]any{
				"apiVersion":   "v1",
				"kind":         "Service",
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "tenant and tenant_type are set when provided",
			result: &EvalResult{
				Matched:    true,
				Object:     map[string]any{"apiVersion": "v1", "kind": "ConfigMap"},
				Group:      "",
				Version:    "v1",
				Kind:       "ConfigMap",
				Tenant:     "my-project",
				TenantType: "Project",
			},
			expected: map[string]any{
				"apiVersion":   "v1",
				"kind":         "ConfigMap",
				"_tenant":      "my-project",
				"_tenant_type": "Project",
			},
		},
		{
			name: "non-policy fields are present alongside policy-covered fields",
			result: &EvalResult{
				Matched: true,
				// The policy only defines Fields for .metadata.name, but the full
				// object is stored — all keys must appear in the output.
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "cfg",
						"namespace": "ns",
						"labels":    map[string]any{"app": "search"},
					},
					"data": map[string]any{
						"extra-key": "extra-value",
					},
				},
				Group:   "",
				Version: "v1",
				Kind:    "ConfigMap",
			},
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      "cfg",
					"namespace": "ns",
					"labels":    map[string]any{"app": "search"},
				},
				"data":         map[string]any{"extra-key": "extra-value"},
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "nil Object produces minimal document with overlays only",
			result: &EvalResult{
				Matched: true,
				Object:  nil,
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			},
			expected: map[string]any{
				"apiVersion":   "apps/v1",
				"kind":         "Deployment",
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "Secret data and stringData are stripped; other fields preserved",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]any{
						"name":   "my-secret",
						"labels": map[string]any{"app": "auth"},
					},
					"type": "kubernetes.io/tls",
					"data": map[string]any{
						"tls.crt": "REDACTED",
						"tls.key": "REDACTED",
					},
					"stringData": map[string]any{
						"password": "hunter2",
					},
				},
				Group:   "",
				Version: "v1",
				Kind:    "Secret",
			},
			expected: map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata": map[string]any{
					"name":   "my-secret",
					"labels": map[string]any{"app": "auth"},
				},
				"type":         "kubernetes.io/tls",
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
		{
			name: "last-applied-configuration annotation is stripped; other annotations preserved",
			result: &EvalResult{
				Matched: true,
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]any{
						"name": "my-deploy",
						"annotations": map[string]any{
							"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
							"custom.io/owner": "team-a",
						},
					},
				},
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			},
			expected: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"name": "my-deploy",
					"annotations": map[string]any{
						"custom.io/owner": "team-a",
					},
				},
				"_tenant":      "platform",
				"_tenant_type": "platform",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := tt.result.Transform()
			assert.Equal(t, tt.expected, doc)
		})
	}
}

// TestEvalResult_Transform_SourceNotMutated verifies that calling Transform()
// does not modify the caller's original u.Object map.
func TestEvalResult_Transform_SourceNotMutated(t *testing.T) {
	sourceObject := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": "my-pod",
			"managedFields": []any{
				map[string]any{"manager": "kubectl"},
			},
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}

	// Snapshot what we expect the source to look like (unchanged) after Transform.
	snapshotMeta := map[string]any{
		"name": "my-pod",
		"managedFields": []any{
			map[string]any{"manager": "kubectl"},
		},
	}

	r := &EvalResult{
		Matched: true,
		Object:  sourceObject,
		Group:   "",
		Version: "v1",
		Kind:    "Pod",
	}

	doc := r.Transform()

	// The doc must NOT have managedFields.
	require.IsType(t, map[string]any{}, doc["metadata"])
	docMeta := doc["metadata"].(map[string]any)
	assert.NotContains(t, docMeta, "managedFields", "doc should have managedFields stripped")

	// The source object's metadata must still have managedFields intact.
	sourceMeta, ok := sourceObject["metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, snapshotMeta, sourceMeta, "source metadata must be unchanged after Transform()")
}

// TestEvalResult_Transform_SecretSourceNotMutated verifies that calling Transform()
// on a Secret does not remove data/stringData from the caller's original object.
func TestEvalResult_Transform_SecretSourceNotMutated(t *testing.T) {
	secretData := map[string]any{"password": "s3cr3t"}
	secretStringData := map[string]any{"token": "abc123"}
	sourceObject := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name": "my-secret",
		},
		"type":       "Opaque",
		"data":       secretData,
		"stringData": secretStringData,
	}

	r := &EvalResult{
		Matched: true,
		Object:  sourceObject,
		Group:   "",
		Version: "v1",
		Kind:    "Secret",
	}

	doc := r.Transform()

	// The doc must NOT contain data or stringData.
	assert.NotContains(t, doc, "data", "doc should have Secret data stripped")
	assert.NotContains(t, doc, "stringData", "doc should have Secret stringData stripped")

	// The source object must still have data and stringData intact.
	assert.Equal(t, secretData, sourceObject["data"], "source Secret data must be unchanged after Transform()")
	assert.Equal(t, secretStringData, sourceObject["stringData"], "source Secret stringData must be unchanged after Transform()")
}
