package indexer

import (
	"testing"

	"go.miloapis.net/search/internal/ownership"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func tenantTestObject(namespace string, refs ...metav1.OwnerReference) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetName("obj")
	obj.SetNamespace(namespace)
	obj.SetOwnerReferences(refs)
	return obj
}

func scopeAnnotations(scopeType, scopeName string) map[string]string {
	a := map[string]string{ScopeTypeAnnotationKey: scopeType}
	if scopeName != "" {
		a[ScopeNameAnnotationKey] = scopeName
	}
	return a
}

func TestTenantForEvent(t *testing.T) {
	userRef := metav1.OwnerReference{APIVersion: "iam.miloapis.com/v1alpha1", Kind: "User", Name: "alice", UID: "u1"}

	tests := []struct {
		name        string
		annotations map[string]string
		obj         *unstructured.Unstructured
		multiTenant bool
		wantName    string
		wantType    string
	}{
		{
			name:        "no scope annotation, organization namespace",
			obj:         tenantTestObject("organization-acme"),
			multiTenant: true,
			wantName:    "acme",
			wantType:    "Organization",
		},
		{
			name:        "no scope annotation, user owner reference",
			obj:         tenantTestObject("", userRef),
			multiTenant: true,
			wantName:    "alice",
			wantType:    "User",
		},
		{
			name:        "no scope annotation, no ownership marker is unscoped",
			obj:         tenantTestObject("default"),
			multiTenant: true,
			wantName:    ownership.Unscoped,
			wantType:    ownership.Unscoped,
		},
		{
			name:        "global write to an organization's object keeps its owner",
			annotations: scopeAnnotations("global", ""),
			obj:         tenantTestObject("organization-acme"),
			multiTenant: true,
			wantName:    "acme",
			wantType:    "Organization",
		},
		{
			name:        "global write to an unowned object stays at the root",
			annotations: scopeAnnotations("global", ""),
			obj:         tenantTestObject("default"),
			multiTenant: true,
			wantName:    "platform",
			wantType:    "Global",
		},
		{
			name:        "project scope is taken from the request",
			annotations: scopeAnnotations("Project", "p-one"),
			obj:         tenantTestObject("organization-acme"),
			multiTenant: true,
			wantName:    "p-one",
			wantType:    "Project",
		},
		{
			name:        "user scope is taken from the request",
			annotations: scopeAnnotations("User", "bob"),
			obj:         tenantTestObject("default", userRef),
			multiTenant: true,
			wantName:    "bob",
			wantType:    "User",
		},
		{
			name:        "single-tenant mode keeps the platform fallback",
			obj:         tenantTestObject("organization-acme"),
			multiTenant: false,
			wantName:    "platform",
			wantType:    "platform",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &auditEvent{Annotations: tt.annotations}
			name, typ := tenantForEvent(event, tt.obj, tt.multiTenant)
			if name != tt.wantName || typ != tt.wantType {
				t.Errorf("tenantForEvent() = %q/%q; want %q/%q", typ, name, tt.wantType, tt.wantName)
			}
		})
	}
}
