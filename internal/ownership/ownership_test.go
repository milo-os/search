package ownership

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func object(namespace string, labels map[string]string, refs ...metav1.OwnerReference) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetName("obj")
	obj.SetNamespace(namespace)
	obj.SetLabels(labels)
	obj.SetOwnerReferences(refs)
	return obj
}

func ref(apiVersion, kind, name string) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: types.UID("uid-" + name)}
}

func TestFromObject(t *testing.T) {
	tests := []struct {
		name   string
		obj    *unstructured.Unstructured
		want   Owner
		wantOK bool
	}{
		{
			name:   "organization namespace",
			obj:    object("organization-acme", nil),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "organization namespace wins over owner reference",
			obj:    object("organization-acme", nil, ref("iam.miloapis.com/v1alpha1", "User", "alice")),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "bare organization prefix is not an owner",
			obj:    object("organization-", nil),
			wantOK: false,
		},
		{
			name:   "organization owner reference",
			obj:    object("", nil, ref("resourcemanager.miloapis.com/v1alpha1", "Organization", "acme")),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "project owner reference",
			obj:    object("milo-system", nil, ref("resourcemanager.miloapis.com/v1alpha1", "Project", "p-one")),
			want:   Owner{Type: TypeProject, Name: "p-one"},
			wantOK: true,
		},
		{
			name:   "user owner reference",
			obj:    object("", nil, ref("iam.miloapis.com/v1alpha1", "User", "alice")),
			want:   Owner{Type: TypeUser, Name: "alice"},
			wantOK: true,
		},
		{
			name: "unrelated owner references are ignored",
			obj: object("", nil,
				ref("apps/v1", "ReplicaSet", "rs"),
				ref("iam.miloapis.com/v1alpha1", "User", "alice")),
			want:   Owner{Type: TypeUser, Name: "alice"},
			wantOK: true,
		},
		{
			name:   "same-named kind in another group is ignored",
			obj:    object("", nil, ref("example.com/v1", "User", "alice")),
			wantOK: false,
		},
		{
			name: "conflicting owner references are undetermined",
			obj: object("", nil,
				ref("iam.miloapis.com/v1alpha1", "User", "alice"),
				ref("resourcemanager.miloapis.com/v1alpha1", "Organization", "acme")),
			wantOK: false,
		},
		{
			name: "conflicting owner references do not fall back to the label",
			obj: object("", map[string]string{OrganizationNameLabel: "acme"},
				ref("iam.miloapis.com/v1alpha1", "User", "alice"),
				ref("iam.miloapis.com/v1alpha1", "User", "bob")),
			wantOK: false,
		},
		{
			name:   "organization name label",
			obj:    object("", map[string]string{OrganizationNameLabel: "acme"}),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "no ownership marker",
			obj:    object("default", map[string]string{"app": "x"}),
			wantOK: false,
		},
		{
			name:   "nil object",
			obj:    nil,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromObject(tt.obj)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("FromObject() = %+v, %v; want %+v, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
