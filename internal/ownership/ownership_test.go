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

func typed(apiVersion, kind, name, namespace string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	return obj
}

func contact(namespace, subjectGroup, subjectKind, subjectName string) *unstructured.Unstructured {
	obj := typed("notification.miloapis.com/v1alpha1", "Contact", "c", namespace)
	obj.Object["spec"] = map[string]any{
		"subjectRef": map[string]any{"apiGroup": subjectGroup, "kind": subjectKind, "name": subjectName},
	}
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
			name:   "organization owns itself",
			obj:    typed("resourcemanager.miloapis.com/v1alpha1", "Organization", "acme", ""),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "user owns itself",
			obj:    typed("iam.miloapis.com/v1alpha1", "User", "alice", ""),
			want:   Owner{Type: TypeUser, Name: "alice"},
			wantOK: true,
		},
		{
			name:   "organization namespace object is owned by its organization",
			obj:    typed("v1", "Namespace", "organization-acme", ""),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "other namespace object has no owner",
			obj:    typed("v1", "Namespace", "milo-system", ""),
			wantOK: false,
		},
		{
			name:   "organization kind in another group does not own itself",
			obj:    typed("example.com/v1", "Organization", "acme", ""),
			wantOK: false,
		},
		{
			name:   "user-subject contact outside an organization namespace is unscoped",
			obj:    contact("milo-system", "iam.miloapis.com", "User", "alice"),
			wantOK: false,
		},
		{
			name:   "contact in an organization namespace is owned by the organization",
			obj:    contact("organization-acme", "iam.miloapis.com", "User", "alice"),
			want:   Owner{Type: TypeOrganization, Name: "acme"},
			wantOK: true,
		},
		{
			name:   "contact without a user subject has no owner",
			obj:    contact("milo-system", "resourcemanager.miloapis.com", "Project", "p-one"),
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
