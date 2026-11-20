package ownership

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	TypeOrganization = "Organization"
	TypeProject      = "Project"
	TypeUser         = "User"

	Unscoped = "unscoped"

	OrganizationNamespacePrefix = "organization-"
	OrganizationNameLabel       = "resourcemanager.miloapis.com/organization-name"

	resourceManagerGroup = "resourcemanager.miloapis.com"
	iamGroup             = "iam.miloapis.com"
	notificationGroup    = "notification.miloapis.com"
)

type Owner struct {
	Type string
	Name string
}

func FromObject(obj *unstructured.Unstructured) (Owner, bool) {
	if obj == nil {
		return Owner{}, false
	}

	gvk := obj.GroupVersionKind()

	if owner, ok := selfOwner(gvk.Group, gvk.Kind, obj.GetName()); ok {
		return owner, true
	}

	if gvk.Group == "" && gvk.Kind == "Namespace" {
		if name, ok := strings.CutPrefix(obj.GetName(), OrganizationNamespacePrefix); ok && name != "" {
			return Owner{Type: TypeOrganization, Name: name}, true
		}
	}

	if name, ok := strings.CutPrefix(obj.GetNamespace(), OrganizationNamespacePrefix); ok && name != "" {
		return Owner{Type: TypeOrganization, Name: name}, true
	}

	if gvk.Group == notificationGroup && gvk.Kind == "Contact" {
		if owner, ok := contactSubject(obj); ok {
			return owner, true
		}
	}

	var found []Owner
	for _, ref := range obj.GetOwnerReferences() {
		owner, ok := fromOwnerReference(ref.APIVersion, ref.Kind, ref.Name)
		if !ok || contains(found, owner) {
			continue
		}
		found = append(found, owner)
	}
	switch len(found) {
	case 1:
		return found[0], true
	case 0:
	default:
		return Owner{}, false
	}

	if name := obj.GetLabels()[OrganizationNameLabel]; name != "" {
		return Owner{Type: TypeOrganization, Name: name}, true
	}

	return Owner{}, false
}

func selfOwner(group, kind, name string) (Owner, bool) {
	if name == "" {
		return Owner{}, false
	}
	switch {
	case group == resourceManagerGroup && kind == TypeOrganization:
		return Owner{Type: TypeOrganization, Name: name}, true
	case group == iamGroup && kind == TypeUser:
		return Owner{Type: TypeUser, Name: name}, true
	}
	return Owner{}, false
}

func contactSubject(obj *unstructured.Unstructured) (Owner, bool) {
	group, _, _ := unstructured.NestedString(obj.Object, "spec", "subjectRef", "apiGroup")
	kind, _, _ := unstructured.NestedString(obj.Object, "spec", "subjectRef", "kind")
	name, _, _ := unstructured.NestedString(obj.Object, "spec", "subjectRef", "name")
	if group != iamGroup || kind != TypeUser || name == "" {
		return Owner{}, false
	}
	return Owner{Type: TypeUser, Name: name}, true
}

func fromOwnerReference(apiVersion, kind, name string) (Owner, bool) {
	if name == "" {
		return Owner{}, false
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return Owner{}, false
	}
	switch {
	case gv.Group == resourceManagerGroup && kind == TypeOrganization:
		return Owner{Type: TypeOrganization, Name: name}, true
	case gv.Group == resourceManagerGroup && kind == TypeProject:
		return Owner{Type: TypeProject, Name: name}, true
	case gv.Group == iamGroup && kind == TypeUser:
		return Owner{Type: TypeUser, Name: name}, true
	}
	return Owner{}, false
}

func contains(owners []Owner, o Owner) bool {
	for _, existing := range owners {
		if existing == o {
			return true
		}
	}
	return false
}
