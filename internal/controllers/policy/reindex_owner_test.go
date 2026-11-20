package policy

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"go.miloapis.net/search/internal/tenant"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

type publishedOwner struct {
	tenant     string
	tenantType string
}

type recordingPublisher struct {
	owners map[string]publishedOwner
}

func (p *recordingPublisher) PublishResource(_ context.Context, resource map[string]any, _, _, _, _, tenantName, tenantType string) error {
	name := (&unstructured.Unstructured{Object: resource}).GetName()
	p.owners[name] = publishedOwner{tenant: tenantName, tenantType: tenantType}
	return nil
}

var widgetGVK = schema.GroupVersionKind{Group: "example.miloapis.com", Version: "v1", Kind: "Widget"}
var widgetGVR = schema.GroupVersionResource{Group: "example.miloapis.com", Version: "v1", Resource: "widgets"}

func widget(name, namespace string, refs ...metav1.OwnerReference) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(widgetGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID("uid-" + name))
	obj.SetOwnerReferences(refs)
	return obj
}

func reindexOwners(t *testing.T, multiTenant bool, tenantInfo tenant.TenantInfo) map[string]publishedOwner {
	t.Helper()

	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(widgetGVK, meta.RESTScopeNamespace)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{widgetGVR: "WidgetList"},
		widget("org-widget", "organization-acme"),
		widget("project-widget", "milo-system", metav1.OwnerReference{
			APIVersion: "resourcemanager.miloapis.com/v1alpha1", Kind: "Project", Name: "p-one", UID: "p1",
		}),
		widget("user-widget", "milo-system", metav1.OwnerReference{
			APIVersion: "iam.miloapis.com/v1alpha1", Kind: "User", Name: "alice", UID: "u1",
		}),
		widget("unowned-widget", "milo-system"),
	)

	pub := &recordingPublisher{owners: map[string]publishedOwner{}}
	r := &ResourceIndexPolicyReconciler{
		RESTMapper:         mapper,
		ReindexPublisher:   pub,
		EnableMultiTenancy: multiTenant,
	}
	policy := &searchv1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets"},
		Spec: searchv1alpha1.ResourceIndexPolicySpec{
			TargetResource: searchv1alpha1.TargetResource{Group: widgetGVK.Group, Version: widgetGVK.Version, Kind: widgetGVK.Kind},
		},
		Status: searchv1alpha1.ResourceIndexPolicyStatus{IndexName: "example-miloapis-com_v1_Widget"},
	}

	n, err := r.publishReindexMessagesForTenant(context.Background(), policy, tenantInfo, client)
	if err != nil {
		t.Fatalf("publishReindexMessagesForTenant: %v", err)
	}
	if n != 4 || len(pub.owners) != 4 {
		t.Fatalf("published %d messages for %d objects, want 4", n, len(pub.owners))
	}
	return pub.owners
}

func TestReindex_PlatformTenant_StoresEachObjectUnderItsOwner(t *testing.T) {
	got := reindexOwners(t, true, tenant.PlatformTenantInfo)

	want := map[string]publishedOwner{
		"org-widget":     {tenant: "acme", tenantType: "Organization"},
		"project-widget": {tenant: "p-one", tenantType: "Project"},
		"user-widget":    {tenant: "alice", tenantType: "User"},
		"unowned-widget": {tenant: "unscoped", tenantType: "unscoped"},
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: published owner %+v, want %+v", name, got[name], w)
		}
	}
	for name, o := range got {
		if o.tenantType == tenant.TenantTypePlatform {
			t.Errorf("%s: stored as platform-owned", name)
		}
	}
}

func TestReindex_ProjectTenant_KeepsProjectOwner(t *testing.T) {
	got := reindexOwners(t, true, tenant.TenantInfo{Name: "p-two", Type: tenant.TenantTypeProject})

	for name, o := range got {
		if o != (publishedOwner{tenant: "p-two", tenantType: "Project"}) {
			t.Errorf("%s: published owner %+v, want Project/p-two", name, o)
		}
	}
}

func TestReindex_SingleTenant_KeepsPlatformOwner(t *testing.T) {
	got := reindexOwners(t, false, tenant.PlatformTenantInfo)

	for name, o := range got {
		if o != (publishedOwner{tenant: "platform", tenantType: "platform"}) {
			t.Errorf("%s: published owner %+v, want platform/platform", name, o)
		}
	}
}
