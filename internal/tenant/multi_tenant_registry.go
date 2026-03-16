package tenant

import (
	"context"
	"fmt"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var projectGVR = schema.GroupVersionResource{
	Group:    "resourcemanager.miloapis.com",
	Version:  "v1alpha1",
	Resource: "projects",
}

// MultiTenantRegistry watches Project resources and maintains per-tenant dynamic clients.
// It replicates Milo's projectprovider pattern without importing the multi-cluster runtime.
type MultiTenantRegistry struct {
	mu sync.RWMutex

	baseConfig *rest.Config

	// platformClient is the dynamic client for the local (platform) cluster.
	platformClient dynamic.Interface

	// projectClients holds per-project clients, keyed by project name.
	projectClients map[string]dynamic.Interface

	// labelSelector optionally filters which projects are indexed (empty = all).
	labelSelector string

	// Lifecycle callbacks.
	onEngage    TenantEngagementCallback
	onDisengage TenantDisengagementCallback
}

// NewMultiTenantRegistry creates a MultiTenantRegistry.
// baseConfig is the REST config for the platform cluster. It is used both to
// build the Project informer client and as the base for per-project proxy configs.
// platformClient is the dynamic client for the platform cluster (returned for
// GetTenantClient("platform")); it is NOT used for the informer.
// labelSelector optionally filters which projects are indexed (empty = all).
// onEngage is called when a new project becomes available (may be nil).
// onDisengage is called when a project is removed (may be nil).
func NewMultiTenantRegistry(
	baseConfig *rest.Config,
	platformClient dynamic.Interface,
	labelSelector string,
	onEngage TenantEngagementCallback,
	onDisengage TenantDisengagementCallback,
) *MultiTenantRegistry {
	return &MultiTenantRegistry{
		baseConfig:     baseConfig,
		platformClient: platformClient,
		projectClients: make(map[string]dynamic.Interface),
		labelSelector:  labelSelector,
		onEngage:       onEngage,
		onDisengage:    onDisengage,
	}
}

// projectRestConfig builds a REST config that proxies requests to the named
// project control plane. Pattern is identical to Milo's project controller
// forProject() method.
func (r *MultiTenantRegistry) projectRestConfig(projectName string) *rest.Config {
	c := rest.CopyConfig(r.baseConfig)
	c.Host = strings.TrimSuffix(r.baseConfig.Host, "/") + "/projects/" + projectName + "/control-plane"
	return c
}

// Run starts the Project informer and blocks until ctx is cancelled.
// This must be called in a goroutine before ListTenants/GetTenantClient are
// expected to reflect project tenants.
func (r *MultiTenantRegistry) Run(ctx context.Context) error {
	// Create a separate dynamic client for the informer. This is distinct from
	// platformClient (which is for callers of GetTenantClient("platform")).
	dynClient, err := dynamic.NewForConfig(r.baseConfig)
	if err != nil {
		return fmt.Errorf("multi-tenant registry: failed to create dynamic client: %w", err)
	}

	lw := &toolscache.ListWatch{
		ListFunc: func(lo metav1.ListOptions) (runtime.Object, error) {
			lo.LabelSelector = r.labelSelector
			return dynClient.Resource(projectGVR).List(ctx, lo)
		},
		WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
			lo.LabelSelector = r.labelSelector
			return dynClient.Resource(projectGVR).Watch(ctx, lo)
		},
	}

	inf := toolscache.NewSharedIndexInformer(lw, &unstructured.Unstructured{}, 0, nil)

	if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			project, ok := obj.(*unstructured.Unstructured)
			if !ok {
				klog.Errorf("MultiTenantRegistry AddFunc: unexpected object type %T", obj)
				return
			}
			r.addProject(ctx, project.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			// obj may be a tombstone when the informer misses the delete event.
			if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			project, ok := obj.(*unstructured.Unstructured)
			if !ok {
				klog.Errorf("MultiTenantRegistry DeleteFunc: unexpected object type %T", obj)
				return
			}
			r.removeProject(project.GetName())
		},
	}); err != nil {
		return fmt.Errorf("multi-tenant registry: failed to add event handler: %w", err)
	}

	go inf.Run(ctx.Done())

	if !toolscache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return fmt.Errorf("MultiTenantRegistry: timed out waiting for informer cache sync")
	}

	<-ctx.Done()
	return nil
}

// addProject creates a project-scoped dynamic client, stores it in the map,
// and fires the onEngage callback in a goroutine so the informer is not blocked.
func (r *MultiTenantRegistry) addProject(ctx context.Context, name string) {
	cfg := r.projectRestConfig(name)
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		klog.Errorf("MultiTenantRegistry: failed to create client for project %q: %v", name, err)
		return
	}

	r.mu.Lock()
	r.projectClients[name] = dc
	r.mu.Unlock()

	klog.Infof("MultiTenantRegistry: project %q engaged", name)

	if r.onEngage != nil {
		go r.onEngage(ctx, TenantInfo{Name: name, Type: TenantTypeProject}, dc)
	}
}

// removeProject removes the project client from the map and fires the
// onDisengage callback synchronously (callers must not block).
func (r *MultiTenantRegistry) removeProject(name string) {
	r.mu.Lock()
	delete(r.projectClients, name)
	r.mu.Unlock()

	klog.Infof("MultiTenantRegistry: project %q disengaged", name)

	if r.onDisengage != nil {
		r.onDisengage(name)
	}
}

// ListTenants returns the platform tenant followed by all currently active
// project tenants. Safe for concurrent use.
func (r *MultiTenantRegistry) ListTenants() []TenantInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tenants := make([]TenantInfo, 0, len(r.projectClients)+1)
	tenants = append(tenants, PlatformTenantInfo)
	for name := range r.projectClients {
		tenants = append(tenants, TenantInfo{Name: name, Type: TenantTypeProject})
	}
	return tenants
}

// GetTenantClient returns the dynamic client for the given tenant.
// Returns nil if the tenant is not found.
func (r *MultiTenantRegistry) GetTenantClient(tenantName string) dynamic.Interface {
	if tenantName == "platform" {
		return r.platformClient
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.projectClients[tenantName]
}
