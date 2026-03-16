package tenant

import (
	"context"
	"strings"

	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"go.miloapis.net/search/internal/indexer"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
)

// filterDeleteClient is the narrow interface of meilisearch.SDKClient that
// ProjectWatcher needs. Using an interface here keeps ProjectWatcher testable
// without a live Meilisearch instance.
type filterDeleteClient interface {
	DeleteDocumentsByFilter(indexUID string, filter string) error
}

// policyCacheReader is the narrow interface of indexer.PolicyCache that
// ProjectWatcher needs. Using an interface here keeps ProjectWatcher testable
// without wiring up a full controller-runtime cache.
type policyCacheReader interface {
	GetPolicies() []*policyevaluation.CachedPolicy
}

// BootstrapFunc is called per-policy when a new tenant engages.
// It is the responsibility of the caller (policy controller/watcher wire-up) to implement this.
// The function should list resources for the given tenant and publish them for re-indexing.
type BootstrapFunc func(ctx context.Context, policy *searchv1alpha1.ResourceIndexPolicy, tenant TenantInfo, client dynamic.Interface) error

// ProjectWatcher handles tenant lifecycle events from the MultiTenantRegistry.
// On engagement, it bootstraps resources from the new project into all existing policies.
// On disengagement, it deletes all documents for that tenant from each index.
type ProjectWatcher struct {
	policyCache   policyCacheReader
	searchSDK     filterDeleteClient
	bootstrapFunc BootstrapFunc
}

// NewProjectWatcher creates a ProjectWatcher.
// bootstrapFunc is called per-policy when a new tenant engages; it may be nil
// (in which case engagement is a no-op). bootstrapFunc is provided by the
// caller to decouple ProjectWatcher from the policy controller implementation.
func NewProjectWatcher(
	policyCache *indexer.PolicyCache,
	searchSDK *meilisearch.SDKClient,
	bootstrapFunc BootstrapFunc,
) *ProjectWatcher {
	return &ProjectWatcher{
		policyCache:   policyCache,
		searchSDK:     searchSDK,
		bootstrapFunc: bootstrapFunc,
	}
}

// OnTenantEngaged is the TenantEngagementCallback.
// It bootstraps all ready policies for the newly engaged project by iterating
// each policy in the cache and invoking bootstrapFunc. Best-effort: failures
// are logged but do not abort processing of other policies.
func (w *ProjectWatcher) OnTenantEngaged(ctx context.Context, tenant TenantInfo, client dynamic.Interface) {
	klog.Infof("ProjectWatcher: tenant %q (%s) engaged; bootstrapping policies", tenant.Name, tenant.Type)

	if w.bootstrapFunc == nil {
		klog.V(4).Infof("ProjectWatcher: no bootstrap func registered; skipping bootstrap for tenant %q", tenant.Name)
		return
	}

	policies := w.policyCache.GetPolicies()
	for _, cp := range policies {
		if cp.Policy.Status.IndexName == "" {
			// Policy not yet initialized; skip.
			continue
		}
		if err := w.bootstrapFunc(ctx, cp.Policy, tenant, client); err != nil {
			klog.Errorf("ProjectWatcher: bootstrap failed for policy %s, tenant %s: %v",
				cp.Policy.Name, tenant.Name, err)
			// Best-effort: continue with other policies.
		}
	}
}

// OnTenantDisengaged is the TenantDisengagementCallback.
// It removes all documents for the disengaged project from each index.
func (w *ProjectWatcher) OnTenantDisengaged(tenantName string) {
	klog.Infof("ProjectWatcher: tenant %q disengaged; cleaning up documents", tenantName)

	if strings.ContainsAny(tenantName, `"\`) {
		klog.Errorf("ProjectWatcher: refusing to delete documents for tenant name with special characters: %q", tenantName)
		return
	}

	filter := `_tenant = "` + tenantName + `"`

	policies := w.policyCache.GetPolicies()
	for _, cp := range policies {
		if cp.Policy.Status.IndexName == "" {
			continue
		}
		if err := w.searchSDK.DeleteDocumentsByFilter(cp.Policy.Status.IndexName, filter); err != nil {
			klog.Errorf("ProjectWatcher: failed to delete documents for tenant %s in index %s: %v",
				tenantName, cp.Policy.Status.IndexName, err)
			// Best-effort: continue with other indices.
		}
	}
}
