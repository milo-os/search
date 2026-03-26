package tenant

import (
	"context"
	"strings"

	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"go.miloapis.net/search/internal/indexer"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
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

// ProjectWatcher handles tenant lifecycle events from the MultiTenantRegistry.
// On disengagement, it deletes all documents for that tenant from each index.
type ProjectWatcher struct {
	policyCache policyCacheReader
	searchSDK   filterDeleteClient
}

// NewProjectWatcher creates a ProjectWatcher.
func NewProjectWatcher(
	policyCache *indexer.PolicyCache,
	searchSDK *meilisearch.SDKClient,
) *ProjectWatcher {
	return &ProjectWatcher{
		policyCache: policyCache,
		searchSDK:   searchSDK,
	}
}

// OnTenantEngaged is the TenantEngagementCallback.
func (w *ProjectWatcher) OnTenantEngaged(_ context.Context, tenant TenantInfo, _ dynamic.Interface) {
	klog.Infof("ProjectWatcher: tenant %q (%s) engaged", tenant.Name, tenant.Type)
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
