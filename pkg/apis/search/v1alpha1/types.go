// +k8s:openapi-gen=true
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceSearchQuery represents a generic search query resource.
//
// This is a base type that can be extended for specific search implementations.
//
// Quick Start:
//
//	apiVersion: search.miloapis.com/v1alpha1
//	kind: ResourceSearchQuery
//	metadata:
//	  name: example-search
//	spec:
//	  query: "your search query"
//	  limit: 100
type ResourceSearchQuery struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ResourceSearchQuerySpec   `json:"spec"`
	Status ResourceSearchQueryStatus `json:"status,omitempty"`
}

// ResourceSearchQuerySpec defines the search parameters.
//
// The actual fields will depend on the specific search implementation.
type ResourceSearchQuerySpec struct {
	// TargetResources limits the search to specific resource types.
	// +optional
	// +listType=atomic
	TargetResources []TargetResource `json:"targetResources,omitempty"`

	// Query is the search query string.
	//
	// +required
	Query string `json:"query"`

	// Limit sets the maximum number of results per page.
	// Default: 10, Maximum: 100.
	//
	// +optional
	Limit int32 `json:"limit,omitempty"`

	// Continue is the pagination cursor for fetching additional pages.
	//
	// Leave empty for the first page. If status.continue is non-empty after a query,
	// copy that value here in a new query with identical parameters to get the next page.
	//
	// +optional
	Continue string `json:"continue,omitempty"`
}

// ResourceSearchQueryStatus contains the query results and pagination state.
type ResourceSearchQueryStatus struct {
	// Results contains the search results.
	//
	// +optional
	// +listType=atomic
	Results []SearchResult `json:"results,omitempty"`

	// DeniedTargetResources lists target resources that were requested but
	// could not be searched because no ResourceIndexPolicy is registered for
	// them in this cluster. The query succeeded for the remaining targets;
	// callers can render a partial-permission notice when this list is non-empty.
	// +optional
	// +listType=atomic
	DeniedTargetResources []TargetResource `json:"deniedTargetResources,omitempty"`

	// Continue is the pagination cursor.
	// Non-empty means more results are available - copy this to spec.continue for the next page.
	// Empty means you have all results.
	// +optional
	Continue string `json:"continue,omitempty"`
}

// TenantInfo identifies the tenant from which a search result originates.
type TenantInfo struct {
	// Name is the tenant name. "platform" for the platform tenant,
	// or the project name for project-scoped resources.
	// +optional
	Name string `json:"name,omitempty"`
	// Type is the tenant type. One of "platform" or "project".
	// +optional
	Type string `json:"type,omitempty"`
}

// SearchResult represents a single search result with its relevance score.
type SearchResult struct {
	// Resource contains the actual Kubernetes resource.
	Resource unstructured.Unstructured `json:"resource"`

	// RelevanceScore is the relevance score from Meilisearch.
	// +optional
	RelevanceScore float64 `json:"relevanceScore,omitempty"`

	// Tenant identifies the tenant from which this result originates.
	// +optional
	Tenant TenantInfo `json:"tenant,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceSearchQueryList is a list of ResourceSearchQuery objects
type ResourceSearchQueryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ResourceSearchQuery `json:"items"`
}
