// +k8s:openapi-gen=true
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SearchQuery represents a generic search query resource.
//
// This is a base type that can be extended for specific search implementations.
//
// Quick Start:
//
//	apiVersion: search.miloapis.com/v1alpha1
//	kind: SearchQuery
//	metadata:
//	  name: example-search
//	spec:
//	  query: "your search query"
//	  limit: 100
type SearchQuery struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SearchQuerySpec   `json:"spec"`
	Status SearchQueryStatus `json:"status,omitempty"`
}

// SearchQuerySpec defines the search parameters.
//
// The actual fields will depend on the specific search implementation.
type SearchQuerySpec struct {
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

// SearchQueryStatus contains the query results and pagination state.
type SearchQueryStatus struct {
	// Results contains the search results.
	//
	// +optional
	// +listType=atomic
	Results []SearchResult `json:"results,omitempty"`

	// Continue is the pagination cursor.
	// Non-empty means more results are available - copy this to spec.continue for the next page.
	// Empty means you have all results.
	// +optional
	Continue string `json:"continue,omitempty"`
}

// SearchResult represents a single search result with its relevance score.
type SearchResult struct {
	// Resource contains the actual Kubernetes resource.
	Resource runtime.RawExtension `json:"resource"`

	// RelevanceScore is the relevance score from Meilisearch.
	// +optional
	RelevanceScore float64 `json:"relevanceScore,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SearchQueryList is a list of SearchQuery objects
type SearchQueryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SearchQuery `json:"items"`
}
