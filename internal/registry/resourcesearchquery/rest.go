package resourcesearchquery

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/golang-jwt/jwt/v5"
	meiliapi "github.com/meilisearch/meilisearch-go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	"go.miloapis.net/search/internal/indexer"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
)

// multiSearcher is the subset of meilisearch.SDKClient that the REST handler
// uses. Declaring it here lets tests substitute a fake without touching the
// SDK client itself.
type multiSearcher interface {
	MultiSearch(indexUIDs []string, query string, limit, offset int64, filter string) (*meiliapi.MultiSearchResponse, error)
}

// policyLister is the subset of indexer.PolicyCache that the REST handler
// uses. Declaring it here lets tests substitute a fake without a real
// controller-runtime cache.
type policyLister interface {
	GetPolicies() []*policyevaluation.CachedPolicy
}

// REST implements a RESTStorage for ResourceSearchQuery API.
type REST struct {
	meiliClient        multiSearcher
	policyCache        policyLister
	maxSearchLimit     int
	defaultSearchLimit int
	secretKey          []byte
	pagingTimeout      time.Duration
}

// Ensure REST implements required interfaces
var _ rest.Storage = &REST{}
var _ rest.Creater = &REST{}
var _ rest.Scoper = &REST{}
var _ rest.SingularNameProvider = &REST{}

// NewREST returns a RESTStorage object that will work against ResourceSearchQuery.
func NewREST(
	meiliClient *meilisearch.SDKClient,
	policyCache *indexer.PolicyCache,
	maxSearchLimit int,
	defaultSearchLimit int,
	pagingSecret []byte,
	pagingTimeout time.Duration,
) *REST {
	return &REST{
		meiliClient:        meiliClient,
		policyCache:        policyCache,
		maxSearchLimit:     maxSearchLimit,
		defaultSearchLimit: defaultSearchLimit,
		secretKey:          pagingSecret,
		pagingTimeout:      pagingTimeout,
	}
}

// New returns an empty object that can be used with Create.
func (r *REST) New() runtime.Object {
	return &searchv1alpha1.ResourceSearchQuery{}
}

// Destroy cleans up its resources on shutdown.
func (r *REST) Destroy() {}

// NamespaceScoped returns true if the storage is namespaced
func (r *REST) NamespaceScoped() bool {
	return false
}

// GetSingularName implements the rest.SingularNameProvider interface
func (r *REST) GetSingularName() string {
	return "resourcesearchquery"
}

// Create creates a new version of a resource.
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	query, ok := obj.(*searchv1alpha1.ResourceSearchQuery)
	if !ok {
		return nil, fmt.Errorf("not a ResourceSearchQuery: %#v", obj)
	}

	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	limit, offset, err := r.validateAndGetPagination(query)
	if err != nil {
		return nil, err
	}

	// Identity
	userInfo, _ := request.UserFrom(ctx)
	if userInfo == nil {
		return nil, apierrors.NewUnauthorized("missing user in request context")
	}

	// Classify targets against registered policies (no SAR — activity pattern).
	policies := r.policyCache.GetPolicies()
	allowed, denied := classifyTargets(query.Spec.TargetResources, policies)
	indexUIDs := indexUIDsFor(allowed, policies)

	// Short-circuit: nothing to search. Return 200 with empty results and the
	// full denied list so callers can render a partial-permission notice.
	if len(indexUIDs) == 0 {
		return resultWithDenied(query, nil, denied, ""), nil
	}

	// Tenant scope filter (unchanged — extractParentContext + buildScopedFilter).
	parent := extractParentContext(userInfo)
	filter := buildScopedFilter(parent)
	if filter != "" {
		klog.V(2).Infof("ResourceSearchQuery applying tenant scope filter type=%q name=%q across %d indices",
			parent.Type, parent.Name, len(indexUIDs))
	}

	// Dispatch to Meilisearch.
	resp, err := r.meiliClient.MultiSearch(indexUIDs, query.Spec.Query, limit, offset, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}

	var results []searchv1alpha1.SearchResult
	for _, hit := range resp.Hits {
		res, err := formatSearchResult(hit)
		if err != nil {
			klog.Warningf("dropping Meilisearch hit due to format error: %v (hit keys: %v)", err, hitKeys(hit))
			continue
		}
		results = append(results, res)
	}

	continueToken := r.calculateNextContinueToken(offset, limit, len(resp.Hits), query)
	return resultWithDenied(query, results, denied, continueToken), nil
}

// resultWithDenied constructs the response object, populating status.results,
// status.deniedTargetResources, and status.continue in one place.
func resultWithDenied(
	query *searchv1alpha1.ResourceSearchQuery,
	results []searchv1alpha1.SearchResult,
	denied []searchv1alpha1.TargetResource,
	continueToken string,
) *searchv1alpha1.ResourceSearchQuery {
	out := query.DeepCopy()
	out.Status = searchv1alpha1.ResourceSearchQueryStatus{
		Results:               results,
		DeniedTargetResources: denied,
		Continue:              continueToken,
	}
	return out
}

// hitKeys returns the keys present in a Meilisearch hit for use in log
// messages. Cheap — does not copy values.
func hitKeys(hit map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(hit))
	for k := range hit {
		keys = append(keys, k)
	}
	return keys
}

type PagingClaims struct {
	Offset          int64                           `json:"offset"`
	Limit           int32                           `json:"limit"`
	Query           string                          `json:"query"`
	TargetResources []searchv1alpha1.TargetResource `json:"targetResources"`
	jwt.RegisteredClaims
}

// validateAndGetPagination validates the limit and continue token from the query and returns
// their effective values.
func (r *REST) validateAndGetPagination(query *searchv1alpha1.ResourceSearchQuery) (int64, int64, error) {
	limit := int64(r.defaultSearchLimit)
	var offset int64 = 0

	// If continue token is provided, it dictates the query state
	if query.Spec.Continue != "" {
		claims := &PagingClaims{}
		token, err := jwt.ParseWithClaims(query.Spec.Continue, claims, func(token *jwt.Token) (interface{}, error) {
			return r.secretKey, nil
		})

		if err != nil || !token.Valid {
			return 0, 0, apierrors.NewBadRequest("invalid continue token")
		}

		// Verify that the query has not changed
		if query.Spec.Query != claims.Query {
			return 0, 0, apierrors.NewBadRequest("query string cannot be changed when using a continue token")
		}
		if int32(limit) != claims.Limit && query.Spec.Limit != 0 {
			// If limit was specified and is different from the token, error.
			// (Note: limit in query.Spec might be 0 if not provided, in which case we use the defaulted limit from the token)
			if query.Spec.Limit != claims.Limit {
				return 0, 0, apierrors.NewBadRequest("limit cannot be changed when using a continue token")
			}
		}
		if !reflect.DeepEqual(query.Spec.TargetResources, claims.TargetResources) {
			return 0, 0, apierrors.NewBadRequest("targetResources cannot be changed when using a continue token")
		}

		// Ensure we use the correct limit/offset from the token
		limit = int64(claims.Limit)
		offset = claims.Offset
		return limit, offset, nil
	}

	// For new queries, validate limit
	if query.Spec.Limit < 0 {
		return 0, 0, apierrors.NewBadRequest("limit cannot be negative")
	}
	if query.Spec.Limit > 0 {
		if int(query.Spec.Limit) > r.maxSearchLimit {
			return 0, 0, apierrors.NewBadRequest(fmt.Sprintf("limit %d exceeds the maximum search limit of %d", query.Spec.Limit, r.maxSearchLimit))
		}
		limit = int64(query.Spec.Limit)
	}

	return limit, offset, nil
}


// calculateNextContinueToken determines the next continue token for pagination.
// If the number of hits on the current page is equal to or greater than the limit,
// it assumes there are more results and returns a signed JWT containing the next offset
// and the original query parameters to ensure consistency.
func (r *REST) calculateNextContinueToken(currentOffset, limit int64, totalHitsOnPage int, query *searchv1alpha1.ResourceSearchQuery) string {
	if totalHitsOnPage > 0 && int64(totalHitsOnPage) >= limit {
		claims := PagingClaims{
			Offset:          currentOffset + limit,
			Limit:           int32(limit),
			Query:           query.Spec.Query,
			TargetResources: query.Spec.TargetResources,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(r.pagingTimeout)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		ss, err := token.SignedString(r.secretKey)
		if err != nil {
			klog.Errorf("Failed to sign paging token: %v", err)
			return ""
		}
		return ss
	}
	return ""
}

// formatSearchResult converts a Meilisearch hit into a SearchResult API object.
func formatSearchResult(hit map[string]json.RawMessage) (searchv1alpha1.SearchResult, error) {
	var score float64
	if s, found := hit["_rankingScore"]; found {
		_ = json.Unmarshal(s, &score)
		score = math.Round(score*10000) / 10000
	}

	// Extract tenant fields before deleting internal fields.
	var tenantName, tenantType string
	if t, found := hit["_tenant"]; found {
		_ = json.Unmarshal(t, &tenantName)
	}
	if tt, found := hit["_tenant_type"]; found {
		_ = json.Unmarshal(tt, &tenantType)
	}
	if tenantName == "" {
		tenantName = "platform"
	}
	if tenantType == "" {
		tenantType = "platform"
	}

	// Remove meilisearch internal fields
	delete(hit, "_rankingScore")
	delete(hit, "_federation")
	delete(hit, "_tenant")
	delete(hit, "_tenant_type")

	b, err := json.Marshal(hit)
	if err != nil {
		return searchv1alpha1.SearchResult{}, err
	}

	var obj unstructured.Unstructured
	if err := obj.UnmarshalJSON(b); err != nil {
		return searchv1alpha1.SearchResult{}, err
	}

	return searchv1alpha1.SearchResult{
		Resource:       obj,
		RelevanceScore: score,
		Tenant: searchv1alpha1.TenantInfo{
			Name: tenantName,
			Type: tenantType,
		},
	}, nil
}
