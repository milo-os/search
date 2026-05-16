package resourcesearchquery

import (
	"context"
	"testing"
	"time"

	meiliapi "github.com/meilisearch/meilisearch-go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"

	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// ---------------------------------------------------------------------------
// Fakes shared across REST handler tests
// ---------------------------------------------------------------------------

// fakeMeiliClient is a test double for the multiSearcher interface.
type fakeMeiliClient struct {
	calls      int
	lastUIDs   []string
	lastQuery  string
	lastLimit  int64
	lastOffset int64
	lastFilter string
	resp       *meiliapi.MultiSearchResponse
}

func newFakeMeili() *fakeMeiliClient {
	return &fakeMeiliClient{resp: &meiliapi.MultiSearchResponse{}}
}

func (f *fakeMeiliClient) MultiSearch(uids []string, q string, limit, offset int64, filter string) (*meiliapi.MultiSearchResponse, error) {
	f.calls++
	f.lastUIDs = uids
	f.lastQuery = q
	f.lastLimit = limit
	f.lastOffset = offset
	f.lastFilter = filter
	return f.resp, nil
}

// ctxWithUser wraps a user.Info into a context the way the API server does.
func ctxWithUser(u user.Info) context.Context {
	return apirequest.WithUser(context.Background(), u)
}

// ---------------------------------------------------------------------------
// Fake policyLister
// ---------------------------------------------------------------------------

// fakePolicyLister implements policyLister with a fixed slice of policies.
type fakePolicyLister struct {
	policies []*policyevaluation.CachedPolicy
}

func (f *fakePolicyLister) GetPolicies() []*policyevaluation.CachedPolicy {
	return f.policies
}

// newPolicyListerWithIndex builds a fakePolicyLister that exposes a single
// ready policy whose target matches group/version/kind and whose index name
// is indexName.
func newPolicyListerWithIndex(group, version, kind, indexName string) *fakePolicyLister {
	return &fakePolicyLister{
		policies: []*policyevaluation.CachedPolicy{
			{
				Policy: &searchv1alpha1.ResourceIndexPolicy{
					ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
					Spec: searchv1alpha1.ResourceIndexPolicySpec{
						TargetResource: searchv1alpha1.TargetResource{
							Group:   group,
							Version: version,
							Kind:    kind,
						},
					},
					Status: searchv1alpha1.ResourceIndexPolicyStatus{
						IndexName: indexName,
					},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Shared test REST constructor
// ---------------------------------------------------------------------------

// newTestREST builds a *REST suitable for unit tests using the supplied fakes.
func newTestREST(
	t *testing.T,
	meili multiSearcher,
	policies policyLister,
) *REST {
	t.Helper()
	return &REST{
		meiliClient:        meili,
		policyCache:        policies,
		maxSearchLimit:     100,
		defaultSearchLimit: 10,
		secretKey:          []byte("test-secret"),
		pagingTimeout:      24 * time.Hour,
	}
}

// newTestProjectQuery builds a minimal ResourceSearchQuery targeting the Project
// kind, used by tests that need a non-empty TargetResources list.
func newTestProjectQuery() *searchv1alpha1.ResourceSearchQuery {
	return testQuery("resourcemanager.miloapis.com", "v1alpha1", "Project")
}

// testQuery builds a minimal ResourceSearchQuery with a single TargetResource
// so that classifyTargets and indexUIDsFor both have something to act on.
func testQuery(group, version, kind string) *searchv1alpha1.ResourceSearchQuery {
	return &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{
			Query: "test",
			TargetResources: []searchv1alpha1.TargetResource{
				{Group: group, Version: version, Kind: kind},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// TestCreate_NoUserReturns401
// ---------------------------------------------------------------------------

func TestCreate_NoUserReturns401(t *testing.T) {
	const (
		group   = "resourcemanager.miloapis.com"
		version = "v1alpha1"
		kind    = "Project"
		index   = "projects-idx"
	)

	meili := newFakeMeili()
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index))
	q := testQuery(group, version, kind)

	// context.Background() has no user — Create must return 401.
	_, err := r.Create(context.Background(), q, nil, nil)
	if err == nil {
		t.Fatal("expected error for missing user")
	}
	if !apierrors.IsUnauthorized(err) {
		t.Fatalf("expected Unauthorized, got %v", err)
	}
	// Meilisearch must not be contacted before identity is established.
	if meili.calls != 0 {
		t.Fatalf("Meilisearch should not be called; calls=%d", meili.calls)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_NoParentContext_PassesEmptyFilter
// ---------------------------------------------------------------------------

func TestCreate_NoParentContext_PassesEmptyFilter(t *testing.T) {
	const (
		group   = "resourcemanager.miloapis.com"
		version = "v1alpha1"
		kind    = "Project"
		index   = "projects-idx"
	)

	meili := newFakeMeili()
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index))
	q := testQuery(group, version, kind)

	// User has no parent-type / parent-name extras — filter must be empty.
	u := &user.DefaultInfo{Name: "alice", Groups: []string{"system:authenticated"}}
	_, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meili.calls != 1 {
		t.Fatalf("expected 1 Meilisearch call, got %d", meili.calls)
	}
	if meili.lastFilter != "" {
		t.Fatalf("expected empty filter, got %q", meili.lastFilter)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_WithParentContext_AppliesScopedFilter
// ---------------------------------------------------------------------------

func TestCreate_WithParentContext_AppliesScopedFilter(t *testing.T) {
	const (
		group   = "resourcemanager.miloapis.com"
		version = "v1alpha1"
		kind    = "Project"
		index   = "projects-idx"
	)

	meili := newFakeMeili()
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index))
	q := testQuery(group, version, kind)

	u := &user.DefaultInfo{
		Name: "alice",
		Extra: map[string][]string{
			"iam.miloapis.com/parent-type": {"Project"},
			"iam.miloapis.com/parent-name": {"acme-prod"},
		},
	}
	_, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `(_tenant_type = "project" AND _tenant = "acme-prod")`
	if meili.lastFilter != want {
		t.Fatalf("filter = %q, want %q", meili.lastFilter, want)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_EmptyTargets_NoFilter
// ---------------------------------------------------------------------------

// When TargetResources is empty, no denial work is done and all ready
// policies are searched. With no parent context the filter must be empty.
func TestCreate_EmptyTargets_NoFilter(t *testing.T) {
	meili := newFakeMeili()
	policies := newPolicyListerWithIndex("a.example.com", "v1", "Foo", "foo-idx")
	r := newTestREST(t, meili, policies)

	q := &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{Query: "hello"},
	}
	u := &user.DefaultInfo{Name: "alice"}
	_, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meili.calls != 1 {
		t.Fatalf("expected 1 Meilisearch call, got %d", meili.calls)
	}
	if meili.lastFilter != "" {
		t.Fatalf("expected empty filter for unscoped user, got %q", meili.lastFilter)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_NoPolicies_ReturnsEmpty
// ---------------------------------------------------------------------------

// When there are no ready policies resolveIndexUIDs returns an empty slice
// and Create short-circuits with an empty result (no Meilisearch call).
func TestCreate_NoPolicies_ReturnsEmpty(t *testing.T) {
	meili := newFakeMeili()
	r := newTestREST(t, meili, &fakePolicyLister{})

	q := &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{Query: "hello"},
	}
	u := &user.DefaultInfo{Name: "alice"}
	out, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, ok := out.(*searchv1alpha1.ResourceSearchQuery)
	if !ok {
		t.Fatalf("expected *ResourceSearchQuery, got %T", out)
	}
	if len(result.Status.Results) != 0 {
		t.Fatalf("expected empty results, got %d", len(result.Status.Results))
	}
	if meili.calls != 0 {
		t.Fatalf("Meilisearch should not be called when no policies exist; calls=%d", meili.calls)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_PaginationValidation
// ---------------------------------------------------------------------------

func TestCreate_PaginationValidation(t *testing.T) {
	policies := &fakePolicyLister{}
	u := &user.DefaultInfo{Name: "alice"}

	t.Run("negative limit returns 400", func(t *testing.T) {
		meili := newFakeMeili()
		r := newTestREST(t, meili, policies)
		q := &searchv1alpha1.ResourceSearchQuery{
			Spec: searchv1alpha1.ResourceSearchQuerySpec{Query: "x", Limit: -1},
		}
		_, err := r.Create(ctxWithUser(u), q, nil, nil)
		if err == nil {
			t.Fatal("expected error for negative limit")
		}
		if !apierrors.IsBadRequest(err) {
			t.Fatalf("expected BadRequest, got %v", err)
		}
	})

	t.Run("limit exceeding max returns 400", func(t *testing.T) {
		meili := newFakeMeili()
		r := newTestREST(t, meili, policies)
		q := &searchv1alpha1.ResourceSearchQuery{
			Spec: searchv1alpha1.ResourceSearchQuerySpec{Query: "x", Limit: 200},
		}
		_, err := r.Create(ctxWithUser(u), q, nil, nil)
		if err == nil {
			t.Fatal("expected error for limit exceeding max")
		}
		if !apierrors.IsBadRequest(err) {
			t.Fatalf("expected BadRequest, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestCreate_PartialDeniedTargets_Returns200WithDeniedList
// ---------------------------------------------------------------------------

// Two targets: Project (has a ready policy) and Widget (no policy).
// Expect: 200 OK, results from the Project index, Widget in deniedTargetResources.
func TestCreate_PartialDeniedTargets_Returns200WithDeniedList(t *testing.T) {
	const (
		group   = "resourcemanager.miloapis.com"
		version = "v1alpha1"
		kind    = "Project"
		index   = "projects-idx"
	)

	meili := newFakeMeili()
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index))

	q := &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{
			Query: "test",
			TargetResources: []searchv1alpha1.TargetResource{
				{Group: group, Version: version, Kind: kind},
				{Group: "example.com", Version: "v1", Kind: "Widget"}, // no policy
			},
		},
	}

	u := &user.DefaultInfo{Name: "alice"}
	out, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, ok := out.(*searchv1alpha1.ResourceSearchQuery)
	if !ok {
		t.Fatalf("expected *ResourceSearchQuery, got %T", out)
	}

	// Meilisearch must be called for the Project index.
	if meili.calls != 1 {
		t.Fatalf("expected 1 Meilisearch call, got %d", meili.calls)
	}
	if len(meili.lastUIDs) != 1 || meili.lastUIDs[0] != index {
		t.Fatalf("expected UIDs [%q], got %v", index, meili.lastUIDs)
	}

	// Widget must appear in deniedTargetResources.
	if len(result.Status.DeniedTargetResources) != 1 {
		t.Fatalf("expected 1 denied target, got %d: %v", len(result.Status.DeniedTargetResources), result.Status.DeniedTargetResources)
	}
	denied := result.Status.DeniedTargetResources[0]
	if denied.Group != "example.com" || denied.Kind != "Widget" {
		t.Errorf("wrong denied target: %+v", denied)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_AllDeniedTargets_Returns200WithEmptyResultsAndDeniedList
// ---------------------------------------------------------------------------

// All targets lack policies. Expect 200, empty results, full denied list.
func TestCreate_AllDeniedTargets_Returns200WithEmptyResultsAndDeniedList(t *testing.T) {
	meili := newFakeMeili()
	r := newTestREST(t, meili, &fakePolicyLister{})

	q := &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{
			Query: "test",
			TargetResources: []searchv1alpha1.TargetResource{
				{Group: "example.com", Version: "v1", Kind: "Widget"},
				{Group: "example.com", Version: "v1", Kind: "Gadget"},
			},
		},
	}

	u := &user.DefaultInfo{Name: "alice"}
	out, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, ok := out.(*searchv1alpha1.ResourceSearchQuery)
	if !ok {
		t.Fatalf("expected *ResourceSearchQuery, got %T", out)
	}

	if meili.calls != 0 {
		t.Fatalf("Meilisearch should not be called when all targets denied; calls=%d", meili.calls)
	}

	if len(result.Status.DeniedTargetResources) != 2 {
		t.Fatalf("expected 2 denied targets, got %d: %v", len(result.Status.DeniedTargetResources), result.Status.DeniedTargetResources)
	}

	if len(result.Status.Results) != 0 {
		t.Fatalf("expected empty results, got %d", len(result.Status.Results))
	}
}

// ---------------------------------------------------------------------------
// TestCreate_NoTargets_ReturnsAllReadyPolicyResults_EmptyDenied
// ---------------------------------------------------------------------------

func TestCreate_NoTargets_ReturnsAllReadyPolicyResults_EmptyDenied(t *testing.T) {
	meili := newFakeMeili()
	policies := newPolicyListerWithIndex("a.example.com", "v1", "Foo", "foo-idx")
	r := newTestREST(t, meili, policies)

	q := &searchv1alpha1.ResourceSearchQuery{
		Spec: searchv1alpha1.ResourceSearchQuerySpec{Query: "hello"},
	}
	u := &user.DefaultInfo{Name: "alice"}
	out, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, ok := out.(*searchv1alpha1.ResourceSearchQuery)
	if !ok {
		t.Fatalf("expected *ResourceSearchQuery, got %T", out)
	}

	if meili.calls != 1 {
		t.Fatalf("expected 1 Meilisearch call, got %d", meili.calls)
	}
	if len(result.Status.DeniedTargetResources) != 0 {
		t.Fatalf("expected no denied targets, got %d: %v", len(result.Status.DeniedTargetResources), result.Status.DeniedTargetResources)
	}
}
