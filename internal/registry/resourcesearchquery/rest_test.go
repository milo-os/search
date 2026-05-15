package resourcesearchquery

import (
	"context"
	"testing"
	"time"

	meiliapi "github.com/meilisearch/meilisearch-go"
	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

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

// newSARFake returns a fake Clientset whose SAR reactor either allows or denies
// every SubjectAccessReview. Named differently from fakeSARClient in authz_test.go
// (which captures per-call state); this simpler helper is sufficient for REST
// handler integration tests.
func newSARFake(t *testing.T, allow bool) *fake.Clientset {
	t.Helper()
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "subjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		sar := action.(clienttesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview)
		sar.Status.Allowed = allow
		if !allow {
			sar.Status.Reason = "test denial"
		}
		return true, sar, nil
	})
	return cs
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

// testPlurals is the default plural map for REST handler tests. It covers the
// Project kind used by newTestProjectQuery / testQuery helpers.
var testPlurals = fakePlurals{
	{Group: "resourcemanager.miloapis.com", Kind: "Project"}: "projects",
}

// newTestREST builds a *REST suitable for unit tests using the supplied fakes.
func newTestREST(
	t *testing.T,
	meili multiSearcher,
	policies policyLister,
	sarCs *fake.Clientset,
) *REST {
	t.Helper()
	return &REST{
		meiliClient:        meili,
		policyCache:        policies,
		pluralCache:        testPlurals,
		sarClient:          sarCs.AuthorizationV1().SubjectAccessReviews(),
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
// so that authorizeTargets and resolveIndexUIDs both have something to act on.
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
	sars := newSARFake(t, true)
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index), sars)
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
	sars := newSARFake(t, true)
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index), sars)
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
	sars := newSARFake(t, true)
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index), sars)
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
// TestCreate_SARDenied_Returns403NoMeili
// ---------------------------------------------------------------------------

func TestCreate_SARDenied_Returns403NoMeili(t *testing.T) {
	const (
		group   = "resourcemanager.miloapis.com"
		version = "v1alpha1"
		kind    = "Project"
		index   = "projects-idx"
	)

	meili := newFakeMeili()
	sars := newSARFake(t, false) // deny all
	r := newTestREST(t, meili, newPolicyListerWithIndex(group, version, kind, index), sars)
	q := testQuery(group, version, kind)

	u := &user.DefaultInfo{Name: "alice"}
	_, err := r.Create(ctxWithUser(u), q, nil, nil)
	if err == nil {
		t.Fatal("expected error from SAR denial")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected Forbidden, got %v", err)
	}
	// Meilisearch must not be contacted when authorization fails.
	if meili.calls != 0 {
		t.Fatalf("Meilisearch should not be called on SAR denial; calls=%d", meili.calls)
	}
}

// ---------------------------------------------------------------------------
// TestCreate_EmptyTargets_NoSARNoFilter
// ---------------------------------------------------------------------------

// When TargetResources is empty, authorizeTargets is a no-op and
// resolveIndexUIDs returns all ready policies. With no parent context the
// filter must be empty.
func TestCreate_EmptyTargets_NoSARNoFilter(t *testing.T) {
	meili := newFakeMeili()
	sars := newSARFake(t, true)
	policies := newPolicyListerWithIndex("a.example.com", "v1", "Foo", "foo-idx")
	r := newTestREST(t, meili, policies, sars)

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
	sars := newSARFake(t, true)
	r := newTestREST(t, meili, &fakePolicyLister{}, sars)

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
	sars := newSARFake(t, true)
	policies := &fakePolicyLister{}
	u := &user.DefaultInfo{Name: "alice"}

	t.Run("negative limit returns 400", func(t *testing.T) {
		meili := newFakeMeili()
		r := newTestREST(t, meili, policies, sars)
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
		r := newTestREST(t, meili, policies, sars)
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
// TestCreate_UnknownKind_Returns403WithoutMeili
// ---------------------------------------------------------------------------

// When the requested Kind is not in the plural cache, authorizeTargets cannot
// construct a SAR ResourceAttributes and must return Forbidden. Meilisearch
// must never be contacted.
func TestCreate_UnknownKind_Returns403WithoutMeili(t *testing.T) {
	sars := newSARFake(t, true) // allow-all; must not be reached
	meili := newFakeMeili()
	// Empty plurals map — Project kind is unknown to authz.
	r := &REST{
		meiliClient:        meili,
		policyCache:        newPolicyListerWithIndex("resourcemanager.miloapis.com", "v1alpha1", "Project", "projects-idx"),
		pluralCache:        fakePlurals{},
		sarClient:          sars.AuthorizationV1().SubjectAccessReviews(),
		maxSearchLimit:     100,
		defaultSearchLimit: 10,
		secretKey:          []byte("test-secret"),
		pagingTimeout:      0,
	}

	u := &user.DefaultInfo{Name: "alice"}
	_, err := r.Create(ctxWithUser(u), newTestProjectQuery(), nil, nil)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected Forbidden, got %v", err)
	}
	if meili.calls != 0 {
		t.Fatalf("Meilisearch should not be called on unknown-kind 403; calls=%d", meili.calls)
	}
}
