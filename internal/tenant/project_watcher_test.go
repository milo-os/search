package tenant

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// spyFilterDeleteClient records calls to DeleteDocumentsByFilter.
type spyFilterDeleteClient struct {
	calls []deleteByFilterCall
	err   error // optional error to return on every call
}

type deleteByFilterCall struct {
	indexUID string
	filter   string
}

func (s *spyFilterDeleteClient) DeleteDocumentsByFilter(indexUID string, filter string) error {
	s.calls = append(s.calls, deleteByFilterCall{indexUID: indexUID, filter: filter})
	return s.err
}

// stubPolicyCacheReader is a simple in-memory implementation of policyCacheReader.
type stubPolicyCacheReader struct {
	policies []*policyevaluation.CachedPolicy
}

func (s *stubPolicyCacheReader) GetPolicies() []*policyevaluation.CachedPolicy {
	return s.policies
}

// newStubPolicyCache builds a stubPolicyCacheReader from a list of v1alpha1 policies.
// Only the Policy and Status fields are populated; CEL compilation is skipped
// because ProjectWatcher only reads Policy.Status.IndexName — it never evaluates.
func newStubPolicyCache(policies ...*v1alpha1.ResourceIndexPolicy) *stubPolicyCacheReader {
	cached := make([]*policyevaluation.CachedPolicy, 0, len(policies))
	for _, p := range policies {
		cached = append(cached, &policyevaluation.CachedPolicy{Policy: p})
	}
	return &stubPolicyCacheReader{policies: cached}
}

func TestProjectWatcher_OnTenantDisengaged_RejectsSpecialCharacters(t *testing.T) {
	spy := &spyFilterDeleteClient{}

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"},
		Status:     v1alpha1.ResourceIndexPolicyStatus{IndexName: "pod-index"},
	}

	w := &ProjectWatcher{
		policyCache: newStubPolicyCache(policy),
		searchSDK:   spy,
	}

	// A tenant name containing a double-quote must be rejected to prevent filter
	// injection into Meilisearch filter expressions.
	w.OnTenantDisengaged(`evil"name`)

	assert.Empty(t, spy.calls, "DeleteDocumentsByFilter must NOT be called for tenant names with special characters")
}

func TestProjectWatcher_OnTenantDisengaged_RejectsBackslash(t *testing.T) {
	spy := &spyFilterDeleteClient{}

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"},
		Status:     v1alpha1.ResourceIndexPolicyStatus{IndexName: "pod-index"},
	}

	w := &ProjectWatcher{
		policyCache: newStubPolicyCache(policy),
		searchSDK:   spy,
	}

	w.OnTenantDisengaged(`evil\name`)

	assert.Empty(t, spy.calls, "DeleteDocumentsByFilter must NOT be called for tenant names containing backslashes")
}

func TestProjectWatcher_OnTenantDisengaged_CallsDeleteByFilter(t *testing.T) {
	spy := &spyFilterDeleteClient{}

	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"},
		Status:     v1alpha1.ResourceIndexPolicyStatus{IndexName: "pod-index"},
	}

	w := &ProjectWatcher{
		policyCache: newStubPolicyCache(policy),
		searchSDK:   spy,
	}

	w.OnTenantDisengaged("my-project")

	require.Len(t, spy.calls, 1, "DeleteDocumentsByFilter should be called once per ready policy")
	assert.Equal(t, "pod-index", spy.calls[0].indexUID)
	assert.Equal(t, `_tenant = "my-project"`, spy.calls[0].filter)
}

func TestProjectWatcher_OnTenantDisengaged_SkipsPoliciesWithNoIndexName(t *testing.T) {
	spy := &spyFilterDeleteClient{}

	// Policy with an empty IndexName should be skipped.
	policy := &v1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "uninitialized-policy"},
		Status:     v1alpha1.ResourceIndexPolicyStatus{IndexName: ""},
	}

	w := &ProjectWatcher{
		policyCache: newStubPolicyCache(policy),
		searchSDK:   spy,
	}

	w.OnTenantDisengaged("my-project")

	assert.Empty(t, spy.calls, "DeleteDocumentsByFilter must not be called when policy has no IndexName")
}

func TestProjectWatcher_OnTenantDisengaged_MultiplePolicies(t *testing.T) {
	spy := &spyFilterDeleteClient{}

	makePolicy := func(name, indexName string) *v1alpha1.ResourceIndexPolicy {
		return &v1alpha1.ResourceIndexPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     v1alpha1.ResourceIndexPolicyStatus{IndexName: indexName},
		}
	}

	w := &ProjectWatcher{
		policyCache: newStubPolicyCache(
			makePolicy("policy-a", "index-a"),
			makePolicy("policy-b", "index-b"),
		),
		searchSDK: spy,
	}

	w.OnTenantDisengaged("tenant-x")

	assert.Len(t, spy.calls, 2, "DeleteDocumentsByFilter should be called once per policy with an IndexName")
	for _, call := range spy.calls {
		assert.Equal(t, `_tenant = "tenant-x"`, call.filter)
	}
}
