package resourcesearchquery

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
)

// buildPolicy constructs a ready CachedPolicy for a single target resource.
// Pass an empty indexName to simulate a policy whose backing index is not yet provisioned.
func buildPolicy(group, version, kind, indexName string) *policyevaluation.CachedPolicy {
	return &policyevaluation.CachedPolicy{
		Policy: &searchv1alpha1.ResourceIndexPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: kind + "-policy"},
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
	}
}

// tr is a local helper for building TargetResource literals in table tests.
func tr(group, version, kind string) searchv1alpha1.TargetResource {
	return searchv1alpha1.TargetResource{Group: group, Version: version, Kind: kind}
}

func TestClassifyTargets(t *testing.T) {
	projectPolicy := buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Project", "projects-idx")
	domainPolicy := buildPolicy("networking.miloapis.com", "v1alpha1", "Domain", "domains-idx")
	notReadyPolicy := buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Org", "") // IndexName empty

	tests := []struct {
		name        string
		targets     []searchv1alpha1.TargetResource
		policies    []*policyevaluation.CachedPolicy
		wantAllowed []searchv1alpha1.TargetResource
		wantDenied  []searchv1alpha1.TargetResource
	}{
		{
			name:     "EmptyTargets_ReturnsAllReadyPolicies",
			targets:  nil,
			policies: []*policyevaluation.CachedPolicy{projectPolicy, domainPolicy, notReadyPolicy},
			wantAllowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("networking.miloapis.com", "v1alpha1", "Domain"),
			},
			wantDenied: nil,
		},
		{
			name: "AllTargetsMatch_NoneDenied",
			targets: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("networking.miloapis.com", "v1alpha1", "Domain"),
			},
			policies: []*policyevaluation.CachedPolicy{projectPolicy, domainPolicy},
			wantAllowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("networking.miloapis.com", "v1alpha1", "Domain"),
			},
			wantDenied: nil,
		},
		{
			name: "MixedAllowedDenied",
			targets: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("example.com", "v1", "Widget"),
			},
			policies:    []*policyevaluation.CachedPolicy{projectPolicy},
			wantAllowed: []searchv1alpha1.TargetResource{tr("resourcemanager.miloapis.com", "v1alpha1", "Project")},
			wantDenied:  []searchv1alpha1.TargetResource{tr("example.com", "v1", "Widget")},
		},
		{
			name: "AllTargetsUnmatched_AllDenied",
			targets: []searchv1alpha1.TargetResource{
				tr("example.com", "v1", "Widget"),
				tr("example.com", "v1", "Gadget"),
			},
			policies:    []*policyevaluation.CachedPolicy{projectPolicy},
			wantAllowed: nil,
			wantDenied: []searchv1alpha1.TargetResource{
				tr("example.com", "v1", "Widget"),
				tr("example.com", "v1", "Gadget"),
			},
		},
		{
			name: "PolicyWithEmptyIndexName_TreatedAsDenied",
			targets: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Org"),
			},
			policies:    []*policyevaluation.CachedPolicy{notReadyPolicy},
			wantAllowed: nil,
			wantDenied:  []searchv1alpha1.TargetResource{tr("resourcemanager.miloapis.com", "v1alpha1", "Org")},
		},
		{
			// classifyTargets must deduplicate allowed targets — a single policy-backed kind appears at most once in the response.
			name: "DuplicateInputTargets_AppearOnceEach",
			targets: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
			},
			policies:    []*policyevaluation.CachedPolicy{projectPolicy},
			wantAllowed: []searchv1alpha1.TargetResource{tr("resourcemanager.miloapis.com", "v1alpha1", "Project")},
			wantDenied:  nil,
		},
		{
			name: "StrictGVKMatching_DifferentVersionDoesNotMatch",
			targets: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1beta1", "Project"),
			},
			policies:    []*policyevaluation.CachedPolicy{projectPolicy},
			wantAllowed: nil,
			wantDenied:  []searchv1alpha1.TargetResource{tr("resourcemanager.miloapis.com", "v1beta1", "Project")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAllowed, gotDenied := classifyTargets(tt.targets, tt.policies)

			if !slices.Equal(gotAllowed, tt.wantAllowed) {
				t.Errorf("allowed:\n  got  %v\n  want %v", gotAllowed, tt.wantAllowed)
			}
			if !slices.Equal(gotDenied, tt.wantDenied) {
				t.Errorf("denied:\n  got  %v\n  want %v", gotDenied, tt.wantDenied)
			}
		})
	}
}

func TestClassifyTargets_EmptyTargets_OrderMatchesPolicySlice(t *testing.T) {
	p1 := buildPolicy("a.example.com", "v1", "Alpha", "alpha-idx")
	p2 := buildPolicy("b.example.com", "v1", "Beta", "beta-idx")

	allowed, denied := classifyTargets(nil, []*policyevaluation.CachedPolicy{p1, p2})
	if len(allowed) != 2 {
		t.Fatalf("expected 2 allowed, got %d", len(allowed))
	}
	if allowed[0].Group != "a.example.com" || allowed[1].Group != "b.example.com" {
		t.Errorf("order not preserved: %v", allowed)
	}
	if len(denied) != 0 {
		t.Errorf("expected no denied, got %v", denied)
	}
}

func TestIndexSearchesFor(t *testing.T) {
	projectPolicy := buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Project", "projects-idx")
	domainPolicy := buildPolicy("networking.miloapis.com", "v1alpha1", "Domain", "domains-idx")
	postPolicy := buildPolicy("feedback.miloapis.com", "v1alpha1", "Post", "posts-idx")
	postPolicy.Policy.Spec.Visibility = searchv1alpha1.IndexVisibilityAllContexts
	explicitTenant := buildPolicy("networking.miloapis.com", "v1alpha1", "Zone", "zones-idx")
	explicitTenant.Policy.Spec.Visibility = searchv1alpha1.IndexVisibilityTenant
	const scope = `(_tenant_type = "user" AND _tenant = "alice")`
	const scopeOrRoot = scope + ` OR _tenant_type IN ["platform", "Global"]`
	// Two cached policies for one kind, as while a duplicate is being
	// replaced: the index is only widened if both are AllContexts.
	issueTenant := buildPolicy("feedback.miloapis.com", "v1alpha1", "Issue", "issues-idx")
	issueAll := buildPolicy("feedback.miloapis.com", "v1alpha1", "Issue", "issues-idx")
	issueAll.Policy.Spec.Visibility = searchv1alpha1.IndexVisibilityAllContexts
	issueAll2 := buildPolicy("feedback.miloapis.com", "v1alpha1", "Issue", "issues-idx")
	issueAll2.Policy.Spec.Visibility = searchv1alpha1.IndexVisibilityAllContexts

	tests := []struct {
		name     string
		allowed  []searchv1alpha1.TargetResource
		policies []*policyevaluation.CachedPolicy
		scope    string
		want     []meilisearch.IndexSearch
	}{
		{
			name:     "EmptyAllowed_ReturnsNil",
			allowed:  nil,
			policies: []*policyevaluation.CachedPolicy{projectPolicy},
			want:     nil,
		},
		{
			name:     "OneAllowed_ReturnsItsIndexUID",
			allowed:  []searchv1alpha1.TargetResource{tr("resourcemanager.miloapis.com", "v1alpha1", "Project")},
			policies: []*policyevaluation.CachedPolicy{projectPolicy},
			want:     []meilisearch.IndexSearch{{IndexUID: "projects-idx"}},
		},
		{
			name: "MultipleAllowed_ReturnsAllIndexUIDs_OrderPreserved",
			allowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("networking.miloapis.com", "v1alpha1", "Domain"),
			},
			policies: []*policyevaluation.CachedPolicy{projectPolicy, domainPolicy},
			want:     []meilisearch.IndexSearch{{IndexUID: "projects-idx"}, {IndexUID: "domains-idx"}},
		},
		{
			name: "AllowedWithEmptyIndexName_Skipped",
			// Should not happen in practice (classifyTargets filters these), but
			// indexSearchesFor is defensive: a policy whose IndexName is empty is skipped.
			allowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Org"),
			},
			policies: []*policyevaluation.CachedPolicy{
				buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Org", ""),
			},
			want: nil,
		},
		{
			name: "Scoped_AllContextsIndexAlsoMatchesRoot",
			allowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("feedback.miloapis.com", "v1alpha1", "Post"),
				tr("networking.miloapis.com", "v1alpha1", "Zone"),
			},
			policies: []*policyevaluation.CachedPolicy{projectPolicy, postPolicy, explicitTenant},
			scope:    scope,
			want: []meilisearch.IndexSearch{
				{IndexUID: "projects-idx", Filter: scope},
				{IndexUID: "posts-idx", Filter: scopeOrRoot},
				{IndexUID: "zones-idx", Filter: scope},
			},
		},
		{
			name:     "Root_AllContextsIndexUnfiltered",
			allowed:  []searchv1alpha1.TargetResource{tr("feedback.miloapis.com", "v1alpha1", "Post")},
			policies: []*policyevaluation.CachedPolicy{postPolicy},
			scope:    "",
			want:     []meilisearch.IndexSearch{{IndexUID: "posts-idx"}},
		},
		{
			name:     "SameKind_TenantThenAllContexts_KeepsTenantFilter",
			allowed:  []searchv1alpha1.TargetResource{tr("feedback.miloapis.com", "v1alpha1", "Issue")},
			policies: []*policyevaluation.CachedPolicy{issueTenant, issueAll},
			scope:    scope,
			want:     []meilisearch.IndexSearch{{IndexUID: "issues-idx", Filter: scope}},
		},
		{
			name:     "SameKind_AllContextsThenTenant_KeepsTenantFilter",
			allowed:  []searchv1alpha1.TargetResource{tr("feedback.miloapis.com", "v1alpha1", "Issue")},
			policies: []*policyevaluation.CachedPolicy{issueAll, issueTenant},
			scope:    scope,
			want:     []meilisearch.IndexSearch{{IndexUID: "issues-idx", Filter: scope}},
		},
		{
			name:     "SameKind_AllAllContexts_Widened",
			allowed:  []searchv1alpha1.TargetResource{tr("feedback.miloapis.com", "v1alpha1", "Issue")},
			policies: []*policyevaluation.CachedPolicy{issueAll, issueAll2},
			scope:    scope,
			want:     []meilisearch.IndexSearch{{IndexUID: "issues-idx", Filter: scopeOrRoot}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexSearchesFor(tt.allowed, tt.policies, tt.scope)
			if len(got) != len(tt.want) {
				t.Fatalf("indexSearchesFor len: got %d (%v), want %d (%v)", len(got), got, len(tt.want), tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("index %d: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
