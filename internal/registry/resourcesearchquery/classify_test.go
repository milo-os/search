package resourcesearchquery

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
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

func TestIndexUIDsFor(t *testing.T) {
	projectPolicy := buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Project", "projects-idx")
	domainPolicy := buildPolicy("networking.miloapis.com", "v1alpha1", "Domain", "domains-idx")

	tests := []struct {
		name     string
		allowed  []searchv1alpha1.TargetResource
		policies []*policyevaluation.CachedPolicy
		want     []string
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
			want:     []string{"projects-idx"},
		},
		{
			name: "MultipleAllowed_ReturnsAllIndexUIDs_OrderPreserved",
			allowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				tr("networking.miloapis.com", "v1alpha1", "Domain"),
			},
			policies: []*policyevaluation.CachedPolicy{projectPolicy, domainPolicy},
			want:     []string{"projects-idx", "domains-idx"},
		},
		{
			name: "AllowedWithEmptyIndexName_Skipped",
			// Should not happen in practice (classifyTargets filters these), but
			// indexUIDsFor is defensive: a policy whose IndexName is empty is skipped.
			allowed: []searchv1alpha1.TargetResource{
				tr("resourcemanager.miloapis.com", "v1alpha1", "Org"),
			},
			policies: []*policyevaluation.CachedPolicy{
				buildPolicy("resourcemanager.miloapis.com", "v1alpha1", "Org", ""),
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexUIDsFor(tt.allowed, tt.policies)
			if len(got) != len(tt.want) {
				t.Fatalf("indexUIDsFor len: got %d (%v), want %d (%v)", len(got), got, len(tt.want), tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
