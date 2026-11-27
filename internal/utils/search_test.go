package utils

import (
	"testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func hashTestSpec() searchv1alpha1.ResourceIndexPolicySpec {
	return searchv1alpha1.ResourceIndexPolicySpec{
		TargetResource: searchv1alpha1.TargetResource{Group: "feedback.miloapis.com", Version: "v1alpha1", Kind: "Post"},
		Conditions:     []searchv1alpha1.PolicyCondition{{Name: "public", Expression: "!has(spec.hidden) || !spec.hidden"}},
		Fields:         []searchv1alpha1.FieldPolicy{{Path: ".spec.title", Searchable: true}},
	}
}

// Visibility only changes how queries are filtered, so it must not change
// the hash and trigger a re-index.
func TestComputeSpecHash_IgnoresVisibility(t *testing.T) {
	base := hashTestSpec()
	want := ComputeSpecHash(&base)
	if want == "" {
		t.Fatal("empty hash")
	}
	for _, v := range []searchv1alpha1.IndexVisibility{
		searchv1alpha1.IndexVisibilityTenant,
		searchv1alpha1.IndexVisibilityAllContexts,
	} {
		spec := hashTestSpec()
		spec.Visibility = v
		if got := ComputeSpecHash(&spec); got != want {
			t.Errorf("visibility %q: hash %s, want %s", v, got, want)
		}
	}

	spec := hashTestSpec()
	spec.Visibility = searchv1alpha1.IndexVisibilityAllContexts
	ComputeSpecHash(&spec)
	if spec.Visibility != searchv1alpha1.IndexVisibilityAllContexts {
		t.Errorf("ComputeSpecHash changed the caller's spec: visibility %q", spec.Visibility)
	}
}

func TestComputeSpecHash_ChangesWithIndexedFields(t *testing.T) {
	base := hashTestSpec()
	baseHash := ComputeSpecHash(&base)
	for name, change := range map[string]func(*searchv1alpha1.ResourceIndexPolicySpec){
		"fields":     func(s *searchv1alpha1.ResourceIndexPolicySpec) { s.Fields[0].Path = ".spec.body" },
		"conditions": func(s *searchv1alpha1.ResourceIndexPolicySpec) { s.Conditions[0].Expression = "true" },
		"target":     func(s *searchv1alpha1.ResourceIndexPolicySpec) { s.TargetResource.Kind = "Release" },
	} {
		t.Run(name, func(t *testing.T) {
			spec := hashTestSpec()
			change(&spec)
			if ComputeSpecHash(&spec) == baseHash {
				t.Errorf("changing %s didn't change the hash", name)
			}
		})
	}
}
