package validation

import (
	"testing"

	"go.miloapis.net/search/internal/cel"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func TestValidateResourceIndexPolicy_Visibility(t *testing.T) {
	celValidator, err := cel.NewValidator(50)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		visibility searchv1alpha1.IndexVisibility
		wantErrors int
	}{
		{"", 0},
		{searchv1alpha1.IndexVisibilityTenant, 0},
		{searchv1alpha1.IndexVisibilityAllContexts, 0},
		{"Everyone", 1},
	}
	for _, tt := range tests {
		t.Run(string(tt.visibility), func(t *testing.T) {
			p := &searchv1alpha1.ResourceIndexPolicy{Spec: searchv1alpha1.ResourceIndexPolicySpec{
				TargetResource: searchv1alpha1.TargetResource{Group: "feedback.miloapis.com", Version: "v1alpha1", Kind: "Post"},
				Fields:         []searchv1alpha1.FieldPolicy{{Path: ".spec.title", Searchable: true}},
				Visibility:     tt.visibility,
			}}
			errs := ValidateResourceIndexPolicy(p, nil, celValidator)
			if len(errs) != tt.wantErrors {
				t.Fatalf("errors = %v, want %d", errs, tt.wantErrors)
			}
			if tt.wantErrors > 0 && errs[0].Field != "spec.visibility" {
				t.Errorf("error field = %q, want spec.visibility", errs[0].Field)
			}
		})
	}
}
