package resourceindexpolicy

import (
	"context"
	"testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func TestPrepareDefaultsVisibilityToTenant(t *testing.T) {
	s := &strategy{}
	for _, tt := range []struct {
		name string
		set  searchv1alpha1.IndexVisibility
		want searchv1alpha1.IndexVisibility
	}{
		{"unset", "", searchv1alpha1.IndexVisibilityTenant},
		{"AllContexts kept", searchv1alpha1.IndexVisibilityAllContexts, searchv1alpha1.IndexVisibilityAllContexts},
	} {
		t.Run(tt.name, func(t *testing.T) {
			created := &searchv1alpha1.ResourceIndexPolicy{Spec: searchv1alpha1.ResourceIndexPolicySpec{Visibility: tt.set}}
			s.PrepareForCreate(context.Background(), created)
			if created.Spec.Visibility != tt.want {
				t.Errorf("create: visibility = %q, want %q", created.Spec.Visibility, tt.want)
			}
			updated := &searchv1alpha1.ResourceIndexPolicy{Spec: searchv1alpha1.ResourceIndexPolicySpec{Visibility: tt.set}}
			s.PrepareForUpdate(context.Background(), updated, &searchv1alpha1.ResourceIndexPolicy{})
			if updated.Spec.Visibility != tt.want {
				t.Errorf("update: visibility = %q, want %q", updated.Spec.Visibility, tt.want)
			}
		})
	}
}
