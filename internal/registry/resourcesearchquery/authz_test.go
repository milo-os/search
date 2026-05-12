package resourcesearchquery

import (
	"context"
	"errors"
	"strings"
	"testing"

	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// fakeSARClient returns a SubjectAccessReviewInterface that calls the supplied
// handler for every Create. Captures every SAR seen for assertions.
func fakeSARClient(t *testing.T, handle func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error)) (
	*fake.Clientset,
	func() []*authzv1.SubjectAccessReview,
) {
	t.Helper()
	cs := fake.NewSimpleClientset()
	var seen []*authzv1.SubjectAccessReview
	cs.PrependReactor("create", "subjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		create := action.(clienttesting.CreateAction)
		sar := create.GetObject().(*authzv1.SubjectAccessReview)
		seen = append(seen, sar.DeepCopy())
		resp, err := handle(sar)
		if resp == nil {
			resp = sar
		}
		return true, resp, err
	})
	return cs, func() []*authzv1.SubjectAccessReview { return seen }
}

func target(group, version, kind string) searchv1alpha1.TargetResource {
	return searchv1alpha1.TargetResource{Group: group, Version: version, Kind: kind}
}

func TestAuthorizeTargets(t *testing.T) {
	userInfo := &user.DefaultInfo{
		Name:   "alice",
		UID:    "uid-1",
		Groups: []string{"system:authenticated", "tenants:acme"},
		Extra: map[string][]string{
			"iam.miloapis.com/parent-type": {"Project"},
			"iam.miloapis.com/parent-name": {"acme-prod"},
		},
	}

	t.Run("nil user returns error", func(t *testing.T) {
		cs, _ := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			t.Fatal("SAR should not be called when user is nil")
			return nil, nil
		})
		err := authorizeTargets(context.Background(), cs.AuthorizationV1().SubjectAccessReviews(), nil, nil)
		if err == nil {
			t.Fatal("expected error for nil user")
		}
		if !strings.Contains(err.Error(), "no user") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty targets is a no-op", func(t *testing.T) {
		cs, seen := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			t.Fatal("SAR should not be called when targets is empty")
			return nil, nil
		})
		err := authorizeTargets(context.Background(), cs.AuthorizationV1().SubjectAccessReviews(), userInfo, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(seen()) != 0 {
			t.Fatalf("expected zero SARs, got %d", len(seen()))
		}
	})

	t.Run("single allowed target", func(t *testing.T) {
		cs, seen := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			sar.Status.Allowed = true
			return sar, nil
		})
		err := authorizeTargets(
			context.Background(),
			cs.AuthorizationV1().SubjectAccessReviews(),
			userInfo,
			[]searchv1alpha1.TargetResource{target("resourcemanager.miloapis.com", "v1alpha1", "Project")},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		all := seen()
		if len(all) != 1 {
			t.Fatalf("expected 1 SAR, got %d", len(all))
		}
		sar := all[0]
		if sar.Spec.User != "alice" || sar.Spec.UID != "uid-1" {
			t.Fatalf("user/uid mismatch: %+v", sar.Spec)
		}
		if sar.Spec.ResourceAttributes == nil {
			t.Fatal("ResourceAttributes nil")
		}
		ra := sar.Spec.ResourceAttributes
		if ra.Group != "resourcemanager.miloapis.com" || ra.Version != "v1alpha1" ||
			ra.Resource != "projects" || ra.Verb != "list" || ra.Namespace != "" {
			t.Fatalf("ResourceAttributes mismatch: %+v", ra)
		}
		if got := sar.Spec.Extra["iam.miloapis.com/parent-type"]; len(got) != 1 || got[0] != "Project" {
			t.Fatalf("Extra parent-type not propagated: %+v", sar.Spec.Extra)
		}
		if got := sar.Spec.Extra["iam.miloapis.com/parent-name"]; len(got) != 1 || got[0] != "acme-prod" {
			t.Fatalf("Extra parent-name not propagated: %+v", sar.Spec.Extra)
		}
	})

	t.Run("single denied target returns 403", func(t *testing.T) {
		cs, _ := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			sar.Status.Allowed = false
			sar.Status.Reason = "user not bound to project"
			return sar, nil
		})
		err := authorizeTargets(
			context.Background(),
			cs.AuthorizationV1().SubjectAccessReviews(),
			userInfo,
			[]searchv1alpha1.TargetResource{target("resourcemanager.miloapis.com", "v1alpha1", "Project")},
		)
		if err == nil {
			t.Fatal("expected error for denied SAR")
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("expected Forbidden, got %v", err)
		}
		if !strings.Contains(err.Error(), "user not bound to project") {
			t.Fatalf("error should cite reason: %v", err)
		}
	})

	t.Run("multi-target allow-all", func(t *testing.T) {
		cs, seen := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			sar.Status.Allowed = true
			return sar, nil
		})
		err := authorizeTargets(
			context.Background(),
			cs.AuthorizationV1().SubjectAccessReviews(),
			userInfo,
			[]searchv1alpha1.TargetResource{
				target("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				target("networking.miloapis.com", "v1alpha1", "Domain"),
				target("networking.miloapis.com", "v1alpha1", "DNSZone"),
			},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := len(seen()); got != 3 {
			t.Fatalf("expected 3 SARs, got %d", got)
		}
	})

	t.Run("multi-target second-denied short-circuits", func(t *testing.T) {
		seenCalls := 0
		cs, _ := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			seenCalls++
			if seenCalls == 1 {
				sar.Status.Allowed = true
				return sar, nil
			}
			sar.Status.Allowed = false
			sar.Status.Reason = "forbidden"
			return sar, nil
		})
		err := authorizeTargets(
			context.Background(),
			cs.AuthorizationV1().SubjectAccessReviews(),
			userInfo,
			[]searchv1alpha1.TargetResource{
				target("resourcemanager.miloapis.com", "v1alpha1", "Project"),
				target("networking.miloapis.com", "v1alpha1", "Domain"),
				target("networking.miloapis.com", "v1alpha1", "DNSZone"),
			},
		)
		if err == nil {
			t.Fatal("expected error on second denial")
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("expected Forbidden, got %v", err)
		}
		if seenCalls != 2 {
			t.Fatalf("expected short-circuit after 2 calls, got %d", seenCalls)
		}
	})

	t.Run("SAR API error wraps and bubbles", func(t *testing.T) {
		cs, _ := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
			return nil, errors.New("network is unreachable")
		})
		err := authorizeTargets(
			context.Background(),
			cs.AuthorizationV1().SubjectAccessReviews(),
			userInfo,
			[]searchv1alpha1.TargetResource{target("resourcemanager.miloapis.com", "v1alpha1", "Project")},
		)
		if err == nil {
			t.Fatal("expected error from SAR API failure")
		}
		if apierrors.IsForbidden(err) {
			t.Fatalf("API failure should not be 403, got %v", err)
		}
		if !strings.Contains(err.Error(), "network is unreachable") {
			t.Fatalf("error should wrap cause: %v", err)
		}
	})
}

func TestPluralForKind(t *testing.T) {
	tests := []struct {
		kind string
		want string
	}{
		{"Project", "projects"},
		{"Domain", "domains"},
		{"DNSZone", "dnszones"},
		{"Contact", "contacts"},
		{"Organization", "organizations"},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			if got := pluralForKind(tt.kind); got != tt.want {
				t.Fatalf("pluralForKind(%q) = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}
