package resourcesearchquery

import (
	"context"
	"errors"
	"strings"
	"testing"

	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// fakePlurals is a test implementation of pluralLookup backed by a map.
type fakePlurals map[schema.GroupKind]string

func (f fakePlurals) Lookup(gk schema.GroupKind) (string, bool) {
	p, ok := f[gk]
	return p, ok
}

// fakeSARClient builds a fake SubjectAccessReview client whose Create reactor
// calls the supplied handler for each invocation. Captures every SAR seen.
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

var (
	gkProject = schema.GroupKind{Group: "resourcemanager.miloapis.com", Kind: "Project"}
	gkDomain  = schema.GroupKind{Group: "networking.miloapis.com", Kind: "Domain"}
)

var allKnown = fakePlurals{
	gkProject: "projects",
	gkDomain:  "domains",
}

func TestAuthorizeTargets_NilUser_ReturnsError(t *testing.T) {
	cs, _ := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		t.Fatal("SAR should not be called when user is nil")
		return nil, nil
	})
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("expected error for nil user")
	}
	if !strings.Contains(err.Error(), "no user") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthorizeTargets_EmptyTargets_NoOp(t *testing.T) {
	cs, seen := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		t.Fatal("SAR should not be called when targets is empty")
		return nil, nil
	})
	userInfo := &user.DefaultInfo{Name: "alice"}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
		userInfo,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seen()) != 0 {
		t.Fatalf("expected zero SARs, got %d", len(seen()))
	}
}

func TestAuthorizeTargets_SingleAllowed_PluralPassedToSAR(t *testing.T) {
	cs, seen := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		sar.Status.Allowed = true
		return sar, nil
	})
	userInfo := &user.DefaultInfo{Name: "alice", UID: "uid-1", Groups: []string{"sg"}}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
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
	ra := all[0].Spec.ResourceAttributes
	if ra.Resource != "projects" {
		t.Fatalf("ResourceAttributes.Resource: got %q, want %q (plural from cache)", ra.Resource, "projects")
	}
	if ra.Verb != "list" {
		t.Fatalf("verb: got %q, want \"list\"", ra.Verb)
	}
}

func TestAuthorizeTargets_SingleDenied_ForbiddenWithPlural(t *testing.T) {
	cs, _ := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		sar.Status.Allowed = false
		sar.Status.Reason = "test denial"
		return sar, nil
	})
	userInfo := &user.DefaultInfo{Name: "alice"}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
		userInfo,
		[]searchv1alpha1.TargetResource{target("resourcemanager.miloapis.com", "v1alpha1", "Project")},
	)
	if err == nil {
		t.Fatal("expected error for denied SAR")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected Forbidden, got %v", err)
	}
	// The error message should contain the plural form: "projects.resourcemanager.miloapis.com"
	if !strings.Contains(err.Error(), "projects.resourcemanager.miloapis.com") {
		t.Fatalf("error should mention plural 'projects.resourcemanager.miloapis.com', got: %v", err)
	}
}

func TestAuthorizeTargets_UnknownKind_403WithoutSAR(t *testing.T) {
	cs, seen := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		t.Fatal("SAR should not be called when kind is unknown to the plural cache")
		return nil, nil
	})
	emptyPlurals := fakePlurals{}
	userInfo := &user.DefaultInfo{Name: "alice"}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		emptyPlurals,
		userInfo,
		[]searchv1alpha1.TargetResource{target("resourcemanager.miloapis.com", "v1alpha1", "Project")},
	)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected Forbidden for unknown kind, got %v", err)
	}
	if !strings.Contains(err.Error(), "unknown resource kind") {
		t.Fatalf("error should mention 'unknown resource kind', got: %v", err)
	}
	if len(seen()) != 0 {
		t.Fatalf("SAR should not be created for unknown kind; got %d SARs", len(seen()))
	}
}

func TestAuthorizeTargets_MultiTarget_UnknownInPositionTwo_ShortCircuits(t *testing.T) {
	cs, seen := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		sar.Status.Allowed = true
		return sar, nil
	})
	// Project is known, but Widget is not.
	plurals := fakePlurals{gkProject: "projects"}
	userInfo := &user.DefaultInfo{Name: "alice"}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		plurals,
		userInfo,
		[]searchv1alpha1.TargetResource{
			target("resourcemanager.miloapis.com", "v1alpha1", "Project"),
			target("g.example.com", "v1alpha1", "Widget"),
		},
	)
	if err == nil {
		t.Fatal("expected error for unknown second target")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected Forbidden, got %v", err)
	}
	if len(seen()) != 1 {
		t.Fatalf("expected exactly 1 SAR (for target 1, then short-circuit on target 2); got %d", len(seen()))
	}
}

func TestAuthorizeTargets_SARAPIError_Wrapped(t *testing.T) {
	cs, _ := fakeSARClient(t, func(*authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		return nil, errors.New("network is unreachable")
	})
	userInfo := &user.DefaultInfo{Name: "alice"}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
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
		t.Fatalf("error should wrap cause, got: %v", err)
	}
}

func TestAuthorizeTargets_ExtraPropagatedToSAR(t *testing.T) {
	cs, seen := fakeSARClient(t, func(sar *authzv1.SubjectAccessReview) (*authzv1.SubjectAccessReview, error) {
		sar.Status.Allowed = true
		return sar, nil
	})
	userInfo := &user.DefaultInfo{
		Name: "alice",
		Extra: map[string][]string{
			"iam.miloapis.com/parent-type": {"Project"},
			"iam.miloapis.com/parent-name": {"acme"},
		},
	}
	err := authorizeTargets(
		context.Background(),
		cs.AuthorizationV1().SubjectAccessReviews(),
		allKnown,
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
	got := all[0].Spec.Extra["iam.miloapis.com/parent-type"]
	if len(got) != 1 || got[0] != "Project" {
		t.Fatalf("parent-type extra not propagated: got %+v", got)
	}
}
