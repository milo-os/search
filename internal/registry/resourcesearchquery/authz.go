package resourcesearchquery

import (
	"context"
	"fmt"
	"strings"

	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	typedauthzv1 "k8s.io/client-go/kubernetes/typed/authorization/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// authorizeTargets runs a SubjectAccessReview per TargetResource (verb "list").
// Returns nil if all checks pass; returns an apierrors.IsForbidden error on the
// first denial; returns a wrapped error on SAR API failures (fail-closed).
// An empty targets slice is a no-op (zero SARs).
func authorizeTargets(
	ctx context.Context,
	sars typedauthzv1.SubjectAccessReviewInterface,
	userInfo user.Info,
	targets []searchv1alpha1.TargetResource,
) error {
	if userInfo == nil {
		return fmt.Errorf("authorization check: no user in request context")
	}
	extra := convertExtra(userInfo.GetExtra())
	for _, t := range targets {
		sar := &authzv1.SubjectAccessReview{
			Spec: authzv1.SubjectAccessReviewSpec{
				User:   userInfo.GetName(),
				UID:    userInfo.GetUID(),
				Groups: userInfo.GetGroups(),
				Extra:  extra,
				ResourceAttributes: &authzv1.ResourceAttributes{
					Group:    t.Group,
					Version:  t.Version,
					Resource: pluralForKind(t.Kind),
					Verb:     "list",
					// Namespace intentionally omitted: cluster-level check.
				},
			},
		}
		resp, err := sars.Create(ctx, sar, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("authorization check failed for %s/%s/%s: %w",
				t.Group, t.Version, t.Kind, err)
		}
		if !resp.Status.Allowed {
			return apierrors.NewForbidden(
				schema.GroupResource{Group: t.Group, Resource: t.Kind},
				"",
				fmt.Errorf("user %q is not authorized to search %s/%s/%s: %s",
					userInfo.GetName(), t.Group, t.Version, t.Kind, resp.Status.Reason),
			)
		}
	}
	return nil
}

// convertExtra translates a user.Info Extra map to the SubjectAccessReview
// Extra shape (a typedef around []string).
func convertExtra(in map[string][]string) map[string]authzv1.ExtraValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]authzv1.ExtraValue, len(in))
	for k, v := range in {
		out[k] = authzv1.ExtraValue(v)
	}
	return out
}

// pluralForKind returns the RBAC plural resource name for a Kind.
// v1: naive lowercase + "s". Adequate for all currently-indexed kinds
// (Project, Domain, DNSZone, Contact, Organization). Replace with a discovery
// client when we need to handle irregular plurals.
func pluralForKind(kind string) string {
	return strings.ToLower(kind) + "s"
}
