package resourcesearchquery

import (
	"context"
	"fmt"

	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	typedauthzv1 "k8s.io/client-go/kubernetes/typed/authorization/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// pluralLookup resolves a (group, kind) pair to its lowercase plural resource
// name. Implemented by *indexer.CRDPluralCache in production; satisfied by a
// test fake in unit tests.
type pluralLookup interface {
	Lookup(gk schema.GroupKind) (string, bool)
}

// authorizeTargets runs a SubjectAccessReview per TargetResource (verb "list").
// Fail-closed on three paths:
//   - unknown kind (no CRD in cache) → 403 Forbidden
//   - SAR denial                     → 403 Forbidden
//   - SAR API call error             → wrapped error (5xx)
//
// An empty targets slice is a no-op (zero SARs).
func authorizeTargets(
	ctx context.Context,
	sars typedauthzv1.SubjectAccessReviewInterface,
	plurals pluralLookup,
	userInfo user.Info,
	targets []searchv1alpha1.TargetResource,
) error {
	if userInfo == nil {
		return fmt.Errorf("authorization check: no user in request context")
	}
	extra := convertExtra(userInfo.GetExtra())

	for _, t := range targets {
		gk := schema.GroupKind{Group: t.Group, Kind: t.Kind}
		plural, ok := plurals.Lookup(gk)
		if !ok {
			// No CRD registered for this kind → no policy can target it.
			// Fail closed. Resource field uses t.Kind because we don't have
			// a canonical plural to cite; the message is explicit about why.
			return apierrors.NewForbidden(
				schema.GroupResource{Group: t.Group, Resource: t.Kind},
				"",
				fmt.Errorf("unknown resource kind %s/%s/%s", t.Group, t.Version, t.Kind),
			)
		}

		sar := &authzv1.SubjectAccessReview{
			Spec: authzv1.SubjectAccessReviewSpec{
				User:   userInfo.GetName(),
				UID:    userInfo.GetUID(),
				Groups: userInfo.GetGroups(),
				Extra:  extra,
				ResourceAttributes: &authzv1.ResourceAttributes{
					Group:    t.Group,
					Version:  t.Version,
					Resource: plural,
					Verb:     "list",
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
				schema.GroupResource{Group: t.Group, Resource: plural},
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
