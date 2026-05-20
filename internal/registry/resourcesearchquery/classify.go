package resourcesearchquery

import (
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// classifyTargets partitions the requested target resources into two groups:
//
//   - allowed: targets for which a ready ResourceIndexPolicy exists (non-empty
//     Status.IndexName). When targets is empty every ready policy's target
//     resource is included in allowed (mirrors the existing "all policies"
//     short-circuit in resolveIndexUIDs).
//
//   - denied: targets that have no matching ready policy. Callers surface these
//     via status.deniedTargetResources instead of failing the request.
//
// Duplicate entries in targets are deduplicated: each unique (Group, Version,
// Kind) triple appears at most once in the output slices.
func classifyTargets(
	targets []searchv1alpha1.TargetResource,
	policies []*policyevaluation.CachedPolicy,
) (allowed, denied []searchv1alpha1.TargetResource) {
	if len(targets) == 0 {
		// No explicit targets: return all ready policies as allowed, none denied.
		for _, cp := range policies {
			if cp.Policy.Status.IndexName != "" {
				allowed = append(allowed, cp.Policy.Spec.TargetResource)
			}
		}
		return allowed, nil
	}

	// Dedup targets while preserving first-seen order.
	seen := make(map[searchv1alpha1.TargetResource]bool, len(targets))
	unique := make([]searchv1alpha1.TargetResource, 0, len(targets))
	for _, t := range targets {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	for _, t := range unique {
		matched := false
		for _, cp := range policies {
			p := cp.Policy
			if p.Spec.TargetResource.Group == t.Group &&
				p.Spec.TargetResource.Version == t.Version &&
				p.Spec.TargetResource.Kind == t.Kind &&
				p.Status.IndexName != "" {
				matched = true
				break
			}
		}
		if matched {
			allowed = append(allowed, t)
		} else {
			denied = append(denied, t)
		}
	}
	return allowed, denied
}

// indexUIDsFor extracts the Meilisearch index UIDs for a slice of allowed
// target resources. It looks up each target against the policy list and
// returns the corresponding Status.IndexName values in the same order as
// allowed. Targets with no matching ready policy are silently skipped (this
// should not happen in practice because classifyTargets already filtered them,
// but the function is defensive).
func indexUIDsFor(
	allowed []searchv1alpha1.TargetResource,
	policies []*policyevaluation.CachedPolicy,
) []string {
	if len(allowed) == 0 {
		return nil
	}
	uids := make([]string, 0, len(allowed))
	for _, t := range allowed {
		for _, cp := range policies {
			p := cp.Policy
			if p.Spec.TargetResource.Group == t.Group &&
				p.Spec.TargetResource.Version == t.Version &&
				p.Spec.TargetResource.Kind == t.Kind &&
				p.Status.IndexName != "" {
				uids = append(uids, p.Status.IndexName)
				break
			}
		}
	}
	return uids
}
