package indexer

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// FallbackPluralLookup resolves a (group, kind) pair to the plural resource
// name using a three-tier strategy:
//
//  1. CRD plural cache (fastest): covers all CRD-backed types watched via the
//     apiextensions.k8s.io informer.
//
//  2. REST mapper (discovery): covers aggregated API server types registered
//     in the kube-apiserver's discovery (e.g. built-in types, milo services
//     that are running and registered as APIServices in this cluster).
//
//  3. Conventional lowercase+s fallback: for resource types that are not
//     present in this cluster at all (e.g. data indexed from an external
//     pipeline whose API server is not registered here). The conventional
//     plural allows a SubjectAccessReview to be constructed so that RBAC can
//     still gate access — if no RBAC rule grants list on the resource, the SAR
//     returns denied and the request is correctly rejected.
//
// This three-tier approach handles the full range of types the search service
// may index: native CRDs, aggregated API server types in-cluster, and types
// whose API servers live outside this cluster.
type FallbackPluralLookup struct {
	crdCache *CRDPluralCache
	mapper   meta.RESTMapper
}

// NewFallbackPluralLookup constructs a FallbackPluralLookup. Both arguments
// are required; mapper should be backed by the kube-apiserver (in-cluster
// config), not by the search apiserver loopback.
func NewFallbackPluralLookup(crdCache *CRDPluralCache, mapper meta.RESTMapper) *FallbackPluralLookup {
	return &FallbackPluralLookup{
		crdCache: crdCache,
		mapper:   mapper,
	}
}

// Lookup returns the plural resource name for a given group/kind.
// It always returns (plural, true) — falling back to the conventional
// lowercase+s plural when neither the CRD cache nor the REST mapper knows
// the type. The caller (authorizeTargets) is responsible for using the plural
// in a SubjectAccessReview; RBAC will deny access if no rule grants list on
// the resulting resource name.
func (f *FallbackPluralLookup) Lookup(gk schema.GroupKind) (string, bool) {
	// Tier 1: CRD informer cache — O(1), no network call.
	if plural, ok := f.crdCache.Lookup(gk); ok {
		return plural, true
	}

	// Tier 2: REST mapper discovery — covers aggregated API server types
	// registered in this cluster's kube-apiserver.
	mappings, err := f.mapper.RESTMappings(gk)
	if err == nil && len(mappings) > 0 {
		// RESTMappings returns all versions; the plural name is stable across them.
		plural := mappings[0].Resource.Resource
		klog.V(4).Infof("FallbackPluralLookup: resolved %s → %q via RESTMapper", gk, plural)
		return plural, true
	}
	if err != nil && !meta.IsNoMatchError(err) {
		klog.V(3).Infof("FallbackPluralLookup: REST mapper error for %s: %v", gk, err)
	}

	// Tier 3: Conventional plural — lowercase kind + "s". Used when the API
	// type is not registered in this cluster (e.g. indexed by an external
	// pipeline). Constructing the SAR with the conventional plural still lets
	// RBAC deny access for unauthorized users; it may produce a false-allow
	// only if a cluster admin has explicitly granted list on an unknown plural,
	// which is an acceptable trade-off for operability.
	conventional := strings.ToLower(gk.Kind) + "s"
	klog.V(4).Infof("FallbackPluralLookup: using conventional plural %q for unknown type %s", conventional, gk)
	return conventional, true
}
