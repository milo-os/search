package indexer

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeRESTMapper is a hand-rolled meta.RESTMapper for the test. We only need
// RESTMappings; everything else returns "not implemented" to surface accidental
// callers loudly.
type fakeRESTMapper struct {
	known       map[schema.GroupKind]string
	errOnLookup error
}

func (f *fakeRESTMapper) RESTMappings(gk schema.GroupKind, _ ...string) ([]*meta.RESTMapping, error) {
	if f.errOnLookup != nil {
		return nil, f.errOnLookup
	}
	plural, ok := f.known[gk]
	if !ok {
		return nil, &meta.NoKindMatchError{GroupKind: gk}
	}
	return []*meta.RESTMapping{{
		Resource: schema.GroupVersionResource{Group: gk.Group, Resource: plural},
	}}, nil
}

func (f *fakeRESTMapper) KindFor(schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, errors.New("not implemented")
}
func (f *fakeRESTMapper) KindsFor(schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeRESTMapper) ResourceFor(schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, errors.New("not implemented")
}
func (f *fakeRESTMapper) ResourcesFor(schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeRESTMapper) RESTMapping(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeRESTMapper) ResourceSingularizer(string) (string, error) {
	return "", errors.New("not implemented")
}

func TestFallbackPluralLookup_Tier1_CRDCacheHit(t *testing.T) {
	t.Parallel()
	crdCache := newTestCache()
	crdCache.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgets"))
	mapper := &fakeRESTMapper{} // empty; should never be consulted

	f := NewFallbackPluralLookup(crdCache, mapper)
	got, ok := f.Lookup(schema.GroupKind{Group: "g.example.com", Kind: "Widget"})
	if !ok || got != "widgets" {
		t.Fatalf("Tier 1 hit: got (%q, %v), want (\"widgets\", true)", got, ok)
	}
}

func TestFallbackPluralLookup_Tier2_RESTMapperHit(t *testing.T) {
	t.Parallel()
	crdCache := newTestCache() // empty — Tier 1 miss
	mapper := &fakeRESTMapper{
		known: map[schema.GroupKind]string{
			{Group: "resourcemanager.miloapis.com", Kind: "Project"}: "projects",
		},
	}

	f := NewFallbackPluralLookup(crdCache, mapper)
	got, ok := f.Lookup(schema.GroupKind{Group: "resourcemanager.miloapis.com", Kind: "Project"})
	if !ok || got != "projects" {
		t.Fatalf("Tier 2 hit: got (%q, %v), want (\"projects\", true)", got, ok)
	}
}

func TestFallbackPluralLookup_Tier3_ConventionalFallback(t *testing.T) {
	t.Parallel()
	crdCache := newTestCache()  // empty — Tier 1 miss
	mapper := &fakeRESTMapper{} // empty — Tier 2 returns NoKindMatchError

	f := NewFallbackPluralLookup(crdCache, mapper)
	got, ok := f.Lookup(schema.GroupKind{Group: "unknown.example.com", Kind: "Gizmo"})
	if !ok || got != "gizmos" {
		t.Fatalf("Tier 3 fallback: got (%q, %v), want (\"gizmos\", true) — lowercase Kind + \"s\"", got, ok)
	}
}

func TestFallbackPluralLookup_Tier3_OnRESTMapperError(t *testing.T) {
	t.Parallel()
	// When the REST mapper returns a non-NoKindMatch error (e.g. transient
	// discovery failure), Lookup must still fall through to Tier 3 rather than
	// propagate the error. The caller's SAR will still gate access.
	crdCache := newTestCache()
	mapper := &fakeRESTMapper{errOnLookup: errors.New("discovery unavailable")}

	f := NewFallbackPluralLookup(crdCache, mapper)
	got, ok := f.Lookup(schema.GroupKind{Group: "g.example.com", Kind: "Thing"})
	if !ok || got != "things" {
		t.Fatalf("Tier 3 on mapper error: got (%q, %v), want (\"things\", true)", got, ok)
	}
}

func TestFallbackPluralLookup_CapitalizedKindLowercased(t *testing.T) {
	t.Parallel()
	// Conventional fallback must lowercase the Kind. "DNSZone" → "dnszones",
	// not "DNSZones". This matches the K8s plural convention (path segment).
	crdCache := newTestCache()
	mapper := &fakeRESTMapper{}

	f := NewFallbackPluralLookup(crdCache, mapper)
	got, ok := f.Lookup(schema.GroupKind{Group: "networking.miloapis.com", Kind: "DNSZone"})
	if !ok || got != "dnszones" {
		t.Fatalf("DNSZone fallback: got (%q, %v), want (\"dnszones\", true)", got, ok)
	}
}
