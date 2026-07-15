package main

import (
	"strings"
	"testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func TestParseKindRef(t *testing.T) {
	cases := []struct {
		in                   string
		kind, group, version string
	}{
		{"Workload", "Workload", "", ""},
		{"Workload.compute.datum.net", "Workload", "compute.datum.net", ""},
		{"Workload.compute.datum.net/v1alpha1", "Workload", "compute.datum.net", "v1alpha1"},
		{"ConfigMap", "ConfigMap", "", ""},
		{"ConfigMap/v1", "ConfigMap", "", "v1"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			k, g, v := parseKindRef(tc.in)
			if k != tc.kind || g != tc.group || v != tc.version {
				t.Fatalf("parseKindRef(%q) = (%q,%q,%q), want (%q,%q,%q)", tc.in, k, g, v, tc.kind, tc.group, tc.version)
			}
		})
	}
}

func kindSet() []indexedKind {
	return indexedKinds([]searchv1alpha1.ResourceIndexPolicy{
		mkPolicy("Workload", "compute.datum.net", "v1alpha1", true),
		mkPolicy("HTTPRoute", "gateway.networking.k8s.io", "v1", false),
		mkPolicy("ConfigMap", "", "v1", true),
		// Same kind in two groups, to exercise ambiguity.
		mkPolicy("Route", "gateway.networking.k8s.io", "v1", true),
		mkPolicy("Route", "networking.datum.net", "v1alpha1", true),
	})
}

func TestResolveKindHappyPath(t *testing.T) {
	kinds := kindSet()
	cases := []struct {
		ref   string
		group string
	}{
		{"Workload", "compute.datum.net"},
		{"workload", "compute.datum.net"},                      // case-insensitive
		{"Workload.compute.datum.net", "compute.datum.net"},    // group-qualified
		{"ConfigMap", ""},                                      // core group
		{"Route.networking.datum.net", "networking.datum.net"}, // disambiguated by group
	}
	for _, tc := range cases {
		t.Run(tc.ref, func(t *testing.T) {
			tr, err := resolveKind(tc.ref, kinds)
			if err != nil {
				t.Fatalf("resolveKind(%q) unexpected error: %v", tc.ref, err)
			}
			if tr.Group != tc.group {
				t.Fatalf("resolveKind(%q).Group = %q, want %q", tc.ref, tr.Group, tc.group)
			}
		})
	}
}

func TestResolveKindUnknownGivesNotFoundWithSuggestion(t *testing.T) {
	_, err := resolveKind("Wrkload", kindSet())
	ce, ok := err.(*cliError)
	if !ok {
		t.Fatalf("expected *cliError, got %T", err)
	}
	if ce.code != exitNotFound {
		t.Fatalf("code = %d, want %d", ce.code, exitNotFound)
	}
	if !strings.Contains(ce.msg, "Workload") {
		t.Errorf("expected a did-you-mean suggesting Workload: %q", ce.msg)
	}
}

func TestResolveKindAmbiguousGivesUsage(t *testing.T) {
	_, err := resolveKind("Route", kindSet())
	ce, ok := err.(*cliError)
	if !ok {
		t.Fatalf("expected *cliError, got %T", err)
	}
	if ce.code != exitUsage {
		t.Fatalf("code = %d, want %d", ce.code, exitUsage)
	}
	if !strings.Contains(ce.msg, "gateway.networking.k8s.io") || !strings.Contains(ce.msg, "networking.datum.net") {
		t.Errorf("ambiguity error should list both candidates: %q", ce.msg)
	}
}

func TestResolveKindNotReadyStillResolves(t *testing.T) {
	// A kind with a policy that is not ready must still resolve (the server, not
	// the client, decides to deny it).
	tr, err := resolveKind("HTTPRoute", kindSet())
	if err != nil {
		t.Fatalf("unexpected error resolving a not-ready kind: %v", err)
	}
	if tr.Kind != "HTTPRoute" || tr.Group != "gateway.networking.k8s.io" {
		t.Fatalf("resolved wrong target: %+v", tr)
	}
}

func TestResourceName(t *testing.T) {
	cases := []struct {
		kind, group, name string
		want              string
	}{
		{"Workload", "compute.datum.net", "payments-api", "workload.compute.datum.net/payments-api"},
		{"ConfigMap", "", "payments-config", "configmap/payments-config"},
	}
	for _, tc := range cases {
		if got := resourceName(tc.kind, tc.group, tc.name); got != tc.want {
			t.Errorf("resourceName(%q,%q,%q) = %q, want %q", tc.kind, tc.group, tc.name, got, tc.want)
		}
	}
}

func TestSplitAPIVersion(t *testing.T) {
	cases := map[string][2]string{
		"compute.datum.net/v1alpha1": {"compute.datum.net", "v1alpha1"},
		"v1":                         {"", "v1"},
	}
	for in, want := range cases {
		g, v := splitAPIVersion(in)
		if g != want[0] || v != want[1] {
			t.Errorf("splitAPIVersion(%q) = (%q,%q), want (%q,%q)", in, g, v, want[0], want[1])
		}
	}
}

func TestPluralize(t *testing.T) {
	cases := map[string]string{
		pluralize(0, "result"): "0 results",
		pluralize(1, "result"): "1 result",
		pluralize(4, "kind"):   "4 kinds",
		pluralize(1, "kind"):   "1 kind",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("pluralize = %q, want %q", got, want)
		}
	}
}

func TestGVLabelAndTargetLabel(t *testing.T) {
	if got := gvLabel("", "v1"); got != "v1" {
		t.Errorf("gvLabel core = %q, want v1", got)
	}
	if got := gvLabel("g", "v1"); got != "g/v1" {
		t.Errorf("gvLabel = %q, want g/v1", got)
	}
	tr := searchv1alpha1.TargetResource{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}
	if got := targetLabel(tr); got != "HTTPRoute (gateway.networking.k8s.io/v1)" {
		t.Errorf("targetLabel = %q", got)
	}
}
