package main

import (
	"encoding/json"
	"strings"
	"testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func readyKinds() []searchv1alpha1.ResourceIndexPolicy {
	return []searchv1alpha1.ResourceIndexPolicy{
		mkPolicy("Workload", "compute.datum.net", "v1alpha1", true),
		mkPolicy("HTTPRoute", "gateway.networking.k8s.io", "v1", false),
		mkPolicy("ConfigMap", "", "v1", true),
		mkPolicy("Project", "resourcemanager.miloapis.com", "v1alpha1", true),
	}
}

func TestQueryTableRendersHeadlineAndRows(t *testing.T) {
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "payments-api", "default", "net-core", "project", 0.98),
				mkResult("resourcemanager.miloapis.com/v1alpha1", "Project", "payments-sandbox", "", "platform", "platform", 0.88),
			}, nil, ""),
		},
		policies: readyKinds(),
	}
	opts := &globalOptions{output: outputTable, color: "never", org: "acme", project: "net-core"}
	out, _, err := execQuery(fake, opts, "payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `2 results for "payments" in acme / net-core`) {
		t.Errorf("headline wrong:\n%s", out)
	}
	if !strings.Contains(out, "kinds searched)") {
		t.Errorf("headline missing kinds-searched count:\n%s", out)
	}
	for _, want := range []string{"KIND", "NAME", "NAMESPACE", "TENANT", "SCORE", "AGE", "payments-api", "0.98", "net-core"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q:\n%s", want, out)
		}
	}
	// Empty namespace renders as a dash, not a blank cell.
	if !strings.Contains(out, "—") {
		t.Errorf("expected an em-dash for the cluster-scoped Project's namespace:\n%s", out)
	}
}

func TestQueryOutputName(t *testing.T) {
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "payments-api", "default", "net-core", "project", 0.98),
				mkResult("v1", "ConfigMap", "payments-config", "default", "net-core", "project", 0.91),
			}, nil, ""),
		},
	}
	out, _, err := execQuery(fake, &globalOptions{output: outputName, color: "never"}, "payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Fields(strings.TrimSpace(out))
	if len(lines) != 2 {
		t.Fatalf("expected 2 name lines, got %d: %q", len(lines), out)
	}
	if lines[0] != "workload.compute.datum.net/payments-api" {
		t.Errorf("line 0 = %q", lines[0])
	}
	if lines[1] != "configmap/payments-config" {
		t.Errorf("core-group name should omit the group: %q", lines[1])
	}
}

func TestQueryOutputJSONCarriesObject(t *testing.T) {
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "payments-api", "default", "net-core", "project", 0.98),
			}, nil, "next-token"),
		},
	}
	out, _, err := execQuery(fake, &globalOptions{output: outputJSON, color: "never"}, "payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, out)
	}
	if obj["kind"] != "ResourceSearchQuery" {
		t.Errorf("json missing kind: %v", obj["kind"])
	}
	status, _ := obj["status"].(map[string]any)
	if status["continue"] != "next-token" {
		t.Errorf("single-page json should expose the continue token verbatim, got %v", status["continue"])
	}
}

func TestQueryWideColumns(t *testing.T) {
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "payments-api", "default", "net-core", "project", 0.98),
			}, nil, ""),
		},
		policies: readyKinds(),
	}
	out, _, err := execQuery(fake, &globalOptions{output: outputWide, color: "never", org: "acme", project: "net-core"}, "payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"GROUP", "VERSION", "TENANT-TYPE", "compute.datum.net", "v1alpha1", "project"} {
		if !strings.Contains(out, want) {
			t.Errorf("wide table missing %q:\n%s", want, out)
		}
	}
}

func TestQueryAllMergesPages(t *testing.T) {
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "a", "default", "net-core", "project", 0.9),
			}, nil, "t1"),
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "b", "default", "net-core", "project", 0.8),
			}, nil, "t2"),
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "c", "default", "net-core", "project", 0.7),
			}, nil, ""),
		},
	}
	out, _, err := execQuery(fake, &globalOptions{output: outputJSON, color: "never"}, "payments", "--all")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.searchCalls != 3 {
		t.Errorf("expected 3 Search calls following continue tokens, got %d", fake.searchCalls)
	}
	// The second and third calls must carry the prior page's continue token.
	if len(fake.sent) == 3 {
		if fake.sent[1].Spec.Continue != "t1" || fake.sent[2].Spec.Continue != "t2" {
			t.Errorf("continue tokens not threaded: %q, %q", fake.sent[1].Spec.Continue, fake.sent[2].Spec.Continue)
		}
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, out)
	}
	status := obj["status"].(map[string]any)
	results := status["results"].([]any)
	if len(results) != 3 {
		t.Errorf("expected 3 merged results, got %d", len(results))
	}
	if status["continue"] != nil && status["continue"] != "" {
		t.Errorf("merged --all object must have an empty continue, got %v", status["continue"])
	}
}

func TestQueryStrictDeniedFailsExit7(t *testing.T) {
	denied := []searchv1alpha1.TargetResource{{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}}
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "checkout-api", "default", "net-core", "project", 0.97),
			}, denied, ""),
		},
		policies: readyKinds(),
	}
	_, _, err := execQuery(fake, &globalOptions{output: outputJSON, color: "never"}, "checkout", "--kind", "Workload", "--kind", "HTTPRoute", "--strict")
	ce, ok := err.(*cliError)
	if !ok {
		t.Fatalf("expected *cliError, got %T: %v", err, err)
	}
	if ce.code != exitPartial {
		t.Fatalf("code = %d, want %d", ce.code, exitPartial)
	}
	if !strings.Contains(ce.msg, "HTTPRoute") {
		t.Errorf("strict error should name the denied kind: %q", ce.msg)
	}
}

func TestQueryDeniedWarningNonStrictExit0(t *testing.T) {
	denied := []searchv1alpha1.TargetResource{{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}}
	fake := &fakeClient{
		pages: []*searchv1alpha1.ResourceSearchQuery{
			mkPage([]searchv1alpha1.SearchResult{
				mkResult("compute.datum.net/v1alpha1", "Workload", "checkout-api", "default", "net-core", "project", 0.97),
			}, denied, ""),
		},
		policies: readyKinds(),
	}
	_, errOut, err := execQuery(fake, &globalOptions{output: outputTable, color: "never", org: "acme", project: "net-core"},
		"checkout", "--kind", "Workload", "--kind", "HTTPRoute")
	if err != nil {
		t.Fatalf("non-strict denied kinds must not fail; got: %v", err)
	}
	if !strings.Contains(errOut, "1 of the 2 kinds you asked for") {
		t.Errorf("warning should count denied of requested: %q", errOut)
	}
	if !strings.Contains(errOut, "HTTPRoute") || !strings.Contains(errOut, "datumctl search kinds") {
		t.Errorf("warning should name the skipped kind and the coverage command: %q", errOut)
	}
}

func TestQueryUnknownKindExit4(t *testing.T) {
	fake := &fakeClient{policies: readyKinds()}
	_, _, err := execQuery(fake, &globalOptions{output: outputTable, color: "never"}, "x", "--kind", "Wrkload")
	ce, ok := err.(*cliError)
	if !ok {
		t.Fatalf("expected *cliError, got %T: %v", err, err)
	}
	if ce.code != exitNotFound {
		t.Fatalf("code = %d, want %d", ce.code, exitNotFound)
	}
	if fake.searchCalls != 0 {
		t.Errorf("an unknown kind must fail before any Search call, got %d calls", fake.searchCalls)
	}
}

func TestQueryZeroResultsExit0(t *testing.T) {
	fake := &fakeClient{
		pages:    []*searchv1alpha1.ResourceSearchQuery{mkPage(nil, nil, "")},
		policies: readyKinds(),
	}
	out, _, err := execQuery(fake, &globalOptions{output: outputTable, color: "never", org: "acme", project: "net-core"}, "nothing")
	if err != nil {
		t.Fatalf("zero matches is success; got: %v", err)
	}
	if !strings.Contains(out, `0 results for "nothing"`) {
		t.Errorf("expected a zero-results headline: %q", out)
	}
}

func TestScopeLabels(t *testing.T) {
	// Datum platform scope (no org/project) must read naturally, not leak the
	// kubeconfig-mode "current cluster" wording.
	datum := &app{opts: &globalOptions{}, resolvedMode: modeDatum}
	if got := datum.scopeLocative(); got != "across the platform" {
		t.Errorf("datum platform locative = %q, want %q", got, "across the platform")
	}
	if got := datum.scopeName(); got != "the platform" {
		t.Errorf("datum platform name = %q, want %q", got, "the platform")
	}
	// Dev/e2e kubeconfig scope keeps the cluster wording.
	kube := &app{opts: &globalOptions{}, resolvedMode: modeKubeconfig}
	if got := kube.scopeLocative(); got != "on the current cluster" {
		t.Errorf("kubeconfig locative = %q, want %q", got, "on the current cluster")
	}
	// Project scope.
	proj := &app{opts: &globalOptions{org: "acme", project: "net-core"}}
	if got := proj.scopeLocative(); got != "in acme / net-core" {
		t.Errorf("project locative = %q, want %q", got, "in acme / net-core")
	}
}

func TestNextPageCommand(t *testing.T) {
	got := nextPageCommand("payments", []string{"Workload", "HTTPRoute"}, 25, true, "TOK")
	want := `datumctl search "payments" --kind Workload --kind HTTPRoute --limit 25 --continue TOK`
	if got != want {
		t.Errorf("nextPageCommand = %q, want %q", got, want)
	}
	// No limit set and no kinds: minimal command.
	if got := nextPageCommand("payments", nil, 0, false, "TOK"); got != `datumctl search "payments" --continue TOK` {
		t.Errorf("minimal nextPageCommand = %q", got)
	}
}
