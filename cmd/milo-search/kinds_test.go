package main

import (
	"encoding/json"
	"strings"
	"testing"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func TestKindsTable(t *testing.T) {
	fake := &fakeClient{policies: readyKinds()}
	out, _, err := execKinds(fake, &globalOptions{output: outputTable, color: "never"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"KIND", "GROUP", "VERSION", "READY", "AGE", "Workload", "compute.datum.net", "True", "False"} {
		if !strings.Contains(out, want) {
			t.Errorf("kinds table missing %q:\n%s", want, out)
		}
	}
	// Core group renders as "(core)".
	if !strings.Contains(out, "(core)") {
		t.Errorf("ConfigMap should show group (core):\n%s", out)
	}
}

func TestKindsWideAddsPolicyAndIndex(t *testing.T) {
	fake := &fakeClient{policies: readyKinds()}
	out, _, err := execKinds(fake, &globalOptions{output: outputWide, color: "never"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"POLICY", "INDEX", "workload-policy", "idx-workload"} {
		if !strings.Contains(out, want) {
			t.Errorf("wide kinds table missing %q:\n%s", want, out)
		}
	}
}

func TestKindsOutputNameGivesKindRefs(t *testing.T) {
	fake := &fakeClient{policies: readyKinds()}
	out, _, err := execKinds(fake, &globalOptions{output: outputName, color: "never"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Workload.compute.datum.net") {
		t.Errorf("expected a qualified kind ref usable as --kind:\n%s", out)
	}
	if !strings.Contains(out, "\nConfigMap\n") && !strings.HasPrefix(out, "ConfigMap\n") {
		t.Errorf("core-group kind ref should be bare 'ConfigMap':\n%s", out)
	}
}

func TestKindsJSONIsPolicyList(t *testing.T) {
	fake := &fakeClient{policies: readyKinds()}
	out, _, err := execKinds(fake, &globalOptions{output: outputJSON, color: "never"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, out)
	}
	if obj["kind"] != "ResourceIndexPolicyList" {
		t.Errorf("json kind = %v, want ResourceIndexPolicyList", obj["kind"])
	}
}

func TestKindsEmptyExit0(t *testing.T) {
	fake := &fakeClient{policies: []searchv1alpha1.ResourceIndexPolicy{}}
	_, errOut, err := execKinds(fake, &globalOptions{output: outputTable, color: "never"})
	if err != nil {
		t.Fatalf("empty kinds is not an error; got: %v", err)
	}
	if !strings.Contains(errOut, "Nothing is searchable yet") {
		t.Errorf("expected an empty-state note on stderr: %q", errOut)
	}
}

func TestKindsForbiddenExit3(t *testing.T) {
	fake := &fakeClient{policyErr: statusErr(403, "Forbidden", "no access")}
	_, _, err := execKinds(fake, &globalOptions{output: outputTable, color: "never"})
	ce, ok := err.(*cliError)
	if !ok {
		t.Fatalf("expected *cliError, got %T: %v", err, err)
	}
	if ce.code != exitForbidden {
		t.Fatalf("code = %d, want %d", ce.code, exitForbidden)
	}
}
