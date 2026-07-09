package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func TestResolveColorPrecedence(t *testing.T) {
	var buf bytes.Buffer // not a *os.File, so "auto" resolves to no color
	cases := []struct {
		name   string
		mode   string
		output string
		want   bool
	}{
		{"always table", "always", outputTable, true},
		{"never table", "never", outputTable, false},
		{"auto pipe", "auto", outputTable, false},
		{"json never colored", "always", outputJSON, false},
		{"yaml never colored", "always", outputYAML, false},
		{"name never colored", "always", outputName, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveColor(tc.mode, &buf, tc.output)
			if got.enabled != tc.want {
				t.Fatalf("resolveColor(%s,%s).enabled = %v, want %v", tc.mode, tc.output, got.enabled, tc.want)
			}
		})
	}
}

func TestNoColorEnvDisablesAuto(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var buf bytes.Buffer
	if resolveColor("auto", &buf, outputTable).enabled {
		t.Fatal("NO_COLOR set but color enabled")
	}
}

func TestEncodeJSONIsValidAndCarriesGVK(t *testing.T) {
	q := mkPage([]searchv1alpha1.SearchResult{
		mkResult("compute.datum.net/v1alpha1", "Workload", "payments-api", "default", "net-core", "project", 0.98),
	}, nil, "")
	q.ObjectMeta = metav1.ObjectMeta{Name: "example"}
	setSearchQueryGVK(q)
	var buf bytes.Buffer
	if err := encodeJSON(&buf, q); err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if back["apiVersion"] != searchAPIGroupVersion || back["kind"] != "ResourceSearchQuery" {
		t.Errorf("JSON missing GVK: apiVersion=%v kind=%v", back["apiVersion"], back["kind"])
	}
}

func TestPluginManifest(t *testing.T) {
	m := pluginManifest()
	if m.Name != "search" || m.APIVersion != 1 || m.MinAPIVersion != 1 {
		t.Fatalf("unexpected manifest: %+v", m)
	}

	// Guard the datumctl contract: api_version fields are integers. Emitting
	// min_api_version as a JSON string is what broke `datumctl plugin install`
	// (it unmarshals into an int field). Serialize and assert the numeric form.
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"min_api_version":1`) {
		t.Fatalf("min_api_version must serialize as an integer, got: %s", b)
	}
	if !strings.Contains(string(b), `"api_version":1`) {
		t.Fatalf("api_version must serialize as an integer, got: %s", b)
	}
}
