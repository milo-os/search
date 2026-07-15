package main

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestEnsureScheme(t *testing.T) {
	cases := map[string]string{
		"api.datum.net":         "https://api.datum.net",
		"https://api.datum.net": "https://api.datum.net",
		"http://localhost:8443": "http://localhost:8443",
		"":                      "",
	}
	for in, want := range cases {
		if got := ensureScheme(in); got != want {
			t.Errorf("ensureScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestControlPlaneHost(t *testing.T) {
	const base = "api.staging.env.datum.net"
	cases := []struct {
		name string
		env  datumEnv
		want string
	}{
		{
			name: "project scopes to the project control plane",
			env:  datumEnv{apiHost: base, project: "datum-cloud"},
			want: "https://api.staging.env.datum.net/apis/resourcemanager.miloapis.com/v1alpha1/projects/datum-cloud/control-plane",
		},
		{
			name: "org (no project) scopes to the org control plane",
			env:  datumEnv{apiHost: base, org: "datum-technology"},
			want: "https://api.staging.env.datum.net/apis/resourcemanager.miloapis.com/v1alpha1/organizations/datum-technology/control-plane",
		},
		{
			name: "project wins over org",
			env:  datumEnv{apiHost: base, org: "datum-technology", project: "datum-cloud"},
			want: "https://api.staging.env.datum.net/apis/resourcemanager.miloapis.com/v1alpha1/projects/datum-cloud/control-plane",
		},
		{
			name: "neither set uses the platform root",
			env:  datumEnv{apiHost: base},
			want: "https://api.staging.env.datum.net",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := controlPlaneHost(tc.env); got != tc.want {
				t.Errorf("controlPlaneHost() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestChooseModeAndUsable(t *testing.T) {
	cases := []struct {
		name           string
		kubeconfigFlag string
		env            datumEnv
		want           transportMode
	}{
		{
			name: "datum env complete selects datum mode",
			env:  datumEnv{apiHost: "api.example.test", credHelper: "/x/datumctl", project: "p"},
			want: modeDatum,
		},
		{
			name: "missing helper falls back to kubeconfig",
			env:  datumEnv{apiHost: "api.example.test"},
			want: modeKubeconfig,
		},
		{
			name:           "explicit --kubeconfig forces kubeconfig even with datum env",
			kubeconfigFlag: "/tmp/kubeconfig",
			env:            datumEnv{apiHost: "api.example.test", credHelper: "/x/datumctl"},
			want:           modeKubeconfig,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := chooseMode(tc.kubeconfigFlag, tc.env); got != tc.want {
				t.Errorf("chooseMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

// writeFakeHelper writes an executable that mimics the datumctl credentials
// helper: `<helper> auth get-token` prints a token to stdout.
func writeFakeHelper(t *testing.T, token string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake credentials helper script is POSIX-only")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "fake-helper")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"auth\" ] && [ \"$2\" = \"get-token\" ]; then\n" +
		"  printf '%s' '" + token + "'\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake helper: %v", err)
	}
	return path
}

func setDatumEnv(t *testing.T, host, org, project, helper string) {
	t.Helper()
	t.Setenv("DATUM_API_HOST", host)
	t.Setenv("DATUM_ORG", org)
	t.Setenv("DATUM_PROJECT", project)
	t.Setenv("DATUM_CREDENTIALS_HELPER", helper)
	t.Setenv("DATUM_SESSION", "")
}

func TestDatumRestConfigUsesSDKToken(t *testing.T) {
	helper := writeFakeHelper(t, "sdk-token-abc")
	setDatumEnv(t, "api.example.test", "acme", "proj-1", helper)

	env := readDatumEnv()
	cfg, ns, err := datumRestConfig(env)
	if err != nil {
		t.Fatalf("datumRestConfig() error = %v", err)
	}
	if cfg.BearerToken != "sdk-token-abc" {
		t.Errorf("BearerToken = %q, want token from credentials helper", cfg.BearerToken)
	}
	wantHost := "https://api.example.test/apis/resourcemanager.miloapis.com/v1alpha1/projects/proj-1/control-plane"
	if cfg.Host != wantHost {
		t.Errorf("Host = %q, want %q", cfg.Host, wantHost)
	}
	if ns != "default" {
		t.Errorf("namespace = %q, want default", ns)
	}
}

func TestDatumRestConfigRequiresUsableEnv(t *testing.T) {
	_, _, err := datumRestConfig(datumEnv{})
	if err == nil {
		t.Fatal("datumRestConfig() with empty env: expected error, got nil")
	}
	ce, ok := err.(*cliError)
	if !ok || ce.code != exitUnavailable {
		t.Fatalf("expected *cliError exitUnavailable, got %T %v", err, err)
	}
}

func TestKubeconfigRestConfigPreserved(t *testing.T) {
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "config")
	const content = `apiVersion: v1
kind: Config
clusters:
- name: dev
  cluster:
    server: https://kube.example.test:6443
contexts:
- name: dev
  context:
    cluster: dev
    user: dev
    namespace: search-dev
current-context: dev
users:
- name: dev
  user:
    token: kubeconfig-token
`
	if err := os.WriteFile(kubeconfig, []byte(content), 0o600); err != nil {
		t.Fatalf("writing kubeconfig: %v", err)
	}

	cfg, ns, err := kubeconfigRestConfig(kubeconfig)
	if err != nil {
		t.Fatalf("kubeconfigRestConfig() error = %v", err)
	}
	if cfg.Host != "https://kube.example.test:6443" {
		t.Errorf("Host = %q, want kubeconfig server", cfg.Host)
	}
	if cfg.BearerToken != "kubeconfig-token" {
		t.Errorf("BearerToken = %q, want kubeconfig token", cfg.BearerToken)
	}
	if ns != "search-dev" {
		t.Errorf("namespace = %q, want search-dev from context", ns)
	}
}

// stubRoundTripper returns a canned response so the logging wrapper can be
// tested without a real server.
type stubRoundTripper struct {
	status string
	code   int
	err    error
}

func (s stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &http.Response{
		StatusCode: s.code,
		Status:     s.status,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func TestLoggingRoundTripperLogsCallAndStatus(t *testing.T) {
	var buf bytes.Buffer
	rt := verboseTransport(&buf)(stubRoundTripper{status: "200 OK", code: 200})
	req, _ := http.NewRequest("POST", "https://api.example/apis/search.miloapis.com/v1alpha1/resourcesearchqueries", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"API call:", "POST", "/apis/search.miloapis.com/v1alpha1/resourcesearchqueries", "200 OK"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
}
