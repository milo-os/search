package indexer

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type fixedProgress struct{ at time.Time }

func (f fixedProgress) LastProgress() time.Time { return f.at }

func probe(t *testing.T, p *probes, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestReadyz_PassesOnlyOnceCacheSyncedAndConsumersAttached(t *testing.T) {
	p := newProbes(time.Minute)

	code, body := probe(t, p, "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "cache") || !strings.Contains(body, "consumers") {
		t.Fatalf("before startup: got %d %q, want 503 naming both the cache and the consumers", code, body)
	}

	p.cacheSynced.Store(true)
	code, body = probe(t, p, "/readyz")
	if code != http.StatusServiceUnavailable || strings.Contains(body, "cache") || !strings.Contains(body, "consumers") {
		t.Fatalf("cache synced only: got %d %q, want 503 naming only the consumers", code, body)
	}

	p.consumersAttached.Store(true)
	if code, body = probe(t, p, "/readyz"); code != http.StatusOK {
		t.Fatalf("cache synced and consumers attached: got %d %q, want 200", code, body)
	}
}

func TestHealthz_FailsOnlyWhenABatcherStalls(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	p := newProbes(30 * time.Minute)
	p.now = func() time.Time { return now }

	if code, body := probe(t, p, "/healthz"); code != http.StatusOK {
		t.Fatalf("during startup, before any batcher runs: got %d %q, want 200", code, body)
	}

	p.watch("audit", fixedProgress{})
	if code, body := probe(t, p, "/healthz"); code != http.StatusOK {
		t.Fatalf("batcher not started yet: got %d %q, want 200", code, body)
	}

	p.watch("audit", fixedProgress{at: now.Add(-30 * time.Minute)})
	p.watch("reindex", fixedProgress{at: now.Add(-time.Second)})
	if code, body := probe(t, p, "/healthz"); code != http.StatusOK {
		t.Fatalf("progress exactly at the threshold: got %d %q, want 200", code, body)
	}

	p.watch("audit", fixedProgress{at: now.Add(-31 * time.Minute)})
	code, body := probe(t, p, "/healthz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "audit") || strings.Contains(body, "reindex") {
		t.Fatalf("audit batcher stalled: got %d %q, want 503 naming only the audit batcher", code, body)
	}
}

func TestResourceIndexerOptions_Validate_LivenessStallFloor(t *testing.T) {
	t.Setenv("MEILISEARCH_API_KEY", "test-key")

	o := NewResourceIndexerOptions()
	if err := o.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	o.LivenessStallThreshold = o.livenessStallFloor()
	if err := o.Validate(); err != nil {
		t.Fatalf("threshold at the floor must validate: %v", err)
	}

	o.LivenessStallThreshold = o.livenessStallFloor() - time.Second
	if err := o.Validate(); err == nil {
		t.Fatal("a threshold under the longest legitimate flush must be rejected")
	}

	o = NewResourceIndexerOptions()
	o.MeilisearchTaskWaitTimeout = time.Hour
	if err := o.Validate(); err == nil {
		t.Fatal("raising the task wait timeout past the default threshold must be rejected")
	}
}

type deploymentManifest struct {
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Name  string `yaml:"name"`
					Ports []struct {
						Name          string `yaml:"name"`
						ContainerPort int    `yaml:"containerPort"`
					} `yaml:"ports"`
					ReadinessProbe *httpProbe `yaml:"readinessProbe"`
					LivenessProbe  *httpProbe `yaml:"livenessProbe"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type httpProbe struct {
	HTTPGet struct {
		Path string `yaml:"path"`
		Port string `yaml:"port"`
	} `yaml:"httpGet"`
}

func TestDeploymentProbes_MatchServedEndpoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not resolve this test file's path")
	}
	manifestPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "config", "base", "resource-indexer", "deployment.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", manifestPath, err)
	}

	var m deploymentManifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		t.Fatalf("decoding %s: %v", manifestPath, err)
	}

	_, portText, err := net.SplitHostPort(NewResourceIndexerOptions().HealthProbeBindAddress)
	if err != nil {
		t.Fatal(err)
	}
	defaultPort, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range m.Spec.Template.Spec.Containers {
		if c.Name != "indexer" {
			continue
		}
		portNumber := map[string]int{}
		for _, port := range c.Ports {
			portNumber[port.Name] = port.ContainerPort
		}

		for _, tc := range []struct {
			kind  string
			probe *httpProbe
			path  string
		}{
			{"readinessProbe", c.ReadinessProbe, "/readyz"},
			{"livenessProbe", c.LivenessProbe, "/healthz"},
		} {
			if tc.probe == nil {
				t.Fatalf("the indexer container in %s defines no %s", manifestPath, tc.kind)
			}
			if tc.probe.HTTPGet.Path != tc.path {
				t.Errorf("%s path is %q, the indexer serves %q", tc.kind, tc.probe.HTTPGet.Path, tc.path)
			}
			if got := portNumber[tc.probe.HTTPGet.Port]; got != defaultPort {
				t.Errorf("%s port %q resolves to %d, the indexer listens on %d by default", tc.kind, tc.probe.HTTPGet.Port, got, defaultPort)
			}
		}
		return
	}
	t.Fatalf("no indexer container in %s", manifestPath)
}
