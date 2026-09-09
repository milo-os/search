package indexer

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestResourceIndexerOptions_Validate_Budget covers the 2 * (http-timeout +
// task-wait-timeout) <= consumerAckWait assertion in Validate(), including the
// boundary at exactly consumerAckWait, where the budget is allowed.
func TestResourceIndexerOptions_Validate_Budget(t *testing.T) {
	t.Setenv("MEILISEARCH_API_KEY", "test-key")

	tests := []struct {
		name        string
		httpTimeout time.Duration
		taskWait    time.Duration
		wantErr     bool
	}{
		{
			name:        "well under the ackWait budget",
			httpTimeout: 30 * time.Second,
			taskWait:    30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "exactly at the ackWait budget",
			httpTimeout: 75 * time.Second,
			taskWait:    75 * time.Second,
			wantErr:     false,
		},
		{
			name:        "one millisecond over the ackWait budget",
			httpTimeout: 75 * time.Second,
			taskWait:    75*time.Second + time.Millisecond,
			wantErr:     true,
		},
		{
			name:        "well over the ackWait budget",
			httpTimeout: 200 * time.Second,
			taskWait:    200 * time.Second,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewResourceIndexerOptions()
			o.MeilisearchHTTPTimeout = tt.httpTimeout
			o.MeilisearchTaskWaitTimeout = tt.taskWait

			err := o.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error: 2 * (%s + %s) = %s exceeds consumerAckWait %s",
					tt.httpTimeout, tt.taskWait, 2*(tt.httpTimeout+tt.taskWait), consumerAckWait)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want no error: 2 * (%s + %s) = %s is within consumerAckWait %s",
					err, tt.httpTimeout, tt.taskWait, 2*(tt.httpTimeout+tt.taskWait), consumerAckWait)
			}
		})
	}
}

// consumerManifest is the subset of the JetStream Consumer manifest fields
// this test needs.
type consumerManifest struct {
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		AckWait string `yaml:"ackWait"`
	} `yaml:"spec"`
}

// TestConsumerAckWait_MatchesManifest fails if the search-indexer consumer's
// ackWait in the shipped manifest drifts from consumerAckWait, which
// Validate() budgets the indexing timeouts against. The two have no other
// link, so nothing else would catch them falling out of step.
func TestConsumerAckWait_MatchesManifest(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not resolve this test file's path")
	}
	// This file lives at cmd/search/indexer/command_test.go, three directories
	// below the module root.
	moduleRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	manifestPath := filepath.Join(moduleRoot, "config", "components", "nats-config", "nats-consumer.yaml")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", manifestPath, err)
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	var found bool
	for {
		var m consumerManifest
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decoding %s: %v", manifestPath, err)
		}
		if m.Metadata.Name != "search-indexer" {
			continue
		}
		found = true

		ackWait, err := time.ParseDuration(m.Spec.AckWait)
		if err != nil {
			t.Fatalf("parsing ackWait %q for the search-indexer consumer in %s: %v", m.Spec.AckWait, manifestPath, err)
		}
		if ackWait != consumerAckWait {
			t.Fatalf("search-indexer consumer ackWait is %s in %s, but consumerAckWait is %s: keep them in step",
				ackWait, manifestPath, consumerAckWait)
		}
	}
	if !found {
		t.Fatalf("no search-indexer consumer found in %s", manifestPath)
	}
}
