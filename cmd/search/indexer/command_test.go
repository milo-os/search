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

// TestResourceIndexerOptions_Validate_AckProgressBudget covers the
// 3 * batch-ack-progress-interval <= consumerAckWait assertion in Validate(),
// including the boundary at exactly consumerAckWait, where the budget is
// allowed. Three intervals per ackWait leaves room for two heartbeats to be
// lost or delayed before JetStream redelivers a message that is still being
// indexed.
func TestResourceIndexerOptions_Validate_AckProgressBudget(t *testing.T) {
	t.Setenv("MEILISEARCH_API_KEY", "test-key")

	tests := []struct {
		name                string
		ackProgressInterval time.Duration
		wantErr             bool
	}{
		{
			name:                "well under the ackWait budget",
			ackProgressInterval: 60 * time.Second,
			wantErr:             false,
		},
		{
			name:                "exactly at the ackWait budget",
			ackProgressInterval: 100 * time.Second,
			wantErr:             false,
		},
		{
			name:                "one millisecond over the ackWait budget",
			ackProgressInterval: 100*time.Second + time.Millisecond,
			wantErr:             true,
		},
		{
			name:                "well over the ackWait budget",
			ackProgressInterval: 5 * time.Minute,
			wantErr:             true,
		},
		{
			name:                "zero is rejected outright",
			ackProgressInterval: 0,
			wantErr:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewResourceIndexerOptions()
			o.BatchAckProgressInterval = tt.ackProgressInterval

			err := o.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error: 3 * %s = %s exceeds consumerAckWait %s",
					tt.ackProgressInterval, 3*tt.ackProgressInterval, consumerAckWait)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want no error: 3 * %s = %s is within consumerAckWait %s",
					err, tt.ackProgressInterval, 3*tt.ackProgressInterval, consumerAckWait)
			}
		})
	}
}

// TestCheckAckWait covers the live-ackWait check made against each consumer at
// startup. The rule is looser than Validate()'s constant-based one: falling
// short of the 3x margin only warns, because the consumer reconcile and this
// pod's rollout are not ordered and a hard failure would crashloop the rollout.
// Fewer than two heartbeats in the window is an error.
func TestCheckAckWait(t *testing.T) {
	tests := []struct {
		name     string
		ackWait  time.Duration
		progress time.Duration
		wantErr  bool
	}{
		{
			name:     "shipped manifest ackWait with the default heartbeat",
			ackWait:  300 * time.Second,
			progress: 60 * time.Second,
			wantErr:  false,
		},
		{
			name:     "live consumer still on the old 120s ackWait warns but starts",
			ackWait:  120 * time.Second,
			progress: 60 * time.Second,
			wantErr:  false,
		},
		{
			name:     "fewer than two heartbeats fit",
			ackWait:  100 * time.Second,
			progress: 60 * time.Second,
			wantErr:  true,
		},
		{
			name:     "exactly at the 3x margin",
			ackWait:  180 * time.Second,
			progress: 60 * time.Second,
			wantErr:  false,
		},
		{
			name:     "one second under two heartbeats",
			ackWait:  119 * time.Second,
			progress: 60 * time.Second,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkAckWait("search-indexer", tt.ackWait, tt.progress)
			if tt.wantErr && err == nil {
				t.Fatalf("checkAckWait(_, %s, %s) = nil, want an error: 2 * %s = %s exceeds the live ackWait",
					tt.ackWait, tt.progress, tt.progress, 2*tt.progress)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("checkAckWait(_, %s, %s) = %v, want no error: 2 * %s = %s is within the live ackWait",
					tt.ackWait, tt.progress, err, tt.progress, 2*tt.progress)
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
