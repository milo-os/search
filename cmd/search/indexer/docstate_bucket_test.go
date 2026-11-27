package indexer

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	searchmetrics "go.miloapis.net/search/internal/metrics"
	"k8s.io/component-base/metrics/testutil"
)

// fakeBucketStatus reports a fixed size.
type fakeBucketStatus struct {
	jetstream.KeyValueStatus
	bytes uint64
}

func (s fakeBucketStatus) Bytes() uint64 { return s.bytes }

// statusKV answers Status as told: with an error, by hanging until the
// context ends, or with a status.
type statusKV struct {
	jetstream.KeyValue
	name  string
	hang  atomic.Bool
	err   atomic.Pointer[error]
	bytes atomic.Uint64
}

func (k *statusKV) Bucket() string { return k.name }

func (k *statusKV) Status(ctx context.Context) (jetstream.KeyValueStatus, error) {
	if k.hang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := k.err.Load(); err != nil {
		return nil, *err
	}
	return fakeBucketStatus{bytes: k.bytes.Load()}, nil
}

func TestReadyz_FollowsDocStateBucket(t *testing.T) {
	const timeout = 50 * time.Millisecond

	p := newProbes(time.Minute)
	p.cacheSynced.Store(true)
	p.consumersAttached.Store(true)

	kv := &statusKV{name: "SEARCH_INDEXER_DOC_STATE"}
	kv.bytes.Store(4096)
	p.checkDocState(docStateBucketCheck(kv, timeout))

	if code, body := probe(t, p, "/readyz"); code != http.StatusOK {
		t.Fatalf("bucket answering: got %d %q, want 200", code, body)
	}
	if got, err := testutil.GetGaugeMetricValue(searchmetrics.IndexerDocStateBucketBytes); err != nil || got != 4096 {
		t.Fatalf("bucket bytes gauge: got %v (%v), want 4096", got, err)
	}

	kv.hang.Store(true)
	start := time.Now()
	code, body := probe(t, p, "/readyz")
	if elapsed := time.Since(start); elapsed > 10*timeout {
		t.Fatalf("a hung bucket held the probe for %s, want about %s", elapsed, timeout)
	}
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "document state bucket SEARCH_INDEXER_DOC_STATE unavailable") {
		t.Fatalf("bucket hung: got %d %q, want 503 naming the bucket", code, body)
	}

	kv.hang.Store(false)
	unavailable := errors.New("nats: no responders available for request")
	kv.err.Store(&unavailable)
	code, body = probe(t, p, "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "no responders") {
		t.Fatalf("bucket failing: got %d %q, want 503 carrying the cause", code, body)
	}

	kv.err.Store(nil)
	if code, body := probe(t, p, "/readyz"); code != http.StatusOK {
		t.Fatalf("bucket recovered: got %d %q, want 200", code, body)
	}
}

func TestResourceIndexerOptions_Validate_DocState(t *testing.T) {
	t.Setenv("MEILISEARCH_API_KEY", "test-key")

	for _, tc := range []struct {
		name    string
		mutate  func(o *ResourceIndexerOptions)
		wantErr bool
	}{
		{"defaults", func(*ResourceIndexerOptions) {}, false},
		{"zero request timeout", func(o *ResourceIndexerOptions) { o.NatsDocStateTimeout = 0 }, true},
		{"negative request timeout", func(o *ResourceIndexerOptions) { o.NatsDocStateTimeout = -time.Second }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := NewResourceIndexerOptions()
			tc.mutate(o)
			if err := o.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, want error: %t", err, tc.wantErr)
			}
		})
	}
}
