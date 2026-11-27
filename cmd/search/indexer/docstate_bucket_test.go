package indexer

import (
	"context"
	"errors"
	"fmt"
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
	cfg   jetstream.KeyValueConfig
}

func (s fakeBucketStatus) Bytes() uint64                    { return s.bytes }
func (s fakeBucketStatus) Config() jetstream.KeyValueConfig { return s.cfg }

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
		{"zero max bytes", func(o *ResourceIndexerOptions) { o.NatsDocStateMaxBytes = 0 }, true},
		{"unlimited max bytes", func(o *ResourceIndexerOptions) { o.NatsDocStateMaxBytes = -1 }, true},
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

// configKV is a bucket that reports cfg as its configuration.
type configKV struct {
	jetstream.KeyValue
	cfg jetstream.KeyValueConfig
}

func (k *configKV) Status(context.Context) (jetstream.KeyValueStatus, error) {
	return fakeBucketStatus{cfg: k.cfg}, nil
}

// fakeKVManager holds at most one bucket. When createErr is set, a create
// fails with it and leaves raced as the bucket, as if another replica had
// created raced first.
type fakeKVManager struct {
	bucket    *configKV
	createErr error
	raced     *configKV
	updateErr error

	creates int
	updates int
}

func (m *fakeKVManager) KeyValue(_ context.Context, name string) (jetstream.KeyValue, error) {
	if m.bucket == nil {
		return nil, jetstream.ErrBucketNotFound
	}
	return m.bucket, nil
}

func (m *fakeKVManager) CreateKeyValue(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	m.creates++
	if m.createErr != nil {
		m.bucket = m.raced
		return nil, m.createErr
	}
	m.bucket = &configKV{cfg: cfg}
	return m.bucket, nil
}

func (m *fakeKVManager) UpdateKeyValue(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	m.updates++
	if m.updateErr != nil {
		return nil, m.updateErr
	}
	m.bucket = &configKV{cfg: cfg}
	return m.bucket, nil
}

func wantedDocStateBucket() jetstream.KeyValueConfig {
	o := NewResourceIndexerOptions()
	o.NatsDocStateReplicas = 3
	return o.docStateBucketConfig()
}

// tunedDocStateBucket is a bucket an operator has changed by hand.
func tunedDocStateBucket() *configKV {
	cfg := wantedDocStateBucket()
	cfg.Replicas = 5
	cfg.MaxBytes = 50 << 30
	return &configKV{cfg: cfg}
}

func TestOpenDocStateBucket(t *testing.T) {
	want := wantedDocStateBucket()

	t.Run("missing bucket is created", func(t *testing.T) {
		for _, manage := range []bool{true, false} {
			m := &fakeKVManager{}
			kv, err := openDocStateBucket(context.Background(), m, want, manage)
			if err != nil {
				t.Fatalf("manage=%t: %v", manage, err)
			}
			if m.creates != 1 || m.updates != 0 {
				t.Fatalf("manage=%t: %d creates and %d updates, want 1 and 0", manage, m.creates, m.updates)
			}
			if got := kv.(*configKV).cfg; got.MaxBytes != want.MaxBytes || got.Replicas != 3 {
				t.Fatalf("manage=%t: created with %+v, want %+v", manage, got, want)
			}
		}
	})

	t.Run("matching bucket is opened as is", func(t *testing.T) {
		m := &fakeKVManager{bucket: &configKV{cfg: want}}
		if _, err := openDocStateBucket(context.Background(), m, want, true); err != nil {
			t.Fatal(err)
		}
		if m.creates != 0 || m.updates != 0 {
			t.Fatalf("%d creates and %d updates, want none", m.creates, m.updates)
		}
	})

	t.Run("drifted bucket is left alone when unmanaged", func(t *testing.T) {
		tuned := tunedDocStateBucket()
		m := &fakeKVManager{bucket: tuned}
		kv, err := openDocStateBucket(context.Background(), m, want, false)
		if err != nil {
			t.Fatal(err)
		}
		if m.updates != 0 || m.creates != 0 {
			t.Fatalf("%d creates and %d updates, want none", m.creates, m.updates)
		}
		if kv != tuned {
			t.Fatal("the existing bucket must be used as it is")
		}
	})

	t.Run("drifted bucket is updated when managed", func(t *testing.T) {
		m := &fakeKVManager{bucket: tunedDocStateBucket()}
		kv, err := openDocStateBucket(context.Background(), m, want, true)
		if err != nil {
			t.Fatal(err)
		}
		if m.updates != 1 {
			t.Fatalf("%d updates, want 1", m.updates)
		}
		if got := kv.(*configKV).cfg; got.Replicas != want.Replicas || got.MaxBytes != want.MaxBytes {
			t.Fatalf("updated to %+v, want %+v", got, want)
		}
	})

	t.Run("managed update keeps unmanaged settings", func(t *testing.T) {
		tuned := tunedDocStateBucket()
		tuned.cfg.Compression = true
		tuned.cfg.Metadata = map[string]string{"owner": "platform"}
		tuned.cfg.MaxValueSize = 1 << 20
		tuned.cfg.Placement = &jetstream.Placement{Cluster: "nats-east"}
		tuned.cfg.RePublish = &jetstream.RePublish{Source: ">", Destination: "repub.>"}
		tuned.cfg.Description = "tuned by hand"
		m := &fakeKVManager{bucket: tuned}

		kv, err := openDocStateBucket(context.Background(), m, want, true)
		if err != nil {
			t.Fatal(err)
		}
		if m.updates != 1 {
			t.Fatalf("%d updates, want 1", m.updates)
		}
		got := kv.(*configKV).cfg
		if got.Replicas != want.Replicas || got.TTL != want.TTL || got.MaxBytes != want.MaxBytes || got.History != want.History || got.Storage != want.Storage {
			t.Fatalf("managed settings not applied: got %+v, want those of %+v", got, want)
		}
		if !got.Compression || got.Metadata["owner"] != "platform" || got.MaxValueSize != 1<<20 ||
			got.Placement == nil || got.Placement.Cluster != "nats-east" ||
			got.RePublish == nil || got.RePublish.Destination != "repub.>" || got.Description != "tuned by hand" {
			t.Fatalf("the update reset settings the indexer does not manage: %+v", got)
		}
	})

	t.Run("refused update keeps the existing bucket", func(t *testing.T) {
		tuned := tunedDocStateBucket()
		m := &fakeKVManager{bucket: tuned, updateErr: errors.New("insufficient resources")}
		kv, err := openDocStateBucket(context.Background(), m, want, true)
		if err != nil {
			t.Fatal(err)
		}
		if kv != tuned {
			t.Fatal("a refused update must leave the existing bucket in use")
		}
	})

	for _, raceErr := range []error{
		fmt.Errorf("%w: %s", jetstream.ErrBucketExists, want.Bucket),
		jetstream.ErrStreamNameAlreadyInUse,
	} {
		t.Run("create race: "+raceErr.Error(), func(t *testing.T) {
			raced := tunedDocStateBucket()
			m := &fakeKVManager{createErr: raceErr, raced: raced}
			kv, err := openDocStateBucket(context.Background(), m, want, false)
			if err != nil {
				t.Fatalf("losing the create race must open the other replica's bucket: %v", err)
			}
			if kv != raced || m.updates != 0 {
				t.Fatalf("got %v with %d updates, want the raced bucket untouched", kv, m.updates)
			}
		})
	}

	t.Run("other create failures are returned", func(t *testing.T) {
		m := &fakeKVManager{createErr: errors.New("jetstream not enabled")}
		if _, err := openDocStateBucket(context.Background(), m, want, true); err == nil {
			t.Fatal("want an error")
		}
	})
}

func TestDocStateBucketDrift(t *testing.T) {
	want := wantedDocStateBucket()
	if drift := docStateBucketDrift(want, want); len(drift) != 0 {
		t.Fatalf("identical configs drift: %v", drift)
	}

	current := want
	current.Replicas = 1
	current.TTL = time.Hour
	current.MaxBytes = -1
	current.History = 5
	current.Storage = jetstream.MemoryStorage
	current.Description = "not managed"
	if drift := docStateBucketDrift(current, want); len(drift) != 5 {
		t.Fatalf("got %d differences %v, want one each for replicas, TTL, max bytes, history and storage", len(drift), drift)
	}
}

func TestDocStateBucketConfig_Defaults(t *testing.T) {
	cfg := NewResourceIndexerOptions().docStateBucketConfig()
	if cfg.MaxBytes != 10737418240 {
		t.Errorf("max bytes default is %d, want 10 GiB so the bucket is never unbounded", cfg.MaxBytes)
	}
	if cfg.Replicas != 1 {
		t.Errorf("replicas default is %d, want 1 for single-node dev and kind clusters", cfg.Replicas)
	}
	if cfg.Bucket != "SEARCH_INDEXER_DOC_STATE" || cfg.TTL != docStateTTLDefault || cfg.History != 1 || cfg.Storage != jetstream.FileStorage {
		t.Errorf("unexpected bucket defaults %+v", cfg)
	}
}

// TestDeploymentReadinessProbe_OutlastsDocStateCheck keeps the kubelet from
// giving up on /readyz at the same moment as the document state check, which
// would hide the reason the replica is unready.
func TestDeploymentReadinessProbe_OutlastsDocStateCheck(t *testing.T) {
	m := readDeploymentManifest(t)
	for _, c := range m.Spec.Template.Spec.Containers {
		if c.Name != "indexer" {
			continue
		}
		if c.ReadinessProbe == nil {
			t.Fatal("the indexer container defines no readinessProbe")
		}
		probeTimeout := time.Duration(c.ReadinessProbe.TimeoutSeconds) * time.Second
		if docState := NewResourceIndexerOptions().NatsDocStateTimeout; probeTimeout <= docState {
			t.Fatalf("readinessProbe timeoutSeconds is %s, which must exceed the default --nats-doc-state-timeout of %s", probeTimeout, docState)
		}
		return
	}
	t.Fatal("no indexer container in the deployment manifest")
}
