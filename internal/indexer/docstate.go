package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

// docState is what an index should hold for one object.
type docState string

const (
	// docIndexed means the object's document belongs in the index.
	docIndexed docState = "indexed"
	// docUnindexed means the object exists but no longer matches the policy
	// behind the index, so its document is removed. A newer write can bring it
	// back.
	docUnindexed docState = "unindexed"
	// docDeleted means the object itself is gone. It wins over every other
	// state regardless of resourceVersion, because an object's uid is never
	// reused and nothing written after its deletion can be current.
	docDeleted docState = "deleted"
)

// docRecord is the shared record for one document in one index. The first
// three fields are the newest state any replica has accepted for it; the Task
// fields describe the latest Meilisearch task known to carry a state for it.
// Meilisearch applies an index's tasks in task-uid order, so the document
// matches the newest state exactly when the task fields agree with the state
// fields.
type docRecord struct {
	RV    uint64          `json:"rv"`
	State docState        `json:"state"`
	Doc   json.RawMessage `json:"doc,omitempty"`

	Task      int64    `json:"task,omitempty"`
	TaskRV    uint64   `json:"taskRv,omitempty"`
	TaskState docState `json:"taskState,omitempty"`
}

// supersedes reports whether r is at least as new as prev and may replace it.
// Equal resourceVersions replace so that a repeat of the same write, or a
// policy change re-evaluating the same object, lands as the latest.
func (r docRecord) supersedes(prev docRecord) bool {
	if prev.State == docDeleted {
		return false
	}
	if r.State == docDeleted {
		return true
	}
	return r.RV >= prev.RV
}

// applied reports whether the latest task recorded for r carries r's state,
// which means the document in Meilisearch will match it once that task runs.
func (r docRecord) applied() bool {
	return r.Task != 0 && r.TaskRV == r.RV && r.TaskState == r.State
}

// errDocStateConflict is returned by a store when the record changed since it
// was read; the caller re-reads and decides again.
var errDocStateConflict = errors.New("document state changed since it was read")

// DocStateStore keeps one docRecord per index/document key with
// compare-and-set semantics. Revision 0 on put creates the key and fails if it
// already exists; any other revision must match the one returned by the read
// the caller is acting on.
type DocStateStore interface {
	Get(ctx context.Context, key string) (rec docRecord, revision uint64, found bool, err error)
	Put(ctx context.Context, key string, rec docRecord, revision uint64) (uint64, error)
}

// memoryDocStateStore is an in-process store. It gives a single batcher
// monotonic writes and lets tests share one arbiter between batchers, but it
// is not shared between replicas and keeps every document it has seen, so
// production wires a NATS KV bucket instead.
type memoryDocStateStore struct {
	mu   sync.Mutex
	recs map[string]memoryDocEntry
}

type memoryDocEntry struct {
	rec docRecord
	rev uint64
}

// NewMemoryDocStateStore returns a DocStateStore held in process memory.
func NewMemoryDocStateStore() DocStateStore {
	return &memoryDocStateStore{recs: make(map[string]memoryDocEntry)}
}

func (s *memoryDocStateStore) Get(_ context.Context, key string) (docRecord, uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.recs[key]
	if !ok {
		return docRecord{}, 0, false, nil
	}
	return e.rec, e.rev, true, nil
}

func (s *memoryDocStateStore) Put(_ context.Context, key string, rec docRecord, revision uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.recs[key]
	if (!ok && revision != 0) || (ok && e.rev != revision) {
		return 0, errDocStateConflict
	}
	e.rec = rec
	e.rev++
	s.recs[key] = e
	return e.rev, nil
}

// natsDocStateStore backs DocStateStore with a JetStream KV bucket, which is
// shared by every indexer replica and linearizable per key.
//
// Every request to the bucket gets its own timeout. The client only applies
// its default timeout to a context without a deadline, and the flush context
// carries the whole-flush deadline, so without one a single hung request would
// hold the flush, and the enqueue turn behind it, for the length of the flush
// timeout.
type natsDocStateStore struct {
	kv      jetstream.KeyValue
	timeout time.Duration
}

// NewNATSDocStateStore returns a DocStateStore on the given KV bucket. Each
// request to the bucket is abandoned after timeout; a timed-out request is an
// ordinary failure, so the flush making it is nacked and redelivered.
func NewNATSDocStateStore(kv jetstream.KeyValue, timeout time.Duration) DocStateStore {
	return &natsDocStateStore{kv: kv, timeout: timeout}
}

// request bounds one call to the bucket by the store's timeout.
func (s *natsDocStateStore) request(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.timeout)
}

func (s *natsDocStateStore) Get(ctx context.Context, key string) (docRecord, uint64, bool, error) {
	entry, err := s.get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return docRecord{}, 0, false, nil
	}
	if err != nil {
		return docRecord{}, 0, false, fmt.Errorf("get document state %s: %w", key, err)
	}
	var rec docRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return docRecord{}, 0, false, fmt.Errorf("decode document state %s: %w", key, err)
	}
	return rec, entry.Revision(), true, nil
}

func (s *natsDocStateStore) get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	ctx, cancel := s.request(ctx)
	defer cancel()
	return s.kv.Get(ctx, key)
}

func (s *natsDocStateStore) write(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	ctx, cancel := s.request(ctx)
	defer cancel()
	if revision == 0 {
		return s.kv.Create(ctx, key, data)
	}
	return s.kv.Update(ctx, key, data, revision)
}

func (s *natsDocStateStore) Put(ctx context.Context, key string, rec docRecord, revision uint64) (uint64, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("encode document state %s: %w", key, err)
	}

	rev, err := s.write(ctx, key, data, revision)
	if err == nil {
		return rev, nil
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return 0, errDocStateConflict
	}

	// A write that timed out may or may not have landed, so a changed
	// revision read back afterwards could be this write's own. It is reported
	// as the failure it is rather than as a conflict, which would send the
	// caller around its compare-and-set loop against a bucket that is not
	// answering.
	if isTimeout(err) {
		return 0, fmt.Errorf("put document state %s: %w", key, err)
	}

	// A document beyond the server's payload limit is still ordered by its
	// resourceVersion; only the copy that lets another flush re-enqueue it is
	// given up, which claimFromRecord reports when it matters.
	if errors.Is(err, nats.ErrMaxPayload) && len(rec.Doc) > 0 {
		klog.Warningf("Document state %s: document exceeds the NATS payload limit, recording its resourceVersion without a copy", key)
		rec.Doc = nil
		return s.Put(ctx, key, rec, revision)
	}

	// The client does not type every wrong-revision rejection, so a changed
	// revision is detected by reading the key back. Only a read that succeeds
	// can show a conflict; when the read fails too, the write's own error is
	// the one worth reporting.
	if entry, getErr := s.get(ctx, key); getErr == nil && entry.Revision() != revision {
		return 0, errDocStateConflict
	}
	return 0, fmt.Errorf("put document state %s: %w", key, err)
}

// isTimeout reports whether err means a request was given up on rather than
// answered.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, nats.ErrTimeout)
}

// docStateKey names a document's record in the store.
func docStateKey(indexUID, docID string) string {
	return indexUID + "." + docID
}

// resourceVersionOf reads a document's resourceVersion as an integer, which
// is how etcd issues them. A document without one, or with one that does not
// parse, sorts before every versioned write.
func resourceVersionOf(doc any) uint64 {
	m, ok := doc.(map[string]any)
	if !ok {
		return 0
	}
	return parseResourceVersion((&unstructured.Unstructured{Object: m}).GetResourceVersion())
}

func parseResourceVersion(rv string) uint64 {
	if rv == "" {
		return 0
	}
	n, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
