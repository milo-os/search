package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startJetStream runs an embedded NATS server with JetStream, storing in a
// temporary directory, and returns a client for it. Both are stopped when the
// test ends.
func startJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	require.NoError(t, err)
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded NATS server did not start")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// newDocStateKV creates a document state bucket configured as the indexer
// creates it: one value per key, file storage.
func newDocStateKV(t *testing.T, js jetstream.JetStream) jetstream.KeyValue {
	t.Helper()
	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:  "SEARCH_INDEXER_DOC_STATE_TEST",
		History: 1,
		Storage: jetstream.FileStorage,
	})
	require.NoError(t, err)
	return kv
}

func newNATSStore(t *testing.T) (DocStateStore, jetstream.KeyValue) {
	t.Helper()
	kv := newDocStateKV(t, startJetStream(t))
	return NewNATSDocStateStore(kv, 5*time.Second), kv
}

// startTogether runs fn on n goroutines released at the same moment and waits
// for all of them.
func startTogether(n int, fn func(i int)) {
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			fn(i)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
}

func TestNATSDocStateStore_GetPutRoundTrip(t *testing.T) {
	store, _ := newNATSStore(t)
	ctx := context.Background()
	key := pendingKey("proxies", "p1")

	_, _, found, err := store.Get(ctx, key)
	require.NoError(t, err)
	assert.False(t, found)

	want := docRecord{RV: 7, State: docIndexed, Doc: json.RawMessage(`{"uid":"p1"}`), Task: 3, TaskRV: 7, TaskState: docIndexed}
	rev, err := store.Put(ctx, key, want, 0)
	require.NoError(t, err)

	got, gotRev, found, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, rev, gotRev)
	assert.Equal(t, want, got)
}

// TestNATSDocStateStore_CreateRace_OneWins covers several replicas claiming a
// document no replica has recorded yet: exactly one create lands and every
// other is told the record changed under it.
func TestNATSDocStateStore_CreateRace_OneWins(t *testing.T) {
	store, _ := newNATSStore(t)
	key := pendingKey("proxies", "p1")

	const racers = 16
	errs := make([]error, racers)
	startTogether(racers, func(i int) {
		_, errs[i] = store.Put(context.Background(), key, docRecord{RV: uint64(i + 1), State: docIndexed}, 0)
	})

	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
			continue
		}
		assert.ErrorIs(t, err, errDocStateConflict)
	}
	assert.Equal(t, 1, wins, "exactly one create must land")
}

// TestNATSDocStateStore_UpdateRace_OneWins covers several replicas acting on
// the same read: exactly one update at that revision lands.
func TestNATSDocStateStore_UpdateRace_OneWins(t *testing.T) {
	store, _ := newNATSStore(t)
	ctx := context.Background()
	key := pendingKey("proxies", "p1")
	rev, err := store.Put(ctx, key, docRecord{RV: 1, State: docIndexed}, 0)
	require.NoError(t, err)

	const racers = 16
	errs := make([]error, racers)
	startTogether(racers, func(i int) {
		_, errs[i] = store.Put(context.Background(), key, docRecord{RV: uint64(i + 2), State: docIndexed}, rev)
	})

	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
			continue
		}
		assert.ErrorIs(t, err, errDocStateConflict)
	}
	assert.Equal(t, 1, wins, "exactly one update at one revision must land")
}

// TestNATSDocStateStore_CompareAndSetLoop_LosesNoUpdate runs the claim step's
// read, decide, write loop from many goroutines at once. Each counts one in
// the record; a lost update would leave the count short.
func TestNATSDocStateStore_CompareAndSetLoop_LosesNoUpdate(t *testing.T) {
	store, _ := newNATSStore(t)
	key := pendingKey("proxies", "p1")

	const workers, increments = 8, 20
	var failures atomic.Int32
	startTogether(workers, func(int) {
		ctx := context.Background()
		for n := 0; n < increments; n++ {
			for {
				rec, rev, _, err := store.Get(ctx, key)
				if err != nil {
					failures.Add(1)
					return
				}
				rec.RV++
				rec.State = docIndexed
				if _, err = store.Put(ctx, key, rec, rev); err == nil {
					break
				}
				if !errors.Is(err, errDocStateConflict) {
					failures.Add(1)
					return
				}
			}
		}
	})

	require.Zero(t, failures.Load())
	rec, _, found, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint64(workers*increments), rec.RV, "every compare-and-set increment must be kept")
}

// landThenFailKV lands an update for which fail reports true, then reports an
// error to the caller anyway, as when the connection drops after the server
// stored the write but before its reply arrived. The error is not a timeout,
// so the store reads the key back to tell a conflict from a failure.
type landThenFailKV struct {
	jetstream.KeyValue
	fail   func(key string, value []byte) bool
	landed atomic.Int32
}

func (k *landThenFailKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	rev, err := k.KeyValue.Update(ctx, key, value, revision)
	if err == nil && k.fail(key, value) {
		k.landed.Add(1)
		return 0, nats.ErrConnectionClosed
	}
	return rev, err
}

// failOnce reports true for the first update whose record matches match.
func failOnce(match func(docRecord) bool) func(string, []byte) bool {
	var done atomic.Bool
	return func(_ string, value []byte) bool {
		var rec docRecord
		if json.Unmarshal(value, &rec) != nil || !match(rec) {
			return false
		}
		return done.CompareAndSwap(false, true)
	}
}

// TestNATSDocStateStore_LandedUpdateReportedAsFailed_IsAConflict pins how the
// store reports an update that landed but failed for its caller: the
// read-back sees a changed revision, so the caller is sent around its
// compare-and-set loop and must recognise its own write there.
func TestNATSDocStateStore_LandedUpdateReportedAsFailed_IsAConflict(t *testing.T) {
	kv := newDocStateKV(t, startJetStream(t))
	wrapped := &landThenFailKV{KeyValue: kv, fail: func(string, []byte) bool { return true }}
	store := NewNATSDocStateStore(wrapped, 5*time.Second)
	ctx := context.Background()
	key := pendingKey("proxies", "p1")

	rev, err := store.Put(ctx, key, docRecord{RV: 1, State: docIndexed}, 0)
	require.NoError(t, err)

	_, err = store.Put(ctx, key, docRecord{RV: 2, State: docIndexed}, rev)
	require.ErrorIs(t, err, errDocStateConflict)

	rec, _, _, err := store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rec.RV, "the write landed although its caller was told otherwise")
}

// TestBatcher_NATS_ConcurrentFlushes_RepairsOverwrite runs the whole protocol,
// claim, enqueue and record, through a real bucket shared by two batchers.
// Replica A claims p1@10 and is held before it enqueues; replica B claims,
// enqueues and records p1@20; A's older write then lands last in Meilisearch,
// so A must enqueue p1@20 again behind it.
func TestBatcher_NATS_ConcurrentFlushes_RepairsOverwrite(t *testing.T) {
	store, _ := newNATSStore(t)
	client := newRVSearchClient()
	replicaA := newOrderingBatcher(client, store)
	replicaB := newOrderingBatcher(client, store)

	var interleaved atomic.Bool
	var mB *boundMsg
	client.onTask = func(_ string, docs []any) {
		if resourceVersionOf(docs[0]) == 10 && interleaved.CompareAndSwap(false, true) {
			mB = flushNow(replicaB, 2, upsertOp("proxies", rvDoc("p1", 20, "poet-jump")))
		}
	}

	mA := flushNow(replicaA, 1, upsertOp("proxies", rvDoc("p1", 10, "")))

	require.True(t, interleaved.Load())
	requireAcked(t, mA, mB)
	assert.Equal(t, []string{"add proxies/p1@20", "add proxies/p1@10", "add proxies/p1@20"}, client.Log())
	assert.Equal(t, "poet-jump", hostnameOf(client.Doc("proxies", "p1")))

	rec, _, found, err := store.Get(context.Background(), pendingKey("proxies", "p1"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint64(20), rec.RV)
	assert.True(t, rec.applied(), "the record must show the newest state as the one landing last: %+v", rec)
}

// TestBatcher_NATS_AmbiguousRecordWrite_StillRepairs covers a record write
// that lands but is reported to the flush as failed. Replica A claims p1@5;
// before A's task is given a uid, replica B claims, enqueues and records
// p1@6. A's task carries p1@5 after B's, so A's record step must enqueue p1@6
// again. A's record write lands, the flush is told it failed, and its retry
// finds its own task already recorded: the repair is still A's to make, since
// no other flush saw a later task.
func TestBatcher_NATS_AmbiguousRecordWrite_StillRepairs(t *testing.T) {
	kv := newDocStateKV(t, startJetStream(t))
	shared := NewNATSDocStateStore(kv, 5*time.Second)
	flaky := &landThenFailKV{KeyValue: kv, fail: failOnce(func(r docRecord) bool {
		return r.Task != 0 && r.TaskRV == 5
	})}

	client := newRVSearchClient()
	replicaA := newOrderingBatcher(client, NewNATSDocStateStore(flaky, 5*time.Second))
	replicaB := newOrderingBatcher(client, shared)

	var interleaved atomic.Bool
	var mB *boundMsg
	client.onTask = func(_ string, docs []any) {
		if resourceVersionOf(docs[0]) == 5 && interleaved.CompareAndSwap(false, true) {
			mB = flushNow(replicaB, 2, upsertOp("proxies", rvDoc("p1", 6, "newer")))
		}
	}

	mA := flushNow(replicaA, 1, upsertOp("proxies", rvDoc("p1", 5, "older")))

	require.True(t, interleaved.Load())
	require.Equal(t, int32(1), flaky.landed.Load(), "replica A's record write must have landed and been reported as failed")
	requireAcked(t, mA, mB)
	assert.Equal(t, []string{"add proxies/p1@6", "add proxies/p1@5", "add proxies/p1@6"}, client.Log(),
		"the flush whose record write landed must still enqueue the newer state behind its own task")
	assert.Equal(t, "newer", hostnameOf(client.Doc("proxies", "p1")))

	rec, _, _, err := shared.Get(context.Background(), pendingKey("proxies", "p1"))
	require.NoError(t, err)
	assert.True(t, rec.applied(), "%+v", rec)
}

// TestBatcher_RecordingAnOlderTask_KeepsTheLaterTasksRecord covers the record
// step's guard against an older task. Replica O claims p1@5 and enqueues it
// as task 1. Before O records, replica L claims p1@6, enqueues it as task 2
// and records it, which settles the document: task 2 lands last and carries
// the newest state. When O then records task 1, the guard leaves L's record
// alone. Without it, O would write its older task over L's, so the record
// would name a task that does not land last and claim it carries p1@5, and O
// would enqueue p1@6 a second time although task 2 already carries it.
func TestBatcher_RecordingAnOlderTask_KeepsTheLaterTasksRecord(t *testing.T) {
	stores := map[string]func(t *testing.T) DocStateStore{
		"memory": func(*testing.T) DocStateStore { return NewMemoryDocStateStore() },
		"nats": func(t *testing.T) DocStateStore {
			store, _ := newNATSStore(t)
			return store
		},
	}
	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			store := newStore(t)
			client := newRVSearchClient()
			older := newOrderingBatcher(client, store)
			later := newOrderingBatcher(client, store)

			var interleaved atomic.Bool
			var mL *boundMsg
			client.afterTask = func(_ string, docs []any, _ int64) {
				if resourceVersionOf(docs[0]) == 5 && interleaved.CompareAndSwap(false, true) {
					mL = flushNow(later, 2, upsertOp("proxies", rvDoc("p1", 6, "newer")))
				}
			}

			mO := flushNow(older, 1, upsertOp("proxies", rvDoc("p1", 5, "older")))

			require.True(t, interleaved.Load())
			requireAcked(t, mO, mL)
			assert.Equal(t, "newer", hostnameOf(client.Doc("proxies", "p1")))
			assert.Equal(t, []string{"add proxies/p1@5", "add proxies/p1@6"}, client.Log(),
				"the flush holding the older task must not enqueue a state the later task already carries")

			rec, _, found, err := store.Get(context.Background(), pendingKey("proxies", "p1"))
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, docRecord{RV: 6, State: docIndexed, Task: 2, TaskRV: 6, TaskState: docIndexed},
				docRecord{RV: rec.RV, State: rec.State, Task: rec.Task, TaskRV: rec.TaskRV, TaskState: rec.TaskState},
				"the record must keep naming the later task")
		})
	}
}
