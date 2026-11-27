package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDocStateTimeout = 50 * time.Millisecond

// fakeKV is a JetStream KV bucket whose calls are scripted by the test. A nil
// function blocks until the call's context ends, which is how a bucket that
// has stopped answering looks to the client.
type fakeKV struct {
	jetstream.KeyValue

	get    func(key string) (jetstream.KeyValueEntry, error)
	create func(key string, value []byte) (uint64, error)
	update func(key string, value []byte, revision uint64) (uint64, error)

	calls        atomic.Int32
	longestLimit atomic.Int64
}

// enter counts a call and notes how long its context allows it to run.
func (f *fakeKV) enter(ctx context.Context) {
	f.calls.Add(1)
	if deadline, ok := ctx.Deadline(); ok {
		limit := int64(time.Until(deadline))
		for {
			prev := f.longestLimit.Load()
			if limit <= prev || f.longestLimit.CompareAndSwap(prev, limit) {
				break
			}
		}
	} else {
		f.longestLimit.Store(int64(time.Hour * 24 * 365))
	}
}

func (f *fakeKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	f.enter(ctx)
	if f.get == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.get(key)
}

func (f *fakeKV) Create(ctx context.Context, key string, value []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	f.enter(ctx)
	if f.create == nil {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.create(key, value)
}

func (f *fakeKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	f.enter(ctx)
	if f.update == nil {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.update(key, value, revision)
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	value    []byte
	revision uint64
}

func (e fakeEntry) Value() []byte    { return e.value }
func (e fakeEntry) Revision() uint64 { return e.revision }

func entryAt(revision uint64) func(string) (jetstream.KeyValueEntry, error) {
	return func(string) (jetstream.KeyValueEntry, error) {
		return fakeEntry{value: []byte(`{"rv":1,"state":"indexed"}`), revision: revision}, nil
	}
}

// longFlushCtx stands in for the flush context, whose deadline is far beyond
// any one store request.
func longFlushCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	return ctx
}

func requireTimedOut(t *testing.T, err error, elapsed time.Duration) {
	t.Helper()
	require.Error(t, err)
	assert.NotErrorIs(t, err, errDocStateConflict, "a timeout must not be mistaken for a lost compare-and-set")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 10*testDocStateTimeout, "the request must give up at the store's timeout, not the flush's")
}

func TestNATSDocStateStore_HungGet_FailsWithinTimeout(t *testing.T) {
	kv := &fakeKV{}
	store := NewNATSDocStateStore(kv, testDocStateTimeout)

	start := time.Now()
	_, _, _, err := store.Get(longFlushCtx(t), "index.doc")
	requireTimedOut(t, err, time.Since(start))
}

func TestNATSDocStateStore_HungWrite_FailsWithinTimeout(t *testing.T) {
	for _, tc := range []struct {
		name     string
		revision uint64
	}{
		{"create", 0},
		{"update", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The read-back would show a changed revision, which must not turn
			// a write of unknown outcome into a conflict.
			kv := &fakeKV{get: entryAt(tc.revision + 1)}
			store := NewNATSDocStateStore(kv, testDocStateTimeout)

			start := time.Now()
			_, err := store.Put(longFlushCtx(t), "index.doc", docRecord{RV: 1, State: docIndexed}, tc.revision)
			requireTimedOut(t, err, time.Since(start))
			assert.Equal(t, int32(1), kv.calls.Load(), "a timed-out write is not read back")
		})
	}
}

func TestNATSDocStateStore_HungReadBack_ReturnsWriteError(t *testing.T) {
	writeErr := errors.New("wrong last sequence")
	kv := &fakeKV{
		update: func(string, []byte, uint64) (uint64, error) { return 0, writeErr },
	}
	store := NewNATSDocStateStore(kv, testDocStateTimeout)

	start := time.Now()
	_, err := store.Put(longFlushCtx(t), "index.doc", docRecord{RV: 1, State: docIndexed}, 3)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, writeErr, "when the read-back fails, the write's own error is reported")
	assert.NotErrorIs(t, err, errDocStateConflict)
	assert.Less(t, elapsed, 10*testDocStateTimeout)
	assert.Equal(t, int32(2), kv.calls.Load())
}

func TestNATSDocStateStore_ReadBackShowsNewerRevision_IsConflict(t *testing.T) {
	kv := &fakeKV{
		get:    entryAt(4),
		update: func(string, []byte, uint64) (uint64, error) { return 0, errors.New("wrong last sequence") },
	}
	store := NewNATSDocStateStore(kv, testDocStateTimeout)

	_, err := store.Put(longFlushCtx(t), "index.doc", docRecord{RV: 1, State: docIndexed}, 3)
	require.ErrorIs(t, err, errDocStateConflict)
}

func TestNATSDocStateStore_PayloadRetry_IsBoundedToo(t *testing.T) {
	hanging := &hangAfterFirstUpdate{fakeKV: &fakeKV{}}
	store := NewNATSDocStateStore(hanging, testDocStateTimeout)

	start := time.Now()
	_, err := store.Put(longFlushCtx(t), "index.doc", docRecord{RV: 1, State: docIndexed, Doc: []byte(`{"uid":"doc"}`)}, 3)
	requireTimedOut(t, err, time.Since(start))
	assert.Equal(t, int32(2), hanging.updates.Load(), "the write is retried once without the document")
}

// hangAfterFirstUpdate fails the first update as too large and leaves every
// later one unanswered until its context ends.
type hangAfterFirstUpdate struct {
	*fakeKV
	updates atomic.Int32
}

func (h *hangAfterFirstUpdate) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	h.enter(ctx)
	if h.updates.Add(1) == 1 {
		return 0, nats.ErrMaxPayload
	}
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestBatcher_HungDocStateStore_NacksWellBeforeFlushTimeout covers a document
// state bucket that stops answering: the flush gives up after one store
// timeout, rather than holding its enqueue turn for the whole flush timeout,
// and nacks every message so none is lost.
func TestBatcher_HungDocStateStore_NacksWellBeforeFlushTimeout(t *testing.T) {
	const docs = 10 * docStateParallelism

	kv := &fakeKV{}
	batcher := NewBatcher(newRVSearchClient(), BatchConfig{
		BatchSize:     1000,
		FlushInterval: time.Hour,
		FlushTimeout:  time.Hour,
		DocState:      NewNATSDocStateStore(kv, testDocStateTimeout),
	})

	var (
		msgs []*boundMsg
		bt   = batch{turn: enqueueTurn{prev: batcher.lastEnqueue, done: make(chan struct{})}}
	)
	for i := 0; i < docs; i++ {
		uid := fmt.Sprintf("doc-%d", i)
		m := &boundMsg{seq: uint64(i + 1)}
		msgs = append(msgs, m)
		bt.msgs = append(bt.msgs, m)
		bt.ops = append(bt.ops, pendingOp{indexUID: "index-1", docID: uid, state: docIndexed, doc: rvDoc(uid, 1, "")})
	}

	start := time.Now()
	batcher.performFlush(bt)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 20*testDocStateTimeout, "a dead store must fail the flush in about one request timeout")
	select {
	case <-bt.turn.done:
	default:
		t.Fatal("the flush must hand on the enqueue turn")
	}
	assert.LessOrEqual(t, kv.calls.Load(), int32(docStateParallelism), "no claim may start once one has failed")
	assert.LessOrEqual(t, time.Duration(kv.longestLimit.Load()), testDocStateTimeout, "every store request carries the store's own timeout")
	for _, m := range msgs {
		require.Zero(t, atomic.LoadInt32(&m.acked), "message %d was acked although nothing was indexed", m.seq)
		require.Equal(t, int32(1), atomic.LoadInt32(&m.naked), "message %d must be nacked for redelivery", m.seq)
	}
}

func TestForEachParallel_StopsLaunchingAfterError(t *testing.T) {
	const workers = 4
	boom := errors.New("boom")

	var started atomic.Int32
	err := forEachParallel(context.Background(), 100, workers, func(ctx context.Context, i int) error {
		started.Add(1)
		if i == 0 {
			return boom
		}
		<-ctx.Done()
		return ctx.Err()
	})

	require.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, context.Canceled, "jobs cut short by the first failure are not reported")
	assert.LessOrEqual(t, started.Load(), int32(workers), "no job may start after one has failed")
}

func TestForEachParallel_ReportsEveryIndependentFailure(t *testing.T) {
	// Neither job fails until both have started, so both failures are their
	// own rather than one cutting the other short.
	var bothStarted sync.WaitGroup
	bothStarted.Add(2)
	err := forEachParallel(context.Background(), 2, 2, func(ctx context.Context, i int) error {
		bothStarted.Done()
		bothStarted.Wait()
		return fmt.Errorf("job %d failed", i)
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "job 0 failed")
	assert.Contains(t, err.Error(), "job 1 failed")
}

func TestForEachParallel_RunsEveryJobWhenNoneFail(t *testing.T) {
	var ran atomic.Int32
	err := forEachParallel(context.Background(), 100, 4, func(ctx context.Context, i int) error {
		ran.Add(1)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, int32(100), ran.Load())
}
