package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/meilisearch/meilisearch-go"
	searchmetrics "go.miloapis.net/search/internal/metrics"
	"k8s.io/klog/v2"
)

// Writes to one document must land in resourceVersion order, yet the events
// for one object are spread across replicas that each batch for a flush
// interval, so an older event's batch can be enqueued after a newer one's. A
// Meilisearch AddDocuments replaces the whole document, which froze objects at
// whichever state flushed last.
//
// The document state store is the arbiter. Reading the current document from
// Meilisearch is not: its writes are queued tasks that take longer to apply
// than a flush interval under load, so a read would miss exactly the newer
// write it needs to see. Each flush therefore:
//
//  1. claims every operation, which replaces an operation the store shows to
//     be superseded with the newer state recorded there;
//  2. enqueues the claimed states, learning the task uid that carries them;
//  3. records that task against each document. Meilisearch applies tasks in
//     uid order, so when the store already names a newer state than the one
//     this task carries, that newer state was enqueued earlier and is about to
//     be overwritten; the flush enqueues it again so it lands last.
//
// Only the flush whose task is the latest recorded performs the repair; a
// flush that sees a higher task uid in the store leaves the outcome to that
// task's flush. A flush that fails between steps is nacked, and the
// redelivery runs the protocol again from the newest recorded state.

const (
	// maxRepairRounds bounds how many times a flush re-enqueues superseded
	// documents before giving up and nacking the batch for redelivery.
	maxRepairRounds = 3

	// maxClaimAttempts bounds the compare-and-set retries on one key.
	maxClaimAttempts = 10

	// docStateParallelism is how many store calls one flush has in flight.
	docStateParallelism = 32
)

// docClaim is one document's state as a flush will apply it.
type docClaim struct {
	indexUID string
	docID    string
	rec      docRecord
	doc      any
}

// flushGroup is one Meilisearch task's worth of claims: claims of one index
// that are all added or all removed, at most one chunk of them.
type flushGroup struct {
	indexUID string
	remove   bool
	claims   []*docClaim

	// task is the uid of the task that carries the group, set once the
	// enqueue returns (see groupTaskUID). Zero means the enqueue failed or the
	// client reported no tasks.
	task int64
}

func (g *flushGroup) enqueue(client SearchClient) ([]*meilisearch.Task, error) {
	if g.remove {
		ids := make([]string, 0, len(g.claims))
		for _, c := range g.claims {
			ids = append(ids, c.docID)
		}
		return client.DeleteDocumentsAsync(g.indexUID, ids)
	}
	docs := make([]any, 0, len(g.claims))
	for _, c := range g.claims {
		docs = append(docs, c.doc)
	}
	return client.AddDocumentsAsync(g.indexUID, docs)
}

// groupClaims splits claims by index and by whether the document is being
// added or removed, and each of those into groups of at most chunkSize claims
// when chunkSize is positive. Each group is one client call, and so, with
// chunkSize no larger than the client's own, one Meilisearch task.
func groupClaims(claims []*docClaim, chunkSize int) []*flushGroup {
	byKey := make(map[string]*flushGroup)
	var groups []*flushGroup
	for _, c := range claims {
		if c == nil {
			continue
		}
		remove := c.rec.State != docIndexed
		key := c.indexUID + "/add"
		if remove {
			key = c.indexUID + "/remove"
		}
		g, ok := byKey[key]
		if !ok || (chunkSize > 0 && len(g.claims) >= chunkSize) {
			g = &flushGroup{indexUID: c.indexUID, remove: remove}
			byKey[key] = g
			groups = append(groups, g)
		}
		g.claims = append(g.claims, c)
	}
	return groups
}

// claimBatch runs the claim step for every operation. A nil entry in the
// result is an operation the store superseded with a state it cannot supply,
// which is left to the flush that recorded that state.
func (b *Batcher) claimBatch(ctx context.Context, ops []pendingOp) ([]*docClaim, error) {
	claims := make([]*docClaim, len(ops))
	err := forEachParallel(ctx, len(ops), docStateParallelism, func(ctx context.Context, i int) error {
		c, err := b.claimOne(ctx, ops[i])
		if err != nil {
			return err
		}
		claims[i] = c
		return nil
	})
	return claims, err
}

// claimOne makes op's state the newest in the store if it is, and returns the
// state the flush should apply: op's own, or the store's when that is newer.
func (b *Batcher) claimOne(ctx context.Context, op pendingOp) (*docClaim, error) {
	key := pendingKey(op.indexUID, op.docID)
	mine := op.record()
	if op.isUpsert() {
		raw, err := json.Marshal(op.doc)
		if err != nil {
			return nil, fmt.Errorf("encode document %s: %w", key, err)
		}
		mine.Doc = raw
	}

	for attempt := 0; attempt < maxClaimAttempts; attempt++ {
		stored, rev, found, err := b.docState.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		if found && !mine.supersedes(stored) {
			searchmetrics.IndexerStaleWritesSuperseded.Inc()
			klog.V(2).Infof("Document %s: %s at resourceVersion %d is superseded by %s at %d already recorded, applying the recorded state",
				key, mine.State, mine.RV, stored.State, stored.RV)
			return claimFromRecord(op.indexUID, op.docID, stored)
		}

		desired := mine
		if found {
			desired.Task, desired.TaskRV, desired.TaskState = stored.Task, stored.TaskRV, stored.TaskState
		}
		if _, err := b.docState.Put(ctx, key, desired, rev); err != nil {
			if errors.Is(err, errDocStateConflict) {
				continue
			}
			return nil, err
		}
		return &docClaim{indexUID: op.indexUID, docID: op.docID, rec: desired, doc: op.doc}, nil
	}
	return nil, fmt.Errorf("document %s: state changed under %d claim attempts", key, maxClaimAttempts)
}

// claimFromRecord builds the claim that applies a stored state. It is nil when
// the store names an indexed state but holds no document for it, which only
// happens when the document was too large to keep; the write that recorded it
// has enqueued it already.
func claimFromRecord(indexUID, docID string, rec docRecord) (*docClaim, error) {
	c := &docClaim{indexUID: indexUID, docID: docID, rec: rec}
	if rec.State != docIndexed {
		return c, nil
	}
	if len(rec.Doc) == 0 {
		klog.Warningf("Document %s/%s: recorded state at resourceVersion %d has no document to apply, leaving it to the flush that recorded it", indexUID, docID, rec.RV)
		return nil, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Doc, &doc); err != nil {
		return nil, fmt.Errorf("decode recorded document %s/%s: %w", indexUID, docID, err)
	}
	c.doc = doc
	return c, nil
}

// recordBatch runs the record step for every claim in every enqueued group
// and returns the claims that must be enqueued again.
func (b *Batcher) recordBatch(ctx context.Context, groups []*flushGroup) ([]*docClaim, error) {
	type job struct {
		claim *docClaim
		task  int64
	}
	var jobs []job
	for _, g := range groups {
		if g.task == 0 {
			continue
		}
		for _, c := range g.claims {
			jobs = append(jobs, job{claim: c, task: g.task})
		}
	}

	repairs := make([]*docClaim, len(jobs))
	err := forEachParallel(ctx, len(jobs), docStateParallelism, func(ctx context.Context, i int) error {
		r, err := b.recordOne(ctx, jobs[i].claim, jobs[i].task)
		if err != nil {
			return err
		}
		repairs[i] = r
		return nil
	})

	var out []*docClaim
	for _, r := range repairs {
		if r != nil {
			out = append(out, r)
		}
	}
	return out, err
}

// recordOne notes that task carries c's state, and returns the newer state to
// enqueue again if the store shows task is about to overwrite it.
func (b *Batcher) recordOne(ctx context.Context, c *docClaim, task int64) (*docClaim, error) {
	key := pendingKey(c.indexUID, c.docID)

	for attempt := 0; attempt < maxClaimAttempts; attempt++ {
		stored, rev, found, err := b.docState.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !found {
			stored = docRecord{RV: c.rec.RV, State: c.rec.State, Doc: c.rec.Doc}
			rev = 0
		}
		// A later task is recorded, so it lands after this one and its flush
		// made the repair decision this flush would make. Writing this older
		// task over it would make the record name a task that does not land
		// last, and would repair a document the later task already settles.
		if task < stored.Task {
			return nil, nil
		}
		if task == stored.Task {
			return ownRecord(c, task, stored)
		}

		stored.Task, stored.TaskRV, stored.TaskState = task, c.rec.RV, c.rec.State
		if _, err := b.docState.Put(ctx, key, stored, rev); err != nil {
			if errors.Is(err, errDocStateConflict) {
				continue
			}
			return nil, err
		}
		if stored.applied() {
			return nil, nil
		}

		klog.V(2).Infof("Document %s: task %d carries %s at resourceVersion %d but %s at %d was enqueued before it, enqueueing the newer state again",
			key, task, c.rec.State, c.rec.RV, stored.State, stored.RV)
		return claimFromRecord(c.indexUID, c.docID, stored)
	}
	return nil, fmt.Errorf("document %s: state changed under %d record attempts", key, maxClaimAttempts)
}

// ownRecord settles a record step that finds its own task already recorded.
// That happens when an earlier attempt's write landed but was reported as
// failed, as when the connection drops before the reply and the read-back
// shows a changed revision, and the attempt is retried.
//
// Equal task uids can only come from this flush's own write. Meilisearch
// gives every enqueue its own uid, and one enqueue carries a key at most
// once; the documents of one group share a uid, but under different keys.
// The claim step copies the task fields forward when it records a newer
// state, so the record may since have moved to a newer state, but its task
// fields still describe this task.
//
// The write that landed is this flush's, so no other flush took on its repair
// decision: when the recorded state is not the one this task carries, the
// newer state must be enqueued again here, or nothing would. Task fields that
// name a different state for this task cannot arise from the protocol and are
// left alone.
func ownRecord(c *docClaim, task int64, stored docRecord) (*docClaim, error) {
	if stored.TaskRV != c.rec.RV || stored.TaskState != c.rec.State {
		klog.Warningf("Document %s/%s: task %d is recorded as carrying %s at resourceVersion %d, but this flush enqueued %s at %d with it, leaving the record as it is",
			c.indexUID, c.docID, task, stored.TaskState, stored.TaskRV, c.rec.State, c.rec.RV)
		return nil, nil
	}
	if stored.applied() {
		return nil, nil
	}
	klog.V(2).Infof("Document %s/%s: task %d carries %s at resourceVersion %d but %s at %d was enqueued before it, enqueueing the newer state again",
		c.indexUID, c.docID, task, c.rec.State, c.rec.RV, stored.State, stored.RV)
	return claimFromRecord(c.indexUID, c.docID, stored)
}

// errJobFailed cancels the jobs forEachParallel still has running once one of
// them has failed.
var errJobFailed = errors.New("another job failed")

// forEachParallel runs fn for every index with at most workers in flight and
// returns every error raised.
//
// One failure already fails the flush, so after the first error no further job
// is started and the ctx handed to those still running is cancelled. Without
// that, a document state store that has stopped answering would fail every
// job in turn, each after its own request timeout, and a large batch would
// hold its flush for dozens of timeouts instead of one. A job cut short this
// way is not reported, since the failure that stopped it already is; the
// result is still an error whenever any job failed, so a caller never takes
// the jobs that did not run for successes.
func forEachParallel(ctx context.Context, n, workers int, fn func(ctx context.Context, i int) error) error {
	if n == 0 {
		return nil
	}
	if workers > n {
		workers = n
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var wg sync.WaitGroup
	var errs flushErrors
	var failed atomic.Bool
	slots := make(chan struct{}, workers)
	for i := 0; i < n; i++ {
		slots <- struct{}{}
		// A failing job marks the failure before it frees its slot, so the
		// check sees it before the next job could take that slot.
		if failed.Load() {
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			err := fn(ctx, i)
			if err == nil {
				return
			}
			if failed.Swap(true) && errors.Is(context.Cause(ctx), errJobFailed) && errors.Is(err, context.Canceled) {
				return
			}
			errs.add(err)
			cancel(errJobFailed)
		}(i)
	}
	wg.Wait()
	return errs.join()
}
