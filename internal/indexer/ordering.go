package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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

// flushGroup is one Meilisearch task's worth of claims: every claim of one
// index that is either added or removed.
type flushGroup struct {
	indexUID string
	remove   bool
	claims   []*docClaim

	// task is the earliest uid of the tasks the group was enqueued as, set
	// once the enqueue returns. Zero means the enqueue failed or the client
	// reported no tasks.
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
// added or removed.
func groupClaims(claims []*docClaim) []*flushGroup {
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
		if !ok {
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
	err := forEachParallel(len(ops), docStateParallelism, func(i int) error {
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
	err := forEachParallel(len(jobs), docStateParallelism, func(i int) error {
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
		if task <= stored.Task {
			return nil, nil
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

// forEachParallel runs fn for every index with at most workers in flight and
// returns every error raised.
func forEachParallel(n, workers int, fn func(i int) error) error {
	if n == 0 {
		return nil
	}
	if workers > n {
		workers = n
	}

	var wg sync.WaitGroup
	var errs flushErrors
	slots := make(chan struct{}, workers)
	for i := 0; i < n; i++ {
		slots <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := fn(i); err != nil {
				errs.add(err)
			}
		}(i)
	}
	wg.Wait()
	return errs.join()
}
