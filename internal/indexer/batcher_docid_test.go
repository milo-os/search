package indexer

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

// TestBatcher_DocumentWithoutStringUID_IsSkippedAndAcked covers a policy
// transform that emits a uid that is not a string. Such a document has no id
// to order it by; if it were claimed, every one of them in an index would
// share one record. It is skipped, the rest of its message is still indexed,
// and its message is acked rather than redelivered forever.
func TestBatcher_DocumentWithoutStringUID_IsSkippedAndAcked(t *testing.T) {
	client := newRVSearchClient()
	store := NewMemoryDocStateStore()
	batcher := newOrderingBatcher(client, store)

	numericUID := rvDoc("ignored", 3, "numeric")
	numericUID["uid"] = 42
	missingUID := rvDoc("ignored", 4, "missing")
	delete(missingUID, "uid")

	m1 := &boundMsg{seq: 1}
	m2 := &boundMsg{seq: 2}
	var jm1, jm2 jetstream.Msg = m1, m2
	batcher.Submit(&jm1, []UpsertOp{{IndexUID: "proxies", Doc: numericUID}}, nil)
	batcher.Submit(&jm2,
		[]UpsertOp{
			{IndexUID: "proxies", Doc: missingUID},
			{IndexUID: "proxies", Doc: rvDoc("p1", 5, "valid")},
		},
		[]DeleteOp{{IndexUID: "gateways", DocID: "", ResourceVersion: 5}},
	)
	batcher.flush()

	requireAcked(t, m1, m2)
	assert.Equal(t, []string{"add proxies/p1@5"}, client.Log())

	recs := store.(*memoryDocStateStore).recs
	assert.Len(t, recs, 1, "only the document with an id may be recorded: %v", recs)
	assert.Contains(t, recs, docStateKey("proxies", "p1"))
}

func TestUpsertDocID(t *testing.T) {
	assert.Equal(t, "p1", upsertDocID(map[string]any{"uid": "p1"}))
	assert.Equal(t, "", upsertDocID(map[string]any{"uid": 42}))
	assert.Equal(t, "", upsertDocID(map[string]any{}))
	assert.Equal(t, "", upsertDocID("not a document"))
}
