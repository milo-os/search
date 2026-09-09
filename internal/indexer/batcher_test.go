package indexer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockSearchClient is a mock implementation of the SearchClient interface
type MockSearchClient struct {
	mock.Mock
}

func (m *MockSearchClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	args := m.Called(indexUID, documents)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*meilisearch.Task), args.Error(1)
}

func (m *MockSearchClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	args := m.Called(indexUID, documentIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*meilisearch.Task), args.Error(1)
}

func (m *MockSearchClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	args := m.Called(tasks)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*meilisearch.Task), args.Error(1)
}

// MockJetStreamMsg is a partial mock for jetstream.Msg
type MockJetStreamMsg struct {
	mock.Mock
	jetstream.Msg
	seq uint64

	// metaErr, when set, makes Metadata() return this error instead of a
	// sequence. trackMessage cannot dedup a message whose metadata errors, so
	// every call is treated as unique.
	metaErr error

	nakMu     sync.Mutex
	nakDelays []time.Duration
}

func (m *MockJetStreamMsg) Ack() error {
	args := m.Called()
	return args.Error(0)
}

// NakWithDelay records the redelivery request. It deliberately does not go
// through testify expectations so that tests which only care about Ack do not
// have to declare one on every message.
func (m *MockJetStreamMsg) NakWithDelay(delay time.Duration) error {
	m.nakMu.Lock()
	defer m.nakMu.Unlock()
	m.nakDelays = append(m.nakDelays, delay)
	return nil
}

// NakDelays returns the delays this message was nacked with, in call order.
func (m *MockJetStreamMsg) NakDelays() []time.Duration {
	m.nakMu.Lock()
	defer m.nakMu.Unlock()
	return append([]time.Duration(nil), m.nakDelays...)
}

func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	// Return a static metadata with the sequence ID configured for this mock
	return &jetstream.MsgMetadata{
		Sequence: jetstream.SequencePair{
			Stream: m.seq,
		},
	}, nil
}

func (m *MockJetStreamMsg) Data() []byte {
	args := m.Called()
	return args.Get(0).([]byte)
}

func TestBatcher_QueueUpsert_FlushOnSize(t *testing.T) {
	mockClient := new(MockSearchClient)
	batchConfig := BatchConfig{
		BatchSize:     2,
		FlushInterval: 1 * time.Hour, // Long interval to ensure size triggers it
	}

	batcher := NewBatcher(mockClient, batchConfig)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	// Expectation: AddDocumentsAsync called once with 2 docs
	mockClient.On("AddDocumentsAsync", "index-1", mock.MatchedBy(func(docs []any) bool {
		return len(docs) == 2
	})).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()

	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil).Once()

	// Queue 2 items with distinct messages
	msg1 := &MockJetStreamMsg{seq: 1}
	msg1.On("Ack").Return(nil)
	var jm1 jetstream.Msg = msg1

	msg2 := &MockJetStreamMsg{seq: 2}
	msg2.On("Ack").Return(nil)
	var jm2 jetstream.Msg = msg2

	batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)
	batcher.QueueUpsert("index-1", map[string]any{"uid": "2"}, &jm2) // This should trigger flush

	// Allow some time for the go routine to flush
	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)
}

func TestBatcher_QueueDelete_FlushOnSize(t *testing.T) {
	mockClient := new(MockSearchClient)
	batchConfig := BatchConfig{
		BatchSize:     2,
		FlushInterval: 1 * time.Hour,
	}

	batcher := NewBatcher(mockClient, batchConfig)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	// Expectation: DeleteDocumentsAsync called once with 2 IDs
	mockClient.On("DeleteDocumentsAsync", "index-1", mock.MatchedBy(func(ids []string) bool {
		return len(ids) == 2
	})).Return([]*meilisearch.Task{{TaskUID: 2}}, nil).Once()

	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil).Once()

	// Queue 2 items with distinct messages
	msg1 := &MockJetStreamMsg{seq: 3}
	msg1.On("Ack").Return(nil)
	var jm1 jetstream.Msg = msg1

	msg2 := &MockJetStreamMsg{seq: 4}
	msg2.On("Ack").Return(nil)
	var jm2 jetstream.Msg = msg2

	batcher.QueueDelete("index-1", "doc-1", &jm1)
	batcher.QueueDelete("index-1", "doc-2", &jm2) // This should trigger flush

	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)
}

func TestBatcher_FlushInterval(t *testing.T) {
	mockClient := new(MockSearchClient)
	batchConfig := BatchConfig{
		BatchSize:     10, // Large batch size
		FlushInterval: 50 * time.Millisecond,
	}

	batcher := NewBatcher(mockClient, batchConfig)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	// Expectation: AddDocumentsAsync called once with 1 doc due to timeout
	mockClient.On("AddDocumentsAsync", "index-1", mock.MatchedBy(func(docs []any) bool {
		return len(docs) == 1
	})).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()

	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil).Once()

	// Queue 1 item (less than batch size)
	msg1 := &MockJetStreamMsg{seq: 5}
	msg1.On("Ack").Return(nil)
	var jm1 jetstream.Msg = msg1

	batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)

	// Wait for interval to pass
	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)
}

func TestBatcher_VerifyConcurrentFlush(t *testing.T) {
	// verifies that flushes happen concurrently but respect semaphore
	mockClient := new(MockSearchClient)
	batchConfig := BatchConfig{
		BatchSize:     1, // Flush every item immediately
		FlushInterval: 1 * time.Hour,
	}

	batcher := NewBatcher(mockClient, batchConfig)

	var wg sync.WaitGroup
	wg.Add(2)

	// We want to simulate slow uploads to verify concurrency
	mockClient.On("AddDocumentsAsync", "index-1", mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(50 * time.Millisecond)
	}).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()

	mockClient.On("AddDocumentsAsync", "index-2", mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(50 * time.Millisecond)
	}).Return([]*meilisearch.Task{{TaskUID: 2}}, nil).Once()

	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil).Times(2)

	// Trigger two flushes rapidly for different indexes
	go func() {
		defer wg.Done()
		msg1 := &MockJetStreamMsg{seq: 6}
		msg1.On("Ack").Return(nil)
		var jm1 jetstream.Msg = msg1
		batcher.QueueUpsert("index-1", map[string]any{"uid": "1"}, &jm1)
	}()
	go func() {
		defer wg.Done()
		msg2 := &MockJetStreamMsg{seq: 7}
		msg2.On("Ack").Return(nil)
		var jm2 jetstream.Msg = msg2
		batcher.QueueUpsert("index-2", map[string]any{"uid": "2"}, &jm2)
	}()

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	mockClient.AssertExpectations(t)
}

func TestBatcher_Flush_ErrorHandling(t *testing.T) {
	mockClient := new(MockSearchClient)
	batchConfig := BatchConfig{
		BatchSize:     1, // Flush every item immediately
		FlushInterval: 1 * time.Hour,
	}
	batcher := NewBatcher(mockClient, batchConfig)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)

	// 1. AddDocumentsAsync Error -> Msg should NOT be Acked
	msg1 := &MockJetStreamMsg{seq: 10}
	msg1.On("Metadata").Return(&jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 10}}, nil)
	// We do NOT expect Ack on msg1

	mockClient.On("AddDocumentsAsync", "index-err-1", mock.Anything).Return(nil, fmt.Errorf("network error")).Once()

	var jm1 jetstream.Msg = msg1
	batcher.QueueUpsert("index-err-1", map[string]any{"uid": "1"}, &jm1)

	// Wait for async flush
	time.Sleep(50 * time.Millisecond)

	// 2. WaitForTasks Error -> Msg should NOT be Acked
	msg2 := &MockJetStreamMsg{seq: 11}
	msg2.On("Metadata").Return(&jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 11}}, nil)
	// We do NOT expect Ack on msg2

	mockClient.On("AddDocumentsAsync", "index-err-2", mock.Anything).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()
	mockClient.On("WaitForTasks", mock.Anything).Return(nil, fmt.Errorf("task failed")).Once()

	var jm2 jetstream.Msg = msg2
	batcher.QueueUpsert("index-err-2", map[string]any{"uid": "2"}, &jm2)

	// Wait for async flush
	time.Sleep(50 * time.Millisecond)

	// 3. DeleteDocumentsAsync Error -> Msg should NOT be Acked
	msg3 := &MockJetStreamMsg{seq: 12}
	msg3.On("Metadata").Return(&jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 12}}, nil)
	// We do NOT expect Ack on msg3

	mockClient.On("DeleteDocumentsAsync", "index-err-3", mock.Anything).Return(nil, fmt.Errorf("delete network error")).Once()

	var jm3 jetstream.Msg = msg3
	batcher.QueueDelete("index-err-3", "doc-3", &jm3)

	time.Sleep(50 * time.Millisecond)

	// 4. Delete WaitForTasks Error -> Msg should NOT be Acked
	msg4 := &MockJetStreamMsg{seq: 13}
	msg4.On("Metadata").Return(&jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 13}}, nil)
	// We do NOT expect Ack on msg4

	mockClient.On("DeleteDocumentsAsync", "index-err-4", mock.Anything).Return([]*meilisearch.Task{{TaskUID: 2}}, nil).Once()
	mockClient.On("WaitForTasks", mock.Anything).Return(nil, fmt.Errorf("delete task failed")).Once()

	var jm4 jetstream.Msg = msg4
	batcher.QueueDelete("index-err-4", "doc-4", &jm4)

	time.Sleep(50 * time.Millisecond)

	mockClient.AssertExpectations(t)
	msg1.AssertNotCalled(t, "Ack")
	msg2.AssertNotCalled(t, "Ack")
	msg3.AssertNotCalled(t, "Ack")
	msg4.AssertNotCalled(t, "Ack")
}

// TestBatcher_QueueDelete_MultiPolicyFanOut_TriggersOnMessageCount covers
// issue #113's delete-side fix: a source message that fans out into one
// delete operation per matching policy must only trigger a flush once
// BatchSize distinct *messages* have been seen, not once BatchSize pending
// delete keys have accumulated, and every operation derived from one message
// must land in the same flush. Submit applies all of a message's operations
// under one lock and evaluates the trigger once afterwards, so message 100's
// ten fanned-out deletes can never be split across two flushes the way
// ten sequential QueueDelete calls could.
func TestBatcher_QueueDelete_MultiPolicyFanOut_TriggersOnMessageCount(t *testing.T) {
	const (
		numIndices = 10
		batchSize  = 100
	)

	mockClient := new(MockSearchClient)

	var mu sync.Mutex
	idCounts := make(map[string]int)
	var deleteCallCount int32
	// completedCount tracks flush completion (WaitForTasks returning), not just
	// DeleteDocumentsAsync being called: the per-index flush goroutines run
	// concurrently, so waiting on deleteCallCount alone can race ahead of the
	// matching WaitForTasks call still being in flight.
	var completedCount int32

	for p := 0; p < numIndices; p++ {
		indexUID := fmt.Sprintf("index-%d", p)
		mockClient.On("DeleteDocumentsAsync", indexUID, mock.Anything).
			Run(func(args mock.Arguments) {
				ids := args.Get(1).([]string)
				mu.Lock()
				idCounts[indexUID] = len(ids)
				mu.Unlock()
				atomic.AddInt32(&deleteCallCount, 1)
			}).
			Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()
	}
	mockClient.On("WaitForTasks", mock.Anything).
		Run(func(mock.Arguments) { atomic.AddInt32(&completedCount, 1) }).
		Return(&meilisearch.Task{Status: "succeeded"}, nil).Times(numIndices)

	batcher := NewBatcher(mockClient, BatchConfig{
		BatchSize:     batchSize,
		FlushInterval: time.Hour,
	})

	submitMessage := func(m int) {
		msg := &MockJetStreamMsg{seq: uint64(m)}
		msg.On("Ack").Return(nil)
		var jm jetstream.Msg = msg
		deletes := make([]DeleteOp, numIndices)
		for p := 0; p < numIndices; p++ {
			deletes[p] = DeleteOp{IndexUID: fmt.Sprintf("index-%d", p), DocID: fmt.Sprintf("doc-%d", m)}
		}
		batcher.Submit(&jm, nil, deletes)
	}

	// Under the old key-count trigger, these first 10 messages (100 pending
	// keys) would already have flushed. Confirm the fix does not.
	for m := 1; m <= 10; m++ {
		submitMessage(m)
	}
	assert.Equal(t, int32(0), atomic.LoadInt32(&deleteCallCount), "flush must not fire on pending-key count alone")

	for m := 11; m <= 99; m++ {
		submitMessage(m)
	}
	assert.Equal(t, int32(0), atomic.LoadInt32(&deleteCallCount), "flush must not fire before the 100th message")

	submitMessage(100)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&completedCount) == numIndices
	}, time.Second, 5*time.Millisecond, "expected exactly one flush, fanned out to all %d indices", numIndices)

	mockClient.AssertExpectations(t)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, idCounts, numIndices)
	// Submit applies message 100's ten deletes atomically and evaluates the
	// trigger only once afterwards, so every index sees all 100 IDs in the
	// single flush that fires - no off-by-one split of message 100 across two
	// batches.
	for p := 0; p < numIndices; p++ {
		assert.Equal(t, 100, idCounts[fmt.Sprintf("index-%d", p)], "index-%d", p)
	}
}

// TestBatcher_TrackMessage_DedupBySequence verifies that queueing the same
// underlying message multiple times (e.g. once per matched policy) tracks it
// for acknowledgement exactly once, keyed on its stream sequence.
func TestBatcher_TrackMessage_DedupBySequence(t *testing.T) {
	mockClient := new(MockSearchClient)
	batcher := NewBatcher(mockClient, BatchConfig{BatchSize: 1000, FlushInterval: time.Hour})

	msg := &MockJetStreamMsg{seq: 42}
	ackDone := make(chan struct{})
	msg.On("Ack").Run(func(mock.Arguments) { close(ackDone) }).Return(nil)
	var jm jetstream.Msg = msg

	const fanOut = 10
	for i := 0; i < fanOut; i++ {
		batcher.QueueDelete("index-1", fmt.Sprintf("doc-%d", i), &jm)
	}

	batcher.mu.Lock()
	assert.Len(t, batcher.deleteMsgs, 1, "the same stream sequence must be tracked once")
	assert.Len(t, batcher.deleteMsgSeqs, 1)
	assert.Len(t, batcher.pendingDeletes, fanOut, "distinct doc IDs are still queued as separate deletes")
	batcher.mu.Unlock()

	mockClient.On("DeleteDocumentsAsync", "index-1", mock.MatchedBy(func(ids []string) bool {
		return len(ids) == fanOut
	})).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()
	mockClient.On("WaitForTasks", mock.Anything).Return(&meilisearch.Task{Status: "succeeded"}, nil).Once()

	batcher.flushDeletes()

	select {
	case <-ackDone:
	case <-time.After(time.Second):
		t.Fatal("flush did not ack the message")
	}

	mockClient.AssertExpectations(t)
	msg.AssertNumberOfCalls(t, "Ack", 1)
}

// TestBatcher_TrackMessage_MetadataErrorTreatedAsUnique verifies the fallback
// path in trackMessage: a message whose Metadata() call errors cannot be
// deduped by stream sequence, so every call to Queue{Upsert,Delete} appends
// it again rather than being dropped or panicking.
func TestBatcher_TrackMessage_MetadataErrorTreatedAsUnique(t *testing.T) {
	mockClient := new(MockSearchClient)
	batcher := NewBatcher(mockClient, BatchConfig{BatchSize: 1000, FlushInterval: time.Hour})

	msg := &MockJetStreamMsg{metaErr: fmt.Errorf("metadata unavailable")}
	var jm jetstream.Msg = msg

	const calls = 10
	for i := 0; i < calls; i++ {
		batcher.QueueDelete("index-1", fmt.Sprintf("doc-%d", i), &jm)
	}

	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	assert.Len(t, batcher.deleteMsgs, calls, "a message whose metadata errors cannot be deduped, so each call is appended")
}

// TestBatcher_NakOnFlushFailure covers issue #113's failure path: whatever
// stage fails (enqueueing the request or waiting for the Meilisearch task),
// every message in the batch must be nacked with the fixed redelivery delay
// and none may be acked.
func TestBatcher_NakOnFlushFailure(t *testing.T) {
	tests := []struct {
		name     string
		isUpsert bool
		addErr   error // AddDocumentsAsync/DeleteDocumentsAsync error; nil means that stage succeeds
		waitErr  error // WaitForTasks error; only used when addErr is nil
	}{
		{name: "upsert enqueue error", isUpsert: true, addErr: fmt.Errorf("add failed")},
		{name: "upsert wait error", isUpsert: true, waitErr: fmt.Errorf("wait failed")},
		{name: "delete enqueue error", isUpsert: false, addErr: fmt.Errorf("delete failed")},
		{name: "delete wait error", isUpsert: false, waitErr: fmt.Errorf("wait failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockSearchClient)
			batcher := NewBatcher(mockClient, BatchConfig{BatchSize: 3, FlushInterval: time.Hour})

			const n = 3
			indexUID := "index-nak"

			if tt.addErr != nil {
				if tt.isUpsert {
					mockClient.On("AddDocumentsAsync", indexUID, mock.Anything).Return(nil, tt.addErr).Once()
				} else {
					mockClient.On("DeleteDocumentsAsync", indexUID, mock.Anything).Return(nil, tt.addErr).Once()
				}
			} else {
				if tt.isUpsert {
					mockClient.On("AddDocumentsAsync", indexUID, mock.Anything).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()
				} else {
					mockClient.On("DeleteDocumentsAsync", indexUID, mock.Anything).Return([]*meilisearch.Task{{TaskUID: 1}}, nil).Once()
				}
				mockClient.On("WaitForTasks", mock.Anything).Return(nil, tt.waitErr).Once()
			}

			msgs := make([]*MockJetStreamMsg, n)
			for i := 0; i < n; i++ {
				msgs[i] = &MockJetStreamMsg{seq: uint64(i + 1)}
			}
			for i, m := range msgs {
				var jm jetstream.Msg = m
				if tt.isUpsert {
					batcher.QueueUpsert(indexUID, map[string]any{"uid": fmt.Sprintf("doc-%d", i)}, &jm)
				} else {
					batcher.QueueDelete(indexUID, fmt.Sprintf("doc-%d", i), &jm)
				}
			}

			require.Eventually(t, func() bool {
				total := 0
				for _, m := range msgs {
					total += len(m.NakDelays())
				}
				return total == n
			}, time.Second, 5*time.Millisecond, "expected all %d messages to be nacked", n)

			mockClient.AssertExpectations(t)
			for _, m := range msgs {
				assert.Equal(t, []time.Duration{nakDelay}, m.NakDelays())
				m.AssertNotCalled(t, "Ack")
			}
		})
	}
}
