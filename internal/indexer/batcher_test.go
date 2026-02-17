package indexer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
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
}

func (m *MockJetStreamMsg) Ack() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
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
