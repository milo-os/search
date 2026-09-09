package meilisearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// taskInfoResponse writes a 202-Accepted TaskInfo body, matching what
// meilisearch-go expects from POST /indexes/{uid}/documents and
// /indexes/{uid}/documents/delete-batch.
func taskInfoResponse(w http.ResponseWriter, taskUID int, indexUID string) {
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"taskUid":    taskUID,
		"indexUid":   indexUID,
		"status":     "enqueued",
		"type":       "documentDeletion",
		"enqueuedAt": "2026-01-01T00:00:00Z",
	})
}

// taskStatusResponse writes the body GET /tasks/{uid} returns while polling.
func taskStatusResponse(w http.ResponseWriter, taskUID int, indexUID, status string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"uid":        taskUID,
		"indexUid":   indexUID,
		"status":     status,
		"type":       "documentDeletion",
		"enqueuedAt": "2026-01-01T00:00:00Z",
	})
}

// TestSDKClient_WaitForTasks_TimesOut covers issue #113's fix on the
// meilisearch side: before the fix, WaitForTask/WaitForTasks had no deadline
// at all (the SDK's non-context WaitForTask call runs against
// context.Background()), so a stuck Meilisearch task blocked forever. Task
// 1's status never leaves "enqueued", so the wait must give up at
// WaitTimeout rather than hang.
func TestSDKClient_WaitForTasks_TimesOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/indexes/x/documents/delete-batch", func(w http.ResponseWriter, r *http.Request) {
		taskInfoResponse(w, 1, "x")
	})
	mux.HandleFunc("/tasks/1", func(w http.ResponseWriter, r *http.Request) {
		taskStatusResponse(w, 1, "x", "enqueued")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := NewSDKClient(SDKConfig{
		Domain:       srv.URL,
		APIKey:       "test",
		WaitTimeout:  300 * time.Millisecond,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	tasks, err := client.DeleteDocumentsAsync("x", []string{"doc-1"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	start := time.Now()
	_, err = client.WaitForTasks(tasks)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "task 1")
	require.Contains(t, err.Error(), `"x"`)

	// Generous bound: well under the 1s test budget and clearly distinguishes
	// "bounded by WaitTimeout" from "blocked indefinitely".
	assert.LessOrEqual(t, elapsed, 700*time.Millisecond, "WaitForTasks took too long to time out")
	assert.GreaterOrEqual(t, elapsed, 250*time.Millisecond, "WaitForTasks returned suspiciously early for a 300ms timeout")
}

// TestSDKClient_WaitForTasks_SucceedsBeforeTimeout verifies the happy path
// still works once polling is bounded by a deadline: a task that succeeds
// partway through polling returns without error, well inside WaitTimeout.
func TestSDKClient_WaitForTasks_SucceedsBeforeTimeout(t *testing.T) {
	var pollCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("/indexes/x/documents/delete-batch", func(w http.ResponseWriter, r *http.Request) {
		taskInfoResponse(w, 1, "x")
	})
	mux.HandleFunc("/tasks/1", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&pollCount, 1)
		status := "enqueued"
		if n >= 3 {
			status = "succeeded"
		}
		taskStatusResponse(w, 1, "x", status)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := NewSDKClient(SDKConfig{
		Domain:       srv.URL,
		APIKey:       "test",
		WaitTimeout:  300 * time.Millisecond,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	tasks, err := client.DeleteDocumentsAsync("x", []string{"doc-1"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	start := time.Now()
	result, err := client.WaitForTasks(tasks)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&pollCount)), 3, "expected the task to still be polled at least 3 times")
	assert.Less(t, elapsed, 300*time.Millisecond, "a task that succeeds should not wait out the full timeout")
}

// TestSDKClient_WaitForTasks_ShareOneDeadline verifies that WaitForTasks
// gives a whole batch of tasks a single deadline instead of a fresh
// WaitTimeout budget per task. Task 1 succeeds partway through polling
// (consuming a meaningful slice of the budget); task 2 never completes. If
// each task got its own fresh timeout, the call would take roughly
// (time for task 1) + WaitTimeout. With a shared deadline it must instead
// return at approximately the original WaitTimeout.
func TestSDKClient_WaitForTasks_ShareOneDeadline(t *testing.T) {
	var pollCount1 int32
	mux := http.NewServeMux()
	mux.HandleFunc("/indexes/x/documents/delete-batch", func(w http.ResponseWriter, r *http.Request) {
		taskInfoResponse(w, 1, "x")
	})
	mux.HandleFunc("/indexes/y/documents/delete-batch", func(w http.ResponseWriter, r *http.Request) {
		taskInfoResponse(w, 2, "y")
	})
	mux.HandleFunc("/tasks/1", func(w http.ResponseWriter, r *http.Request) {
		// PollInterval is 20ms; ~10 polls (including the immediate first
		// check) is roughly 180ms, leaving task 2 well short of a fresh
		// 300ms budget of its own but easily inside a single 300ms deadline
		// shared from the call's start.
		n := atomic.AddInt32(&pollCount1, 1)
		status := "enqueued"
		if n >= 10 {
			status = "succeeded"
		}
		taskStatusResponse(w, 1, "x", status)
	})
	mux.HandleFunc("/tasks/2", func(w http.ResponseWriter, r *http.Request) {
		taskStatusResponse(w, 2, "y", "enqueued") // never completes
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := NewSDKClient(SDKConfig{
		Domain:       srv.URL,
		APIKey:       "test",
		WaitTimeout:  300 * time.Millisecond,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	tasksX, err := client.DeleteDocumentsAsync("x", []string{"doc-1"})
	require.NoError(t, err)
	tasksY, err := client.DeleteDocumentsAsync("y", []string{"doc-1"})
	require.NoError(t, err)
	tasks := append(tasksX, tasksY...)
	require.Len(t, tasks, 2)

	start := time.Now()
	_, err = client.WaitForTasks(tasks)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "task 2")

	// Independent per-task budgets would total roughly 180ms (task 1) + 300ms
	// (task 2) = ~480ms or more. A shared deadline caps the whole call at
	// approximately the original 300ms.
	assert.Less(t, elapsed, 450*time.Millisecond, "tasks did not appear to share a single deadline")
	assert.GreaterOrEqual(t, elapsed, 250*time.Millisecond)
}

// TestSDKClient_DeleteDocumentsAsync_DoesNotRetryOnHTTPTimeout covers
// withRetry's retry predicate, which only retries "EOF" and "connection
// reset" errors. An HTTP client timeout error matches neither, so a stuck
// Meilisearch response must fail after a single HTTPTimeout rather than
// running the full MaxRetries loop (which, with backoff, would take several
// times as long).
func TestSDKClient_DeleteDocumentsAsync_DoesNotRetryOnHTTPTimeout(t *testing.T) {
	const httpTimeout = 150 * time.Millisecond

	mux := http.NewServeMux()
	mux.HandleFunc("/indexes/x/documents/delete-batch", func(w http.ResponseWriter, r *http.Request) {
		// Longer than HTTPTimeout so the client gives up first, but short
		// enough that httptest.Server.Close (which waits for in-flight
		// handlers) doesn't blow the test's time budget.
		time.Sleep(350 * time.Millisecond)
		taskInfoResponse(w, 1, "x")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := NewSDKClient(SDKConfig{
		Domain:      srv.URL,
		APIKey:      "test",
		HTTPTimeout: httpTimeout,
		MaxRetries:  3,
		RetryDelay:  500 * time.Millisecond,
	})
	require.NoError(t, err)

	start := time.Now()
	_, err = client.DeleteDocumentsAsync("x", []string{"doc-1"})
	elapsed := time.Since(start)

	require.Error(t, err)
	// A 3-attempt retry loop with backoff (500ms + 1000ms of sleeping alone)
	// would take well over a second; returning after a single HTTP timeout
	// should be close to httpTimeout and comfortably under twice that.
	assert.Less(t, elapsed, 2*httpTimeout, "DeleteDocumentsAsync appears to have retried instead of failing after one HTTP timeout")
	assert.GreaterOrEqual(t, elapsed, httpTimeout-50*time.Millisecond)
}
