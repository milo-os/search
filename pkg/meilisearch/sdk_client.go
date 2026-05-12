package meilisearch

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/meilisearch/meilisearch-go"
)

type SDKConfig struct {
	// Domain is the URL of the Meilisearch instance
	Domain string
	// APIKey is the API key for the Meilisearch instance
	APIKey string
	// WaitTimeout is the timeout for waiting for tasks to complete
	WaitTimeout time.Duration
	// HTTPTimeout is the timeout for HTTP requests
	HTTPTimeout time.Duration
	// ChunkSize is the number of documents to process in a single chunk
	ChunkSize int
	// MaxRetries is the maximum number of retries for transient errors
	MaxRetries int
	// RetryDelay is the base delay between retries
	RetryDelay time.Duration
}

type SDKClient struct {
	client      meilisearch.ServiceManager
	waitTimeout time.Duration
	chunkSize   int
	maxRetries  int
	retryDelay  time.Duration
}

func NewSDKClient(config SDKConfig) (*SDKClient, error) {
	// env fallbacks
	if config.APIKey == "" {
		config.APIKey = os.Getenv("MEILISEARCH_API_KEY")
	}
	if config.Domain == "" {
		config.Domain = os.Getenv("MEILISEARCH_DOMAIN")
	}

	if config.APIKey == "" || config.Domain == "" {
		klog.Error("Meilisearch API key or domain is not set")
		return nil, fmt.Errorf("meilisearch API key or domain is not set")
	}

	if config.HTTPTimeout == 0 {
		config.HTTPTimeout = 60 * time.Second
	}

	httpClient := &http.Client{
		Timeout: config.HTTPTimeout,
	}

	client := meilisearch.New(
		config.Domain,
		meilisearch.WithAPIKey(config.APIKey),
		meilisearch.WithCustomClient(httpClient),
	)

	chunkSize := config.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	retryDelay := config.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 500 * time.Millisecond
	}

	return &SDKClient{
		client:      client,
		waitTimeout: config.WaitTimeout,
		chunkSize:   chunkSize,
		maxRetries:  maxRetries,
		retryDelay:  retryDelay,
	}, nil
}

// IndexExists checks if an index with the given UID exists.
func (s *SDKClient) IndexExists(uid string) (bool, error) {
	_, err := s.client.GetIndex(uid)
	if err != nil {
		// Check for specific "index_not_found" code from Meilisearch
		if strings.Contains(err.Error(), "index_not_found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CreateIndex creates an index with the given UID and waits for completion.
func (s *SDKClient) CreateIndex(uid string) (*meilisearch.Task, error) {
	// Create index with primary key
	resp, err := s.client.CreateIndex(&meilisearch.IndexConfig{
		Uid:        uid,
		PrimaryKey: "uid",
	})
	if err != nil {
		klog.Errorf("Failed to create index: %v", err)
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	// Wait for task to complete for index creation as it's a structural change
	task, err := s.waitForTask(resp.TaskUID)
	if err != nil {
		return nil, err
	}

	klog.Infof("Index created successfully: %s", uid)
	return task, nil
}

// GetIndexCreationTask returns the index creation task for the given index UID.
func (s *SDKClient) GetIndexCreationTask(indexUID string) (*meilisearch.Task, error) {
	resp, err := s.client.GetTasks(&meilisearch.TasksQuery{
		IndexUIDS: []string{indexUID},
		Types:     []meilisearch.TaskType{"indexCreation"},
	})
	if err != nil {
		// If index is not found, it means no task exists for it
		if strings.Contains(err.Error(), "index_not_found") {
			klog.Infof("Index %s not found (index_not_found), assuming no creation task", indexUID)
			return nil, nil
		}
		klog.Errorf("Failed to get index creation task: %v", err)
		return nil, fmt.Errorf("failed to get index creation task: %w", err)
	}
	if len(resp.Results) == 0 {
		klog.Infof("No index creation task found for index %s", indexUID)
		return nil, nil
	}
	// Return the most recent task
	return &resp.Results[0], nil
}

// withRetry executes a function with a simple retry logic for transient network errors.
func (s *SDKClient) withRetry(operation string, fn func() (*meilisearch.Task, error)) (*meilisearch.Task, error) {
	var lastErr error
	for i := 0; i < s.maxRetries; i++ {
		if i > 0 {
			klog.Warningf("Retrying Meilisearch %s operation (attempt %d/%d) after error: %v", operation, i+1, s.maxRetries, lastErr)
			time.Sleep(time.Duration(i) * s.retryDelay)
		}
		task, err := fn()
		if err == nil {
			return task, nil
		}
		lastErr = err
		// Only retry on suspected transient network errors
		errMsg := err.Error()
		if !strings.Contains(errMsg, "EOF") && !strings.Contains(errMsg, "connection reset") && !strings.Contains(errMsg, "timeout") {
			return nil, err
		}
	}
	return nil, fmt.Errorf("operation %s failed after %d attempts: %w", operation, s.maxRetries, lastErr)
}

// AddDocumentsAsync enqueues documents in chunks and returns the tasks.
func (s *SDKClient) AddDocumentsAsync(indexUID string, documents []any) ([]*meilisearch.Task, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	klog.Infof("Index %s: Adding %d documents in chunks of %d", indexUID, len(documents), s.chunkSize)

	return s.processInChunks(len(documents), s.chunkSize, "AddDocuments", func(start, end int) (*meilisearch.Task, error) {
		resp, err := s.client.Index(indexUID).AddDocuments(documents[start:end], nil)
		if err != nil {
			return nil, err
		}
		return &meilisearch.Task{TaskUID: resp.TaskUID, IndexUID: indexUID}, nil
	})
}

// DeleteDocumentsAsync enqueues document deletions in chunks and returns the tasks.
func (s *SDKClient) DeleteDocumentsAsync(indexUID string, documentIDs []string) ([]*meilisearch.Task, error) {
	if len(documentIDs) == 0 {
		return nil, nil
	}

	// Deduplicate and validate IDs
	uniqueIDs := make([]string, 0, len(documentIDs))
	seen := make(map[string]struct{})
	for _, id := range documentIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			uniqueIDs = append(uniqueIDs, id)
		}
	}

	if len(uniqueIDs) == 0 {
		return nil, nil
	}

	klog.Infof("Index %s: Deleting %d documents in chunks of %d", indexUID, len(uniqueIDs), s.chunkSize)

	return s.processInChunks(len(uniqueIDs), s.chunkSize, "DeleteDocuments", func(start, end int) (*meilisearch.Task, error) {
		resp, err := s.client.Index(indexUID).DeleteDocuments(uniqueIDs[start:end], nil)
		if err != nil {
			return nil, err
		}
		return &meilisearch.Task{TaskUID: resp.TaskUID, IndexUID: indexUID}, nil
	})
}

// processInChunks is a helper to process items in chunks with retry logic.
func (s *SDKClient) processInChunks(totalItems int, chunkSize int, operationName string, fn func(start, end int) (*meilisearch.Task, error)) ([]*meilisearch.Task, error) {
	var tasks []*meilisearch.Task

	for i := 0; i < totalItems; i += chunkSize {
		end := i + chunkSize
		if end > totalItems {
			end = totalItems
		}

		task, err := s.withRetry(fmt.Sprintf("%s_Chunk_%d", operationName, i/chunkSize), func() (*meilisearch.Task, error) {
			return fn(i, end)
		})

		if err != nil {
			return tasks, err // Return what we managed to enqueue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// WaitForTasks waits for a slice of tasks to complete and returns the last one.
func (s *SDKClient) WaitForTasks(tasks []*meilisearch.Task) (*meilisearch.Task, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	var lastTask *meilisearch.Task
	for _, t := range tasks {
		res, err := s.WaitForTaskCompletion(t)
		if err != nil {
			return res, err
		}
		lastTask = res
	}
	return lastTask, nil
}

// WaitForTaskCompletion waits for a task to succeed and returns an error if it fails or times out.
func (s *SDKClient) WaitForTaskCompletion(task *meilisearch.Task) (*meilisearch.Task, error) {
	if task == nil {
		return nil, nil
	}
	res, err := s.waitForTask(task.TaskUID)
	if err != nil {
		return nil, err
	}
	if res.Status != meilisearch.TaskStatusSucceeded {
		errMsg := ""
		if res.Error.Message != "" {
			errMsg = fmt.Sprintf(": %s (%s)", res.Error.Message, res.Error.Code)
		}
		klog.Errorf("Task %d failed with status %s%s", res.TaskUID, res.Status, errMsg)
		return res, fmt.Errorf("task %d failed: %s%s", res.TaskUID, res.Status, errMsg)
	}
	return res, nil
}

// WaitForTask waits for the given task to complete.
func (s *SDKClient) WaitForTask(taskUid int64) (*meilisearch.Task, error) {
	return s.waitForTask(taskUid)
}

// waitForTask waits for the given task to complete. Internal use.
func (s *SDKClient) waitForTask(taskUid int64) (*meilisearch.Task, error) {
	task, err := s.client.WaitForTask(taskUid, s.waitTimeout)
	if err != nil {
		klog.Errorf("Failed to wait for task %d: %v", taskUid, err)
		return nil, fmt.Errorf("failed to wait for task: %w", err)
	}
	return task, nil
}

// IsTaskPending checks if the given task is pending.
func (s *SDKClient) IsTaskPending(task *meilisearch.Task) bool {
	return task.Status == meilisearch.TaskStatusEnqueued || task.Status == meilisearch.TaskStatusProcessing
}

// IsTaskFailed checks if the given task has failed.
func (s *SDKClient) IsTaskFailed(task *meilisearch.Task) bool {
	return task.Status == meilisearch.TaskStatusFailed || task.Status == meilisearch.TaskStatusCanceled
}

// IsTaskSucceeded checks if the given task has succeeded.
func (s *SDKClient) IsTaskSucceeded(task *meilisearch.Task) bool {
	return task.Status == meilisearch.TaskStatusSucceeded
}

// GetSearchableAttributes returns the searchable attributes for the given index.
func (s *SDKClient) GetSearchableAttributes(indexUID string) ([]string, error) {
	resp, err := s.client.Index(indexUID).GetSearchableAttributes()
	if err != nil {
		klog.Errorf("Failed to get searchable attributes for index %s: %v", indexUID, err)
		return nil, fmt.Errorf("failed to get searchable attributes: %w", err)
	}
	// If nil, it means all attributes are searchable (default behavior of Meilisearch)
	// However, the typed return is []string. Meilisearch-go returns *[]string
	if resp == nil {
		return []string{"*"}, nil
	}
	return *resp, nil
}

// GetFilterableAttributes returns the filterable attributes for the given index.
func (s *SDKClient) GetFilterableAttributes(indexUID string) ([]string, error) {
	resp, err := s.client.Index(indexUID).GetFilterableAttributes()
	if err != nil {
		klog.Errorf("Failed to get filterable attributes for index %s: %v", indexUID, err)
		return nil, fmt.Errorf("failed to get filterable attributes: %w", err)
	}
	if resp == nil {
		return nil, nil
	}
	// Convert []interface{} to []string
	result := make([]string, 0, len(*resp))
	for _, v := range *resp {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result, nil
}

// UpdateFilterableAttributes updates the filterable attributes for the given index.
func (s *SDKClient) UpdateFilterableAttributes(indexUID string, attributes []string) (*meilisearch.Task, error) {
	klog.Infof("Updating filterable attributes for index %s: %v", indexUID, attributes)
	// The meilisearch-go SDK expects *[]interface{} for filterable attributes.
	iface := make([]interface{}, len(attributes))
	for i, a := range attributes {
		iface[i] = a
	}
	resp, err := s.client.Index(indexUID).UpdateFilterableAttributes(&iface)
	if err != nil {
		klog.Errorf("Failed to update filterable attributes for index %s: %v", indexUID, err)
		return nil, fmt.Errorf("failed to update filterable attributes: %w", err)
	}
	return &meilisearch.Task{TaskUID: resp.TaskUID, IndexUID: indexUID}, nil
}

// DeleteDocumentsByFilter deletes documents matching the given filter expression and
// waits for the task to complete before returning.
func (s *SDKClient) DeleteDocumentsByFilter(indexUID string, filter string) error {
	resp, err := s.client.Index(indexUID).DeleteDocumentsByFilter(filter, nil)
	if err != nil {
		klog.Errorf("Failed to delete documents by filter for index %s: %v", indexUID, err)
		return fmt.Errorf("failed to delete documents by filter for index %s: %w", indexUID, err)
	}

	task, err := s.waitForTask(resp.TaskUID)
	if err != nil {
		return fmt.Errorf("failed to wait for delete-by-filter task: %w", err)
	}

	if task.Status != meilisearch.TaskStatusSucceeded {
		return fmt.Errorf("delete-by-filter task failed with status %s: %s", task.Status, task.Error.Message)
	}

	klog.Infof("Documents deleted by filter %q from index %s", filter, indexUID)
	return nil
}

// UpdateSearchableAttributes updates the searchable attributes for the given index.
func (s *SDKClient) UpdateSearchableAttributes(indexUID string, attributes []string) (*meilisearch.Task, error) {
	klog.Infof("Updating searchable attributes for index %s: %v", indexUID, attributes)
	resp, err := s.client.Index(indexUID).UpdateSearchableAttributes(&attributes)
	if err != nil {
		klog.Errorf("Failed to update searchable attributes for index %s: %v", indexUID, err)
		return nil, fmt.Errorf("failed to update searchable attributes: %w", err)
	}

	return &meilisearch.Task{TaskUID: resp.TaskUID, IndexUID: indexUID}, nil
}

// DeleteAllDocuments deletes all documents from the given index and waits for the
// task to complete before returning. If the index does not exist, it is a no-op.
func (s *SDKClient) DeleteAllDocuments(indexUID string) error {
	exists, err := s.IndexExists(indexUID)
	if err != nil {
		return fmt.Errorf("failed to check if index %s exists: %w", indexUID, err)
	}
	if !exists {
		klog.Infof("Index %s not found, skipping document deletion", indexUID)
		return nil
	}

	resp, err := s.client.Index(indexUID).DeleteAllDocuments(nil)
	if err != nil {
		return fmt.Errorf("failed to delete all documents from index %s: %w", indexUID, err)
	}

	task, err := s.waitForTask(resp.TaskUID)
	if err != nil {
		return fmt.Errorf("failed to wait for document deletion task: %w", err)
	}

	if task.Status != meilisearch.TaskStatusSucceeded {
		return fmt.Errorf("document deletion task failed with status %s: %s", task.Status, task.Error.Message)
	}

	klog.Infof("All documents deleted from index %s", indexUID)
	return nil
}

// DeleteIndex deletes the index with the given UID and waits for the task to
// complete. If the index does not exist, it is a no-op.
func (s *SDKClient) DeleteIndex(indexUID string) error {
	exists, err := s.IndexExists(indexUID)
	if err != nil {
		return fmt.Errorf("failed to check if index %s exists: %w", indexUID, err)
	}
	if !exists {
		klog.Infof("Index %s not found, skipping deletion", indexUID)
		return nil
	}

	resp, err := s.client.DeleteIndex(indexUID)
	if err != nil {
		return fmt.Errorf("failed to delete index %s: %w", indexUID, err)
	}

	task, err := s.waitForTask(resp.TaskUID)
	if err != nil {
		return fmt.Errorf("failed to wait for index deletion task: %w", err)
	}

	if task.Status != meilisearch.TaskStatusSucceeded {
		return fmt.Errorf("index deletion task failed with status %s: %s", task.Status, task.Error.Message)
	}

	klog.Infof("Index %s deleted", indexUID)
	return nil
}

// GetSettingsUpdateTask returns the most recent settings update task for the given index.
func (s *SDKClient) GetSettingsUpdateTask(indexUID string) (*meilisearch.Task, error) {
	resp, err := s.client.GetTasks(&meilisearch.TasksQuery{
		IndexUIDS: []string{indexUID},
		Types:     []meilisearch.TaskType{"settingsUpdate"},
	})
	if err != nil {
		klog.Errorf("Failed to get settings update task: %v", err)
		return nil, fmt.Errorf("failed to get settings update task: %w", err)
	}
	if len(resp.Results) == 0 {
		return nil, nil // No settings update task found
	}
	// Return the most recent task
	return &resp.Results[0], nil
}

// MultiSearch performs a search query across multiple indices.
// If filter is non-empty, it's applied identically to every per-index query
// as a Meilisearch filter expression. Pass "" to skip filtering.
func (s *SDKClient) MultiSearch(
	indexUIDs []string,
	query string,
	limit, offset int64,
	filter string,
) (*meilisearch.MultiSearchResponse, error) {
	if len(indexUIDs) == 0 {
		return nil, fmt.Errorf("no index UIDs provided")
	}

	queries := make([]*meilisearch.SearchRequest, 0, len(indexUIDs))
	for _, uid := range indexUIDs {
		req := &meilisearch.SearchRequest{
			IndexUID:         uid,
			Query:            query,
			ShowRankingScore: true,
		}
		if filter != "" {
			req.Filter = filter
		}
		queries = append(queries, req)
	}

	req := &meilisearch.MultiSearchRequest{
		Queries: queries,
		Federation: &meilisearch.MultiSearchFederation{
			Limit:  limit,
			Offset: offset,
		},
	}

	klog.V(6).Infof("MultiSearch across indices %v with query %q, limit %d, offset %d, filter %q",
		indexUIDs, query, limit, offset, filter)
	resp, err := s.client.MultiSearch(req)
	if err != nil {
		klog.Errorf("MultiSearch failed across indices %v with filter %q: %v", indexUIDs, filter, err)
		return nil, fmt.Errorf("multi-search failed: %w", err)
	}
	return resp, nil
}
