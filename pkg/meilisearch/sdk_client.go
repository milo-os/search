package meilisearch

import (
	"fmt"
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
}

type SDKClient struct {
	client      meilisearch.ServiceManager
	waitTimeout time.Duration
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

	client := meilisearch.New(config.Domain, meilisearch.WithAPIKey(config.APIKey))

	return &SDKClient{client: client}, nil
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

// CreateIndex creates an index with the given UID.
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

	// Wait for task to complete
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

// waitForTask waits for the given task to complete.
func (s *SDKClient) waitForTask(taskUid int64) (*meilisearch.Task, error) {
	task, err := s.client.WaitForTask(taskUid, s.waitTimeout)
	if err != nil {
		klog.Errorf("Failed to wait for task: %v", err)
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
