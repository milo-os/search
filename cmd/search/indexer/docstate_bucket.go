package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	searchmetrics "go.miloapis.net/search/internal/metrics"
	"k8s.io/klog/v2"
)

// keyValueManager is the part of a JetStream context that opens, creates and
// updates KV buckets.
type keyValueManager interface {
	KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error)
	CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error)
	UpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error)
}

// openDocStateBucket opens the document state bucket, creating it with cfg
// if it does not exist.
//
// An existing bucket is compared with cfg on the settings the indexer
// manages, and any difference is logged. It is only brought in line with cfg
// when manage is set: an operator may have tuned the bucket by hand, raising
// its replicas or byte limit for example, and re-applying the flag values on
// every start would quietly undo that. Even then only the managed settings
// change; see managedDocStateBucketConfig. Should the update be refused, the
// bucket is used as it is, since a bucket whose settings differ still orders
// writes correctly.
func openDocStateBucket(ctx context.Context, js keyValueManager, cfg jetstream.KeyValueConfig, manage bool) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, cfg.Bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		klog.Infof("Creating document state bucket %s", cfg.Bucket)
		kv, err = js.CreateKeyValue(ctx, cfg)
		if err == nil {
			return kv, nil
		}
		// Replicas start together, so another one may have created the
		// bucket between the lookup and the create. Its bucket is the one to
		// use, held to the same rules as any other existing bucket.
		if !errors.Is(err, jetstream.ErrBucketExists) && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return nil, fmt.Errorf("failed to create document state bucket %s: %w", cfg.Bucket, err)
		}
		klog.Infof("Document state bucket %s was created by another replica, opening it", cfg.Bucket)
		kv, err = js.KeyValue(ctx, cfg.Bucket)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open document state bucket %s: %w", cfg.Bucket, err)
	}

	status, err := kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the status of document state bucket %s: %w", cfg.Bucket, err)
	}
	current := status.Config()
	drift := docStateBucketDrift(current, cfg)
	if len(drift) == 0 {
		return kv, nil
	}

	if !manage {
		klog.Warningf("Document state bucket %s differs from the configured settings (%s); leaving it as it is because --nats-doc-state-manage-bucket is false",
			cfg.Bucket, strings.Join(drift, ", "))
		return kv, nil
	}

	klog.Infof("Document state bucket %s differs from the configured settings (%s); updating it", cfg.Bucket, strings.Join(drift, ", "))
	updated, err := js.UpdateKeyValue(ctx, managedDocStateBucketConfig(current, cfg))
	if err != nil {
		klog.Errorf("Failed to update document state bucket %s, using it with its current settings: %v", cfg.Bucket, err)
		return kv, nil
	}
	return updated, nil
}

// managedDocStateBucketConfig is the bucket's current configuration with only
// the settings the indexer manages taken from want. An update replaces the
// whole configuration, so sending want as it is would also reset everything
// the indexer does not compare, such as compression, metadata, placement, the
// value size limit or republishing, which an operator may have set.
func managedDocStateBucketConfig(current, want jetstream.KeyValueConfig) jetstream.KeyValueConfig {
	cfg := current
	cfg.Replicas = want.Replicas
	cfg.TTL = want.TTL
	cfg.MaxBytes = want.MaxBytes
	cfg.History = want.History
	cfg.Storage = want.Storage
	return cfg
}

// docStateBucketDrift describes each setting the indexer manages where the
// bucket's current configuration differs from the wanted one.
func docStateBucketDrift(current, want jetstream.KeyValueConfig) []string {
	var drift []string
	note := func(name string, got, wanted any) {
		drift = append(drift, fmt.Sprintf("%s is %v, want %v", name, got, wanted))
	}
	if current.Replicas != want.Replicas {
		note("replicas", current.Replicas, want.Replicas)
	}
	if current.TTL != want.TTL {
		note("TTL", current.TTL, want.TTL)
	}
	if current.MaxBytes != want.MaxBytes {
		note("max bytes", current.MaxBytes, want.MaxBytes)
	}
	if current.History != want.History {
		note("history", current.History, want.History)
	}
	if current.Storage != want.Storage {
		note("storage", current.Storage, want.Storage)
	}
	return drift
}

// bucketStatuser is the part of a JetStream KV bucket the readiness check
// uses.
type bucketStatuser interface {
	Bucket() string
	Status(ctx context.Context) (jetstream.KeyValueStatus, error)
}

// docStateBucketCheck returns a readiness check that asks the bucket for its
// status within timeout. A bucket that cannot answer that cannot answer the
// claims every flush starts with either, and every batch is nacked; reporting
// the replica unready makes that outage visible, and holds back a rollout,
// instead of leaving a Ready pod that indexes nothing. The status also carries
// the bucket's size, which is published for alerting before the bucket fills.
func docStateBucketCheck(kv bucketStatuser, timeout time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		status, err := kv.Status(ctx)
		if err != nil {
			return fmt.Errorf("document state bucket %s unavailable: %w", kv.Bucket(), err)
		}
		searchmetrics.IndexerDocStateBucketBytes.Set(float64(status.Bytes()))
		return nil
	}
}
