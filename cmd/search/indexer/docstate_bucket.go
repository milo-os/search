package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	searchmetrics "go.miloapis.net/search/internal/metrics"
)

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
