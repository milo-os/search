package metrics

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "search"

	indexerSubsystem = "indexer"
)

var (
	// ResourceSearchQueryTotal tracks the total number of search queries
	ResourceSearchQueryTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Name:           "query_total",
			Help:           "Total number of search queries",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status"},
	)

	// ResourceSearchQueryDuration tracks the duration of search queries
	ResourceSearchQueryDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Name:           "query_duration_seconds",
			Help:           "Duration of search queries in seconds",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 14),
		},
		[]string{"operation"},
	)

	// IndexerPendingOperations tracks the number of operations buffered in the
	// indexer batcher but not yet handed to a flush.
	IndexerPendingOperations = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "pending_operations",
			Help:           "Number of operations currently buffered in the indexer batcher",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type"},
	)

	// IndexerInFlightFlushes tracks the number of flush goroutines currently
	// talking to Meilisearch. It is capped by --batch-max-inflight-flushes.
	IndexerInFlightFlushes = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "inflight_flushes",
			Help:           "Number of indexer flushes currently in flight",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// IndexerFlushDuration tracks how long a flush takes, including the wait for
	// the Meilisearch tasks it enqueued.
	IndexerFlushDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "flush_duration_seconds",
			Help:           "Duration of indexer flushes in seconds",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.05, 2, 14),
		},
		[]string{"type"},
	)

	// IndexerFlushTotal tracks the outcome of indexer flushes.
	IndexerFlushTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "flush_total",
			Help:           "Total number of indexer flushes by type and outcome",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type", "status"},
	)

	// IndexerFlushSlotWait tracks the time spent waiting for a free in-flight
	// flush slot. A non-zero value means the indexer is applying backpressure to
	// the JetStream consumer.
	IndexerFlushSlotWait = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "flush_slot_wait_seconds",
			Help:           "Time spent waiting for a free in-flight flush slot in seconds",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 18),
		},
		[]string{"type"},
	)

	// IndexerMessagesNacked tracks messages returned to JetStream for redelivery
	// because their batch failed to flush.
	IndexerMessagesNacked = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      indexerSubsystem,
			Name:           "messages_nacked_total",
			Help:           "Total number of NATS messages nacked after a failed indexer flush",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type"},
	)
)

// init registers all custom metrics with the legacy registry
// This ensures they're included in the /metrics endpoint
func init() {
	legacyregistry.MustRegister(
		ResourceSearchQueryTotal,
		ResourceSearchQueryDuration,
		IndexerPendingOperations,
		IndexerInFlightFlushes,
		IndexerFlushDuration,
		IndexerFlushTotal,
		IndexerFlushSlotWait,
		IndexerMessagesNacked,
	)
}
