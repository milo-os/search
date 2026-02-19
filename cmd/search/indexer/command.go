package indexer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.miloapis.net/search/internal/indexer"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// ResourceIndexerOptions holds the configuration for the resource indexer.
type ResourceIndexerOptions struct {
	// NATS connection and consumer settings
	NatsURL               string
	NatsAuditConsumerName string
	NatsStreamName        string

	// NATS re-index consumer settings (separate REINDEX_EVENTS stream)
	NatsReindexStream       string
	NatsReindexConsumerName string

	// Meilisearch connection and timeout settings
	MeilisearchTaskWaitTimeout time.Duration
	MeilisearchHTTPTimeout     time.Duration
	MeilisearchDomain          string
	MeilisearchChunkSize       int
	MeilisearchMaxRetries      int
	MeilisearchRetryDelay      time.Duration

	// Batching and throughput tuning
	BatchSize                 int
	FlushInterval             time.Duration
	BatchMaxConcurrentUploads int
}

// NewResourceIndexerOptions creates a new ResourceIndexerOptions with default values.
func NewResourceIndexerOptions() *ResourceIndexerOptions {
	return &ResourceIndexerOptions{
		NatsURL:                    "nats://nats.nats-system.svc.cluster.local:4222",
		NatsAuditConsumerName:      "search-indexer",
		NatsStreamName:             "AUDIT_EVENTS",
		NatsReindexStream:          "REINDEX_EVENTS",
		NatsReindexConsumerName:    "search-reindexer",
		MeilisearchTaskWaitTimeout: 4 * time.Second,
		MeilisearchHTTPTimeout:     60 * time.Second,
		MeilisearchDomain:          "http://meilisearch.meilisearch-system.svc.cluster.local:7700",
		MeilisearchChunkSize:       1000,
		BatchSize:                  1000,
		FlushInterval:              5 * time.Second,
		MeilisearchMaxRetries:      3,
		MeilisearchRetryDelay:      500 * time.Millisecond,
		BatchMaxConcurrentUploads:  100,
	}
}

// AddFlags adds the flags for the resource indexer to the command.
func (o *ResourceIndexerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.NatsURL, "nats-url", o.NatsURL, "The URL of the NATS server.")
	fs.StringVar(&o.NatsAuditConsumerName, "nats-audit-consumer-name", o.NatsAuditConsumerName, "The name of the audit-events JetStream consumer (must match the manifest).")
	fs.StringVar(&o.NatsStreamName, "nats-stream-name", o.NatsStreamName, "The name of the audit-events JetStream stream.")

	fs.StringVar(&o.NatsReindexStream, "nats-reindex-stream", o.NatsReindexStream, "The JetStream stream name for re-index messages.")
	fs.StringVar(&o.NatsReindexConsumerName, "nats-reindex-consumer-name", o.NatsReindexConsumerName, "The name of the re-index JetStream consumer (must match the manifest).")

	fs.StringVar(&o.MeilisearchDomain, "meilisearch-domain", o.MeilisearchDomain, "Domain of the Meilisearch instance.")
	fs.DurationVar(&o.MeilisearchTaskWaitTimeout, "meilisearch-task-wait-timeout", o.MeilisearchTaskWaitTimeout, "Timeout for waiting for Meilisearch tasks to complete.")
	fs.DurationVar(&o.MeilisearchHTTPTimeout, "meilisearch-http-timeout", o.MeilisearchHTTPTimeout, "Timeout for HTTP requests to Meilisearch.")
	fs.IntVar(&o.MeilisearchChunkSize, "meilisearch-chunk-size", o.MeilisearchChunkSize, "The number of documents to process in a single chunk.")
	fs.IntVar(&o.BatchSize, "batch-size", o.BatchSize, "The batch size for upserts and deletes.")
	fs.DurationVar(&o.FlushInterval, "flush-interval", o.FlushInterval, "The flush interval for upserts and deletes.")
	fs.IntVar(&o.MeilisearchMaxRetries, "meilisearch-max-retries", o.MeilisearchMaxRetries, "The maximum number of retries for transient Meilisearch errors.")
	fs.DurationVar(&o.MeilisearchRetryDelay, "meilisearch-retry-delay", o.MeilisearchRetryDelay, "The base delay between Meilisearch retries.")
	fs.IntVar(&o.BatchMaxConcurrentUploads, "batch-max-concurrent-uploads", o.BatchMaxConcurrentUploads, "The maximum number of concurrent uploads to Meilisearch.")
}

// Validate checks if the resource indexer options are valid.
func (o *ResourceIndexerOptions) Validate() error {
	if o.NatsURL == "" {
		return fmt.Errorf("nats-url must be set")
	}
	if o.NatsAuditConsumerName == "" {
		return fmt.Errorf("nats-consummer-name must be set")
	}
	if o.NatsStreamName == "" {
		return fmt.Errorf("nats-stream-name must be set")
	}
	if o.NatsReindexStream == "" {
		return fmt.Errorf("nats-reindex-stream must be set")
	}
	if o.NatsReindexConsumerName == "" {
		return fmt.Errorf("nats-reindex-consumer-name must be set")
	}
	if o.MeilisearchDomain == "" {
		return fmt.Errorf("meilisearch-domain must be set")
	}
	if os.Getenv("MEILISEARCH_API_KEY") == "" {
		return fmt.Errorf("meilisearch-api-key must be set")
	}
	if o.MeilisearchChunkSize < 500 {
		return fmt.Errorf("meilisearch-chunk-size must be greater than 500")
	}
	if o.BatchSize < 500 {
		return fmt.Errorf("batch-size must be greater than 500")
	}
	if o.FlushInterval < 1*time.Second {
		return fmt.Errorf("flush-interval must be greater than 1s")
	}
	if o.MeilisearchMaxRetries < 1 {
		return fmt.Errorf("meilisearch-max-retries must be greater than 0")
	}
	if o.MeilisearchRetryDelay < 0 {
		return fmt.Errorf("meilisearch-retry-delay must be non-negative")
	}
	if o.BatchMaxConcurrentUploads < 1 {
		return fmt.Errorf("batch-max-concurrent-uploads must be greater than 0")
	}

	return nil
}

func (o *ResourceIndexerOptions) Complete() error {
	return nil
}

// NewIndexerCommand creates the indexer subcommand.
func NewIndexerCommand() *cobra.Command {
	o := NewResourceIndexerOptions()

	cmd := &cobra.Command{
		Use:   "indexer",
		Short: "Start the resource indexer",
		Long:  `Start the resource indexer to consume audit logs and index resources.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return Run(o, cmd.Context())
		},
	}

	o.AddFlags(cmd.Flags())

	return cmd
}

// Run starts the indexer consumer
func Run(o *ResourceIndexerOptions, ctx context.Context) error {
	// Build a scheme and REST config for the controller-runtime cache.
	scheme := runtime.NewScheme()
	if err := searchv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add v1alpha1 scheme: %w", err)
	}

	cfg, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	// Create a controller-runtime cache that uses a watch stream (informer)
	// to keep ResourceIndexPolicies in-sync.
	k8sCache, err := runtimecache.New(cfg, runtimecache.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create controller-runtime cache: %w", err)
	}

	// Create and start the policy cache
	indexPolicyCache, err := indexer.NewPolicyCache(k8sCache, true)
	if err != nil {
		return fmt.Errorf("failed to create policy cache: %w", err)
	}

	reindexPolicyCache, err := indexer.NewPolicyCache(k8sCache, false)
	if err != nil {
		return fmt.Errorf("failed to create policy cache: %w", err)
	}

	go func() {
		if err := indexPolicyCache.Start(ctx); err != nil {
			klog.Errorf("Index Policy cache stopped: %v", err)
		}
	}()

	go func() {
		if err := reindexPolicyCache.Start(ctx); err != nil {
			klog.Errorf("Reindex Policy cache stopped: %v", err)
		}
	}()

	// Connect to NATS
	klog.Infof("Connecting to NATS at %s...", o.NatsURL)
	nc, err := nats.Connect(o.NatsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	auditStream, err := js.Stream(ctx, o.NatsStreamName)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", o.NatsStreamName, err)
	}

	// Consumer is declared in config/components/nats-config/nats-consumer.yaml
	auditConsumer, err := auditStream.Consumer(ctx, o.NatsAuditConsumerName)
	if err != nil {
		return fmt.Errorf("failed to get consumer %s: %w", o.NatsAuditConsumerName, err)
	}

	// ── Re-index consumer (separate REINDEX_EVENTS stream) ──────────────────
	// The stream is declared in config/components/nats-streams/reindex-stream.yaml
	reindexStream, err := js.Stream(ctx, o.NatsReindexStream)
	if err != nil {
		return fmt.Errorf("failed to get re-index stream %s: %w", o.NatsReindexStream, err)
	}

	// Consumer is declared in config/components/nats-config/nats-consumer.yaml
	reindexJSConsumer, err := reindexStream.Consumer(ctx, o.NatsReindexConsumerName)
	if err != nil {
		return fmt.Errorf("failed to get re-index consumer %s: %w", o.NatsReindexConsumerName, err)
	}

	// ── Meilisearch client ──────────────────────────────────────────────────
	searchClient, err := meilisearch.NewSDKClient(meilisearch.SDKConfig{
		Domain:      o.MeilisearchDomain,
		APIKey:      os.Getenv("MEILISEARCH_API_KEY"),
		WaitTimeout: o.MeilisearchTaskWaitTimeout,
		ChunkSize:   o.MeilisearchChunkSize,
		HTTPTimeout: o.MeilisearchHTTPTimeout,
		MaxRetries:  o.MeilisearchMaxRetries,
		RetryDelay:  o.MeilisearchRetryDelay,
	})
	if err != nil {
		return fmt.Errorf("failed to create search client: %w", err)
	}

	batchConfig := indexer.BatchConfig{
		BatchSize:            o.BatchSize,
		FlushInterval:        o.FlushInterval,
		MaxConcurrentUploads: o.BatchMaxConcurrentUploads,
	}

	// Create separate batchers for audit events and re-indexing events
	// so they don't block each other and can be tuned independently if needed.
	auditBatcher := indexer.NewBatcher(searchClient, batchConfig)
	reindexBatcher := indexer.NewBatcher(searchClient, batchConfig)

	// Start both batchers
	auditBatcher.Start(ctx)
	reindexBatcher.Start(ctx)

	auditIdx := indexer.NewIndexer(auditConsumer, indexPolicyCache, auditBatcher)
	reindexIdx := indexer.NewReindexConsumer(reindexJSConsumer, reindexPolicyCache, reindexBatcher)

	klog.Info("Starting audit indexer and re-index consumer...")

	consumerCtx, cancelConsumers := context.WithCancel(ctx)
	defer cancelConsumers()

	errCh := make(chan error, 2)

	go func() {
		if err := auditIdx.Start(consumerCtx); err != nil {
			errCh <- fmt.Errorf("audit indexer: %w", err)
		} else {
			errCh <- nil
		}
	}()

	go func() {
		if err := reindexIdx.Start(consumerCtx); err != nil {
			errCh <- fmt.Errorf("reindex consumer: %w", err)
		} else {
			errCh <- nil
		}
	}()

	select {
	case err := <-errCh:
		cancelConsumers()
		return err
	case <-ctx.Done():
		return nil
	}
}
