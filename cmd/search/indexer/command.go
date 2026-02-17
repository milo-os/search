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
	policyv1alpha1 "go.miloapis.net/search/pkg/apis/policy/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// ResourceIndexerOptions holds the configuration for the resource indexer.
type ResourceIndexerOptions struct {
	NatsURL                         string
	NatsSubject                     string
	NatsQueueGroup                  string
	NatsDurableName                 string
	NatsStreamName                  string
	NatsAckWait                     time.Duration
	NatsMaxInFlight                 int
	ResourceIndexPolicySyncInterval time.Duration

	MeilisearchTaskWaitTimeout time.Duration
	MeilisearchHTTPTimeout     time.Duration
	MeilisearchDomain          string
	MeilisearchChunkSize       int
	BatchSize                  int
	FlushInterval              time.Duration
	MeilisearchMaxRetries      int
	MeilisearchRetryDelay      time.Duration
	BatchMaxConcurrentUploads  int
}

// NewResourceIndexerOptions creates a new ResourceIndexerOptions with default values.
func NewResourceIndexerOptions() *ResourceIndexerOptions {
	return &ResourceIndexerOptions{
		NatsURL:                         "nats://nats.nats-system.svc.cluster.local:4222",
		NatsSubject:                     "audit.>",
		NatsQueueGroup:                  "search-indexer",
		NatsDurableName:                 "search-indexer",
		NatsStreamName:                  "AUDIT_EVENTS",
		NatsAckWait:                     120 * time.Second,
		NatsMaxInFlight:                 10000,
		ResourceIndexPolicySyncInterval: 2 * time.Minute,
		MeilisearchTaskWaitTimeout:      4 * time.Second,
		MeilisearchHTTPTimeout:          60 * time.Second,
		MeilisearchDomain:               "http://meilisearch.meilisearch-system.svc.cluster.local:7700",
		MeilisearchChunkSize:            1000,
		BatchSize:                       1000,
		FlushInterval:                   5 * time.Second,
		MeilisearchMaxRetries:           3,
		MeilisearchRetryDelay:           500 * time.Millisecond,
		BatchMaxConcurrentUploads:       100,
	}
}

// AddFlags adds the flags for the resource indexer to the command.
func (o *ResourceIndexerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.NatsURL, "nats-url", o.NatsURL, "The URL of the NATS server.")
	fs.StringVar(&o.NatsSubject, "nats-subject", o.NatsSubject, "The NATS subject to subscribe to.")
	fs.StringVar(&o.NatsQueueGroup, "nats-queue-group", o.NatsQueueGroup, "The NATS queue group for load balancing.")
	fs.StringVar(&o.NatsDurableName, "nats-durable-name", o.NatsDurableName, "The durable name for the JetStream consumer.")
	fs.StringVar(&o.NatsStreamName, "nats-stream-name", o.NatsStreamName, "The name of the JetStream stream.")
	fs.DurationVar(&o.NatsAckWait, "nats-ack-wait", o.NatsAckWait, "The time to wait for an acknowledgement.")
	fs.IntVar(&o.NatsMaxInFlight, "nats-max-in-flight", o.NatsMaxInFlight, "The maximum number of in-flight messages.")
	fs.DurationVar(&o.ResourceIndexPolicySyncInterval, "resource-index-policy-sync-interval", o.ResourceIndexPolicySyncInterval, "How often to re-sync ResourceIndexPolicies.")
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
	if o.NatsSubject == "" {
		return fmt.Errorf("nats-subject must be set")
	}
	if o.NatsQueueGroup == "" {
		return fmt.Errorf("nats-queue-group must be set")
	}
	if o.NatsDurableName == "" {
		return fmt.Errorf("nats-durable-name must be set")
	}
	if o.NatsStreamName == "" {
		return fmt.Errorf("nats-stream-name must be set")
	}
	if o.NatsAckWait == 0 {
		return fmt.Errorf("nats-ack-wait must be set")
	}
	if o.NatsMaxInFlight < 1 {
		return fmt.Errorf("nats-max-in-flight must be greater than 0")
	}
	if o.ResourceIndexPolicySyncInterval < 10*time.Second {
		return fmt.Errorf("resource-index-policy-sync-interval must be at least 10s")
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
	// Build a Kubernetes client for listing policies
	scheme := runtime.NewScheme()
	if err := policyv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add v1alpha1 scheme: %w", err)
	}

	cfg, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create and start the policy cache
	policyCache, err := indexer.NewPolicyCache(k8sClient, o.ResourceIndexPolicySyncInterval)
	if err != nil {
		return fmt.Errorf("failed to create policy cache: %w", err)
	}

	go func() {
		if err := policyCache.Start(ctx); err != nil {
			klog.Errorf("Policy cache stopped: %v", err)
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

	stream, err := js.Stream(ctx, o.NatsStreamName)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", o.NatsStreamName, err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       o.NatsDurableName,
		FilterSubject: o.NatsSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: o.NatsMaxInFlight,
		AckWait:       o.NatsAckWait,
	})
	if err != nil {
		return fmt.Errorf("failed to get/create consumer %s: %w", o.NatsDurableName, err)
	}

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

	batcher := indexer.NewBatcher(searchClient, batchConfig)
	idx := indexer.NewIndexer(consumer, policyCache, batcher)

	klog.Info("Starting indexer...")
	return idx.Start(ctx)
}
