package indexer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
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
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// consumerAckWait mirrors the ackWait configured on the JetStream consumers in
// config/components/nats-config/nats-consumer.yaml. It is not exposed to this
// process, so it is duplicated here to bound the indexing timeouts.
const consumerAckWait = 300 * time.Second

const (
	// heartbeatMargin is how many heartbeat intervals an ackWait must fit. The
	// spare intervals absorb a lost or delayed tick without JetStream giving up on
	// a message that is still being indexed.
	heartbeatMargin = 3
	// heartbeatHardFloor is the point below which a single delayed tick already
	// redelivers messages, so falling under it is an error rather than a warning.
	heartbeatHardFloor = 2
)

// indexesPerFlushBudget is the assumed maximum number of index policies a
// single message can fan out to during one flush. Each in-flight flush issues
// one upload per index, so the upload semaphore must be able to accommodate
// every in-flight flush's per-index uploads at once, or flushes stall holding
// their flush slots while waiting on the upload semaphore and memory stays
// pinned. Taken from the production profile (see indexer.DefaultMaxInFlightFlushes).
const indexesPerFlushBudget = 10

// batchMaxConcurrentUploadsDefault sits above the floor the upload semaphore has
// to clear, indexer.DefaultMaxInFlightFlushes * indexesPerFlushBudget
// (16 * 10 = 160), leaving headroom for a policy count that grows past the
// profiled fan-out before the invariant in Validate trips.
const batchMaxConcurrentUploadsDefault = 200

// ResourceIndexerOptions holds the configuration for the resource indexer.
type ResourceIndexerOptions struct {
	// NATS connection and consumer settings
	NatsURL               string
	NatsAuditConsumerName string
	NatsStreamName        string
	NatsTLSCA             string
	NatsTLSCert           string
	NatsTLSKey            string

	// NATS re-index consumer settings (separate REINDEX_EVENTS stream)
	NatsReindexStream       string
	NatsReindexConsumerName string

	// Meilisearch connection and timeout settings
	MeilisearchTaskWaitTimeout  time.Duration
	MeilisearchTaskPollInterval time.Duration
	MeilisearchHTTPTimeout      time.Duration
	MeilisearchDomain           string
	MeilisearchChunkSize        int
	MeilisearchMaxRetries       int
	MeilisearchRetryDelay       time.Duration

	// Batching and throughput tuning
	BatchSize                 int
	FlushInterval             time.Duration
	BatchMaxConcurrentUploads int
	BatchMaxInFlightFlushes   int
	BatchAckProgressInterval  time.Duration

	// Observability
	MetricsBindAddress string

	// Multi-tenancy settings.
	EnableMultiTenancy bool

	// PprofBindAddress is the host:port that the net/http/pprof debug server
	// binds to. Empty (the default) disables the pprof server entirely.
	PprofBindAddress string
}

// NewResourceIndexerOptions creates a new ResourceIndexerOptions with default values.
func NewResourceIndexerOptions() *ResourceIndexerOptions {
	return &ResourceIndexerOptions{
		NatsURL:                     "nats://nats.nats-system.svc.cluster.local:4222",
		NatsAuditConsumerName:       "search-indexer",
		NatsStreamName:              "AUDIT_EVENTS",
		NatsReindexStream:           "REINDEX_EVENTS",
		NatsReindexConsumerName:     "search-reindexer",
		MeilisearchTaskWaitTimeout:  10 * time.Minute,
		MeilisearchTaskPollInterval: 500 * time.Millisecond,
		MeilisearchHTTPTimeout:      60 * time.Second,
		MeilisearchDomain:           "http://meilisearch.meilisearch-system.svc.cluster.local:7700",
		MeilisearchChunkSize:        1000,
		BatchSize:                   1000,
		FlushInterval:               10 * time.Second,
		MeilisearchMaxRetries:       3,
		MeilisearchRetryDelay:       500 * time.Millisecond,
		BatchMaxConcurrentUploads:   batchMaxConcurrentUploadsDefault,
		BatchMaxInFlightFlushes:     indexer.DefaultMaxInFlightFlushes,
		BatchAckProgressInterval:    indexer.DefaultAckProgressInterval,
		MetricsBindAddress:          ":8080",
		EnableMultiTenancy:          false,
		PprofBindAddress:            "",
	}
}

// AddFlags adds the flags for the resource indexer to the command.
func (o *ResourceIndexerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.NatsURL, "nats-url", o.NatsURL, "The URL of the NATS server.")
	fs.StringVar(&o.NatsAuditConsumerName, "nats-audit-consumer-name", o.NatsAuditConsumerName, "The name of the audit-events JetStream consumer (must match the manifest).")
	fs.StringVar(&o.NatsStreamName, "nats-stream-name", o.NatsStreamName, "The name of the audit-events JetStream stream.")

	fs.StringVar(&o.NatsReindexStream, "nats-reindex-stream", o.NatsReindexStream, "The JetStream stream name for re-index messages.")
	fs.StringVar(&o.NatsReindexConsumerName, "nats-reindex-consumer-name", o.NatsReindexConsumerName, "The name of the re-index JetStream consumer (must match the manifest).")
	fs.StringVar(&o.NatsTLSCA, "nats-tls-ca", o.NatsTLSCA, "The path to the NATS TLS CA file.")
	fs.StringVar(&o.NatsTLSCert, "nats-tls-cert", o.NatsTLSCert, "The path to the NATS TLS certificate file.")
	fs.StringVar(&o.NatsTLSKey, "nats-tls-key", o.NatsTLSKey, "The path to the NATS TLS key file.")

	fs.StringVar(&o.MeilisearchDomain, "meilisearch-domain", o.MeilisearchDomain, "Domain of the Meilisearch instance.")
	fs.DurationVar(&o.MeilisearchTaskWaitTimeout, "meilisearch-task-wait-timeout", o.MeilisearchTaskWaitTimeout, "Maximum time to wait for a Meilisearch task to complete before the batch is nacked and redelivered. This is a backstop against a wedged Meilisearch, not a throughput knob: a running flush heartbeats its messages every batch-ack-progress-interval, so the batch stays alive for as long as it waits and this no longer needs to sit under the consumer ackWait.")
	fs.DurationVar(&o.MeilisearchTaskPollInterval, "meilisearch-task-poll-interval", o.MeilisearchTaskPollInterval, "How often to poll Meilisearch for task completion while waiting.")
	fs.DurationVar(&o.MeilisearchHTTPTimeout, "meilisearch-http-timeout", o.MeilisearchHTTPTimeout, "Timeout for HTTP requests to Meilisearch.")
	fs.IntVar(&o.MeilisearchChunkSize, "meilisearch-chunk-size", o.MeilisearchChunkSize, "The number of documents to process in a single chunk.")
	fs.IntVar(&o.BatchSize, "batch-size", o.BatchSize, "The batch size for upserts and deletes.")
	fs.DurationVar(&o.FlushInterval, "flush-interval", o.FlushInterval, "The flush interval for upserts and deletes.")
	fs.IntVar(&o.MeilisearchMaxRetries, "meilisearch-max-retries", o.MeilisearchMaxRetries, "The maximum number of retries for transient Meilisearch errors.")
	fs.DurationVar(&o.MeilisearchRetryDelay, "meilisearch-retry-delay", o.MeilisearchRetryDelay, "The base delay between Meilisearch retries.")
	// Each in-flight flush fans out one upload per index, so this must exceed
	// batch-max-inflight-flushes times the policy count or uploads throttle here
	// instead of at the in-flight cap, leaving flushes stalled while holding their
	// slots.
	fs.IntVar(&o.BatchMaxConcurrentUploads, "batch-max-concurrent-uploads", o.BatchMaxConcurrentUploads, "The maximum number of concurrent uploads to Meilisearch. Keep it above batch-max-inflight-flushes times the number of index policies, since each flush issues one upload per index.")
	fs.IntVar(&o.BatchMaxInFlightFlushes, "batch-max-inflight-flushes", o.BatchMaxInFlightFlushes, "The maximum number of flushes in flight at once. Each in-flight flush pins its batch of NATS messages in memory, so this bounds the indexer's memory use and applies backpressure to the consumer. The default is tuned against measured Meilisearch task latency; see indexer.DefaultMaxInFlightFlushes before changing it.")

	fs.DurationVar(&o.BatchAckProgressInterval, "batch-ack-progress-interval", o.BatchAckProgressInterval, "How often a running flush tells JetStream its messages are still in progress, extending ackWait. Must stay comfortably under the consumer ackWait so a heartbeat always lands inside the window.")

	fs.StringVar(&o.MetricsBindAddress, "metrics-bind-address", o.MetricsBindAddress, "The address the metrics endpoint binds to. Set to an empty string to disable metrics serving.")

	// Multi-tenancy
	fs.BoolVar(&o.EnableMultiTenancy, "enable-multi-tenancy", o.EnableMultiTenancy, "Enable multi-tenant mode to index resources from all project control planes.")

	// Debugging
	fs.StringVar(&o.PprofBindAddress, "pprof-bind-address", o.PprofBindAddress, "The `host:port` to bind the net/http/pprof debug server to. Empty (the default) disables pprof; setting it enables the net/http/pprof handlers under /debug/pprof/. Binding to 127.0.0.1:6060 keeps the endpoint reachable only via kubectl port-forward (port-forward runs inside the pod network namespace, so localhost works) and not from the cluster network.")
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
	// Each in-flight flush issues one upload per index, so the upload semaphore
	// must accommodate every in-flight flush's per-index uploads at once.
	// Otherwise flushes stall waiting on the upload semaphore while still holding
	// their flush slots, and the memory those batches pin is held longer than the
	// in-flight cap implies.
	if minUploads := o.BatchMaxInFlightFlushes * indexesPerFlushBudget; o.BatchMaxConcurrentUploads < minUploads {
		return fmt.Errorf("batch-max-concurrent-uploads %d must be at least batch-max-inflight-flushes (%d) times the per-flush index fan-out (%d) = %d", o.BatchMaxConcurrentUploads, o.BatchMaxInFlightFlushes, indexesPerFlushBudget, minUploads)
	}
	if o.BatchMaxInFlightFlushes < 1 {
		return fmt.Errorf("batch-max-inflight-flushes must be greater than 0")
	}
	if o.MeilisearchTaskWaitTimeout <= 0 {
		return fmt.Errorf("meilisearch-task-wait-timeout must be greater than 0")
	}
	if o.MeilisearchTaskPollInterval <= 0 {
		return fmt.Errorf("meilisearch-task-poll-interval must be greater than 0")
	}
	if o.BatchAckProgressInterval <= 0 {
		return fmt.Errorf("batch-ack-progress-interval must be greater than 0")
	}
	// A running flush heartbeats its messages every BatchAckProgressInterval, so
	// the Meilisearch task wait no longer counts against ackWait. What must hold
	// instead is that a heartbeat always lands well inside the window: at
	// heartbeatMargin intervals per ackWait, the spare ones can be lost or delayed
	// before JetStream gives up on a message that is still being indexed. The ackWait lives on the consumer
	// manifest and is not visible to this process, so the shipped value is
	// asserted here.
	//
	// Residual case, accepted rather than modelled: a prefetched message can sit
	// in the consumer's buffer waiting for the handler while an earlier queue call
	// blocks on a flush slot, and nothing heartbeats a message the batcher has not
	// been handed yet. Under a deep backlog that wait can exceed ackWait and
	// redeliver up to the 500-message prefetch window per pod. The redelivered
	// work is idempotent upserts and deletes, so it costs throughput rather than
	// correctness; the backpressure warning log and a rise in the
	// search_indexer_flush_slot_wait_seconds histogram are the signal that it is
	// happening.
	if budget := heartbeatMargin * o.BatchAckProgressInterval; budget > consumerAckWait {
		return fmt.Errorf("%d * batch-ack-progress-interval is %s, which exceeds the consumer ackWait of %s: lower batch-ack-progress-interval", heartbeatMargin, budget, consumerAckWait)
	}
	if o.PprofBindAddress != "" {
		if _, _, err := net.SplitHostPort(o.PprofBindAddress); err != nil {
			return fmt.Errorf("pprof-bind-address %q is not a valid host:port: %w", o.PprofBindAddress, err)
		}
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
	ctrllog.SetLogger(klog.NewKlogr())

	// Serve the batcher and search metrics; the indexer has no other HTTP surface.
	if err := serveMetrics(ctx, o.MetricsBindAddress); err != nil {
		return err
	}

	// Optional pprof debug server. Disabled unless --pprof-bind-address is set.
	if o.PprofBindAddress != "" {
		startPprofServer(ctx, o.PprofBindAddress)
	}

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

	// Register handlers for both caches. They share the same underlying informer.
	if err := indexPolicyCache.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("failed to register index policy handlers: %w", err)
	}
	if err := reindexPolicyCache.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("failed to register reindex policy handlers: %w", err)
	}

	// Start the shared cache and wait for it to be synced.
	go func() {
		klog.Info("Starting shared Kubernetes cache...")
		if err := k8sCache.Start(ctx); err != nil {
			klog.Fatalf("Kubernetes cache stopped with error: %v", err)
		}
	}()

	klog.Info("Waiting for cache to sync...")
	if !k8sCache.WaitForCacheSync(ctx) {
		return fmt.Errorf("failed to sync Kubernetes cache")
	}
	klog.Info("Cache synced successfully")

	// Connect to NATS
	klog.Infof("Connecting to NATS at %s...", o.NatsURL)

	var natsOpts []nats.Option
	if o.NatsTLSCert != "" && o.NatsTLSKey != "" {
		if o.NatsTLSCA != "" {
			klog.Infof("Using NATS TLS CA from %s", o.NatsTLSCA)
			natsOpts = append(natsOpts, nats.RootCAs(o.NatsTLSCA))
		}
		klog.Infof("Using NATS TLS cert from %s and key from %s", o.NatsTLSCert, o.NatsTLSKey)
		natsOpts = append(natsOpts, nats.ClientCert(o.NatsTLSCert, o.NatsTLSKey))
	}

	nc, err := nats.Connect(o.NatsURL, natsOpts...)
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

	auditInfo, err := auditConsumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get info for consumer %s: %w", o.NatsAuditConsumerName, err)
	}
	if err := checkAckWait(o.NatsAuditConsumerName, auditInfo.Config.AckWait, o.BatchAckProgressInterval); err != nil {
		return err
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

	reindexInfo, err := reindexJSConsumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get info for re-index consumer %s: %w", o.NatsReindexConsumerName, err)
	}
	if err := checkAckWait(o.NatsReindexConsumerName, reindexInfo.Config.AckWait, o.BatchAckProgressInterval); err != nil {
		return err
	}

	// ── Meilisearch client ──────────────────────────────────────────────────
	searchClient, err := meilisearch.NewSDKClient(meilisearch.SDKConfig{
		Domain:       o.MeilisearchDomain,
		APIKey:       os.Getenv("MEILISEARCH_API_KEY"),
		WaitTimeout:  o.MeilisearchTaskWaitTimeout,
		PollInterval: o.MeilisearchTaskPollInterval,
		ChunkSize:    o.MeilisearchChunkSize,
		HTTPTimeout:  o.MeilisearchHTTPTimeout,
		MaxRetries:   o.MeilisearchMaxRetries,
		RetryDelay:   o.MeilisearchRetryDelay,
	})
	if err != nil {
		return fmt.Errorf("failed to create search client: %w", err)
	}

	batchConfig := indexer.BatchConfig{
		BatchSize:            o.BatchSize,
		FlushInterval:        o.FlushInterval,
		MaxConcurrentUploads: o.BatchMaxConcurrentUploads,
		MaxInFlightFlushes:   o.BatchMaxInFlightFlushes,
		AckProgressInterval:  o.BatchAckProgressInterval,
	}

	// Create separate batchers for audit events and re-indexing events
	// so they don't block each other and can be tuned independently if needed.
	auditBatcher := indexer.NewBatcher(searchClient, batchConfig)
	reindexBatcher := indexer.NewBatcher(searchClient, batchConfig)

	// Start both batchers
	auditBatcher.Start(ctx)
	reindexBatcher.Start(ctx)

	auditIdx := indexer.NewIndexer(auditConsumer, indexPolicyCache, auditBatcher, o.EnableMultiTenancy)
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

// checkAckWait checks the heartbeat interval against a consumer's live ackWait,
// which can disagree with consumerAckWait: the constant tracks the manifest, but
// the NACK reconcile and this pod's rollout are not ordered, and whether NACK
// applies an AckWait change to an existing durable consumer is unverified, so a
// long-lived consumer can still be running with the value it was created with.
//
// The rule is deliberately looser than Validate()'s. Failing on the full
// heartbeatMargin would crashloop every pod of a rollout that landed before the
// consumer was reconciled, taking down indexing to protect it. So a shortfall
// against that margin is only a warning: the batch still gets heartbeats, just
// with less room for one to be lost. It is an error only when fewer than
// heartbeatHardFloor heartbeats fit in the window, where a single delayed tick
// means JetStream redelivers messages that are still being indexed.
func checkAckWait(name string, ackWait time.Duration, progress time.Duration) error {
	klog.Infof("Consumer %s has a live ackWait of %s; heartbeating in-flight batches every %s", name, ackWait, progress)

	if budget := heartbeatHardFloor * progress; budget > ackWait {
		return fmt.Errorf("consumer %s has a live ackWait of %s, but %d * batch-ack-progress-interval is %s, so a heartbeat can miss the window entirely: lower batch-ack-progress-interval or bring the consumer up to the %s the manifest expects",
			name, ackWait, heartbeatHardFloor, budget, consumerAckWait)
	}

	if budget := heartbeatMargin * progress; budget > ackWait {
		klog.Warningf("Consumer %s has a live ackWait of %s, which is under the %s that %d * batch-ack-progress-interval (%s) wants: heartbeats still fit, but only just. Expected the manifest value of %s; check whether the consumer has been reconciled.",
			name, ackWait, budget, heartbeatMargin, progress, consumerAckWait)
	}

	return nil
}

// serveMetrics starts the Prometheus endpoint and shuts it down when ctx is
// cancelled. An empty address disables metrics serving. The listener is bound
// synchronously so a port clash fails startup instead of leaving the process
// running without metrics.
func serveMetrics(ctx context.Context, addr string) error {
	if addr == "" {
		klog.Info("Metrics endpoint disabled")
		return nil
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind metrics endpoint on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", legacyregistry.Handler())

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		klog.Infof("Serving metrics on %s/metrics", listener.Addr())
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Errorf("Metrics server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			klog.Errorf("Failed to shut down metrics server: %v", err)
		}
	}()

	return nil
}

// startPprofServer starts a net/http/pprof server on addr in its own goroutine
// and shuts it down when ctx is cancelled.
//
// The handlers are registered on a dedicated ServeMux rather than by importing
// net/http/pprof for its side effects, which would mutate http.DefaultServeMux
// and expose the profiles on any other server using the default mux.
//
// Failures here are logged and never fatal: profiling must not be able to take
// down the indexer.
func startPprofServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	// pprof.Index also serves the runtime/pprof named profiles (heap, goroutine,
	// allocs, block, mutex, threadcreate) under /debug/pprof/<name>.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		klog.Infof("Starting pprof server on %s (endpoints under /debug/pprof/)", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Errorf("pprof server stopped with error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			klog.Errorf("failed to shut down pprof server: %v", err)
		}
	}()
}
