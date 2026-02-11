package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.miloapis.net/search/internal/indexer"
	"k8s.io/klog/v2"
)

// ResourceIndexerOptions holds the configuration for the resource indexer.
type ResourceIndexerOptions struct {
	NatsURL         string
	NatsSubject     string
	NatsQueueGroup  string
	NatsDurableName string
	NatsStreamName  string
	NatsAckWait     time.Duration
	NatsMaxInFlight int
}

// NewResourceIndexerOptions creates a new ResourceIndexerOptions with default values.
func NewResourceIndexerOptions() *ResourceIndexerOptions {
	return &ResourceIndexerOptions{
		NatsURL:         "nats://nats.nats-system.svc.cluster.local:4222",
		NatsSubject:     "audit.>",
		NatsQueueGroup:  "search-indexer",
		NatsDurableName: "search-indexer",
		NatsStreamName:  "AUDIT_EVENTS",
		NatsAckWait:     30 * time.Second,
		NatsMaxInFlight: 10,
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

	idx := indexer.NewIndexer(consumer)

	klog.Info("Starting indexer...")
	return idx.Start(ctx)
}
