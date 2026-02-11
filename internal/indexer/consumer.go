package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/klog/v2"
)

// Indexer is the component responsible for indexing resources.
type Indexer struct {
	consumer jetstream.Consumer
}

// NewIndexer creates a new Indexer instance.
func NewIndexer(consumer jetstream.Consumer) *Indexer {
	return &Indexer{
		consumer: consumer,
	}
}

// Start starts the indexer consumer loop.
func (i *Indexer) Start(ctx context.Context) error {
	// Consume messages
	cons, err := i.consumer.Consume(func(msg jetstream.Msg) {
		// Parse the message to get the AuditID
		var event struct {
			AuditID string `json:"auditID"`
		}
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			klog.Errorf("Failed to unmarshal audit event: %v", err)
		} else {
			klog.Infof("Received audit event: %s", event.AuditID)
		}

		// In a real implementation, we would process the message here.
		// For now, we just ACK it.
		if err := msg.Ack(); err != nil {
			klog.Errorf("Failed to ACK message: %v", err)
		} else {
			klog.V(2).Infof("Acknowledged message")
		}
	})
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	defer cons.Stop()

	klog.Info("Indexer started successfully")

	// Wait for context cancellation
	<-ctx.Done()
	klog.Info("Shutting down indexer...")
	return nil
}
