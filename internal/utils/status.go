package utils

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// UpdateStatusIfChanged checks if the status has changed and updates it if necessary.
// It compares oldStatus and newStatus using k8s.io/apimachinery/pkg/api/equality.Semantic.DeepEqual.
// The obj argument should be the object with the *new* status set, ready to be updated.
func UpdateStatusIfChanged(ctx context.Context, c client.Client, logger logr.Logger, obj client.Object, oldStatus interface{}, newStatus interface{}) error {
	if !equality.Semantic.DeepEqual(oldStatus, newStatus) {

		logger.Info("Updating status")

		if err := c.Status().Update(ctx, obj); err != nil {
			logger.Error(err, "Failed to update status")
			return fmt.Errorf("failed to update status: %w", err)
		}
	} else {
		logger.Info("Status unchanged, skipping update")
	}
	return nil
}
