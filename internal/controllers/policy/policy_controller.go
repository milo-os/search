package policy

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/internal/policy/validation"
	"go.miloapis.net/search/internal/utils"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"

	"go.miloapis.net/search/pkg/meilisearch"
)

// ResourceIndexPolicyReconciler reconciles a ResourceIndexPolicy object
type ResourceIndexPolicyReconciler struct {
	Client       client.Client
	Scheme       *runtime.Scheme
	CelValidator *cel.Validator
	SearchSDK    *meilisearch.SDKClient

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

const (
	ReadyConditionType      = "Ready"
	ReadyConditionReason    = "PolicyReady"
	NotReadyConditionReason = "PolicyNotReady"

	SearchIndexReadyConditionType = "SearchIndexReady"
	IndexCreatedReason            = "IndexCreated"
	IndexPendingReason            = "IndexPending"
	IndexFailedReason             = "IndexFailed"

	ValidationConditionType  = "PolicyValidation"
	ValidationReadyReason    = "PolicyValidationReady"
	ValidationNotReadyReason = "PolicyValidationNotReady"
)

// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies/status,verbs=get;update;patch

// Reconcile matches the state of the cluster with the desired state of a ResourceIndexPolicy.
func (r *ResourceIndexPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithName("resourceindexpolicy-controller")

	logger.Info("Reconciling ResourceIndexPolicy")

	// Get the policy
	policy := &searchv1alpha1.ResourceIndexPolicy{}
	err := r.Client.Get(ctx, req.NamespacedName, policy)
	if errors.IsNotFound(err) {
		logger.Info("ResourceIndexPolicy not found, probably deleted.")
		return ctrl.Result{}, nil
	} else if err != nil {
		logger.Error(err, "Failed to get ResourceIndexPolicy")
		return ctrl.Result{}, err
	}

	// Check if the policy is being deleted
	if policy.GetDeletionTimestamp() != nil {
		logger.Info("ResourceIndexPolicy is being deleted")
		return ctrl.Result{}, nil
	}

	// As webhook validation may change in the future, we ensure that
	// current policies are still valid with the newest validation logic.
	valErr := validation.ValidateResourceIndexPolicy(policy, r.CelValidator)

	// Prepare ready condition
	readyCondition := metav1.Condition{
		Type:    ReadyConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  ReadyConditionReason,
		Message: "ResourceIndexPolicy is valid and indexed in the search provider",
	}

	// Prepare validation condition
	validationCondition := metav1.Condition{
		Type:    ValidationConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  ValidationReadyReason,
		Message: "ResourceIndexPolicy is valid",
	}

	// Prepare search index condition
	searchIndexCondition := metav1.Condition{
		Type:    SearchIndexReadyConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  IndexCreatedReason,
		Message: "Search index provider is ready",
	}

	indexingErr := field.ErrorList{}
	// Create search index if not exists
	searchIndex := utils.GetSearchIndex(policy.Spec.TargetResource)

	exists, err := r.SearchSDK.IndexExists(searchIndex)
	if err != nil {
		indexingErr = append(indexingErr, field.InternalError(field.NewPath("spec", "targetResource"), err))
	} else if exists {
		// Index exists, we are good
		logger.Info("Search index provider is ready")
		searchIndexCondition.Status = metav1.ConditionTrue
		searchIndexCondition.Reason = IndexCreatedReason
		searchIndexCondition.Message = "Search index provider is ready"
	} else {
		// Index does not exist, check if we create it or if it is pending
		indexTask, err := r.SearchSDK.GetIndexCreationTask(searchIndex)
		if err != nil {
			logger.Error(err, "Failed to get index creation task")
			indexingErr = append(indexingErr, field.InternalError(field.NewPath("spec", "targetResource"), err))
		}
		if indexTask == nil && err == nil {
			// Index does not exist (and no task found), create it
			logger.Info("Creating search index")
			_, err = r.SearchSDK.CreateIndex(searchIndex)
			if err != nil {
				logger.Error(err, "Failed to create search index")
				indexingErr = append(indexingErr, field.InternalError(field.NewPath("spec", "targetResource"), err))
			}
		}
		if indexTask != nil && err == nil {
			// Index exists, get actual state
			if r.SearchSDK.IsTaskPending(indexTask) {
				logger.Info("Index creation is pending in search provider")
				searchIndexCondition.Status = metav1.ConditionFalse
				searchIndexCondition.Reason = IndexPendingReason
				searchIndexCondition.Message = "Index creation is pending in search provider"
			} else if r.SearchSDK.IsTaskFailed(indexTask) {
				logger.Error(err, "Index creation failed in search provider")
				searchIndexCondition.Status = metav1.ConditionFalse
				searchIndexCondition.Reason = IndexFailedReason
				searchIndexCondition.Message = "Index creation failed: " + indexTask.Error.Message
			} else {
				logger.Info("Index creation completed in search provider")
			}
		}
	}

	errResult := false
	// Verify validation errors
	if len(valErr) > 0 {
		errResult = true
		validationCondition.Status = metav1.ConditionFalse
		validationCondition.Reason = ValidationNotReadyReason
		validationCondition.Message = valErr.ToAggregate().Error()
		logger.Error(valErr.ToAggregate(), "ResourceIndexPolicy validation failed")
	}

	// Verify indexing errors
	if len(indexingErr) > 0 {
		errResult = true
		searchIndexCondition.Status = metav1.ConditionFalse
		searchIndexCondition.Reason = IndexFailedReason
		searchIndexCondition.Message = indexingErr.ToAggregate().Error()
		logger.Error(indexingErr.ToAggregate(), "ResourceIndexPolicy indexing failed")
	}

	// Verify ready condition
	if validationCondition.Status == metav1.ConditionFalse || searchIndexCondition.Status == metav1.ConditionFalse {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = NotReadyConditionReason
		readyCondition.Message = "ResourceIndexPolicy is not ready"
	}

	// Clone policy to update status (we need the original status to compare against)
	oldStatus := policy.Status.DeepCopy()
	policy.Status.IndexName = searchIndex
	meta.SetStatusCondition(&policy.Status.Conditions, readyCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, validationCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, searchIndexCondition)

	if err := utils.UpdateStatusIfChanged(ctx, r.Client, logger, policy, oldStatus, &policy.Status); err != nil {
		return ctrl.Result{}, err
	}

	if errResult {
		return ctrl.Result{}, fmt.Errorf("ResourceIndexPolicy has errors")
	}

	// If index is pending, requeue after 5 seconds to check if index is ready
	if searchIndexCondition.Reason == IndexPendingReason {
		logger.Info("Index creation is pending, requeuing after 5 seconds")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceIndexPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&searchv1alpha1.ResourceIndexPolicy{}).
		Complete(r)
}
