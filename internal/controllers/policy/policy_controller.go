package policy

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

	SearchableAttributesConditionType = "SearchableAttributesConfigured"
	AttributesSyncedReason            = "AttributesSynced"
	AttributesUpdatingReason          = "AttributesUpdating"
	AttributesFailedReason            = "AttributesFailed"
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

	// Prepare searchable attributes condition
	searchableAttributesCondition := metav1.Condition{
		Type:    SearchableAttributesConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  AttributesSyncedReason,
		Message: "Searchable attributes are configured",
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

		shouldCreate := true
		if indexTask != nil {
			if r.SearchSDK.IsTaskPending(indexTask) {
				shouldCreate = false
				logger.Info("Index creation is pending in search provider")
				searchIndexCondition.Status = metav1.ConditionFalse
				searchIndexCondition.Reason = IndexPendingReason
				searchIndexCondition.Message = "Index creation is pending in search provider"
			} else if r.SearchSDK.IsTaskFailed(indexTask) {
				// We will retry, but let's log the previous failure
				logger.Info("Previous index creation failed", "error", indexTask.Error.Message)
			} else {
				// Task succeeded but index is missing -> Stale task from deleted index?
				logger.Info("Found succeeded creation task for missing index, recreating")
			}
		}

		if shouldCreate && err == nil {
			logger.Info("Creating search index")
			_, err = r.SearchSDK.CreateIndex(searchIndex)
			if err != nil {
				logger.Error(err, "Failed to create search index")
				indexingErr = append(indexingErr, field.InternalError(field.NewPath("spec", "targetResource"), err))
				searchIndexCondition.Status = metav1.ConditionFalse
				searchIndexCondition.Reason = IndexFailedReason
				searchIndexCondition.Message = "Index creation failed: " + err.Error()
			}
		}
	}

	// Manage Searchable Attributes
	if searchIndexCondition.Status == metav1.ConditionTrue {
		// 1. Calculate desired attributes
		desiredAttributes := []string{}
		for _, f := range policy.Spec.Fields {
			if f.Searchable {
				// Meilisearch expects "metadata.name" not ".metadata.name"
				path := strings.TrimPrefix(f.Path, ".")
				desiredAttributes = append(desiredAttributes, path)
			}
		}
		sort.Strings(desiredAttributes)

		// 2. Check for pending updates to avoid race conditions
		settingsTask, err := r.SearchSDK.GetSettingsUpdateTask(searchIndex)
		if err != nil {
			logger.Error(err, "Failed to get settings update task")
			searchableAttributesCondition.Status = metav1.ConditionFalse
			searchableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
			searchableAttributesCondition.Message = "Failed to check settings status"
		}

		isPending := false
		if settingsTask != nil {
			if r.SearchSDK.IsTaskPending(settingsTask) {
				isPending = true
				searchableAttributesCondition.Status = metav1.ConditionFalse
				searchableAttributesCondition.Reason = AttributesUpdatingReason
				searchableAttributesCondition.Message = "Searchable attributes update is pending"
				logger.Info("Searchable attributes update is pending")
			} else if r.SearchSDK.IsTaskFailed(settingsTask) {
				logger.Info("Previous settings update failed", "error", settingsTask.Error.Message)
				// We don't block here, we'll try to update again if needed
			}
		}

		// 3. If not pending, check current vs desired and update if needed
		if !isPending {
			currentAttributes, err := r.SearchSDK.GetSearchableAttributes(searchIndex)
			if err != nil {
				logger.Error(err, "Failed to get current searchable attributes")
				searchableAttributesCondition.Status = metav1.ConditionFalse
				searchableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
				searchableAttributesCondition.Message = "Failed to get current attributes"
			} else {
				sort.Strings(currentAttributes)

				// Compare slices
				equal := len(currentAttributes) == len(desiredAttributes)
				if equal {
					for i := range currentAttributes {
						if currentAttributes[i] != desiredAttributes[i] {
							equal = false
							break
						}
					}
				}

				if !equal {
					logger.Info("Updating searchable attributes", "current", currentAttributes, "desired", desiredAttributes)
					_, err := r.SearchSDK.UpdateSearchableAttributes(searchIndex, desiredAttributes)
					if err != nil {
						logger.Error(err, "Failed to update searchable attributes")
						searchableAttributesCondition.Status = metav1.ConditionFalse
						searchableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
						searchableAttributesCondition.Message = "Failed to update attributes: " + err.Error()
					} else {
						searchableAttributesCondition.Status = metav1.ConditionFalse
						searchableAttributesCondition.Reason = AttributesUpdatingReason
						searchableAttributesCondition.Message = "Updating searchable attributes"
					}
				}
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
	if validationCondition.Status == metav1.ConditionFalse ||
		searchIndexCondition.Status == metav1.ConditionFalse ||
		searchableAttributesCondition.Status == metav1.ConditionFalse {
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
	meta.SetStatusCondition(&policy.Status.Conditions, searchableAttributesCondition)

	if err := utils.UpdateStatusIfChanged(ctx, r.Client, logger, policy, oldStatus, &policy.Status); err != nil {
		return ctrl.Result{}, err
	}

	if errResult {
		return ctrl.Result{}, fmt.Errorf("ResourceIndexPolicy has errors")
	}

	// If index is pending or attributes updating, requeue after 5 seconds
	if searchIndexCondition.Reason == IndexPendingReason || searchableAttributesCondition.Reason == AttributesUpdatingReason {
		logger.Info("Index or attributes pending, requeuing after 5 seconds")
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
