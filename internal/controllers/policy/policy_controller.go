package policy

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/internal/policy/validation"
	"go.miloapis.net/search/internal/utils"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// ResourceIndexPolicyReconciler reconciles a ResourceIndexPolicy object
type ResourceIndexPolicyReconciler struct {
	Client       client.Client
	Scheme       *runtime.Scheme
	CelValidator *cel.Validator
}

const (
	ReadyConditionType     = "Ready"
	ValidConditionReason   = "Valid"
	InvalidConditionReason = "Invalid"
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

	// Prepare status update
	newCondition := metav1.Condition{
		Type:               ReadyConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             ValidConditionReason,
		Message:            "ResourceIndexPolicy is valid",
		LastTransitionTime: metav1.Now(),
	}

	// If valdiation errors, set the status to false
	if len(valErr) > 0 {
		newCondition.Status = metav1.ConditionFalse
		newCondition.Reason = InvalidConditionReason
		newCondition.Message = valErr.ToAggregate().Error()
		logger.Error(valErr.ToAggregate(), "ResourceIndexPolicy validation failed")
	}

	// Clone policy to update status (we need the original status to compare against)
	oldStatus := policy.Status.DeepCopy()
	meta.SetStatusCondition(&policy.Status.Conditions, newCondition)

	if err := utils.UpdateStatusIfChanged(ctx, r.Client, logger, policy, oldStatus, &policy.Status); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceIndexPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&searchv1alpha1.ResourceIndexPolicy{}).
		Complete(r)
}
