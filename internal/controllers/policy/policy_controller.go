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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/internal/policy/validation"
	"go.miloapis.net/search/internal/tenant"
	"go.miloapis.net/search/internal/utils"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"

	"go.miloapis.net/search/pkg/meilisearch"
)

// ResourceReindexPublisher is implemented by any component that can enqueue a
// single Kubernetes resource for background re-indexing (e.g. the NATS-backed
type ResourceReindexPublisher interface {
	// PublishResource publishes a single resource object for re-indexing.
	// policyName, indexName, and specHash identify the policy version that triggered the re-index
	// so the consumer can evaluate against the correct policy conditions.
	// tenant and tenantType identify which tenant the resource belongs to;
	// use "platform" and "platform" for single-tenant deployments.
	PublishResource(ctx context.Context, resource map[string]any, resourceID, policyName, indexName, specHash, tenant, tenantType string) error
}

// ResourceIndexPolicyReconciler reconciles a ResourceIndexPolicy object
type ResourceIndexPolicyReconciler struct {
	Client       client.Client
	Scheme       *runtime.Scheme
	CelValidator *cel.Validator
	SearchSDK    *meilisearch.SDKClient

	// DynamicClient is used to list target resources when a policy changes so
	// that each resource can be published to the re-index queue individually.
	DynamicClient dynamic.Interface

	// RESTMapper resolves a GVK to its REST resource name and scope so we can
	// call the dynamic client correctly.
	RESTMapper meta.RESTMapper

	// ReindexPublisher is called once per target resource after a
	// successful reconcile to trigger background re-indexing.
	ReindexPublisher ResourceReindexPublisher

	// TenantRegistry provides the set of active tenants and per-tenant dynamic
	// clients. When nil, the reconciler falls back to single-tenant mode using
	// DynamicClient directly with "platform" as the tenant identity.
	TenantRegistry tenant.TenantRegistry

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	Finalizers finalizer.Finalizers
}

const (
	FinalizerName = "search.miloapis.com/cleanup"

	ReadyConditionType         = "Ready"
	ReadyConditionReason       = "PolicyReady"
	NotReadyConditionReason    = "PolicyNotReady"
	TerminatingConditionReason = "Terminating"

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

	FilterableAttributesConditionType = "FilterableAttributesConfigured"

	ReindexingConditionType    = "Reindexing"
	ReindexingCompleteReason   = "ReindexingComplete"
	ReindexingInProgressReason = "ReindexingInProgress"
)

// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch
// +kubebuilder:rbac:groups=resourcemanager.miloapis.com,resources=projects,verbs=get;list;watch

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

	// Run finalizers:
	finalizeResult, err := r.Finalizers.Finalize(ctx, policy)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to run finalizers: %w", err)
	}
	if finalizeResult.Updated {
		logger.Info("Finalizer updated the policy object, persisting to API server")
		if updateErr := r.Client.Update(ctx, policy); updateErr != nil {
			if errors.IsConflict(updateErr) {
				logger.Info("Conflict updating policy after finalizer update; requeuing")
				return ctrl.Result{Requeue: true}, nil
			}
			logger.Error(updateErr, "Failed to update ResourceIndexPolicy after finalizer update")
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	if policy.GetDeletionTimestamp() != nil {
		logger.Info("ResourceIndexPolicy is being deleted, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	// List all policies to check for global uniqueness constraints
	allPolicies := &searchv1alpha1.ResourceIndexPolicyList{}
	if err := r.Client.List(ctx, allPolicies); err != nil {
		logger.Error(err, "Failed to list all ResourceIndexPolicies for validation")
		return ctrl.Result{}, err
	}
	otherPolicies := make([]*searchv1alpha1.ResourceIndexPolicy, len(allPolicies.Items))
	for i := range allPolicies.Items {
		otherPolicies[i] = &allPolicies.Items[i]
	}

	// As webhook validation may change in the future, we ensure that
	// current policies are still valid with the newest validation logic.
	valErr := validation.ValidateResourceIndexPolicy(policy, otherPolicies, r.CelValidator)

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

	// Prepare filterable attributes condition
	filterableAttributesCondition := metav1.Condition{
		Type:    FilterableAttributesConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  AttributesSyncedReason,
		Message: "Filterable attributes are configured",
	}

	// Prepare reindexing condition
	reindexingCondition := metav1.Condition{
		Type:    ReindexingConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  ReindexingCompleteReason,
		Message: "Reindexing completed",
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
				// ParsePath handles bracket notation (e.g. ".metadata.annotations['key']")
				// and returns segments that are joined with dots to produce the
				// Meilisearch-compatible attribute name (e.g. "metadata.annotations.key").
				segments := evaluation.ParsePath(f.Path)
				if len(segments) > 0 {
					desiredAttributes = append(desiredAttributes, strings.Join(segments, "."))
				}
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

	// Manage Filterable Attributes
	// The baseline filterable attributes ensure that _tenant and _tenant_type are always
	// present so multi-tenant filter queries work regardless of policy field configuration.
	baseFilterableAttributes := []string{"uid", "metadata.name", "metadata.namespace", "_tenant", "_tenant_type"}
	if searchIndexCondition.Status == metav1.ConditionTrue {
		// Check for pending settings update (shared with searchable attributes above)
		settingsTaskForFilter, err := r.SearchSDK.GetSettingsUpdateTask(searchIndex)
		if err != nil {
			logger.Error(err, "Failed to get settings update task for filterable attributes")
			filterableAttributesCondition.Status = metav1.ConditionFalse
			filterableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
			filterableAttributesCondition.Message = "Failed to check settings status"
		}

		isFilterPending := false
		if settingsTaskForFilter != nil && r.SearchSDK.IsTaskPending(settingsTaskForFilter) {
			isFilterPending = true
			filterableAttributesCondition.Status = metav1.ConditionFalse
			filterableAttributesCondition.Reason = AttributesUpdatingReason
			filterableAttributesCondition.Message = "Filterable attributes update is pending"
			logger.Info("Filterable attributes update is pending")
		}

		if !isFilterPending {
			currentFilterable, err := r.SearchSDK.GetFilterableAttributes(searchIndex)
			if err != nil {
				logger.Error(err, "Failed to get current filterable attributes")
				filterableAttributesCondition.Status = metav1.ConditionFalse
				filterableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
				filterableAttributesCondition.Message = "Failed to get current filterable attributes"
			} else {
				// Ensure all baseline attributes are present; preserve any extras already configured.
				desired := mergeFilterableAttributes(currentFilterable, baseFilterableAttributes)
				sort.Strings(desired)

				current := make([]string, len(currentFilterable))
				copy(current, currentFilterable)
				sort.Strings(current)

				equal := len(current) == len(desired)
				if equal {
					for i := range current {
						if current[i] != desired[i] {
							equal = false
							break
						}
					}
				}

				if !equal {
					logger.Info("Updating filterable attributes", "current", current, "desired", desired)
					_, err := r.SearchSDK.UpdateFilterableAttributes(searchIndex, desired)
					if err != nil {
						logger.Error(err, "Failed to update filterable attributes")
						filterableAttributesCondition.Status = metav1.ConditionFalse
						filterableAttributesCondition.Reason = fmt.Sprintf("%s: %s", AttributesFailedReason, err.Error())
						filterableAttributesCondition.Message = "Failed to update filterable attributes: " + err.Error()
					} else {
						filterableAttributesCondition.Status = metav1.ConditionFalse
						filterableAttributesCondition.Reason = AttributesUpdatingReason
						filterableAttributesCondition.Message = "Updating filterable attributes"
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

	// Determine if reindexing is needed by comparing a SHA-256 hash of the
	// current spec against the hash stored in status.
	currentHash := computeSpecHash(&policy.Spec)
	storedHash := policy.Status.CurrentGeneration
	needsReindexing := currentHash != storedHash

	if needsReindexing {
		reindexingCondition.Status = metav1.ConditionFalse
		reindexingCondition.Reason = ReindexingInProgressReason
		reindexingCondition.Message = "Reindexing in progress"
	}

	// Verify ready condition
	if validationCondition.Status == metav1.ConditionFalse ||
		searchIndexCondition.Status == metav1.ConditionFalse ||
		searchableAttributesCondition.Status == metav1.ConditionFalse ||
		filterableAttributesCondition.Status == metav1.ConditionFalse ||
		reindexingCondition.Status == metav1.ConditionFalse {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = NotReadyConditionReason
		readyCondition.Message = "ResourceIndexPolicy is not ready"
	}

	// Determine if actions made to Meilisearch are pending to complete
	isPending := searchIndexCondition.Reason == IndexPendingReason ||
		searchableAttributesCondition.Reason == AttributesUpdatingReason ||
		filterableAttributesCondition.Reason == AttributesUpdatingReason

	// Clone policy to update status (we need the original status to compare against)
	oldStatus := policy.Status.DeepCopy()
	policy.Status.IndexName = searchIndex
	meta.SetStatusCondition(&policy.Status.Conditions, readyCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, validationCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, searchIndexCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, searchableAttributesCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, filterableAttributesCondition)
	meta.SetStatusCondition(&policy.Status.Conditions, reindexingCondition)

	// Update status now. This persists any "InProgress" state before we start
	// the long-running re-indexing operation.
	if err := utils.UpdateStatusIfChanged(ctx, r.Client, logger, policy, oldStatus, &policy.Status); err != nil {
		return ctrl.Result{}, err
	}

	if errResult {
		return ctrl.Result{}, fmt.Errorf("ResourceIndexPolicy has errors")
	}

	// If index is pending or attributes updating, requeue after 5 seconds
	if isPending {
		logger.Info("Index or attributes pending, requeuing after 5 seconds")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Trigger background re-indexing
	if needsReindexing {
		logger.Info("Triggering background re-indexing", "specHash", currentHash)
		if err := r.publishReindexMessages(ctx, policy); err != nil {
			logger.Error(err, "Failed to publish re-index messages")
			reindexingCondition.Status = metav1.ConditionFalse
			reindexingCondition.Reason = "ReindexingFailed"
			reindexingCondition.Message = "Failed to publish re-index messages: " + err.Error()
		} else {
			// Success! Persist the current spec hash so we don't re-index on
			// the next reconcile unless the spec actually changes again.
			policy.Status.CurrentGeneration = currentHash

			reindexingCondition.Status = metav1.ConditionTrue
			reindexingCondition.Reason = ReindexingCompleteReason
			reindexingCondition.Message = fmt.Sprintf("Reindexing published for spec hash %s", currentHash[:8])
		}

		meta.SetStatusCondition(&policy.Status.Conditions, reindexingCondition)

		// Re-evaluate Ready condition based on the result of re-indexing.
		if reindexingCondition.Status == metav1.ConditionTrue {
			readyCondition.Status = metav1.ConditionTrue
			readyCondition.Reason = ReadyConditionReason
			readyCondition.Message = "ResourceIndexPolicy is valid and indexed"
		} else {
			readyCondition.Status = metav1.ConditionFalse
			readyCondition.Reason = NotReadyConditionReason
			readyCondition.Message = "Reindexing failed: " + reindexingCondition.Message
		}
		meta.SetStatusCondition(&policy.Status.Conditions, readyCondition)

		if err := r.Client.Status().Update(ctx, policy); err != nil {
			logger.Error(err, "Failed to update status after reindexing")
			return ctrl.Result{}, err
		}
	}

	if reindexingCondition.Status == metav1.ConditionFalse {
		return ctrl.Result{}, fmt.Errorf("Error when indexing policies")
	}

	return ctrl.Result{}, nil

}

// mergeFilterableAttributes returns a deduplicated union of existing and required attributes.
// It preserves any extras already configured in the index while ensuring all required
// attributes are present.
func mergeFilterableAttributes(existing, required []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(required))
	result := make([]string, 0, len(existing)+len(required))
	for _, a := range existing {
		if _, ok := seen[a]; !ok {
			seen[a] = struct{}{}
			result = append(result, a)
		}
	}
	for _, a := range required {
		if _, ok := seen[a]; !ok {
			seen[a] = struct{}{}
			result = append(result, a)
		}
	}
	return result
}

// computeSpecHash delegates to the shared utility for computing a policy spec hash.
func computeSpecHash(spec *searchv1alpha1.ResourceIndexPolicySpec) string {
	return utils.ComputeSpecHash(spec)
}

// publishReindexMessages iterates all active tenants and publishes one reindex
// message per resource per tenant to the re-index queue.
// In single-tenant mode (TenantRegistry is nil), it behaves identically to the
// original implementation, using DynamicClient with "platform" as the tenant.
func (r *ResourceIndexPolicyReconciler) publishReindexMessages(
	ctx context.Context,
	policy *searchv1alpha1.ResourceIndexPolicy,
) error {
	logger := logf.FromContext(ctx).WithName("resourceindexpolicy-controller")

	// Determine which tenants to publish for. When TenantRegistry is nil we
	// fall back to single-tenant mode: just the platform tenant using the
	// reconciler's own DynamicClient.
	type tenantEntry struct {
		info   tenant.TenantInfo
		client dynamic.Interface
	}

	var tenants []tenantEntry
	if r.TenantRegistry != nil {
		for _, ti := range r.TenantRegistry.ListTenants() {
			dc := r.TenantRegistry.GetTenantClient(ti.Name)
			if dc == nil {
				logger.Info("No client for tenant; skipping", "tenant", ti.Name)
				continue
			}
			tenants = append(tenants, tenantEntry{info: ti, client: dc})
		}
	} else {
		tenants = []tenantEntry{
			{
				info:   tenant.PlatformTenantInfo,
				client: r.DynamicClient,
			},
		}
	}

	var firstErr error
	totalPublished := 0

	for _, te := range tenants {
		n, err := r.publishReindexMessagesForTenant(ctx, policy, te.info, te.client)
		totalPublished += n
		if err != nil {
			logger.Error(err, "Failed to publish re-index messages for tenant; continuing",
				"tenant", te.info.Name)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	logger.Info("Published re-index messages", "policy", policy.Name, "count", totalPublished)
	return firstErr
}

// publishReindexMessagesForTenant lists all resources matching the policy's
// TargetResource using the provided dynamic client and publishes one reindex
// message per resource, stamped with the given tenant identity.
// It returns the number of messages published and the first error encountered.
func (r *ResourceIndexPolicyReconciler) publishReindexMessagesForTenant(
	ctx context.Context,
	policy *searchv1alpha1.ResourceIndexPolicy,
	tenantInfo tenant.TenantInfo,
	dynamicClient dynamic.Interface,
) (int, error) {
	logger := logf.FromContext(ctx).WithName("resourceindexpolicy-controller").
		WithValues("tenant", tenantInfo.Name)

	target := policy.Spec.TargetResource
	gvk := schema.GroupVersionKind{
		Group:   target.Group,
		Version: target.Version,
		Kind:    target.Kind,
	}

	// Resolve the REST mapping so we know the plural resource name and scope.
	mapping, err := r.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		if meta.IsNoMatchError(err) {
			logger.Info("Target resource type not found, skipping re-index", "gvk", gvk.String())
			return 0, nil
		}
		logger.Error(err, "RESTMapper failure", "gvk", gvk.String(), "group", gvk.Group, "version", gvk.Version, "kind", gvk.Kind)
		return 0, fmt.Errorf("failed to get REST mapping for %v: %w", gvk, err)
	}

	// Determine whether the resource is cluster-scoped or namespace-scoped.
	var resourceClient dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameRoot {
		resourceClient = dynamicClient.Resource(mapping.Resource)
	} else {
		resourceClient = dynamicClient.Resource(mapping.Resource).Namespace(metav1.NamespaceAll)
	}

	// Page through all resources and publish one reindex event per resource.
	var continueToken string
	pageSize := int64(500)
	published := 0

	for {
		list, err := resourceClient.List(ctx, metav1.ListOptions{
			Limit:    pageSize,
			Continue: continueToken,
		})
		if err != nil {
			return published, fmt.Errorf("failed to list %v resources: %w", gvk, err)
		}

		indexName := policy.Status.IndexName
		specHash := computeSpecHash(&policy.Spec)

		for i := range list.Items {
			obj := &list.Items[i]
			logger.Info("Publishing re-index message", "resource", obj.GetName(), "namespace", obj.GetNamespace())
			// Use "reindex/<policyName>/<tenant>/<uid>" as the resourceID so that
			// duplicate messages from rapid policy updates are deduplicated by NATS.
			resourceID := fmt.Sprintf("reindex/%s/%s/%s", policy.Name, tenantInfo.Name, obj.GetUID())
			if err := r.ReindexPublisher.PublishResource(ctx, obj.Object, resourceID, policy.Name, indexName, specHash, tenantInfo.Name, tenantInfo.Type); err != nil {
				logger.Error(err, "Failed to publish re-index message",
					"resource", obj.GetName(), "namespace", obj.GetNamespace())
				continue
			}
			published++
		}

		continueToken = list.GetContinue()
		if continueToken == "" {
			break
		}
	}

	logger.Info("Published re-index messages for tenant", "policy", policy.Name, "tenant", tenantInfo.Name, "count", published)
	return published, nil
}

// resourceIndexPolicyFinalizer handles Meilisearch cleanup when a
// ResourceIndexPolicy is deleted.
type resourceIndexPolicyFinalizer struct {
	Client    client.Client
	SearchSDK *meilisearch.SDKClient
}

func (f *resourceIndexPolicyFinalizer) Finalize(ctx context.Context, obj client.Object) (finalizer.Result, error) {
	log := logf.FromContext(ctx).WithName("resourceindexpolicy-finalizer")
	log.Info("Finalizing ResourceIndexPolicy")

	policy, ok := obj.(*searchv1alpha1.ResourceIndexPolicy)
	if !ok {
		return finalizer.Result{}, fmt.Errorf("object is not a ResourceIndexPolicy")
	}

	// Set Ready=False immediately so consumers can observe the terminating state.
	oldStatus := policy.Status.DeepCopy()
	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    ReadyConditionType,
		Status:  metav1.ConditionFalse,
		Reason:  TerminatingConditionReason,
		Message: "ResourceIndexPolicy is being deleted",
	})
	if err := utils.UpdateStatusIfChanged(ctx, f.Client, log, policy, oldStatus, &policy.Status); err != nil {
		log.Error(err, "Failed to update ResourceIndexPolicy status")
		return finalizer.Result{}, err
	}

	// Remove all documents then delete the index itself.
	// Calculate instead of using from status.indexName to avoid race condition
	// where the index status update was not made
	searchIndex := utils.GetSearchIndex(policy.Spec.TargetResource)

	log.Info("Deleting all documents from search index", "index", searchIndex)
	if err := f.SearchSDK.DeleteAllDocuments(searchIndex); err != nil {
		log.Error(err, "Failed to delete all documents from search index")
		return finalizer.Result{}, err
	}

	log.Info("Deleting search index", "index", searchIndex)
	if err := f.SearchSDK.DeleteIndex(searchIndex); err != nil {
		log.Error(err, "Failed to delete search index")
		return finalizer.Result{}, err
	}

	log.Info("ResourceIndexPolicy cleanup complete")
	return finalizer.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceIndexPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Finalizers = finalizer.NewFinalizers()
	if err := r.Finalizers.Register(FinalizerName, &resourceIndexPolicyFinalizer{
		Client:    r.Client,
		SearchSDK: r.SearchSDK,
	}); err != nil {
		return fmt.Errorf("failed to register finalizer: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&searchv1alpha1.ResourceIndexPolicy{}).
		Complete(r)
}
