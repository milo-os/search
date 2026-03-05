package indexer

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"
)

// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=search.miloapis.com,resources=resourceindexpolicies/status,verbs=get;list;watch

// PolicyCache maintains a thread-safe cache of compiled ResourceIndexPolicies.
// It uses a controller-runtime informer to watch the API server for changes
// via a watch stream, keeping policies in-sync without polling.
type PolicyCache struct {
	mu       sync.RWMutex
	policies map[string]*policyevaluation.CachedPolicy
	celEnv   *cel.Env
	cache    runtimecache.Cache

	// requireReadyCondition, when true, ensures that only policies with a "Ready"
	// condition set to "True" are cached. This is mandatory for the primary
	// indexer to ensure it only processes resources for fully initialized policies.
	requireReadyCondition bool
}

// NewPolicyCache creates a new PolicyCache backed by the given controller-runtime cache.
// The requireReadyCondition parameter determines if the cache should strictly enforce
// the "Ready" status condition on policies before caching them.
func NewPolicyCache(c runtimecache.Cache, requireReadyCondition bool) (*PolicyCache, error) {
	klog.Info("Initializing policy cache environment")
	env, err := internalcel.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL env: %w", err)
	}

	klog.Info("Policy cache created")

	return &PolicyCache{
		policies:              make(map[string]*policyevaluation.CachedPolicy),
		celEnv:                env,
		cache:                 c,
		requireReadyCondition: requireReadyCondition,
	}, nil
}

// RegisterHandlers registers informer event handlers for ResourceIndexPolicy objects.
func (c *PolicyCache) RegisterHandlers(ctx context.Context) error {
	klog.Info("Registering policy cache informer handlers")

	informer, err := c.cache.GetInformer(ctx, &v1alpha1.ResourceIndexPolicy{})
	if err != nil {
		return fmt.Errorf("failed to get ResourceIndexPolicy informer: %w", err)
	}

	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			policy, ok := obj.(*v1alpha1.ResourceIndexPolicy)
			if !ok {
				klog.Errorf("PolicyCache AddFunc: unexpected object type %T", obj)
				return
			}
			c.upsertPolicy(policy)
		},
		UpdateFunc: func(_, newObj any) {
			policy, ok := newObj.(*v1alpha1.ResourceIndexPolicy)
			if !ok {
				klog.Errorf("PolicyCache UpdateFunc: unexpected object type %T", newObj)
				return
			}
			c.upsertPolicy(policy)
		},
		DeleteFunc: func(obj any) {
			// obj may be a DeletedFinalStateUnknown tombstone.
			if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			policy, ok := obj.(*v1alpha1.ResourceIndexPolicy)
			if !ok {
				klog.Errorf("PolicyCache DeleteFunc: unexpected object type %T", obj)
				return
			}
			c.deletePolicy(policy.Name)
		},
	}); err != nil {
		return fmt.Errorf("failed to add event handler to ResourceIndexPolicy informer: %w", err)
	}

	return nil
}

// upsertPolicy compiles the given policy and stores it in the cache.
// Policies that are not Ready are skipped.
func (c *PolicyCache) upsertPolicy(p *v1alpha1.ResourceIndexPolicy) {
	key := p.Name

	// Evict policies that are being deleted so the indexer stops processing new
	// resources against an index that is about to be torn down. DeletionTimestamp
	// is set by Kubernetes as soon as a delete request is received — before the
	// finalizer runs — giving us the earliest possible eviction signal.
	if p.DeletionTimestamp != nil {
		klog.Infof("Policy %s is being deleted; evicting from cache", key)
		c.deletePolicy(key)
		return
	}

	// If strict ready checking is enabled, we only cache policies that are fully Ready.
	// This prevents the primary indexer from processing events for policies that are
	// still being initialized (e.g. index creation or initial re-indexing).
	if c.requireReadyCondition {
		if !meta.IsStatusConditionTrue(p.Status.Conditions, "Ready") {
			klog.Infof("Policy %s is not yet Ready (Ready=True condition missing); skipping cache", key)
			c.deletePolicy(key)
			return
		}
	}

	cached := &policyevaluation.CachedPolicy{
		Policy:     p,
		Conditions: make(map[string]cel.Program),
	}

	for _, cond := range p.Spec.Conditions {
		ast, issues := c.celEnv.Compile(cond.Expression)
		if issues != nil && issues.Err() != nil {
			klog.Errorf("Failed to compile condition %q for policy %s: %v", cond.Name, key, issues.Err())
			continue
		}

		if ast.OutputType() != cel.BoolType {
			klog.Errorf("Condition %q for policy %s must evaluate to boolean", cond.Name, key)
			continue
		}

		prg, err := c.celEnv.Program(ast)
		if err != nil {
			klog.Errorf("Failed to build program for condition %q of policy %s: %v", cond.Name, key, err)
			continue
		}

		cached.Conditions[cond.Name] = prg
	}

	klog.Infof("Policy %s compiled and cached", key)

	c.mu.Lock()
	c.policies[key] = cached
	c.mu.Unlock()
}

// deletePolicy removes a policy from the cache by name.
func (c *PolicyCache) deletePolicy(name string) {
	c.mu.Lock()
	delete(c.policies, name)
	c.mu.Unlock()
	klog.Infof("Policy %s removed from cache", name)
}

// Start starts the underlying cache.
func (c *PolicyCache) Start(ctx context.Context) error {
	return c.cache.Start(ctx)
}

// WaitForCacheSync waits for the underlying cache to sync.
func (c *PolicyCache) WaitForCacheSync(ctx context.Context) bool {
	return c.cache.WaitForCacheSync(ctx)
}

// GetPolicies returns a snapshot of all cached policies.
func (c *PolicyCache) GetPolicies() []*policyevaluation.CachedPolicy {
	c.mu.RLock()
	defer c.mu.RUnlock()

	policies := make([]*policyevaluation.CachedPolicy, 0, len(c.policies))
	for _, p := range c.policies {
		policies = append(policies, p)
	}
	return policies
}
