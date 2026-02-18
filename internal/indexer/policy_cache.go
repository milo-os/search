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

// PolicyCache maintains a thread-safe cache of compiled ResourceIndexPolicies.
// It uses a controller-runtime informer to watch the API server for changes
// via a watch stream, keeping policies in-sync without polling.
type PolicyCache struct {
	mu       sync.RWMutex
	policies map[string]*policyevaluation.CachedPolicy
	celEnv   *cel.Env
	cache    runtimecache.Cache
}

// NewPolicyCache creates a new PolicyCache backed by the given controller-runtime
// cache. The cache must be started (via Start) before policies are available.
func NewPolicyCache(c runtimecache.Cache) (*PolicyCache, error) {
	klog.Info("Creating policy cache")
	env, err := internalcel.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL env: %w", err)
	}

	klog.Info("Policy cache created")

	return &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env,
		cache:    c,
	}, nil
}

// Start registers informer event handlers for ResourceIndexPolicy objects
func (c *PolicyCache) Start(ctx context.Context) error {
	klog.Info("Starting policy cache informer")

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

	// Start runs all informers and blocks until ctx is cancelled.
	if err := c.cache.Start(ctx); err != nil {
		return fmt.Errorf("policy cache informer stopped with error: %w", err)
	}
	return nil
}

// upsertPolicy compiles the given policy and stores it in the cache.
// Policies that are not Ready are skipped.
func (c *PolicyCache) upsertPolicy(p *v1alpha1.ResourceIndexPolicy) {
	key := p.Name

	if !meta.IsStatusConditionTrue(p.Status.Conditions, "Ready") {
		klog.Infof("Policy %s is not Ready, removing from cache if present", key)
		c.deletePolicy(key)
		return
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
