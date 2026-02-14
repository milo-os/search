package indexer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	internalcel "go.miloapis.net/search/internal/cel"
	policyevaluation "go.miloapis.net/search/internal/policy/evaluation"
	"go.miloapis.net/search/pkg/apis/policy/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PolicyCache maintains a thread-safe cache of compiled ResourceIndexPolicies.
// It polls the API server on a fixed interval to refresh the cache.
type PolicyCache struct {
	mu       sync.RWMutex
	policies map[string]*policyevaluation.CachedPolicy
	celEnv   *cel.Env
	client   client.Client
	interval time.Duration
}

// NewPolicyCache creates a new PolicyCache that refreshes every interval.
func NewPolicyCache(k8sClient client.Client, interval time.Duration) (*PolicyCache, error) {
	klog.Info("Creating policy cache")
	env, err := internalcel.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL env: %w", err)
	}

	klog.Info("Policy cache created")

	return &PolicyCache{
		policies: make(map[string]*policyevaluation.CachedPolicy),
		celEnv:   env,
		client:   k8sClient,
		interval: interval,
	}, nil
}

// Start begins the polling loop. It performs an initial sync, then refreshes
// every interval until ctx is cancelled.
func (c *PolicyCache) Start(ctx context.Context) error {
	klog.Info("Starting initial sync of policy cache")
	// Initial sync
	if err := c.refresh(ctx); err != nil {
		klog.Errorf("Initial policy cache sync failed: %v", err)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.Info("Policy cache polling stopped")
			return nil
		case <-ticker.C:
			if err := c.refresh(ctx); err != nil {
				klog.Errorf("Failed to refresh policy cache: %v", err)
			}
		}
	}
}

// refresh lists all ResourceIndexPolicies and rebuilds the cache.
func (c *PolicyCache) refresh(ctx context.Context) error {
	list := &v1alpha1.ResourceIndexPolicyList{}
	if err := c.client.List(ctx, list); err != nil {
		return fmt.Errorf("failed to list ResourceIndexPolicies: %w", err)
	}
	klog.V(2).Infof("Found %d ResourceIndexPolicies", len(list.Items))

	newPolicies := make(map[string]*policyevaluation.CachedPolicy, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		key := p.Name

		if !meta.IsStatusConditionTrue(p.Status.Conditions, "Ready") {
			klog.V(2).Infof("Policy %s is not Ready, skipping", key)
			continue
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

		klog.Infof("Policy %s compiled", key)
		newPolicies[key] = cached
	}

	c.mu.Lock()
	c.policies = newPolicies
	c.mu.Unlock()

	klog.V(2).Infof("Policy cache refreshed: %d policies loaded", len(newPolicies))
	return nil
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
