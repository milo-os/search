package indexer

import (
	"context"
	"fmt"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"
)

// CRDPluralCache resolves a (group, kind) pair to the plural resource name
// declared on the matching CustomResourceDefinition's spec.names.plural.
//
// It watches CustomResourceDefinitions through the shared controller-runtime
// cache and keeps an in-memory map fresh in response to CRD events.
//
// Lifecycle mirrors PolicyCache: construct, RegisterHandlers, Start, then
// WaitForCacheSync. The apiserver post-start hook orchestrates all three.
// Consumers must not call Lookup before WaitForCacheSync returns true;
// before that point the map is empty and every lookup will miss.
type CRDPluralCache struct {
	mu      sync.RWMutex
	plurals map[schema.GroupKind]string

	cache runtimecache.Cache
}

// NewCRDPluralCache constructs the cache. Handler registration and informer
// startup are deferred to RegisterHandlers/Start so callers can orchestrate
// them via the genericapiserver post-start hook.
func NewCRDPluralCache(c runtimecache.Cache) *CRDPluralCache {
	cache := &CRDPluralCache{
		plurals: make(map[schema.GroupKind]string),
		cache:   c,
	}
	klog.Info("Created CRD plural cache")
	return cache
}

// RegisterHandlers attaches Add/Update/Delete handlers to the CRD informer.
func (c *CRDPluralCache) RegisterHandlers(ctx context.Context) error {
	informer, err := c.cache.GetInformer(ctx, &apiextensionsv1.CustomResourceDefinition{})
	if err != nil {
		return fmt.Errorf("getting CRD informer: %w", err)
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    c.onAddOrUpdate,
		UpdateFunc: func(_, newObj any) { c.onAddOrUpdate(newObj) },
		DeleteFunc: c.onDelete,
	}); err != nil {
		return fmt.Errorf("adding CRD event handler: %w", err)
	}
	klog.Info("Registered CRD plural cache informer handlers")
	return nil
}

// Start delegates to the underlying controller-runtime cache. Idempotent
// across multiple cache consumers sharing the same runtimecache.Cache.
func (c *CRDPluralCache) Start(ctx context.Context) error {
	return c.cache.Start(ctx)
}

// WaitForCacheSync blocks until the CRD informer has populated. Returns false
// if the context is cancelled before sync completes.
func (c *CRDPluralCache) WaitForCacheSync(ctx context.Context) bool {
	return c.cache.WaitForCacheSync(ctx)
}

// Lookup returns the plural resource name for a given group/kind, or
// ("", false) if no CRD has been observed for that pair.
func (c *CRDPluralCache) Lookup(gk schema.GroupKind) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.plurals[gk]
	return p, ok
}

func (c *CRDPluralCache) onAddOrUpdate(obj any) {
	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		klog.Errorf("CRDPluralCache: unexpected object type %T", obj)
		return
	}
	gk := schema.GroupKind{Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind}
	c.mu.Lock()
	c.plurals[gk] = crd.Spec.Names.Plural
	c.mu.Unlock()
}

func (c *CRDPluralCache) onDelete(obj any) {
	if tomb, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		klog.Errorf("CRDPluralCache: unexpected object type %T", obj)
		return
	}
	gk := schema.GroupKind{Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind}
	c.mu.Lock()
	delete(c.plurals, gk)
	c.mu.Unlock()
}
