package apiserver

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	typedauthzv1 "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/klog/v2"

	"go.miloapis.net/search/internal/indexer"
	_ "go.miloapis.net/search/internal/metrics"
	"go.miloapis.net/search/internal/registry/policy/resourceindexpolicy"
	"go.miloapis.net/search/internal/registry/resourcesearchquery"
	"go.miloapis.net/search/pkg/apis/search/install"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"go.miloapis.net/search/pkg/meilisearch"
)

var (
	// Scheme defines the runtime type system for API object serialization.
	Scheme = runtime.NewScheme()
	// Codecs provides serializers for API objects.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	install.Install(Scheme)

	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// Register the types as internal as well to support watches and other internal operations
	// that expect an internal version.
	Scheme.AddKnownTypes(schema.GroupVersion{Group: searchv1alpha1.GroupName, Version: runtime.APIVersionInternal},
		&searchv1alpha1.ResourceIndexPolicy{},
		&searchv1alpha1.ResourceIndexPolicyList{},
		&searchv1alpha1.ResourceSearchQuery{},
		&searchv1alpha1.ResourceSearchQueryList{},
	)

	// Register unversioned meta types required by the API machinery.
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// ExtraConfig extends the generic apiserver configuration with search-specific settings.
type ExtraConfig struct {
	MeiliClient *meilisearch.SDKClient
	PolicyCache *indexer.PolicyCache
	// CRDCache is the raw controller-runtime cache for CustomResourceDefinitions.
	// It is used only for lifecycle management (RegisterHandlers, Start,
	// WaitForCacheSync) in the post-start hook. Query routing uses PluralLookup.
	CRDCache *indexer.CRDPluralCache
	// PluralLookup resolves (group, kind) to plural resource names for SAR
	// authorization. Backed by FallbackPluralLookup which tries the CRD cache
	// first and falls back to the REST mapper for aggregated API server types.
	PluralLookup       *indexer.FallbackPluralLookup
	SARClient          typedauthzv1.SubjectAccessReviewInterface
	MaxSearchLimit     int
	DefaultSearchLimit int
	PagingSecret       []byte
	PagingTimeout      time.Duration
}

// Config combines generic and search-specific configuration.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// SearchServer is the search aggregated apiserver.
type SearchServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig prevents incomplete configuration from being used.
// Embeds a private pointer that can only be created via Complete().
type CompletedConfig struct {
	*completedConfig
}

// Complete validates and fills default values for the configuration.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	return CompletedConfig{&c}
}

// New creates and initializes the SearchServer with storage and API groups.
func (c completedConfig) New() (*SearchServer, error) {
	genericServer, err := c.GenericConfig.New("search-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	s := &SearchServer{
		GenericAPIServer: genericServer,
	}

	// Install 'search' API group
	searchAPIGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(searchv1alpha1.GroupName, Scheme, metav1.ParameterCodec, Codecs)
	searchV1alpha1Storage := map[string]rest.Storage{}

	// Add policy resources
	policyStorage, err := resourceindexpolicy.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	searchV1alpha1Storage["resourceindexpolicies"] = policyStorage.Store
	searchV1alpha1Storage["resourceindexpolicies/status"] = policyStorage.Status

	// Add resourcesearchquery resources
	resourcesearchqueryStorage := resourcesearchquery.NewREST(
		c.ExtraConfig.MeiliClient,
		c.ExtraConfig.PolicyCache,
		c.ExtraConfig.PluralLookup,
		c.ExtraConfig.SARClient,
		c.ExtraConfig.MaxSearchLimit,
		c.ExtraConfig.DefaultSearchLimit,
		c.ExtraConfig.PagingSecret,
		c.ExtraConfig.PagingTimeout,
	)
	searchV1alpha1Storage["resourcesearchqueries"] = resourcesearchqueryStorage

	searchAPIGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = searchV1alpha1Storage

	if err := s.GenericAPIServer.InstallAPIGroup(&searchAPIGroupInfo); err != nil {
		return nil, err
	}

	// Add PostStartHook to start policy cache
	s.GenericAPIServer.AddPostStartHookOrDie("search-policy-cache", func(ctx genericapiserver.PostStartHookContext) error {
		if err := c.ExtraConfig.PolicyCache.RegisterHandlers(ctx.Context); err != nil {
			return fmt.Errorf("failed to register policy cache handlers: %w", err)
		}

		go func() {
			klog.Info("Starting Search policy cache...")
			if err := c.ExtraConfig.PolicyCache.Start(ctx.Context); err != nil {
				klog.Errorf("Policy cache stopped with error: %v", err)
			}
		}()

		if !c.ExtraConfig.PolicyCache.WaitForCacheSync(ctx.Context) {
			return fmt.Errorf("failed to wait for policy cache sync")
		}
		return nil
	})

	// Start the CRD plural cache. This is a SEPARATE controller-runtime cache
	// from the policy cache — CRDs live on the kube-apiserver (not on this
	// apiserver's loopback), so the CRD cache uses in-cluster config rather
	// than LoopbackClientConfig. The two caches have independent lifecycles
	// and are started/synced concurrently by their respective post-start hooks.
	s.GenericAPIServer.AddPostStartHookOrDie("search-crd-plural-cache", func(ctx genericapiserver.PostStartHookContext) error {
		if err := c.ExtraConfig.CRDCache.RegisterHandlers(ctx.Context); err != nil {
			return fmt.Errorf("failed to register CRD plural cache handlers: %w", err)
		}

		go func() {
			klog.Info("Starting Search CRD plural cache...")
			if err := c.ExtraConfig.CRDCache.Start(ctx.Context); err != nil {
				klog.Errorf("CRD plural cache stopped with error: %v", err)
			}
		}()

		if !c.ExtraConfig.CRDCache.WaitForCacheSync(ctx.Context) {
			return fmt.Errorf("failed to wait for CRD plural cache sync")
		}
		return nil
	})

	klog.Info("Search server initialized successfully")

	return s, nil
}

// Run starts the server.
func (s *SearchServer) Run(ctx context.Context) error {
	return s.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
