package apiserver

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"

	_ "go.miloapis.net/search/internal/metrics"
	"go.miloapis.net/search/internal/registry/policy/resourceindexpolicy"
	"go.miloapis.net/search/pkg/apis/search/install"
	searchinstall "go.miloapis.net/search/pkg/apis/search/install"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

var (
	// Scheme defines the runtime type system for API object serialization.
	Scheme = runtime.NewScheme()
	// Codecs provides serializers for API objects.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	install.Install(Scheme)
	searchinstall.Install(Scheme)

	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// Register the types as internal as well to support watches and other internal operations
	// that expect an internal version.
	Scheme.AddKnownTypes(schema.GroupVersion{Group: searchv1alpha1.GroupName, Version: runtime.APIVersionInternal},
		&searchv1alpha1.ResourceIndexPolicy{},
		&searchv1alpha1.ResourceIndexPolicyList{},
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
	// Add custom configuration here as needed
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

	searchAPIGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = searchV1alpha1Storage

	if err := s.GenericAPIServer.InstallAPIGroup(&searchAPIGroupInfo); err != nil {
		return nil, err
	}

	klog.Info("Search server initialized successfully")

	return s, nil
}

// Run starts the server.
func (s *SearchServer) Run(ctx context.Context) error {
	return s.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
