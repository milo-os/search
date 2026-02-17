package resourceindexpolicy

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// NewREST returns a RESTStorage object for ResourceIndexPolicy and its status subresource.
func NewREST(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*ResourceREST, error) {
	strategy := NewStrategy(scheme)
	statusStrategy := statusStrategy{strategy}

	store := &registry.Store{
		NewFunc:                   func() runtime.Object { return &searchv1alpha1.ResourceIndexPolicy{} },
		NewListFunc:               func() runtime.Object { return &searchv1alpha1.ResourceIndexPolicyList{} },
		DefaultQualifiedResource:  searchv1alpha1.Resource("resourceindexpolicies"),
		SingularQualifiedResource: searchv1alpha1.Resource("resourceindexpolicy"),

		CreateStrategy: strategy,
		UpdateStrategy: strategy,
		DeleteStrategy: strategy,

		TableConvertor: rest.NewDefaultTableConvertor(searchv1alpha1.Resource("resourceindexpolicies")),
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = statusStrategy

	return &ResourceREST{
		Store:  store,
		Status: &StatusREST{Store: &statusStore},
	}, nil
}

// ResourceREST implements the REST storage for ResourceIndexPolicy.
type ResourceREST struct {
	*registry.Store
	Status *StatusREST
}

// StatusREST implements the REST storage for the status subresource.
type StatusREST struct {
	*registry.Store
}
