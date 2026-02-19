package resourceindexpolicy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"

	"go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/internal/policy/validation"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// ensure strategy implements RESTCreateStrategy and RESTUpdateStrategy
var _ rest.RESTCreateStrategy = &strategy{}
var _ rest.RESTUpdateStrategy = &strategy{}
var _ rest.RESTDeleteStrategy = &strategy{}
var _ rest.RESTUpdateStrategy = &statusStrategy{}

type strategy struct {
	runtime.ObjectTyper
	names.NameGenerator
	celValidator *cel.Validator
	lister       rest.Lister
}

type statusStrategy struct {
	*strategy
}

var _ rest.RESTUpdateStrategy = statusStrategy{}

func (statusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPolicy := obj.(*searchv1alpha1.ResourceIndexPolicy)
	oldPolicy := old.(*searchv1alpha1.ResourceIndexPolicy)
	newPolicy.Spec = oldPolicy.Spec
}

func (s statusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return s.validate(ctx, obj)
}

// NewStrategy creates a new strategy for ResourceIndexPolicy
func NewStrategy(typer runtime.ObjectTyper) *strategy {
	// Initialize CEL validator with max depth 50
	v, err := cel.NewValidator(50)
	if err != nil {
		// Panic is acceptable here as this happens during server startup
		panic(fmt.Errorf("failed to initialize CEL validator: %w", err))
	}

	return &strategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
		celValidator:  v,
	}
}

func (s *strategy) SetLister(l rest.Lister) {
	s.lister = l
}

func (strategy) NamespaceScoped() bool {
	return false // Cluster-scoped
}

func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	// No modification needed
}

func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	// No modification needed
}

func (s strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return s.validate(ctx, obj)
}

func (strategy) AllowCreateOnUpdate() bool {
	return false
}

func (strategy) AllowUnconditionalUpdate() bool {
	return false
}

func (strategy) Canonicalize(obj runtime.Object) {
}

func (s strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return s.validate(ctx, obj)
}

func (s strategy) validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	policy, ok := obj.(*searchv1alpha1.ResourceIndexPolicy)
	if !ok {
		return field.ErrorList{field.InternalError(field.NewPath(""), fmt.Errorf("expected ResourceIndexPolicy, got %T", obj))}
	}

	var otherPolicies []*searchv1alpha1.ResourceIndexPolicy
	if s.lister != nil {
		listObj, err := s.lister.List(ctx, nil)
		if err == nil {
			if list, ok := listObj.(*searchv1alpha1.ResourceIndexPolicyList); ok {
				for i := range list.Items {
					otherPolicies = append(otherPolicies, &list.Items[i])
				}
			}
		}
	}

	return validation.ValidateResourceIndexPolicy(policy, otherPolicies, s.celValidator)
}

func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
