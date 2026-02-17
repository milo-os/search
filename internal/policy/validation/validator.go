package validation

import (
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.miloapis.net/search/internal/cel"
	"go.miloapis.net/search/internal/jsonpath"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// ValidateResourceIndexPolicy validates a ResourceIndexPolicy.
// It checks:
// 1. CEL expressions in conditions
// 2. JSONPath syntax in fields
func ValidateResourceIndexPolicy(policy *searchv1alpha1.ResourceIndexPolicy, celValidator *cel.Validator) field.ErrorList {
	var allErrs field.ErrorList

	// Validate CEL expressions in conditions
	for i, condition := range policy.Spec.Conditions {
		errs := celValidator.Validate(condition.Expression)
		for _, err := range errs {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "conditions").Index(i).Child("expression"),
				condition.Expression,
				err,
			))
		}
	}

	// Validate JSONPath in fields
	for i, fieldPolicy := range policy.Spec.Fields {
		if err := jsonpath.ValidatePath(fieldPolicy.Path); err != "" {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "fields").Index(i).Child("path"),
				fieldPolicy.Path,
				err,
			))
		}
	}

	// Validate repeated field names
	seenFieldPaths := make(map[string]bool)
	for i, fieldPolicy := range policy.Spec.Fields {
		if seenFieldPaths[fieldPolicy.Path] {
			allErrs = append(allErrs, field.Duplicate(field.NewPath("spec", "fields").Index(i).Child("path"), fieldPolicy.Path))
		}
		seenFieldPaths[fieldPolicy.Path] = true
	}

	// Validate repeated condition names
	seenConditionNames := make(map[string]bool)
	for i, condition := range policy.Spec.Conditions {
		if seenConditionNames[condition.Name] {
			allErrs = append(allErrs, field.Duplicate(field.NewPath("spec", "conditions").Index(i).Child("name"), condition.Name))
		}
		seenConditionNames[condition.Name] = true
	}

	return allErrs
}
