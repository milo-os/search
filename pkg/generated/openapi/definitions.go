package openapi

import (
	common "k8s.io/kube-openapi/pkg/common"
	spec "k8s.io/kube-openapi/pkg/validation/spec"
)

// GetOpenAPIDefinitionsWithUnstructured wraps the generated GetOpenAPIDefinitions
// and adds the missing definition for unstructured.Unstructured, which is a
// special Kubernetes type that does not carry +k8s:openapi-gen markers in upstream.
//
// unstructured.Unstructured is an open-ended JSON object (the same wire format as
// runtime.RawExtension) so the OpenAPI schema is simply "type: object" with
// x-kubernetes-preserve-unknown-fields to allow arbitrary fields.
func GetOpenAPIDefinitionsWithUnstructured(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	defs := GetOpenAPIDefinitions(ref)
	defs["k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Unstructured represents a Kubernetes resource as an arbitrary JSON object.",
				Type:        []string{"object"},
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-preserve-unknown-fields": true,
				},
			},
		},
	}
	return defs
}
