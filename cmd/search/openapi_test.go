package main

import (
	"strings"
	"testing"

	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	openapiutil "k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"

	searchapiserver "go.miloapis.net/search/internal/apiserver"
	"go.miloapis.net/search/pkg/generated/openapi"
)

// TestOpenAPIGVKExtensions verifies that OpenAPI schemas include x-kubernetes-group-version-kind
// extensions required for Server-Side Apply (SSA) to work correctly.
//
// This is a regression test for the SSA failure:
// "no corresponding type for search.miloapis.com/v1alpha1, Kind=ResourceIndexPolicy"
//
// Root cause: The DefinitionNamer only returns GVK extensions when looking up names
// in REST-friendly format. The OpenAPI builder uses reflection to get Go module paths,
// so we need GetDefinitionName to transform Go module paths before DefinitionNamer lookup.
func TestOpenAPIGVKExtensions(t *testing.T) {
	namer := apiopenapi.NewDefinitionNamer(searchapiserver.Scheme)

	// Custom GetDefinitionName that transforms Go module paths to REST-friendly format
	// before looking up in DefinitionNamer. This ensures GVK extensions are returned.
	getDefinitionName := func(name string) (string, spec.Extensions) {
		if strings.Contains(name, "/") {
			name = openapiutil.ToRESTFriendlyName(name)
		}
		return namer.GetDefinitionName(name)
	}

	defs := openapi.GetOpenAPIDefinitionsWithUnstructured(func(path string) spec.Ref {
		return spec.Ref{}
	})

	testCases := []struct {
		goModulePath  string
		expectedGroup string
		expectedKind  string
	}{
		{
			goModulePath:  "go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicy",
			expectedGroup: "search.miloapis.com",
			expectedKind:  "ResourceIndexPolicy",
		},
		{
			goModulePath:  "go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicyList",
			expectedGroup: "search.miloapis.com",
			expectedKind:  "ResourceIndexPolicyList",
		},
		{
			goModulePath:  "go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceSearchQuery",
			expectedGroup: "search.miloapis.com",
			expectedKind:  "ResourceSearchQuery",
		},
		{
			goModulePath:  "go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceSearchQueryList",
			expectedGroup: "search.miloapis.com",
			expectedKind:  "ResourceSearchQueryList",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.goModulePath, func(t *testing.T) {
			// Verify the definition exists with Go module path key
			if _, ok := defs[tc.goModulePath]; !ok {
				t.Fatalf("Type %q not found in OpenAPI definitions", tc.goModulePath)
			}

			// Verify GetDefinitionName returns GVK extensions after transformation
			defName, extensions := getDefinitionName(tc.goModulePath)
			if extensions == nil {
				t.Fatalf("No extensions returned for %q (transformed to %q) - GVK extension missing! "+
					"This will cause SSA to fail with 'no corresponding type' error", tc.goModulePath, defName)
			}

			gvkExt, ok := extensions["x-kubernetes-group-version-kind"]
			if !ok {
				t.Fatalf("x-kubernetes-group-version-kind extension not found for %q", tc.goModulePath)
			}

			gvks, ok := gvkExt.([]any)
			if !ok {
				t.Fatalf("GVK extension is not an array: %T", gvkExt)
			}

			found := false
			for _, gvk := range gvks {
				gvkMap, ok := gvk.(map[string]any)
				if !ok {
					continue
				}
				if gvkMap["group"] == tc.expectedGroup &&
					gvkMap["version"] == "v1alpha1" &&
					gvkMap["kind"] == tc.expectedKind {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected GVK {group: %q, version: v1alpha1, kind: %q} not found in extensions: %v",
					tc.expectedGroup, tc.expectedKind, gvkExt)
			}
		})
	}
}

// TestUnstructuredTypeIncluded verifies that the Unstructured type is included
// in the definitions (required for SearchResult which embeds Unstructured).
func TestUnstructuredTypeIncluded(t *testing.T) {
	defs := openapi.GetOpenAPIDefinitionsWithUnstructured(func(path string) spec.Ref {
		return spec.Ref{}
	})

	unstructuredKey := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
	if _, ok := defs[unstructuredKey]; !ok {
		t.Errorf("Unstructured type %q not found in definitions", unstructuredKey)
	}
}

// TestOldGetDefinitionNameMissesGVK demonstrates that the old approach (passing
// Go module paths directly to DefinitionNamer without transformation) fails to
// return GVK extensions. This proves the fix is necessary.
func TestOldGetDefinitionNameMissesGVK(t *testing.T) {
	namer := apiopenapi.NewDefinitionNamer(searchapiserver.Scheme)

	// The OLD approach: pass the Go module path directly to namer without
	// transforming to REST-friendly format first. This was the pre-fix behavior
	// where GetDefinitionName did: namer.GetDefinitionName(name) -> ToRESTFriendlyName()
	// instead of: ToRESTFriendlyName(name) -> namer.GetDefinitionName()
	goModulePath := "go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicy"

	// DefinitionNamer's internal map uses REST-friendly names (from scheme), so
	// looking up a Go module path directly returns nil extensions
	_, extensions := namer.GetDefinitionName(goModulePath)
	if extensions != nil {
		if _, ok := extensions["x-kubernetes-group-version-kind"]; ok {
			t.Fatal("Expected the old approach to NOT return GVK extensions, but it did. " +
				"If this passes, the old code was actually fine and the fix is unnecessary.")
		}
	}

	// Now verify the fixed approach DOES return extensions
	fixedName := openapiutil.ToRESTFriendlyName(goModulePath)
	_, fixedExtensions := namer.GetDefinitionName(fixedName)
	if fixedExtensions == nil {
		t.Fatal("Fixed approach should return extensions")
	}
	if _, ok := fixedExtensions["x-kubernetes-group-version-kind"]; !ok {
		t.Fatal("Fixed approach should return x-kubernetes-group-version-kind extension")
	}
}

// TestOpenAPIV3RefConsistency verifies that $refs in OpenAPI definitions use
// REST-friendly names that match the definition names produced by GetDefinitionName.
// When these diverge, SSA's TypeConverter cannot resolve types.
func TestOpenAPIV3RefConsistency(t *testing.T) {
	namer := apiopenapi.NewDefinitionNamer(searchapiserver.Scheme)

	getDefinitionName := func(name string) (string, spec.Extensions) {
		if strings.Contains(name, "/") {
			name = openapiutil.ToRESTFriendlyName(name)
		}
		return namer.GetDefinitionName(name)
	}

	// Build definitions the same way the server should — refs use getDefinitionName
	defs := openapi.GetOpenAPIDefinitionsWithUnstructured(func(name string) spec.Ref {
		defName, _ := getDefinitionName(name)
		return spec.MustCreateRef("#/components/schemas/" + openapicommon.EscapeJsonPointer(defName))
	})

	// Check that all Search types are present with Go module path keys
	searchTypes := []string{
		"go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicy",
		"go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicyList",
		"go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceSearchQuery",
		"go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceSearchQueryList",
	}

	for _, typeName := range searchTypes {
		if _, ok := defs[typeName]; !ok {
			keys := make([]string, 0, len(defs))
			for k := range defs {
				if strings.Contains(k, "search") {
					keys = append(keys, k)
				}
			}
			t.Errorf("Type %q not found in definitions. Search-related keys: %v", typeName, keys)
		}
	}

	// Verify that $refs within definitions resolve to valid definition names.
	// Pick ResourceIndexPolicy which references ResourceIndexPolicySpec etc.
	policyDef := defs["go.miloapis.net/search/pkg/apis/search/v1alpha1.ResourceIndexPolicy"]
	for propName, prop := range policyDef.Schema.Properties {
		if prop.Ref.String() != "" {
			ref := prop.Ref.String()
			// The ref should use REST-friendly naming (com.miloapis... not go.miloapis.net/...)
			if strings.Contains(ref, "go.miloapis.net/") {
				t.Errorf("Property %q has $ref using Go module path format %q — "+
					"should use REST-friendly format for SSA compatibility", propName, ref)
			}
		}
	}
}
