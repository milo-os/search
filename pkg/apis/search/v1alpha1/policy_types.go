// +k8s:openapi-gen=true
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceIndexPolicy defines a policy for indexing resources.
type ResourceIndexPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   ResourceIndexPolicySpec   `json:"spec"`
	Status ResourceIndexPolicyStatus `json:"status,omitempty"`
}

// ResourceIndexPolicySpec serves as the specification for a ResourceIndexPolicy.
// +kubebuilder:validation:XValidation:rule="self.conditions.all(c, self.conditions.exists_one(x, x.name == c.name))",message="condition names must be unique"
// +kubebuilder:validation:XValidation:rule="self.fields.all(f, self.fields.exists_one(x, x.path == f.path))",message="field paths must be unique"
type ResourceIndexPolicySpec struct {
	// TargetResource identifies the resource type this policy applies to.
	// +kubebuilder:validation:Required
	TargetResource TargetResource `json:"targetResource"`

	// Conditions filter which resources are indexed using CEL expressions.
	// When no conditions are specified, all resources of the target type are indexed.
	// Multiple conditions can be specified and are evaluated with OR semantics - a
	// resource is indexed if it satisfies ANY condition. Use && within a
	// single expression to require multiple criteria together.
	//
	// Each condition has:
	// - name: A unique identifier for the condition, used in status reporting
	//   and debugging to identify which condition(s) matched a resource.
	// - expression: A CEL expression that must evaluate to a boolean. The
	//   resource is available as the root object in the expression context.
	//
	// Available CEL operations:
	// - Field access: spec.replicas, metadata.name, status.phase
	// - Map access: metadata.labels["app"], metadata.annotations["key"]
	// - Comparisons: ==, !=, <, <=, >, >=
	// - Logical operators: &&, ||, !
	// - String functions: contains(), startsWith(), endsWith(), matches()
	// - List functions: exists(), all(), size(), map(), filter()
	// - Membership: "value" in list, "key" in map
	// - Ternary: condition ? trueValue : falseValue
	// +kubebuilder:validation:MaxItems=10
	// +listType=map
	// +listMapKey=name
	Conditions []PolicyCondition `json:"conditions,omitempty"`

	// Fields defines which fields from the resource are searchable.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=10
	// +listType=map
	// +listMapKey=path
	Fields []FieldPolicy `json:"fields"`
}

// TargetResource identifies a specific Kubernetes resource type.
// Uses a versioned reference since field paths may differ between API versions.
type TargetResource struct {
	// Group is the API group of the resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Group string `json:"group"`

	// Version is the API version of the resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`

	// Kind is the kind of the resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
}

// PolicyCondition defines a CEL condition for filtering resources.
type PolicyCondition struct {
	// Name is a unique identifier for the condition.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Name string `json:"name"`

	// Expression is a CEL expression that must evaluate to a boolean.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Expression string `json:"expression"`
}

// FieldPolicy defines how a resource field behaves in search operations.
type FieldPolicy struct {
	// Path is the JSONPath to the field value.
	// Supports nested paths and map key access using bracket notation.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Path string `json:"path"`

	// Searchable indicates if the field should be included in full-text search.
	// +kubebuilder:default=false
	// +optional
	Searchable bool `json:"searchable,omitempty"`
}

// ResourceIndexPolicyStatus serves as the status for a ResourceIndexPolicy.
type ResourceIndexPolicyStatus struct {
	// Conditions represents the latest available observations of the policy's state.
	// +kubebuilder:default={{type: "Ready", status: "Unknown", reason: "Unknown", message: "Waiting for control plane to reconcile", lastTransitionTime: "1970-01-01T00:00:00Z"}}
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// IndexName is the name of the search index created for this policy.
	// +optional
	IndexName string `json:"indexName,omitempty"`

	// CurrentGeneration is the most recent generation of the policy that the
	// controller has successfully reconciled and triggered re-indexing for.
	// Re-indexing is triggered whenever generation != CurrentGeneration, which
	// covers both the first reconciliation and any subsequent spec changes.
	// +optional
	CurrentGeneration string `json:"currentGeneration,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceIndexPolicyList is a list of ResourceIndexPolicy objects.
type ResourceIndexPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ResourceIndexPolicy `json:"items"`
}
