package evaluation

import (
	"strings"

	"github.com/google/cel-go/cel"
	"go.miloapis.net/search/pkg/apis/search/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// EvalResult holds the evaluation outcome for a matched resource.
type EvalResult struct {
	// Matched is true if the resource satisfied any condition.
	Matched bool
	// Object is the full source object from the matched resource (u.Object). It
	// is populated only when Matched is true. Shared reference; Transform()
	// deep-copies before any mutation.
	Object map[string]any
	// Group is the API group of the matching policy.
	Group string
	// Version is the API version of the matching policy.
	Version string
	// Kind is the kind of the matching policy.
	Kind string
	// Tenant is the tenant name for the indexed document (e.g. "platform" or a project name).
	// When empty, Transform() defaults to "platform".
	Tenant string
	// TenantType is the tenant type for the indexed document (e.g. "platform" or "project").
	// When empty, Transform() defaults to "platform".
	TenantType string
}

// PolicyEvaluator evaluates whether a Kubernetes resource matches a policy
// and extracts the configured fields.
type PolicyEvaluator interface {
	Evaluate(u *unstructured.Unstructured) (*EvalResult, error)
}

// CachedPolicy represents a compiled policy ready for evaluation.
type CachedPolicy struct {
	Policy     *v1alpha1.ResourceIndexPolicy
	Conditions map[string]cel.Program
}

// Evaluate checks if the resource matches the policy's target GVK and conditions.
// When matched, the full source object is stored on the result for use by Transform().
func (cp *CachedPolicy) Evaluate(u *unstructured.Unstructured) (*EvalResult, error) {
	result := &EvalResult{
		Group:   cp.Policy.Spec.TargetResource.Group,
		Version: cp.Policy.Spec.TargetResource.Version,
		Kind:    cp.Policy.Spec.TargetResource.Kind,
	}

	// 1. Check GVK match
	gvk := u.GroupVersionKind()
	target := cp.Policy.Spec.TargetResource
	if gvk.Group != target.Group || gvk.Version != target.Version || gvk.Kind != target.Kind {
		return result, nil
	}

	// 2. Build CEL activation from the resource
	activation := map[string]any{}
	if val, ok := u.Object["metadata"]; ok {
		activation["metadata"] = val
	}
	if val, ok := u.Object["spec"]; ok {
		activation["spec"] = val
	}

	if val, ok := u.Object["status"]; ok {
		activation["status"] = val
	}

	// 3. Evaluate conditions (OR semantics). If no conditions are configured,
	// the resource matches unconditionally.
	if len(cp.Conditions) == 0 {
		result.Matched = true
	}
	for name, prg := range cp.Conditions {
		out, _, err := prg.Eval(activation)
		if err != nil {
			klog.Errorf("Policy %s condition %q evaluation error: %v", cp.Policy.Name, name, err)
			continue
		}
		if val, ok := out.Value().(bool); ok && val {
			result.Matched = true
			break
		}
	}

	if !result.Matched {
		return result, nil
	}

	// 4. Store the full source object so Transform() can build the complete document.
	result.Object = u.Object

	return result, nil
}

// Transform converts the evaluation result into a document for indexing.
// It deep-copies the full source object, strips managedFields, and overlays
// policy-derived metadata (apiVersion, kind, _tenant, _tenant_type).
func (r *EvalResult) Transform() map[string]any {
	// Deep-copy so we never mutate the shared source object.
	doc := runtime.DeepCopyJSON(r.Object)
	if doc == nil {
		doc = make(map[string]any)
	}

	// Strip metadata noise in a single type assertion.
	if meta, ok := doc["metadata"].(map[string]any); ok {
		// managedFields is verbose tracking data that is not useful for search.
		delete(meta, "managedFields")

		// Strip the last-applied-configuration annotation universally. This annotation
		// can mirror unredacted Secret payloads when objects are managed via kubectl apply,
		// making it the largest single field on most objects and a potential data-leak vector.
		if annotations, ok := meta["annotations"].(map[string]any); ok {
			delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
			if len(annotations) == 0 {
				delete(meta, "annotations")
			}
		}
	}

	// Strip Secret fields that contain sensitive data
	if r.Group == "" && r.Version == "v1" && r.Kind == "Secret" {
		delete(doc, "data")
		delete(doc, "stringData")
	}

	// Overlay policy GVK metadata — these unconditionally win over any values
	// the source object may have carried for the same keys.
	if r.Group != "" {
		doc["apiVersion"] = r.Group + "/" + r.Version
	} else {
		doc["apiVersion"] = r.Version
	}
	doc["kind"] = r.Kind

	// Add tenant metadata fields. Default to "platform" when not set so that
	// single-tenant deployments produce consistent filterable attribute values.
	tenant := r.Tenant
	if tenant == "" {
		tenant = "platform"
	}
	tenantType := r.TenantType
	if tenantType == "" {
		tenantType = "platform"
	}
	doc["_tenant"] = tenant
	doc["_tenant_type"] = tenantType

	return doc
}

// ParsePath converts ".spec.firstName" or ".metadata.labels['department']"
// into []string{"spec", "firstName"} or []string{"metadata", "labels", "department"}.
func ParsePath(path string) []string {
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil
	}

	var segments []string
	for path != "" {
		if strings.HasPrefix(path, "[") {
			end := strings.Index(path, "]")
			if end == -1 {
				return nil
			}
			key := strings.Trim(path[1:end], "\"'")
			segments = append(segments, key)
			path = strings.TrimPrefix(path[end+1:], ".")
		} else {
			// Find next dot or bracket
			nextDot := strings.Index(path, ".")
			nextBracket := strings.Index(path, "[")

			var cutAt int
			switch {
			case nextDot == -1 && nextBracket == -1:
				segments = append(segments, path)
				return segments
			case nextDot == -1:
				cutAt = nextBracket
			case nextBracket == -1:
				cutAt = nextDot
			case nextDot < nextBracket:
				cutAt = nextDot
			default:
				cutAt = nextBracket
			}

			segments = append(segments, path[:cutAt])
			path = strings.TrimPrefix(path[cutAt:], ".")
		}
	}
	return segments
}
