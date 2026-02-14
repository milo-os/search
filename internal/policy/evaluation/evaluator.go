package evaluation

import (
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"go.miloapis.net/search/pkg/apis/policy/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

// EvalResult holds the evaluation outcome for a matched resource.
type EvalResult struct {
	// Matched is true if the resource satisfied any condition.
	Matched bool
	// Fields contains the extracted field values from the resource, keyed by path.
	Fields map[string]any
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

// Evaluate checks if the resource matches the policy's target GVK, conditions,
// and extracts the configured fields from matching resources.
func (cp *CachedPolicy) Evaluate(u *unstructured.Unstructured) (*EvalResult, error) {
	result := &EvalResult{Fields: map[string]any{}}

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

	// 3. Evaluate conditions (OR semantics)
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

	// 4. Extract fields from the matched resource using the path segments
	// 4. Extract fields from the matched resource using the path segments
	for _, field := range cp.Policy.Spec.Fields {
		segments := parsePath(field.Path)
		if len(segments) == 0 {
			continue
		}

		var current any = u.Object
		found := true
		for _, key := range segments {
			if m, ok := current.(map[string]any); ok {
				if val, exists := m[key]; exists {
					current = val
					continue
				}
			}
			// Handle array index access (e.g. "0" for [0])
			if list, ok := current.([]any); ok {
				if idx, err := strconv.Atoi(key); err == nil {
					if idx >= 0 && idx < len(list) {
						current = list[idx]
						continue
					}
				}
			}

			found = false
			break
		}

		if found {
			result.Fields[field.Path] = current
		}
	}

	return result, nil
}

// parsePath converts ".spec.firstName" or ".metadata.labels['department']"
// into []string{"spec", "firstName"} or []string{"metadata", "labels", "department"}.
func parsePath(path string) []string {
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
