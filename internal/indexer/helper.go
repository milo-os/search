package indexer

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/klog/v2"
)

// resolveUID attempts to resolve the unique identifier (UID) for the resource
// associated with an audit event. It employs a multi-step strategy:
//
//  1. **ResponseObject Metadata:** Checks `metadata.uid` within the `ResponseObject`.
//     This is reliable when the full resource is returned (e.g., standard `create`, `get`,
//     and some `delete` responses).
//  2. **ObjectRef UID:** Checks `ObjectRef.UID` from the audit event itself. This is
//     often populated by the API server but can be missing in some edge cases or early in the request.
//  3. **Status Object Details:** Checks `details.uid` within the `ResponseObject` if it
//     appears to be a `Status` kind (common for `delete` responses where the resource is already gone).
//
// Returns the resolved UID string, or an empty string if resolution fails.
func resolveUID(event *auditEvent) string {
	// 1. Try metadata.uid from the response object if available.
	// This covers standard resources returned in the response.
	if event.ResponseObject != nil {
		u := &unstructured.Unstructured{Object: event.ResponseObject}
		if uid := string(u.GetUID()); uid != "" {
			return uid
		}
	}

	// 2. Fallback to audit event ObjectRef UID.
	// This is the standard "pointer" to the object in the audit log.
	if event.ObjectRef.UID != "" {
		return event.ObjectRef.UID
	}

	// 3. Try Status object details (common in Delete responses).
	// If the response is a Status object, the UID might be in details.
	if event.ResponseObject != nil {
		if details, ok := event.ResponseObject["details"].(map[string]any); ok {
			if uid, ok := details["uid"].(string); ok {
				return uid
			}
		}
	}

	return ""
}

// logMissingUIDDetails logs useful debugging information when a UID cannot be resolved.
func logMissingUIDDetails(event *auditEvent) {
	var keys []string
	if event.ResponseObject != nil {
		for k := range event.ResponseObject {
			keys = append(keys, k)
		}
	}
	// Try to serialize a snippet of the response object for better context if it's small enough,
	// otherwise just keys.
	snippet := fmt.Sprintf("ResponseObject keys: %v", keys)

	// If ResponseObject is a Status, log it more clearly
	if event.ResponseObject != nil {
		if kind, ok := event.ResponseObject["kind"].(string); ok && kind == "Status" {
			snippet = fmt.Sprintf("ResponseObject is Status: %+v", event.ResponseObject)
		}
	} else {
		snippet = "ResponseObject is nil"
	}

	klog.Warningf("Could not resolve UID for %s %s/%s (auditID: %s). Details: %s. Skipping.",
		event.Verb, event.ObjectRef.Resource, event.ObjectRef.Name, event.AuditID, snippet)
}

// ensureUID ensures the document has a UID, setting it to resourceUID if missing.
func ensureUID(doc map[string]any, resourceUID string) {
	if _, ok := doc["uid"]; !ok {
		doc["uid"] = resourceUID
	}
}
