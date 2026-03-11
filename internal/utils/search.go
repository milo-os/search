package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func GetSearchIndex(tg searchv1alpha1.TargetResource) string {
	res := tg.Group + "_" + tg.Version + "_" + tg.Kind
	return strings.ReplaceAll(res, ".", "-")
}

// ComputeSpecHash returns a SHA-256 hex digest of the policy spec.
// Used by both the controller (to detect spec changes) and the re-index
// consumer (to verify cache freshness before evaluating).
func ComputeSpecHash(spec *searchv1alpha1.ResourceIndexPolicySpec) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
