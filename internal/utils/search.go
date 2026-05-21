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

// TODO: remove reindex seed in flavor of a API for forcing reindexing
// reindexSeed is mixed into every spec hash. Bump to force a full re-index of all policies.
const reindexSeed = "2026-05-19"

// ComputeSpecHash returns a seeded SHA-256 hex digest of the policy spec.
// Used by both the controller (to detect spec changes) and the re-index
// consumer (to verify cache freshness before evaluating).
// Bump reindexSeed to force a full re-index of all policies.
func ComputeSpecHash(spec *searchv1alpha1.ResourceIndexPolicySpec) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	h := sha256.New()
	h.Write(b)
	h.Write([]byte(reindexSeed))
	return hex.EncodeToString(h.Sum(nil))
}
