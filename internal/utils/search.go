package utils

import (
	"strings"

	policyv1alpha1 "go.miloapis.net/search/pkg/apis/policy/v1alpha1"
)

func GetSearchIndex(tg policyv1alpha1.TargetResource) string {
	res := tg.Group + "_" + tg.Version + "_" + tg.Kind
	return strings.ReplaceAll(res, ".", "-")
}
