package utils

import (
	"strings"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func GetSearchIndex(tg searchv1alpha1.TargetResource) string {
	res := tg.Group + "_" + tg.Version + "_" + tg.Kind
	return strings.ReplaceAll(res, ".", "-")
}
