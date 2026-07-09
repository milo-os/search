package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

func newKindsCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kinds",
		Short: "List what is searchable right now",
		Long: `List every kind that is searchable, with its group, version, and readiness.

This is a consumer view assembled from the platform's index policies: a kind is
searchable once its index is Ready. When a kind you want is missing or not
ready, that is a job for the platform operators who own indexing — use -o wide
(or --verbose) to name the backing policy and index for a precise conversation.`,
		Example: `  # What can I search?
  datumctl search kinds

  # Include the backing policy and index name
  datumctl search kinds -o wide`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			policies, err := a.policies(ctx)
			if err != nil {
				return policyError(err, false)
			}

			switch a.opts.output {
			case outputJSON:
				return encodeJSON(a.io.Out, indexPolicyList(policies))
			case outputYAML:
				return encodeYAML(a.io.Out, indexPolicyList(policies))
			case outputName:
				for _, k := range indexedKinds(policies) {
					_, _ = fmt.Fprintln(a.io.Out, kindRef(k))
				}
				return nil
			}
			return a.renderKindsTable(indexedKinds(policies))
		},
	}
	return cmd
}

// kindRef renders a kind as a --kind-ready reference: "Kind.group" (or just
// "Kind" for the core group).
func kindRef(k indexedKind) string {
	if k.Group == "" {
		return k.Kind
	}
	return k.Kind + "." + k.Group
}

// indexPolicyList reconstructs a typed, GVK-stamped list for -o json|yaml so the
// output is the real search.miloapis.com/v1alpha1 resource.
func indexPolicyList(policies []searchv1alpha1.ResourceIndexPolicy) *searchv1alpha1.ResourceIndexPolicyList {
	list := &searchv1alpha1.ResourceIndexPolicyList{Items: make([]searchv1alpha1.ResourceIndexPolicy, len(policies))}
	copy(list.Items, policies)
	for i := range list.Items {
		list.Items[i].APIVersion = searchAPIGroupVersion
		list.Items[i].Kind = "ResourceIndexPolicy"
	}
	setIndexPolicyListGVK(list)
	return list
}

func (a *app) renderKindsTable(kinds []indexedKind) error {
	if len(kinds) == 0 {
		if !a.opts.quiet {
			_, _ = fmt.Fprintln(a.io.ErrOut, "Nothing is searchable yet. An admin needs to set up indexing before search can return anything.")
		}
		return nil
	}
	wide := a.opts.output == outputWide
	var headers []string
	if wide {
		headers = []string{"KIND", "GROUP", "VERSION", "READY", "POLICY", "INDEX", "AGE"}
	} else {
		headers = []string{"KIND", "GROUP", "VERSION", "READY", "AGE"}
	}
	t := newTable(a.io.Out, headers)
	for _, k := range kinds {
		ready := k.ReadyText
		if a.color.enabled {
			if k.Ready {
				ready = colorize(ready, colorGreen)
			} else {
				ready = colorize(ready, colorYellow)
			}
		}
		age := humanDuration(k.Created)
		if wide {
			t.row(k.Kind, k.groupDisplay(), k.Version, ready, orDash(k.Policy), orDash(k.IndexName), age)
		} else {
			t.row(k.Kind, k.groupDisplay(), k.Version, ready, age)
		}
	}
	return t.flush()
}
