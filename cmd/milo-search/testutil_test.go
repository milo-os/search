package main

import (
	"bytes"
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// fakeClient is an in-memory searchClient for command tests. Search returns the
// configured pages in order (so --all paging can be exercised); ListIndexPolicies
// returns the configured policies.
type fakeClient struct {
	pages     []*searchv1alpha1.ResourceSearchQuery
	searchErr error
	policies  []searchv1alpha1.ResourceIndexPolicy
	policyErr error

	searchCalls int
	sent        []*searchv1alpha1.ResourceSearchQuery
}

func (f *fakeClient) Search(_ context.Context, q *searchv1alpha1.ResourceSearchQuery) (*searchv1alpha1.ResourceSearchQuery, error) {
	f.sent = append(f.sent, q.DeepCopy())
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	idx := f.searchCalls
	f.searchCalls++
	if idx < len(f.pages) {
		return f.pages[idx], nil
	}
	return &searchv1alpha1.ResourceSearchQuery{}, nil
}

func (f *fakeClient) ListIndexPolicies(_ context.Context) (*searchv1alpha1.ResourceIndexPolicyList, error) {
	if f.policyErr != nil {
		return nil, f.policyErr
	}
	return &searchv1alpha1.ResourceIndexPolicyList{Items: f.policies}, nil
}

// newTestApp wires an app with buffer-backed streams and an injected client so
// command behavior can be asserted without a real cluster or TTY.
func newTestApp(cs searchClient, opts *globalOptions) (*app, *bytes.Buffer, *bytes.Buffer) {
	if opts == nil {
		opts = &globalOptions{output: outputTable, color: "never"}
	}
	if opts.output == "" {
		opts.output = outputTable
	}
	if opts.color == "" {
		opts.color = "never"
	}
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	a := &app{
		io:   IOStreams{In: strings.NewReader(""), Out: out, ErrOut: errOut},
		opts: opts,
	}
	a.clientFactory = func() (searchClient, error) { return cs, nil }
	a.resolveColor()
	return a, out, errOut
}

// execQuery runs the query subcommand against a fake client and returns the
// stdout/stderr buffers and the Execute error.
func execQuery(cs searchClient, opts *globalOptions, args ...string) (string, string, error) {
	a, out, errOut := newTestApp(cs, opts)
	cmd := newQueryCommand(a)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errOut.String(), err
}

// execKinds runs the kinds subcommand against a fake client.
func execKinds(cs searchClient, opts *globalOptions, args ...string) (string, string, error) {
	a, out, errOut := newTestApp(cs, opts)
	cmd := newKindsCommand(a)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errOut.String(), err
}

// mkResult builds a SearchResult with an unstructured resource payload.
func mkResult(apiVersion, kind, name, namespace, tenant, tenantType string, score float64) searchv1alpha1.SearchResult {
	meta := map[string]interface{}{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	obj := map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   meta,
	}
	return searchv1alpha1.SearchResult{
		Resource:       unstructured.Unstructured{Object: obj},
		RelevanceScore: score,
		Tenant:         searchv1alpha1.TenantInfo{Name: tenant, Type: tenantType},
	}
}

// mkPage builds a ResourceSearchQuery response with the given status.
func mkPage(results []searchv1alpha1.SearchResult, denied []searchv1alpha1.TargetResource, cont string) *searchv1alpha1.ResourceSearchQuery {
	return &searchv1alpha1.ResourceSearchQuery{
		Status: searchv1alpha1.ResourceSearchQueryStatus{
			Results:               results,
			DeniedTargetResources: denied,
			Continue:              cont,
		},
	}
}

// mkPolicy builds a ResourceIndexPolicy with a Ready condition.
func mkPolicy(kind, group, version string, ready bool) searchv1alpha1.ResourceIndexPolicy {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	return searchv1alpha1.ResourceIndexPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(kind) + "-policy"},
		Spec: searchv1alpha1.ResourceIndexPolicySpec{
			TargetResource: searchv1alpha1.TargetResource{Group: group, Version: version, Kind: kind},
		},
		Status: searchv1alpha1.ResourceIndexPolicyStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: status, Reason: "T", Message: "m"}},
			IndexName:  "idx-" + strings.ToLower(kind),
		},
	}
}
