package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	versioned "go.miloapis.net/search/pkg/generated/clientset/versioned"
)

// searchClient is the small surface the commands need from the search API:
// create a ResourceSearchQuery (one synchronous "ask a question" call) and list
// the index policies that back `kinds` and --kind resolution. It is an
// interface so tests can inject a fake without a real cluster.
type searchClient interface {
	// Search creates a ResourceSearchQuery and returns the server's response,
	// whose status carries results, deniedTargetResources, and continue.
	Search(ctx context.Context, q *searchv1alpha1.ResourceSearchQuery) (*searchv1alpha1.ResourceSearchQuery, error)
	// ListIndexPolicies lists every ResourceIndexPolicy (cluster-scoped).
	ListIndexPolicies(ctx context.Context) (*searchv1alpha1.ResourceIndexPolicyList, error)
}

// restSearchClient is the real searchClient backed by the generated typed
// clientset.
type restSearchClient struct {
	cs versioned.Interface
}

func (c restSearchClient) Search(ctx context.Context, q *searchv1alpha1.ResourceSearchQuery) (*searchv1alpha1.ResourceSearchQuery, error) {
	// ResourceSearchQuery is cluster-scoped and create-only; the answer arrives
	// in the create response's status.
	return c.cs.SearchV1alpha1().ResourceSearchQueries().Create(ctx, q, metav1.CreateOptions{})
}

func (c restSearchClient) ListIndexPolicies(ctx context.Context) (*searchv1alpha1.ResourceIndexPolicyList, error) {
	return c.cs.SearchV1alpha1().ResourceIndexPolicies().List(ctx, metav1.ListOptions{})
}

// globalOptions holds the flags shared by every subcommand.
type globalOptions struct {
	kubeconfig string
	namespace  string
	output     string
	quiet      bool
	color      string // auto | always | never
	verbose    bool

	// Overridable context (flags > env > config).
	org     string
	project string
}

// app is the runtime context threaded through commands. It resolves the
// transport and client lazily so that read-only, no-API paths (help,
// --plugin-manifest, completion, version) never touch credentials or the
// network.
type app struct {
	io   IOStreams
	opts *globalOptions

	// clientFactory builds the searchClient. It is a field so tests can inject a
	// fake without a real cluster.
	clientFactory func() (searchClient, error)

	// Memoized client so repeat callers within a command don't rebuild config,
	// re-fetch a token, or re-emit verbose diagnostics.
	clientResolved bool
	cachedClient   searchClient
	cachedErr      error

	// Memoized index-policy listing. Both `kinds` and --kind resolution read the
	// same bounded list; caching keeps a query that resolves kinds and echoes a
	// "kinds searched" count to a single list call.
	policiesResolved  bool
	cachedPolicies    []searchv1alpha1.ResourceIndexPolicy
	cachedPoliciesErr error

	// resolvedMode records the transport the client factory chose, so scope
	// labels can tell a platform-scoped datum call ("across the platform") from a
	// dev/e2e kubeconfig one ("on the current cluster").
	resolvedMode transportMode

	color colorState
}

// newApp wires the default, real transport-backed client factory.
func newApp(streams IOStreams, opts *globalOptions) *app {
	a := &app{io: streams, opts: opts}
	a.clientFactory = a.defaultClientFactory
	return a
}

// resolveDatum reads the datumctl-injected context and reconciles it with the
// explicit --org/--project overrides, returning the effective scope and the
// chosen transport mode.
func (a *app) resolveDatum() (datumEnv, transportMode) {
	env := readDatumEnv()
	// Effective scope: explicit --org/--project flags override the context
	// datumctl injects. The scope is carried by the control-plane URL path, so
	// keep env and opts in sync for both the transport and verbose diagnostics.
	if a.opts.org == "" {
		a.opts.org = env.org
	}
	if a.opts.project == "" {
		a.opts.project = env.project
	}
	env.org = a.opts.org
	env.project = a.opts.project
	return env, chooseMode(a.opts.kubeconfig, env)
}

func (a *app) defaultClientFactory() (searchClient, error) {
	env, mode := a.resolveDatum()
	a.resolvedMode = mode
	cfg, _, err := restConfigFor(mode, a.opts.kubeconfig, env)
	if err != nil {
		return nil, err
	}
	// --verbose: surface the resolved scope, transport, and API host on stderr,
	// and wrap the transport so every API call (method + path) is logged. stdout
	// (the json/yaml data contract) is untouched.
	if a.opts.verbose {
		a.vlogf("resolved scope: %s", a.scopeName())
		a.vlogf("transport: %s", mode)
		a.vlogf("API host: %s", cfg.Host)
		cfg.Wrap(verboseTransport(a.io.ErrOut))
	}
	cs, err := newSearchClientset(cfg)
	if err != nil {
		return nil, err
	}
	return restSearchClient{cs: cs}, nil
}

// client resolves the searchClient once, memoizing the result so repeat callers
// within a command don't rebuild config, re-fetch a token, or re-emit verbose
// diagnostics.
func (a *app) client() (searchClient, error) {
	if !a.clientResolved {
		a.cachedClient, a.cachedErr = a.clientFactory()
		a.clientResolved = true
	}
	return a.cachedClient, a.cachedErr
}

// policies lists the ResourceIndexPolicy objects once and memoizes them. It is
// shared by `kinds` and --kind resolution.
func (a *app) policies(ctx context.Context) ([]searchv1alpha1.ResourceIndexPolicy, error) {
	if a.policiesResolved {
		return a.cachedPolicies, a.cachedPoliciesErr
	}
	a.policiesResolved = true
	cs, err := a.client()
	if err != nil {
		a.cachedPoliciesErr = err
		return nil, err
	}
	list, err := cs.ListIndexPolicies(ctx)
	if err != nil {
		// Cache the raw error; callers classify it with policyError so the "you
		// need read access to index policies" message (not the searcher-role one)
		// is what surfaces, with call-site-specific remediation.
		a.cachedPoliciesErr = err
		return nil, err
	}
	a.cachedPolicies = list.Items
	return a.cachedPolicies, nil
}

// policyError classifies an error from listing index policies. A 403 here is
// about read access to index policies — a different permission than searching —
// so it must not be mistaken for the searcher-role denial. kindPath tailors the
// remediation for the --kind resolution path (where dropping --kind may work).
func policyError(err error, kindPath bool) error {
	if ce, ok := err.(*cliError); ok {
		return ce
	}
	if httpStatusCode(err) == 403 {
		return policyAccessError(kindPath)
	}
	return classifyError(err)
}

// classify maps a search-call error to a cliError, naming the resolved scope in
// the forbidden and unreachable messages.
func (a *app) classify(err error) error {
	return classifyWithScope(err, a.scopeName())
}

// resolveColor computes the color decision for this invocation against the data
// stream, the requested output format, and the --color mode.
func (a *app) resolveColor() {
	a.color = resolveColor(a.opts.color, a.io.Out, a.opts.output)
}

// scopeName returns a bare "org / project" descriptor for verbose diagnostics
// and error messages. With no org/project it names the platform (datum mode) or
// the current cluster (dev/e2e kubeconfig mode).
func (a *app) scopeName() string {
	switch {
	case a.opts.org != "" && a.opts.project != "":
		return fmt.Sprintf("%s / %s", a.opts.org, a.opts.project)
	case a.opts.project != "":
		return a.opts.project
	case a.opts.org != "":
		return a.opts.org
	case a.resolvedMode == modeDatum:
		return "the platform"
	default:
		return "the current cluster"
	}
}

// scopeLocative returns the scope as a phrase that reads after "results for X",
// e.g. "in acme / net-core", "across the platform", or "on the current cluster".
func (a *app) scopeLocative() string {
	switch {
	case a.opts.org != "" && a.opts.project != "":
		return fmt.Sprintf("in %s / %s", a.opts.org, a.opts.project)
	case a.opts.project != "":
		return "in " + a.opts.project
	case a.opts.org != "":
		return "in " + a.opts.org
	case a.resolvedMode == modeDatum:
		return "across the platform"
	default:
		return "on the current cluster"
	}
}

// vlogf writes a diagnostic line to stderr only when --verbose is set.
func (a *app) vlogf(format string, args ...any) {
	if a.opts.verbose {
		_, _ = fmt.Fprintf(a.io.ErrOut, format+"\n", args...)
	}
}
