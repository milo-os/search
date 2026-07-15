package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.datum.net/datumctl/plugin"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/transport"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	versioned "go.miloapis.net/search/pkg/generated/clientset/versioned"
)

// verboseTransport returns a transport wrapper that logs each API call's method
// and path to w. Used under --verbose so the user can see the exact calls made
// ("why did I get this result?") without polluting stdout.
func verboseTransport(w io.Writer) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{inner: rt, w: w}
	}
}

type loggingRoundTripper struct {
	inner http.RoundTripper
	w     io.Writer
}

func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := l.inner.RoundTrip(req)
	if err != nil {
		_, _ = fmt.Fprintf(l.w, "API call: %s %s -> error: %v\n", req.Method, req.URL.Path, err)
		return resp, err
	}
	_, _ = fmt.Fprintf(l.w, "API call: %s %s -> %s\n", req.Method, req.URL.Path, resp.Status)
	return resp, err
}

// transportMode names how the plugin reaches the search API.
type transportMode string

const (
	// modeDatum uses the datumctl-injected context: a short-lived bearer token
	// from the credentials helper against DATUM_API_HOST, scoped to the active
	// org/project. This is the production path.
	modeDatum transportMode = "datum"
	// modeKubeconfig uses a standard kubeconfig (KUBECONFIG / --kubeconfig /
	// in-cluster). This is what the e2e/dev path uses, since the kind cluster has
	// no Datum front door.
	modeKubeconfig transportMode = "kubeconfig"
)

// datumEnv captures the environment contract datumctl uses to dispatch to a
// plugin. The plugin never holds a long-lived credential: it asks the helper for
// a fresh token immediately before building a client.
type datumEnv struct {
	org        string
	project    string
	apiHost    string
	credHelper string
}

// readDatumEnv reads the datumctl-injected context via the plugin SDK. The SDK
// resolves the same DATUM_* variables the plugin would read by hand; routing
// through it keeps the plugin aligned with the host's contract.
func readDatumEnv() datumEnv {
	ctx := plugin.Context()
	return datumEnv{
		org:        ctx.Org,
		project:    ctx.Project,
		apiHost:    ctx.APIHost,
		credHelper: ctx.CredentialsHelper,
	}
}

// usable reports whether the Datum transport has everything it needs. We require
// both an API host and a credentials helper; org/project may be empty for
// platform-scoped callers.
func (d datumEnv) usable() bool {
	return d.apiHost != "" && d.credHelper != ""
}

// chooseMode decides the transport. An explicit --kubeconfig always forces
// kubeconfig mode (so verification against the dev cluster is unambiguous).
// Otherwise, if datumctl injected a usable context, use it; else fall back to
// kubeconfig/in-cluster.
func chooseMode(kubeconfigFlag string, env datumEnv) transportMode {
	if kubeconfigFlag != "" {
		return modeKubeconfig
	}
	if env.usable() {
		return modeDatum
	}
	return modeKubeconfig
}

// restConfigFor builds a rest.Config for the chosen mode and returns the default
// namespace. Both search resources (ResourceSearchQuery, ResourceIndexPolicy)
// are cluster-scoped, so the namespace is carried only for parity with the rest
// of datumctl and the dev/e2e escape hatch; it does not scope these calls.
func restConfigFor(mode transportMode, kubeconfigFlag string, env datumEnv) (*rest.Config, string, error) {
	switch mode {
	case modeDatum:
		return datumRestConfig(env)
	default:
		return kubeconfigRestConfig(kubeconfigFlag)
	}
}

// datumRestConfig fetches a fresh token from the credentials helper and builds a
// rest.Config pointed at the Datum API host.
func datumRestConfig(env datumEnv) (*rest.Config, string, error) {
	if !env.usable() {
		return nil, "", newCLIError(exitUnavailable,
			"this doesn't look like it's running under datumctl — no API host or credentials were passed in").
			withFix("run it as \"datumctl search ...\", or point at a cluster yourself with --kubeconfig")
	}
	// The SDK execs DATUM_CREDENTIALS_HELPER (the datumctl binary) to mint a fresh,
	// short-lived token, honoring the active session. The plugin never persists it.
	token, err := plugin.Token()
	if err != nil {
		return nil, "", newCLIError(exitUnavailable, "couldn't get an access token from datumctl").
			withFix("run \"datumctl login\", then try again").withCause(err)
	}
	cfg := &rest.Config{
		Host:        controlPlaneHost(env),
		BearerToken: token,
	}
	cfg.UserAgent = userAgent()
	// The active project/org scope is encoded in the control-plane URL path (see
	// controlPlaneHost); the search resources are cluster-scoped, so the
	// namespace is unused. "default" mirrors what datumctl itself targets.
	return cfg, "default", nil
}

// controlPlaneHost builds the fully-qualified search API base URL for the active
// Datum scope. datumctl injects DATUM_API_HOST as a bare hostname (e.g.
// "api.datum.net") and conveys scope via DATUM_PROJECT/DATUM_ORG; a project's
// (or org's) Milo control plane is addressed by a path prefix off that host —
// the same construction datumctl and the other plugins use. With neither set,
// the bare platform root is used, for cluster-scoped, operator-level calls.
func controlPlaneHost(env datumEnv) string {
	base := strings.TrimRight(ensureScheme(env.apiHost), "/")
	switch {
	case env.project != "":
		return fmt.Sprintf("%s/apis/resourcemanager.miloapis.com/v1alpha1/projects/%s/control-plane", base, env.project)
	case env.org != "":
		return fmt.Sprintf("%s/apis/resourcemanager.miloapis.com/v1alpha1/organizations/%s/control-plane", base, env.org)
	default:
		return base
	}
}

// ensureScheme prepends https:// when host has no scheme. datumctl provides
// DATUM_API_HOST without one; client-go needs an absolute URL or it routes the
// request to an HTML-serving endpoint, surfacing as "serializer for text/html".
func ensureScheme(host string) string {
	if host == "" || strings.Contains(host, "://") {
		return host
	}
	return "https://" + host
}

// kubeconfigRestConfig loads a standard kubeconfig, honoring --kubeconfig and
// KUBECONFIG, and falls back to in-cluster config when no kubeconfig is found.
func kubeconfigRestConfig(kubeconfigFlag string) (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigFlag != "" {
		rules.ExplicitPath = kubeconfigFlag
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})

	cfg, err := cc.ClientConfig()
	if err != nil {
		// Last resort: in-cluster (running inside a pod).
		if inCluster, icErr := rest.InClusterConfig(); icErr == nil {
			inCluster.UserAgent = userAgent()
			return inCluster, "default", nil
		}
		return nil, "", newCLIError(exitUnavailable, "couldn't find a cluster to talk to").
			withFix("set KUBECONFIG or pass --kubeconfig to point at one").withCause(err)
	}
	cfg.UserAgent = userAgent()

	ns, _, nsErr := cc.Namespace()
	if nsErr != nil || ns == "" {
		ns = "default"
	}
	return cfg, ns, nil
}

func userAgent() string {
	return fmt.Sprintf("milo-search/%s", pluginVersion)
}

// newSearchClientset builds the generated typed clientset for the
// search.miloapis.com/v1alpha1 group from a rest.Config. The control-plane scope
// lives in cfg.Host; the clientset sets the group-version and "/apis" path
// itself. JSON is forced because the aggregated apiserver does not serve
// protobuf for this group.
func newSearchClientset(cfg *rest.Config) (versioned.Interface, error) {
	c := rest.CopyConfig(cfg)
	if c.ContentType == "" {
		c.ContentType = "application/json"
	}
	cs, err := versioned.NewForConfig(c)
	if err != nil {
		return nil, newCLIError(exitUnavailable, "couldn't set up a connection to the search service").withCause(err)
	}
	return cs, nil
}

// setSearchQueryGVK stamps the group-version-kind onto a ResourceSearchQuery so
// the cli-runtime printers emit apiVersion/kind for -o json|yaml. The typed
// decoder clears TypeMeta on read, so this is re-applied before rendering.
func setSearchQueryGVK(q *searchv1alpha1.ResourceSearchQuery) {
	q.APIVersion = searchAPIGroupVersion
	q.Kind = "ResourceSearchQuery"
}

// setIndexPolicyListGVK stamps the GVK onto a ResourceIndexPolicyList for
// -o json|yaml.
func setIndexPolicyListGVK(l *searchv1alpha1.ResourceIndexPolicyList) {
	l.APIVersion = searchAPIGroupVersion
	l.Kind = "ResourceIndexPolicyList"
}
