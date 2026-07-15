package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// queryOptions holds the flags for a search, shared by the `query` subcommand
// and the bare-args sugar on the root command.
type queryOptions struct {
	kinds  []string // --kind, as typed (Kind[.group][/version]); resolved lazily
	limit  int      // --limit (0 = server default)
	cont   string   // --continue token
	all    bool     // --all: follow continue tokens and merge
	strict bool     // --strict: fail on any denied kind
}

// addQueryFlags registers the query flags on cmd. When hideNiche is set (the
// root command hosting the bare-args sugar), the niche and reserved flags are
// hidden from help but still parse — they are documented under
// `datumctl search query --help`.
func addQueryFlags(cmd *cobra.Command, o *queryOptions, a *app, hideNiche bool) {
	f := cmd.Flags()
	f.StringArrayVar(&o.kinds, "kind", nil, "Limit the search to a kind (repeatable): Kind, Kind.group, or Kind.group/version")
	f.IntVar(&o.limit, "limit", 0, "Maximum results per page (1–100; server default 10)")
	f.StringVar(&o.cont, "continue", "", "Continue token from a previous page (bound to the query text, --kind set, and --limit)")
	f.BoolVar(&o.all, "all", false, "Follow continue tokens and return every page merged")
	f.BoolVar(&o.strict, "strict", false, "Fail (exit 7 SEARCH_PARTIAL_COVERAGE) if any requested kind is not searchable")

	// Reserved roadmap flags: registered so they parse, but rejected with a clear
	// usage error. Search only does full-text relevance today.
	f.String("filter", "", "Not supported yet: filter results by field (on the search API's roadmap)")
	f.StringArray("facet", nil, "Not supported yet: facet counts (on the search API's roadmap)")
	f.String("sort", "", "Not supported yet: sort order (on the search API's roadmap)")

	if hideNiche {
		for _, n := range []string{"continue", "strict", "filter", "facet", "sort"} {
			_ = f.MarkHidden(n)
		}
	}

	_ = cmd.RegisterFlagCompletionFunc("kind", a.completeKinds)
}

func newQueryCommand(a *app) *cobra.Command {
	o := &queryOptions{}
	cmd := &cobra.Command{
		Use:     "query <text>",
		Aliases: []string{"q"},
		Short:   "Search platform resources and render the ranked answer",
		Long: `Ask a question and get a relevance-ordered answer.

A search creates a ResourceSearchQuery and reads back its status synchronously —
nothing is persisted and there is no query to fetch afterwards. Use -o yaml to
see the exact resource exchanged with the server.

An empty query is browse mode: 'query "" --kind Workload' lists every indexed
Workload. Continue tokens are bound to the query text, --kind set, and --limit
they were issued for; changing any of those mid-pagination starts a new search.`,
		Example: `  # Ask a question
  datumctl search query "payments"

  # Scope to kinds and cap the page size
  datumctl search query "checkout" --kind Workload --kind HTTPRoute --limit 25

  # Browse every indexed Workload
  datumctl search query "" --kind Workload

  # Complete answer for a script (all pages, fail on partial coverage)
  datumctl search query "legacy-billing" --kind Workload --all --strict -o json`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// An empty query is valid (browse mode) only when explicitly passed as
			// "". With no positional at all there is nothing to ask.
			if len(args) == 0 {
				return newCLIError(exitUsage, "you didn't pass anything to search for").
					withFix("add a query, or \"\" to browse everything that's indexed")
			}
			return a.runQuery(cmd, o, strings.Join(args, " "))
		},
	}
	addQueryFlags(cmd, o, a, false)
	return cmd
}

// runQuery executes a search: validate flags, resolve --kind, fetch (one page or
// all), enforce --strict, and render.
func (a *app) runQuery(cmd *cobra.Command, o *queryOptions, query string) error {
	// Reserved roadmap flags fail fast, before any API call.
	for _, name := range []string{"filter", "facet", "sort"} {
		if cmd.Flags().Changed(name) {
			return reservedFlagError(name)
		}
	}

	limitSet := cmd.Flags().Changed("limit")
	if limitSet && (o.limit < 1 || o.limit > 100) {
		return usageErrorf("--limit must be between 1 and 100 (you asked for %d)", o.limit)
	}

	ctx := context.Background()

	// Resolve --kind against the indexed-kinds list. Kinds that have a policy but
	// aren't ready still resolve here; the server reports them as denied.
	var targets []searchv1alpha1.TargetResource
	if len(o.kinds) > 0 {
		policies, err := a.policies(ctx)
		if err != nil {
			return policyError(err, true)
		}
		targets, err = resolveKinds(o.kinds, indexedKinds(policies))
		if err != nil {
			return err
		}
	}

	cs, err := a.client()
	if err != nil {
		return err
	}

	base := &searchv1alpha1.ResourceSearchQuery{
		ObjectMeta: metav1.ObjectMeta{Name: generateQueryName()},
		Spec: searchv1alpha1.ResourceSearchQuerySpec{
			Query:           query,
			TargetResources: targets,
			Continue:        o.cont,
		},
	}
	if limitSet {
		base.Spec.Limit = int32(o.limit)
	}

	result, err := a.fetchQuery(ctx, cs, base, o.all)
	if err != nil {
		return err
	}

	// --strict: any denied kind fails the command with the partial-coverage code
	// before results are rendered.
	if o.strict && len(result.Status.DeniedTargetResources) > 0 {
		return partialCoverageError(result.Status.DeniedTargetResources)
	}

	return a.renderQuery(&queryRender{
		query:    query,
		targets:  targets,
		kindRefs: o.kinds,
		limit:    o.limit,
		limitSet: limitSet,
		all:      o.all,
		result:   result,
	})
}

// fetchQuery runs the search. Without --all it returns the single page verbatim
// (continue token intact for scripts). With --all it follows continue tokens
// until they run out and returns one object with the concatenated results and an
// empty continue.
func (a *app) fetchQuery(ctx context.Context, cs searchClient, base *searchv1alpha1.ResourceSearchQuery, all bool) (*searchv1alpha1.ResourceSearchQuery, error) {
	first, err := cs.Search(ctx, base)
	if err != nil {
		return nil, a.classify(err)
	}
	if !all || first.Status.Continue == "" {
		return first, nil
	}

	merged := first.DeepCopy()
	cont := first.Status.Continue
	for cont != "" {
		next := base.DeepCopy()
		next.Spec.Continue = cont
		page, err := cs.Search(ctx, next)
		if err != nil {
			return nil, a.classify(err)
		}
		merged.Status.Results = append(merged.Status.Results, page.Status.Results...)
		cont = page.Status.Continue
	}
	merged.Status.Continue = ""
	return merged, nil
}

// queryRender bundles everything the renderer needs.
type queryRender struct {
	query    string
	targets  []searchv1alpha1.TargetResource
	kindRefs []string
	limit    int
	limitSet bool
	all      bool
	result   *searchv1alpha1.ResourceSearchQuery
}

func (a *app) renderQuery(rc *queryRender) error {
	res := rc.result

	// Coverage warning (denied kinds) always goes to stderr, in every output
	// mode, so a piped -o json stays clean while the coverage gap is still loud.
	a.renderDeniedWarning(len(rc.targets), res.Status.DeniedTargetResources)

	switch a.opts.output {
	case outputJSON:
		setSearchQueryGVK(res)
		return encodeJSON(a.io.Out, res)
	case outputYAML:
		setSearchQueryGVK(res)
		return encodeYAML(a.io.Out, res)
	case outputName:
		for i := range res.Status.Results {
			r := &res.Status.Results[i]
			group, _ := splitAPIVersion(r.Resource.GetAPIVersion())
			_, _ = fmt.Fprintln(a.io.Out, resourceName(r.Resource.GetKind(), group, r.Resource.GetName()))
		}
		return nil
	}

	// Human table (table | wide).
	if !a.opts.quiet {
		a.printHeadline(rc)
	}
	if len(res.Status.Results) > 0 {
		if err := a.printResultsTable(res.Status.Results); err != nil {
			return err
		}
	}
	if !a.opts.quiet {
		a.printContinueFooter(rc)
	}
	return nil
}

// printHeadline prints the "N results for … <scope> (M kinds searched)" line.
func (a *app) printHeadline(rc *queryRender) {
	n := len(rc.result.Status.Results)
	headline := fmt.Sprintf("%s for %q %s", pluralize(n, "result"), rc.query, a.scopeLocative())
	if m, known := a.kindsSearched(context.Background(), rc.targets, rc.result.Status.DeniedTargetResources); known {
		headline += fmt.Sprintf("  (%s searched)", pluralize(m, "kind"))
	}
	if a.color.enabled {
		headline = colorize(headline, colorBold)
	}
	_, _ = fmt.Fprintln(a.io.Out, headline)
	_, _ = fmt.Fprintln(a.io.Out)
}

// kindsSearched reports how many kinds the query actually searched. When the
// query is kind-scoped it is exact (requested minus denied); otherwise it is a
// best-effort count of ready index policies, omitted if listing fails.
func (a *app) kindsSearched(ctx context.Context, targets, denied []searchv1alpha1.TargetResource) (int, bool) {
	if len(targets) > 0 {
		return len(targets) - len(denied), true
	}
	policies, err := a.policies(ctx)
	if err != nil {
		return 0, false
	}
	ready := 0
	for _, k := range indexedKinds(policies) {
		if k.Ready {
			ready++
		}
	}
	return ready, true
}

func (a *app) printResultsTable(results []searchv1alpha1.SearchResult) error {
	wide := a.opts.output == outputWide
	var headers []string
	if wide {
		headers = []string{"KIND", "GROUP", "VERSION", "NAME", "NAMESPACE", "TENANT", "TENANT-TYPE", "SCORE", "AGE"}
	} else {
		headers = []string{"KIND", "NAME", "NAMESPACE", "TENANT", "SCORE", "AGE"}
	}
	t := newTable(a.io.Out, headers)
	for i := range results {
		r := &results[i]
		group, version := splitAPIVersion(r.Resource.GetAPIVersion())
		kind := r.Resource.GetKind()
		name := r.Resource.GetName()
		ns := orDash(r.Resource.GetNamespace())
		tenant := orDash(r.Tenant.Name)
		score := fmt.Sprintf("%.2f", r.RelevanceScore)
		age := humanDuration(r.Resource.GetCreationTimestamp())
		if wide {
			gd := group
			if gd == "" {
				gd = "(core)"
			}
			t.row(kind, gd, version, name, ns, tenant, orDash(r.Tenant.Type), score, age)
		} else {
			t.row(kind, name, ns, tenant, score, age)
		}
	}
	return t.flush()
}

// printContinueFooter names the exact next-page command when a continue token
// remains. It is suppressed for --all (already exhausted) and off a TTY (a piped
// run does not want the interactive hint).
func (a *app) printContinueFooter(rc *queryRender) {
	token := rc.result.Status.Continue
	if token == "" || rc.all || !isTerminal(a.io.Out) {
		return
	}
	_, _ = fmt.Fprintln(a.io.ErrOut, "There's more. Run this to see the next page:")
	_, _ = fmt.Fprintf(a.io.ErrOut, "  %s\n", nextPageCommand(rc.query, rc.kindRefs, rc.limit, rc.limitSet, token))
}

// nextPageCommand builds the copy-paste next-page invocation, echoing the query
// and the flags the continue token is bound to (query text, --kind set, --limit).
func nextPageCommand(query string, kindRefs []string, limit int, limitSet bool, token string) string {
	next := fmt.Sprintf("datumctl search %q", query)
	for _, k := range kindRefs {
		next += " --kind " + k
	}
	if limitSet {
		next += " --limit " + itoa(limit)
	}
	next += " --continue " + token
	return next
}

// renderDeniedWarning surfaces status.deniedTargetResources as a stderr warning
// naming each kind that isn't ready yet and pointing at `datumctl search kinds`.
func (a *app) renderDeniedWarning(requested int, denied []searchv1alpha1.TargetResource) {
	if len(denied) == 0 {
		return
	}
	verb, pron, was := "isn't", "it", "was"
	if len(denied) != 1 {
		verb, pron, was = "aren't", "they", "were"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Warning: %d of the %d kinds you asked for %s ready to search yet, so %s %s skipped:\n",
		len(denied), requested, verb, pron, was)
	for _, d := range denied {
		fmt.Fprintf(&b, "           %s\n", targetLabel(d))
	}
	b.WriteString("         Run \"datumctl search kinds\" to see what's ready.")
	_, _ = fmt.Fprintln(a.io.ErrOut, b.String())
}

// partialCoverageError is the --strict failure: any kind that isn't searchable
// aborts with the SEARCH_PARTIAL_COVERAGE code so automation can insist zero
// results means zero matches, not zero coverage.
func partialCoverageError(denied []searchv1alpha1.TargetResource) *cliError {
	var b strings.Builder
	b.WriteString("stopping — some of the kinds you asked for aren't ready to search yet:")
	for _, d := range denied {
		fmt.Fprintf(&b, "\n  %s", targetLabel(d))
	}
	return newCLIError(exitPartial, b.String()).
		withFix("run \"datumctl search kinds\" to see what's ready, or drop --strict to search the rest anyway")
}

// completeKinds is the dynamic shell-completion source for --kind: the indexed
// kinds, queried live from the API.
func (a *app) completeKinds(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	policies, err := a.policies(context.Background())
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveError
	}
	seen := map[string]bool{}
	var out []string
	for _, k := range indexedKinds(policies) {
		if !seen[k.Kind] {
			seen[k.Kind] = true
			out = append(out, k.Kind)
		}
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}
