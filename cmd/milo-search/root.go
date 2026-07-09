package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// newRootCommand builds the full command tree. The plugin is dispatched by
// datumctl as `milo-search <args>`, presenting to the user as `datumctl search`.
func newRootCommand(io IOStreams) *cobra.Command {
	opts := &globalOptions{output: outputTable, color: "auto"}
	a := newApp(io, opts)

	// rootQuery backs the bare-args sugar (`datumctl search "payments"`), which
	// shares the query command's flags and behavior.
	rootQuery := &queryOptions{}

	root := &cobra.Command{
		Use:   "search [query]",
		Short: "Search platform resources on Datum",
		Long: `Search platform resources on Datum.

Ask a question and get a relevance-ordered answer:

  datumctl search "payments"
  datumctl search "checkout" --kind Workload --kind HTTPRoute

Search covers indexed, non-sensitive resources and is scoped by tenant, not
filtered per object — a query is authorized as a whole, and every result row
shows the tenant it came from. Run "datumctl search kinds" to see exactly what
is searchable right now.

Bare arguments that don't name a subcommand are treated as the query; use "--"
to force literal terms (datumctl search -- kinds searches for the word "kinds").
Output is a human table by default; -o json|yaml is a stable contract for
scripts (data on stdout, diagnostics on stderr).`,
		Args:          cobra.ArbitraryArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		Example: `  # Search everything for "payments"
  datumctl search "payments"

  # Scope to specific kinds and page through results
  datumctl search "checkout" --kind Workload --limit 25

  # See what is searchable
  datumctl search kinds`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Bare invocation with no args prints help; otherwise the args are the
			// query (identical behavior to the `query` subcommand).
			if len(args) == 0 {
				return cmd.Help()
			}
			return a.runQuery(cmd, rootQuery, strings.Join(args, " "))
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if !isValidOutput(opts.output) {
				return newCLIError(exitUsage, fmt.Sprintf("%q isn't a valid output format", opts.output)).
					withFix("choose one of: table, wide, json, yaml, name")
			}
			switch opts.color {
			case "auto", "always", "never":
			default:
				return newCLIError(exitUsage, fmt.Sprintf("--color %q isn't valid", opts.color)).
					withFix("use auto, always, or never")
			}
			a.resolveColor()
			// No service-entitlement preflight: search is not a gated service, so
			// every command may talk to the API directly. There is nothing to gate
			// and nothing that prompts.
			return nil
		},
	}
	root.SuggestionsMinimumDistance = 2

	// Note: --plugin-manifest is handled by plugin.ServeManifest in main() before
	// cobra runs, so it is intentionally not registered as a cobra flag here.
	pf := root.PersistentFlags()
	pf.StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to a kubeconfig (forces kubeconfig transport; dev/e2e only)")
	pf.StringVarP(&opts.namespace, "namespace", "n", "", "Namespace escape hatch for dev/e2e (search resources are cluster-scoped)")
	pf.StringVarP(&opts.output, "output", "o", outputTable, "Output format: table|wide|json|yaml|name")
	pf.BoolVarP(&opts.quiet, "quiet", "q", false, "Suppress the headline and pagination footer; print only results")
	pf.StringVar(&opts.color, "color", "auto", "Colorize output: auto|always|never")
	pf.BoolVarP(&opts.verbose, "verbose", "v", false, "Verbose diagnostics on stderr (resolved scope, API host, calls)")
	pf.StringVar(&opts.org, "org", "", "Override the active organization for this invocation")
	pf.StringVar(&opts.project, "project", "", "Override the active project for this invocation")

	// The bare-args sugar shares the query flags. Niche flags are hidden from the
	// top-level help (progressive disclosure) but still parse; they are documented
	// under `datumctl search query --help`.
	addQueryFlags(root, rootQuery, a, true)

	root.AddCommand(newQueryCommand(a))
	root.AddCommand(newKindsCommand(a))
	root.AddCommand(newVersionCommand(io))

	return root
}

func newVersionCommand(io IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the plugin version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _ = fmt.Fprintf(io.Out, "milo-search %s (search API %s)\n", pluginVersion, searchAPIGroupVersion)
			return nil
		},
	}
}
