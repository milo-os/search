// Command milo-search is the search plugin for datumctl. It presents the
// search.miloapis.com/v1alpha1 API as a small, question-shaped, read-only
// command surface (query and kinds), reusing the user's datumctl
// identity/context in production and a standard kubeconfig for dev/e2e
// clusters.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"go.datum.net/datumctl/plugin"
)

func main() {
	// Serve --plugin-manifest via the datumctl SDK before cobra runs, so the
	// manifest is emitted even if flag parsing would otherwise fail. ServeManifest
	// prints the JSON and exits 0 when the flag is present; otherwise it returns.
	plugin.ServeManifest(pluginManifest())

	io := stdStreams()
	root := newRootCommand(io)

	// Flag parse errors are usage errors (exit 2), not generic failures.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageErrorf("%v", err)
	})

	err := root.Execute()
	os.Exit(renderExit(io, err))
}

// renderExit prints a clean, search-aware error (no Go stack trace) and returns
// the contractual exit code. Returns 0 on success. The frame matches datumctl
// core: a lowercase red "error:" line, then — after a blank line — an unlabeled,
// dimmed advice line with the exact next command. The "exit status N # SYMBOL"
// trailer is a --verbose-only diagnostic; the process exit code ($?) is the
// machine contract and is unchanged.
func renderExit(io IOStreams, err error) int {
	if err == nil {
		return exitOK
	}

	ce := toCLIError(err)
	color := errorColorEnabled(io.ErrOut)

	// Primary error line, on stderr so machine output on stdout stays clean.
	prefix := "error:"
	if color {
		prefix = colorize(prefix, colorRed)
	}
	_, _ = fmt.Fprintf(io.ErrOut, "%s %s\n", prefix, ce.msg)

	// Advice: an unlabeled follow-on line after one blank line, dimmed when color
	// is on. It carries the imperative next action / command.
	if ce.fix != "" {
		advice := ce.fix
		if color {
			advice = colorize(advice, colorFaint)
		}
		_, _ = fmt.Fprintf(io.ErrOut, "\n%s\n", advice)
	}

	// Under --verbose/--debug only: the symbolic exit-code name (for CI log
	// readers) and the underlying cause (stack-trace-equivalent detail).
	if verboseEnabled() {
		if name := exitCodeNames[ce.code]; name != "" && ce.code != exitOK {
			_, _ = fmt.Fprintf(io.ErrOut, "exit status %d   # %s\n", ce.code, name)
		}
		if ce.cause != nil {
			_, _ = fmt.Fprintf(io.ErrOut, "cause: %v\n", ce.cause)
		}
	}
	return ce.code
}

// toCLIError normalizes any error into a *cliError. Cobra-origin command errors
// (unknown command/flag) are usage errors; everything else is classified by
// HTTP status or transport heuristics.
func toCLIError(err error) *cliError {
	if ce, isCLI := err.(*cliError); isCLI {
		return ce
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "unknown command") ||
		strings.HasPrefix(msg, "unknown flag") ||
		strings.HasPrefix(msg, "unknown shorthand flag") ||
		strings.Contains(msg, "requires") && strings.Contains(msg, "arg") ||
		strings.HasPrefix(msg, "accepts") {
		return newCLIError(exitUsage, msg)
	}
	return classifyError(err)
}

// verboseEnabled reports whether -v/--verbose or --debug appears in os.Args. We
// read os.Args directly because renderExit runs after cobra may have failed to
// parse flags, so the parsed flag value may be unavailable.
func verboseEnabled() bool {
	for _, a := range os.Args[1:] {
		if a == "-v" || a == "--verbose" || a == "--debug" {
			return true
		}
	}
	return false
}

// itoa is a tiny dependency-free int-to-string for exit codes (avoids importing
// strconv just for this).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
