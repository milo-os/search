package main

import (
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Exit codes are a stable contract: automation branches on them. 0 is success
// (including a query with zero matches); each documented non-zero code names a
// distinct failure class. Codes 5 and 9 are intentionally unused so the
// numbering stays aligned across datumctl plugins.
const (
	exitOK          = 0 // success (including zero matches)
	exitError       = 1 // generic / unexpected error (SEARCH_ERROR)
	exitUsage       = 2 // invalid flags or arguments, incl. reserved flags (SEARCH_USAGE)
	exitForbidden   = 3 // HTTP 403 RBAC denial / search not enabled (SEARCH_FORBIDDEN)
	exitNotFound    = 4 // unknown kind / HTTP 404 (SEARCH_NOT_FOUND)
	exitInvalid     = 6 // HTTP 400/422 rejected request, notably a bad continue token (SEARCH_INVALID)
	exitPartial     = 7 // --strict only: requested kinds were not searchable (SEARCH_PARTIAL_COVERAGE)
	exitUnavailable = 8 // unreachable service or expired session (SEARCH_UNAVAILABLE)
)

// exitCodeNames maps each exit code to its documented symbolic name, used in
// help text and --verbose diagnostics. Codes 5 and 9 are deliberately absent.
var exitCodeNames = map[int]string{
	exitOK:          "OK",
	exitError:       "SEARCH_ERROR",
	exitUsage:       "SEARCH_USAGE",
	exitForbidden:   "SEARCH_FORBIDDEN",
	exitNotFound:    "SEARCH_NOT_FOUND",
	exitInvalid:     "SEARCH_INVALID",
	exitPartial:     "SEARCH_PARTIAL_COVERAGE",
	exitUnavailable: "SEARCH_UNAVAILABLE",
}

// cliError carries a rendered, human-facing message plus a precise exit code.
// It is what RunE handlers return so that main() can both print a clean message
// (no Go stack trace) and exit with the contractual code.
type cliError struct {
	code int
	// msg is the primary message, rendered after the lowercase "error:" prefix.
	msg string
	// fix is optional advice, rendered as an unlabeled line after a blank line.
	fix string
	// cause is retained for --verbose/--debug rendering only.
	cause error
}

func (e *cliError) Error() string { return e.msg }
func (e *cliError) Unwrap() error { return e.cause }

func newCLIError(code int, msg string) *cliError {
	return &cliError{code: code, msg: msg}
}

func (e *cliError) withFix(fix string) *cliError {
	e.fix = fix
	return e
}

func (e *cliError) withCause(err error) *cliError {
	e.cause = err
	return e
}

// usageErrorf builds a usage (exit 2) error.
func usageErrorf(format string, a ...any) *cliError {
	return newCLIError(exitUsage, fmt.Sprintf(format, a...))
}

// reservedFlagNoun names each roadmap flag in plain words for its rejection.
var reservedFlagNoun = map[string]string{
	"filter": "filtering",
	"facet":  "facet counts",
	"sort":   "sorting",
}

// reservedFlagError rejects a roadmap flag (--filter/--facet/--sort). Search
// only does full-text relevance today; the flag is a plan, not a feature.
func reservedFlagError(flag string) *cliError {
	noun := reservedFlagNoun[flag]
	if noun == "" {
		noun = flag
	}
	return newCLIError(exitUsage, fmt.Sprintf(
		"--%s isn't supported yet — %s is on the search API's roadmap; today search is full-text only", flag, noun)).
		withFix(fmt.Sprintf("drop --%s and rerun", flag))
}

// httpStatusCode extracts the HTTP status code from a Kubernetes API error,
// returning 0 when the error does not carry one (e.g. a transport failure).
func httpStatusCode(err error) int {
	if status, isStatus := asAPIStatus(err); isStatus {
		return int(status.Status().Code)
	}
	return 0
}

// asAPIStatus unwraps err to a Kubernetes APIStatus when possible.
func asAPIStatus(err error) (apierrors.APIStatus, bool) {
	if err == nil {
		return nil, false
	}
	var s apierrors.APIStatus
	if errors.As(err, &s) {
		return s, true
	}
	return nil, false
}

// classifyError maps an arbitrary API error into a cliError with the right exit
// code, using a generic scope. Callers with a resolved scope should use
// classifyWithScope so the message can name the org/project.
func classifyError(err error) *cliError {
	return classifyWithScope(err, "")
}

// classifyWithScope is classifyError with a known scope (e.g. "acme / net-core")
// to name in the forbidden and unreachable messages.
func classifyWithScope(err error, scope string) *cliError {
	if err == nil {
		return nil
	}
	if ce, isCLI := err.(*cliError); isCLI {
		return ce
	}
	switch httpStatusCode(err) {
	case 401:
		return sessionExpiredError(err)
	case 403:
		return searchForbiddenError(scope, err)
	case 404:
		return newCLIError(exitNotFound, apiMessage(err)).withCause(err)
	case 400, 422:
		msg := apiMessage(err)
		switch {
		case isContinueTokenMismatch(msg):
			return continueTokenMismatchError(err)
		case mentionsContinueToken(msg):
			return continueTokenExpiredError(err)
		default:
			return invalidRequestError(err)
		}
	}
	// No HTTP status: most likely the service is unreachable.
	if isConnectionError(err.Error()) {
		return unavailableError(scope, err)
	}
	return newCLIError(exitError, err.Error()).withCause(err)
}

// sessionExpiredError renders the 401 case: the login the plugin borrowed from
// datumctl is no longer valid.
func sessionExpiredError(cause error) *cliError {
	return newCLIError(exitUnavailable, "your session has expired or was signed out").
		withFix("run \"datumctl login\", then retry").
		withCause(cause)
}

// searchForbiddenError renders a 403 on the search itself: the caller lacks the
// role that lets them run a query. It names that role because that is the fix.
func searchForbiddenError(scope string, cause error) *cliError {
	where := "here"
	if scope != "" {
		where = "in " + scope
	}
	return newCLIError(exitForbidden, fmt.Sprintf(
		"you don't have permission to search %s — searching needs the \"search.miloapis.com-searcher\" role", where)).
		withFix("ask an admin for that role, or check that search is turned on for this project").
		withCause(cause)
}

// policyAccessError renders a 403 when listing what's searchable: that read is a
// different permission than searching, so the fix is different too. On the
// --kind path the query itself may still be allowed, so offer to drop --kind.
func policyAccessError(kindPath bool) *cliError {
	fix := "ask an admin for read access to those index policies, then try again"
	if kindPath {
		fix = "ask an admin for read access to those index policies, or drop --kind to search without narrowing to a kind"
	}
	return newCLIError(exitForbidden,
		"you don't have access to see what's searchable — that needs read access to the platform's index policies").
		withFix(fix)
}

// unavailableError renders a connection failure: the service can't be reached.
func unavailableError(scope string, cause error) *cliError {
	where := "the search service"
	if scope != "" {
		where = "the search service for " + scope
	}
	return newCLIError(exitUnavailable, fmt.Sprintf("couldn't reach %s", where)).
		withFix("check your connection and try again in a moment; the service may be briefly unavailable").
		withCause(cause)
}

// continueTokenExpiredError covers a token that has aged out or is garbage.
func continueTokenExpiredError(cause error) *cliError {
	return newCLIError(exitInvalid,
		"that continue token has expired or isn't valid anymore — they only work for a limited time").
		withFix("rerun the search without --continue to start over").
		withCause(cause)
}

// continueTokenMismatchError covers a token replayed with different parameters.
func continueTokenMismatchError(cause error) *cliError {
	return newCLIError(exitInvalid,
		"that continue token was for a different search — a token only works with the exact query text, --kind set, and --limit it came from").
		withFix("rerun the search without --continue to start over").
		withCause(cause)
}

// invalidRequestError covers a generic 400/422 the server rejected.
func invalidRequestError(cause error) *cliError {
	return newCLIError(exitInvalid, fmt.Sprintf("the search service couldn't run that request: %s", apiMessage(cause))).
		withCause(cause)
}

// mentionsContinueToken matches the server's continue-token messages (expiry or
// garbage token) so a 400 is rendered as a token error rather than a bare 4xx.
func mentionsContinueToken(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "continue token")
}

// isContinueTokenMismatch matches the server's parameter-binding rejection
// (query text, limit, or targetResources changed under a replayed token).
func isContinueTokenMismatch(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "cannot be changed when using a continue token")
}

func isConnectionError(msg string) bool {
	m := strings.ToLower(msg)
	for _, s := range []string{
		"connection refused", "no such host", "i/o timeout", "deadline exceeded",
		"tls", "dial tcp", "eof", "connect:", "unable to connect",
	} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

// apiMessage returns the server-provided status message when available, else
// the raw error string.
func apiMessage(err error) string {
	if status, isStatus := asAPIStatus(err); isStatus {
		if m := status.Status().Message; m != "" {
			return m
		}
	}
	return err.Error()
}
