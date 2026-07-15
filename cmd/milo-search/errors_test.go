package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// statusErr builds a Kubernetes API error carrying a specific HTTP code.
func statusErr(code int32, reason metav1.StatusReason, msg string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    code,
		Reason:  reason,
		Message: msg,
	}}
}

func TestClassifyErrorExitCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"unauthorized", statusErr(401, metav1.StatusReasonUnauthorized, "Unauthorized"), exitUnavailable},
		{"forbidden", statusErr(403, metav1.StatusReasonForbidden, "no access"), exitForbidden},
		{"notfound", statusErr(404, metav1.StatusReasonNotFound, "nope"), exitNotFound},
		{"invalid", statusErr(400, metav1.StatusReasonBadRequest, "bad"), exitInvalid},
		{"unprocessable", statusErr(422, metav1.StatusReasonInvalid, "bad"), exitInvalid},
		{"continue-token", statusErr(400, metav1.StatusReasonBadRequest, "invalid continue token"), exitInvalid},
		{"connrefused", errors.New("dial tcp 1.2.3.4:443: connection refused"), exitUnavailable},
		{"generic", errors.New("something odd"), exitError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ce := classifyError(tc.err)
			if ce.code != tc.want {
				t.Fatalf("classifyError(%s).code = %d, want %d", tc.name, ce.code, tc.want)
			}
		})
	}
}

func TestContinueTokenExpiredMessage(t *testing.T) {
	// A stale/garbage token (400 "invalid continue token") must read as expired,
	// not as a parameter mismatch.
	ce := classifyError(statusErr(400, metav1.StatusReasonBadRequest, "invalid continue token"))
	if ce.code != exitInvalid {
		t.Fatalf("code = %d, want %d", ce.code, exitInvalid)
	}
	if !strings.Contains(ce.msg, "expired") {
		t.Errorf("stale continue-token error should say it expired: %q", ce.msg)
	}
	if strings.Contains(ce.msg, "different search") {
		t.Errorf("stale token must not be blamed on a different query: %q", ce.msg)
	}
}

func TestContinueTokenMismatchMessage(t *testing.T) {
	ce := classifyError(statusErr(400, metav1.StatusReasonBadRequest, "query string cannot be changed when using a continue token"))
	if ce.code != exitInvalid {
		t.Fatalf("code = %d, want %d", ce.code, exitInvalid)
	}
	if !strings.Contains(ce.msg, "different search") || !strings.Contains(ce.msg, "query text") {
		t.Errorf("mismatch error should explain the parameter binding: %q", ce.msg)
	}
}

func TestUnauthorizedIsSessionExpired(t *testing.T) {
	ce := classifyError(statusErr(401, metav1.StatusReasonUnauthorized, "Unauthorized"))
	if ce.code != exitUnavailable {
		t.Fatalf("code = %d, want %d (SEARCH_UNAVAILABLE)", ce.code, exitUnavailable)
	}
	if !strings.Contains(ce.msg, "session") {
		t.Errorf("401 should read as an expired session: %q", ce.msg)
	}
	if !strings.Contains(ce.fix, "datumctl login") {
		t.Errorf("401 fix should point at datumctl login: %q", ce.fix)
	}
}

func TestSearchForbiddenNamesSearcherRole(t *testing.T) {
	ce := searchForbiddenError("net-core", nil)
	if ce.code != exitForbidden {
		t.Fatalf("code = %d, want %d", ce.code, exitForbidden)
	}
	if !strings.Contains(ce.msg, "search.miloapis.com-searcher") {
		t.Errorf("forbidden error should name the searcher role: %q", ce.msg)
	}
	if !strings.Contains(ce.msg, "net-core") {
		t.Errorf("forbidden error should name the project: %q", ce.msg)
	}
}

func TestPolicyAccessErrorIsNotSearcherRole(t *testing.T) {
	// Listing what's searchable is a different permission than searching, so the
	// 403 there must not tell the user they need the searcher role.
	ce, ok := policyError(statusErr(403, metav1.StatusReasonForbidden, "no access"), true).(*cliError)
	if !ok {
		t.Fatalf("expected *cliError")
	}
	if ce.code != exitForbidden {
		t.Fatalf("code = %d, want %d", ce.code, exitForbidden)
	}
	if strings.Contains(ce.msg, "searcher") {
		t.Errorf("policy-access error must not mention the searcher role: %q", ce.msg)
	}
	if !strings.Contains(ce.msg, "index policies") {
		t.Errorf("policy-access error should name index-policy read access: %q", ce.msg)
	}
	if !strings.Contains(ce.fix, "--kind") {
		t.Errorf("on the --kind path the fix should offer to drop --kind: %q", ce.fix)
	}
}

func TestReservedFlagError(t *testing.T) {
	ce := reservedFlagError("filter")
	if ce.code != exitUsage {
		t.Fatalf("code = %d, want %d", ce.code, exitUsage)
	}
	if !strings.Contains(ce.msg, "--filter") || !strings.Contains(ce.msg, "roadmap") {
		t.Errorf("reserved-flag error should name the flag and call it roadmap: %q", ce.msg)
	}
	if !strings.Contains(ce.fix, "drop --filter") {
		t.Errorf("reserved-flag fix should say to drop the flag: %q", ce.fix)
	}
}

func TestHTTPStatusCode(t *testing.T) {
	if got := httpStatusCode(statusErr(400, metav1.StatusReasonBadRequest, "x")); got != 400 {
		t.Fatalf("httpStatusCode = %d, want 400", got)
	}
	if got := httpStatusCode(errors.New("plain")); got != 0 {
		t.Fatalf("httpStatusCode(plain) = %d, want 0", got)
	}
	if got := httpStatusCode(nil); got != 0 {
		t.Fatalf("httpStatusCode(nil) = %d, want 0", got)
	}
}

func TestToCLIErrorUsage(t *testing.T) {
	cases := []string{
		"unknown command \"foo\" for \"search\"",
		"unknown flag: --bogus",
		"unknown shorthand flag: 'x' in -x",
		"accepts 1 arg(s), received 0",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			ce := toCLIError(errors.New(msg))
			if ce.code != exitUsage {
				t.Fatalf("toCLIError(%q).code = %d, want %d", msg, ce.code, exitUsage)
			}
		})
	}
}

func TestRenderExitSuccess(t *testing.T) {
	io := IOStreams{Out: &strings.Builder{}, ErrOut: &strings.Builder{}}
	if code := renderExit(io, nil); code != exitOK {
		t.Fatalf("renderExit(nil) = %d, want 0", code)
	}
}

func TestRenderExitFrame(t *testing.T) {
	var errBuf strings.Builder
	io := IOStreams{Out: &strings.Builder{}, ErrOut: &errBuf}
	code := renderExit(io, unavailableError("acme / net-core", nil))
	if code != exitUnavailable {
		t.Fatalf("code = %d, want %d", code, exitUnavailable)
	}
	out := errBuf.String()
	// Lowercase "error:" prefix, message, then an unlabeled advice line after a
	// blank line — no "Error:"/"Fix:" labels.
	if !strings.HasPrefix(out, "error: couldn't reach the search service for acme / net-core\n") {
		t.Errorf("primary line wrong:\n%q", out)
	}
	if strings.Contains(out, "Fix:") || strings.Contains(out, "Error:") {
		t.Errorf("old-frame labels leaked:\n%q", out)
	}
	if !strings.Contains(out, "\n\ncheck your connection") {
		t.Errorf("advice should follow one blank line, unlabeled:\n%q", out)
	}
	// The symbolic trailer is a --verbose-only diagnostic.
	if strings.Contains(out, "SEARCH_UNAVAILABLE") || strings.Contains(out, "exit status") {
		t.Errorf("trailer must be hidden without --verbose:\n%q", out)
	}
}

func TestRenderExitVerboseShowsTrailer(t *testing.T) {
	old := os.Args
	os.Args = []string{"milo-search", "--verbose"}
	defer func() { os.Args = old }()

	var errBuf strings.Builder
	io := IOStreams{Out: &strings.Builder{}, ErrOut: &errBuf}
	renderExit(io, unavailableError("acme / net-core", nil))
	out := errBuf.String()
	if !strings.Contains(out, "exit status 8   # SEARCH_UNAVAILABLE") {
		t.Errorf("--verbose should show the symbolic trailer:\n%q", out)
	}
}

func TestExitCodesFiveAndNineUnused(t *testing.T) {
	if _, ok := exitCodeNames[5]; ok {
		t.Error("exit code 5 must be unused")
	}
	if _, ok := exitCodeNames[9]; ok {
		t.Error("exit code 9 must be unused")
	}
}
