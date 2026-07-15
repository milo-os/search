package main

import (
	"bytes"
	"strings"
	"testing"
)

// execRoot runs the full command tree with the given args. Only exercise paths
// that do not reach the network (help, version, flag validation, reserved-flag
// rejection); the real client factory would otherwise dial a cluster.
func execRoot(args ...string) (string, string, error) {
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	root := newRootCommand(IOStreams{In: strings.NewReader(""), Out: out, ErrOut: errOut})
	root.SetOut(out)
	root.SetErr(errOut)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestRootNoArgsPrintsHelpNoError(t *testing.T) {
	out, _, err := execRoot()
	if err != nil {
		t.Fatalf("bare invocation should print help and not error, got: %v", err)
	}
	if !strings.Contains(out, "Available Commands:") {
		t.Errorf("expected help output, got:\n%s", out)
	}
}

func TestRootVersion(t *testing.T) {
	out, _, err := execRoot("version")
	if err != nil {
		t.Fatalf("version errored: %v", err)
	}
	if !strings.Contains(out, "milo-search") {
		t.Errorf("version output = %q", out)
	}
}

func TestBareArgsSugarRejectsReservedFlag(t *testing.T) {
	// The sugar form shares the query flags, so a reserved flag must be rejected
	// with a usage exit before any network call.
	_, _, err := execRoot("payments", "--filter", "spec.x==1")
	ce := toCLIError(err)
	if ce.code != exitUsage {
		t.Fatalf("code = %d, want %d", ce.code, exitUsage)
	}
	if !strings.Contains(ce.msg, "--filter") {
		t.Errorf("message should name --filter: %q", ce.msg)
	}
}

func TestInvalidOutputRejectedBeforeRun(t *testing.T) {
	_, _, err := execRoot("kinds", "-o", "bogus")
	ce := toCLIError(err)
	if ce.code != exitUsage {
		t.Fatalf("code = %d, want %d", ce.code, exitUsage)
	}
}

func TestInvalidColorRejected(t *testing.T) {
	_, _, err := execRoot("kinds", "--color", "chartreuse")
	if toCLIError(err).code != exitUsage {
		t.Fatalf("invalid --color should be a usage error")
	}
}
