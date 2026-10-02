#!/usr/bin/env python3
"""Run curl in a detached Pod and print its persisted response, even on fast exit."""

import os
import secrets
import signal
import subprocess
import sys
import time


def main():
    pod = "curl-probe-" + secrets.token_hex(8)
    namespace = os.environ.get("NAMESPACE", "default")
    timeout = float(os.environ.get("CURL_PROBE_TIMEOUT_SECONDS", "75"))
    kubectl = ["kubectl", "--namespace", namespace, "--request-timeout=5s"]

    def run(*args, timeout=10):
        return subprocess.run(
            kubectl + list(args), stdout=subprocess.PIPE, check=True, timeout=timeout
        ).stdout

    def interrupted(signum, _frame):
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    result = 1
    try:
        # Never attach: a quick response can finish before kubectl starts streaming.
        created = run(
            "run", pod, "--image=curlimages/curl", "--restart=Never", "--attach=false",
            '--overrides={"spec":{"activeDeadlineSeconds":75}}',
            "--", "curl", "--silent", "--show-error", "--fail-with-body",
            "--connect-timeout", "10", "--max-time", "45", *sys.argv[1:]
        )
        sys.stderr.buffer.write(created)
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(f"Timed out waiting for {pod} to finish")
            state = run(
                "get", "pod", pod, "-o",
                "jsonpath={.status.phase}{' '}{.status.containerStatuses[0].state.terminated.exitCode}",
                timeout=min(10, remaining),
            ).decode().split()
            if state and state[0] in ("Succeeded", "Failed"):
                # Persisted logs remain available after termination. Keep lifecycle
                # output on stderr so Chainsaw assertions see only the response.
                logs = run("logs", pod)
                if len(state) != 2 or not state[1].isdigit():
                    sys.stderr.buffer.write(logs)
                    raise RuntimeError(f"{pod} has no terminated container exit code")
                result = int(state[1])
                if result:
                    sys.stderr.buffer.write(logs)
                    print(f"{pod}: curl exited with status {result}", file=sys.stderr)
                elif state[0] != "Succeeded":
                    sys.stderr.buffer.write(logs)
                    raise RuntimeError(f"{pod} failed despite a zero container exit code")
                else:
                    sys.stdout.buffer.write(logs)
                    sys.stdout.buffer.flush()
                break
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        result = 1
        print(f"curl probe failed: {error}", file=sys.stderr)
    finally:
        if result:
            try:
                sys.stderr.buffer.write(run("describe", "pod", pod, timeout=5))
            except (OSError, subprocess.SubprocessError) as error:
                print(f"Could not describe {pod}: {error}", file=sys.stderr)
        try:
            sys.stderr.buffer.write(run(
                "delete", "pod", pod, "--ignore-not-found=true", "--wait=false"
            ))
        except (OSError, subprocess.SubprocessError) as error:
            print(f"Could not clean up {pod}: {error}", file=sys.stderr)
            result = result or 1
    return result


if __name__ == "__main__":
    sys.exit(main())
