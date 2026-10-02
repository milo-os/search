"""Exercise the real helper against a fake kubectl, without a Kubernetes cluster."""

import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


HELPER = Path(__file__).with_name("curl_probe.py")
FAKE_KUBECTL = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with open(os.environ["PROBE_CALLS"], "a") as calls:
    calls.write(json.dumps(args) + "\n")
command = args[3]
scenario = os.environ["PROBE_SCENARIO"]
if command == "run":
    print("pod created")
    if scenario == "create_failure":
        sys.exit(1)
elif command == "get":
    if scenario == "pending":
        print("Pending")
    elif scenario == "failed_with_zero_exit":
        print("Failed 0")
    elif scenario == "missing_exit":
        print("Succeeded")
    elif scenario in ("curl_failure", "curl_and_cleanup_failure"):
        print("Failed 22")
    elif scenario == "get_failure":
        sys.exit(1)
    else:
        print("Succeeded 0")
elif command == "logs":
    if scenario == "logs_failure":
        sys.exit(1)
    sys.stdout.write('{"hits":[{"name":"expected-resource"}]}')
elif command == "describe":
    print("Pod diagnostics")
elif command == "delete":
    print("pod deleted")
    if scenario in ("cleanup_failure", "curl_and_cleanup_failure"):
        sys.exit(1)
else:
    raise AssertionError(args)
'''


class CurlProbeTest(unittest.TestCase):
    def run_probe(self, scenario, interrupt=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kubectl = root / "kubectl"
            kubectl.write_text(FAKE_KUBECTL)
            kubectl.chmod(0o755)
            calls = root / "calls.jsonl"
            env = {
                **os.environ,
                "PATH": directory + os.pathsep + os.environ["PATH"],
                "NAMESPACE": "probe-test",
                "CURL_PROBE_TIMEOUT_SECONDS": "0.2" if scenario == "pending" and not interrupt else "5",
                "PROBE_SCENARIO": scenario,
                "PROBE_CALLS": str(calls),
            }
            args = [sys.executable, str(HELPER), "-H", "X-Test: a b", "http://example.test"]
            if interrupt:
                with subprocess.Popen(args, env=env, text=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE) as process:
                    try:
                        deadline = time.monotonic() + 3
                        while not (calls.exists() and '"get"' in calls.read_text()):
                            self.assertLess(time.monotonic(), deadline, "Probe did not start polling")
                            time.sleep(0.02)
                        process.send_signal(signal.SIGTERM)
                        stdout, stderr = process.communicate(timeout=10)
                        result = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
                    finally:
                        if process.poll() is None:
                            process.kill()
            else:
                result = subprocess.run(args, env=env, text=True, capture_output=True, timeout=10)
            commands = [json.loads(line) for line in calls.read_text().splitlines()]
        self.assertEqual(commands[-1][3], "delete", commands)
        self.assertIn("--wait=false", commands[-1])
        self.assertTrue(all(c[:3] == ["--namespace", "probe-test", "--request-timeout=5s"] for c in commands))
        pod = commands[0][4]
        self.assertTrue(pod.startswith("curl-probe-"))
        self.assertEqual(commands[-1][5], pod)
        return result, commands

    def test_already_completed_pod_has_exact_response(self):
        result, commands = self.run_probe("success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '{"hits":[{"name":"expected-resource"}]}')
        self.assertEqual([c[3] for c in commands], ["run", "get", "logs", "delete"])
        self.assertIn("--attach=false", commands[0])
        self.assertIn('--overrides={"spec":{"activeDeadlineSeconds":75}}', commands[0])
        self.assertIn("--fail-with-body", commands[0])
        self.assertIn("--max-time", commands[0])
        self.assertIn("X-Test: a b", commands[0])
        self.assertIn("pod created", result.stderr)
        self.assertIn("pod deleted", result.stderr)

    def test_matching_response_cannot_hide_curl_failure(self):
        result, _ = self.run_probe("curl_failure")
        self.assertEqual(result.returncode, 22, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn("expected-resource", result.stderr)
        self.assertIn("Pod diagnostics", result.stderr)

    def test_pending_pod_times_out_and_is_removed(self):
        result, commands = self.run_probe("pending")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Timed out waiting", result.stderr)
        self.assertNotIn("logs", [c[3] for c in commands])

    def test_interrupted_probe_cleans_up_and_preserves_signal_status(self):
        result, _ = self.run_probe("pending", interrupt=True)
        self.assertEqual(result.returncode, 128 + signal.SIGTERM)
        self.assertEqual(result.stdout, "")

    def test_log_read_failure_is_not_success(self):
        result, _ = self.run_probe("logs_failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")

    def test_missing_exit_code_is_not_success(self):
        result, _ = self.run_probe("missing_exit")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no terminated container exit code", result.stderr)

    def test_failed_phase_with_zero_exit_is_not_success(self):
        result, _ = self.run_probe("failed_with_zero_exit")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("failed despite a zero container exit code", result.stderr)

    def test_failed_create_still_attempts_cleanup(self):
        result, _ = self.run_probe("create_failure")
        self.assertNotEqual(result.returncode, 0)

    def test_failed_status_read_still_attempts_cleanup(self):
        result, _ = self.run_probe("get_failure")
        self.assertNotEqual(result.returncode, 0)

    def test_cleanup_failure_does_not_mask_curl_failure(self):
        result, _ = self.run_probe("curl_and_cleanup_failure")
        self.assertEqual(result.returncode, 22)
        self.assertIn("Could not clean up", result.stderr)

    def test_cleanup_failure_fails_successful_probe(self):
        result, _ = self.run_probe("cleanup_failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Could not clean up", result.stderr)


if __name__ == "__main__":
    unittest.main()
