import json
import os
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest

import native_terminal


class TerminalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(TESTED_SHA="a" * 40, CONTROLLER_SHA="b" * 40,
                        RUN_ID="23", RUN_ATTEMPT="1", SCENARIO="row",
                        RESULT="success", TEST_OUTCOME="success", IMAGE="image",
                        EXECUTION_SOURCE="linux-runner")
        (self.root / "expected-tests.json").write_text(json.dumps({
            "selector": "^TestNative$", "expected": [["api", "TestNative"]]}))

    def log(self, text):
        (self.root / "ceph-tests.log").write_text("Running Ceph test suite: api\n" + text)

    def record(self, **changes):
        return native_terminal.record(self.env | changes, self.root)

    def test_completed(self):
        self.log("--- PASS: TestNative (1s)\n")
        result = self.record()
        self.assertTrue(result["accepted"])
        self.assertEqual(result["test_evidence"], "completed")
        self.assertEqual(result["native_cleanup"], "unverified")
        self.assertEqual(result["result_artifact_upload"], "unobserved")

    def test_cancelled_after_parent_pass_preserves_gate_without_acceptance(self):
        self.log("--- PASS: TestNative (1s)\n")
        result = self.record(TEST_OUTCOME="cancelled", RESULT="cancelled")
        self.assertTrue(result["test_gate_passed"])
        self.assertFalse(result["accepted"])
        self.assertEqual(result["test_result"], "cancelled")

    def test_no_required_verdict_cannot_pass(self):
        for text in ("=== RUN   TestNative\n", "--- PASS: TestOther (1s)\n",
                     "    --- PASS: TestNative/child (1s)\n"):
            with self.subTest(text=text):
                self.log(text)
                result = self.record()
                self.assertFalse(result["accepted"])
                self.assertEqual(result["test_result"], "unknown")
                self.assertEqual(result["result"], "failure")
                self.assertEqual(result["test_evidence"], "partial")
                for path in (self.root / "scenario-result").iterdir():
                    path.unlink()
                (self.root / "scenario-result").rmdir()

    def test_missing_log(self):
        result = self.record()
        self.assertEqual(result["test_evidence"], "missing")
        self.assertFalse(result["accepted"])

    def test_missing_snapshot_cannot_pass(self):
        (self.root / "expected-tests.json").unlink()
        self.log("--- PASS: TestNative (1s)\n")
        self.assertFalse(self.record()["accepted"])

    def test_failed_test_is_completed_evidence(self):
        self.log("--- FAIL: TestNative (1s)\n")
        result = self.record(TEST_OUTCOME="failure", RESULT="failure")
        self.assertEqual(result["test_evidence"], "completed")
        self.assertEqual(result["test_result"], "failed")
        self.assertFalse(result["accepted"])

    def test_bundle_upload_and_cleanup_are_independent(self):
        self.log("--- PASS: TestNative (1s)\n")
        result = self.record(EVIDENCE_PILOT="true",
                             EVIDENCE_COLLECTION="success", EVIDENCE_UPLOAD="failure")
        self.assertEqual(result["test_result"], "passed")
        self.assertEqual(result["evidence_result"], "incomplete")
        self.assertEqual(result["native_cleanup"], "verified")
        self.assertFalse(result["accepted"])

    def test_complete_bundle_acceptance(self):
        self.log("--- PASS: TestNative (1s)\n")
        result = self.record(EVIDENCE_PILOT="true",
                             EVIDENCE_COLLECTION="success", EVIDENCE_UPLOAD="success")
        self.assertTrue(result["accepted"])
        self.assertEqual(result["native_cleanup"], "verified")
        self.assertEqual(result["result_artifact_upload"], "unobserved")

    def test_cli_records_missing_verdict_and_exits_unsuccessfully(self):
        self.log("=== RUN   TestNative\n")
        command = subprocess.run(
            [sys.executable, str(Path(native_terminal.__file__)),
             "--directory", str(self.root)], env=os.environ | self.env,
            capture_output=True, text=True, check=False)
        self.assertEqual(command.returncode, 1, command.stderr)
        result = json.loads((self.root / "scenario-result/result.json").read_text())
        self.assertEqual(result["result"], "failure")
        self.assertFalse(result["accepted"])

    def test_preserve_first_attempt(self):
        self.log("=== RUN   TestNative\n")
        first = self.record(RESULT="cancelled", TEST_OUTCOME="cancelled")
        with self.assertRaises(FileExistsError):
            self.record(RUN_ATTEMPT="2")
        second_dir = self.root / "attempt2"
        second_dir.mkdir()
        (second_dir / "expected-tests.json").write_bytes((self.root / "expected-tests.json").read_bytes())
        (second_dir / "ceph-tests.log").write_text(
            "Running Ceph test suite: api\n--- PASS: TestNative (1s)\n")
        second = native_terminal.record(self.env | {"RUN_ATTEMPT": "2"}, second_dir)
        self.assertTrue(second["accepted"])
        self.assertFalse(first["accepted"])
        self.assertEqual(json.loads((self.root / "scenario-result/result.json").read_text()), first)

    def test_workflow_attempt_identity_and_trusted_recorder(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/native-scenarios.yml").read_text()
        self.assertLess(workflow.index("cp .trusted-native/tools/native_terminal.py"),
                        workflow.index("ref: ${{ inputs.ref }}"))
        for prefix in ("scenario", "ceph-diagnostics", "native-success"):
            self.assertIn("name: " + prefix + "-${{ github.run_id }}-${{ github.run_attempt }}-", workflow)
        record = workflow[workflow.index("      - name: Record the scenario result"):]
        self.assertIn("if: always()", record)
        self.assertIn("if-no-files-found: error", record)


if __name__ == "__main__":
    unittest.main()
