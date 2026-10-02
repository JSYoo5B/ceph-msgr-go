import io
import unittest

from ci_failure_annotations import annotations


class FailureAnnotationsTest(unittest.TestCase):
    def test_completed_failure_keeps_only_current_test_diagnostics(self):
        log = io.StringIO("=== RUN   TestEarlier\n    old_test.go:1: old result\n"
                          "--- PASS: TestEarlier (1s)\n=== RUN   TestCurrent\n"
                          "    current_test.go:42: server rejected command\n"
                          "--- FAIL: TestCurrent (1s)\nFAIL\n")
        self.assertEqual(annotations(log, 1), [
            "::error::--- FAIL: TestCurrent (1s)",
            "::error file=current_test.go,line=42::server rejected command",
        ])

    def test_abrupt_exit_reports_last_test_and_bounded_progress(self):
        lines = ["=== RUN   TestEarlier\n", "    old_test.go:1: old result\n",
                 "--- PASS: TestEarlier (1s)\n", "=== RUN   TestStress\n"]
        lines.extend(f"    stress_test.go:12: calls={i}\n" for i in range(12))
        result = annotations(lines, 137)
        self.assertEqual(len(result), 11)
        self.assertIn("code 137", result[0])
        self.assertIn("Last started Go test: TestStress", result[0])
        self.assertEqual(result[1], "::error file=stress_test.go,line=12::calls=2")
        self.assertEqual(result[-1], "::error file=stress_test.go,line=12::calls=11")
        self.assertNotIn("old result", "\n".join(result))
        self.assertNotIn("OOM", "\n".join(result))

    def test_bootstrap_failure_does_not_invent_a_running_test(self):
        result = annotations(["docker pull failed\n"], 125)
        self.assertEqual(len(result), 1)
        self.assertIn("code 125", result[0])
        self.assertIn("No Go test start was observed", result[0])

    def test_suite_paths_and_diagnostics_follow_the_current_package(self):
        log = ["Running Ceph test suite: api\n", "=== RUN   TestPublic\n",
               "    commands_test.go:42: public command failed\n", "--- FAIL: TestPublic\n",
               "Running Ceph test suite: client\n", "=== RUN   TestPrivate\n",
               "    integration_pause_test.go:7: internal session failed\n", "--- FAIL: TestPrivate\n"]
        result = annotations(log, 1)
        self.assertEqual(result[1], "::error file=integration/commands_test.go,line=42::public command failed")
        self.assertEqual(result[3], "::error file=integration_pause_test.go,line=7::internal session failed")
        abrupt = annotations(log[:3] + ["--- PASS: TestPublic\n", "Running Ceph test suite: client\n"], 137)
        self.assertEqual(len(abrupt), 1)
        self.assertIn("No Go test start was observed", abrupt[0])

    def test_success_does_not_emit_failure_annotations(self):
        self.assertEqual(annotations(["=== RUN   TestOK\n--- PASS: TestOK\n"], 0), [])

    def test_diagnostic_workflow_command_characters_are_escaped(self):
        log = ["=== RUN   TestFailure\n", "    file_test.go:7: 50%\rnext\n",
               "--- FAIL: TestFailure\n"]
        self.assertEqual(annotations(log, 1)[1], "::error file=file_test.go,line=7::50%25%0Dnext")


if __name__ == "__main__":
    unittest.main()
