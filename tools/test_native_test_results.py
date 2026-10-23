import tempfile
from pathlib import Path
import unittest

import native_test_results as results
import scenario_plan as plan


class NativeResultTests(unittest.TestCase):
    def request(self):
        return {"selector": "^TestRequired$", "expected": [["api", "TestRequired"]]}

    def test_successful_skip_and_empty_process_are_not_execution(self):
        for text in ("PASS\n", "Running Ceph test suite: api\n--- SKIP: TestRequired (0.00s)\nPASS\n"):
            self.assertFalse(results.verify(self.request(), text)["passed"])

    def test_empty_secondary_suite_is_allowed_but_not_missing_primary_test(self):
        text = "Running Ceph test suite: api\n--- PASS: TestRequired (1.00s)\nRunning Ceph test suite: client\nPASS\n"
        self.assertTrue(results.verify(self.request(), text)["passed"])
        self.assertFalse(results.verify(self.request(), text.replace("TestRequired", "TestOther"))["passed"])

    def test_subtests_partial_lines_failures_and_duplicates_cannot_satisfy_gate(self):
        for line in ("    --- PASS: TestRequired/subtest (0.00s)",
                     "--- PASS: TestRequired (", "--- FAIL: TestRequired (0.00s)"):
            self.assertFalse(results.verify(self.request(), "Running Ceph test suite: api\n" + line)["passed"])
        line = "--- PASS: TestRequired (0.00s)\n"
        self.assertFalse(results.verify(self.request(), "Running Ceph test suite: api\n" + line * 2)["passed"])

    def test_every_selected_test_and_package_identity_matter(self):
        request = self.request()
        request["expected"].append(["client", "TestRequired"])
        text = "Running Ceph test suite: api\n--- PASS: TestRequired (0.00s)\n"
        self.assertFalse(results.verify(request, text)["passed"])
        self.assertTrue(results.verify(request, text + "Running Ceph test suite: client\n--- PASS: TestRequired (0.00s)\n")["passed"])

    def test_default_profile_records_skips_without_requiring_optional_tests(self):
        request = {"selector": "", "expected": []}
        text = "Running Ceph test suite: api\n--- PASS: TestNative (0.00s)\n--- SKIP: TestOptional (0.00s)\n"
        verdict = results.verify(request, text)
        self.assertTrue(verdict["passed"])
        self.assertEqual(verdict["observed"][1]["result"], "SKIP")
        self.assertFalse(results.verify(request, text.replace("PASS", "SKIP"))["passed"])

    def test_snapshot_uses_tested_source_and_rejects_missing_or_subtest_selector(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "integration"
            directory.mkdir()
            (directory / "new_test.go").write_text("func TestNew(t *testing.T) {}\n", encoding="utf-8")
            self.assertEqual(results.snapshot(root, "^TestNew$")["expected"], [["api", "TestNew"]])
            for selector in ("^TestAbsent$", "TestNew/subtest"):
                with self.assertRaises(ValueError):
                    results.snapshot(root, selector)

    def test_compression_catalog_selection_requires_its_actual_api_parent_pass(self):
        selector = "^TestCephMsgrCompressionOfferIntegration$"
        rows = [row for row in plan.load_catalog() if row["params"].get("test_run") == selector]
        self.assertEqual(len(rows), 1)
        self.assertIn(rows[0]["id"], plan.load_mandatory())
        request = results.snapshot(plan.ROOT, rows[0]["params"]["test_run"])
        name = "TestCephMsgrCompressionOfferIntegration"
        self.assertEqual(request["expected"], [["api", name]])
        self.assertTrue(results.verify(request, "Running Ceph test suite: api\n--- PASS: %s (1.00s)\n" % name)["passed"])
        for text in (
            "PASS\n",
            "Running Ceph test suite: api\n--- SKIP: %s (0.00s)\nPASS\n" % name,
            "Running Ceph test suite: api\n--- PASS: TestCephOSDIOIntegration (1.00s)\n",
            "Running Ceph test suite: client\n--- PASS: %s (1.00s)\n" % name,
            "Running Ceph test suite: api\n    --- PASS: %s/child (1.00s)\n" % name,
        ):
            with self.subTest(text=text):
                self.assertFalse(results.verify(request, text)["passed"])

    def test_trusted_workflow_captures_expectation_then_gates_process_success(self):
        text = plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        self.assertLess(text.index("ref: ${{ github.workflow_sha }}"), text.index("ref: ${{ inputs.ref }}"))
        self.assertLess(text.index("native_test_results.py\" snapshot"), text.index("sh integration/run.sh"))
        self.assertLess(text.index("sh integration/run.sh"), text.index("native_test_results.py\" verify"))
        self.assertIn('if test "$result" -eq 0; then result=1; fi', text)
        self.assertIn("scenario-result/tests.json", text)


if __name__ == "__main__":
    unittest.main()
