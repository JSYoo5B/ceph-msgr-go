import re
import tempfile
from pathlib import Path
import unittest

import native_test_results as results
import scenario_plan as plan
from test_go_test_list import SHAPES, module, tests_file


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
        root = module(self, {"new_test.go": tests_file("func TestNew(t *testing.T) {}")})
        client = root / "cephmsgr"
        client.mkdir()
        (client / "client_test.go").write_text(tests_file("func TestNewClient(t *testing.T) {}", package="cephmsgr"),
                                               encoding="utf-8")
        self.assertEqual(results.snapshot(root, "^TestNew$")["expected"], [["api", "TestNew"]])
        self.assertEqual(results.snapshot(root, "^TestNew")["expected"], [["api", "TestNew"], ["client", "TestNewClient"]])
        for selector in ("^TestAbsent$", "TestNew/subtest"):
            with self.assertRaises(ValueError):
                results.snapshot(root, selector)

    def test_snapshot_selects_the_tests_go_lists(self):
        root = module(self, SHAPES)
        (root / "cephmsgr").mkdir()
        (root / "cephmsgr" / "doc.go").write_text("package cephmsgr\n", encoding="utf-8")
        # Literal expectation: TestMain, the method, the lowercase helper and the
        # build-constrained test are not selectable; the bare Test and the
        # aliased-import test are.
        self.assertEqual(results.snapshot(root, "^Test")["expected"], [["api", "Test"], ["api", "TestAlias"], ["api", "TestX"]])
        for selector in ("^TestMain$", "^TestY$", "^Testable$", "^TestIgnored$"):
            with self.subTest(selector), self.assertRaises(ValueError):
                results.snapshot(root, selector)
        request = results.snapshot(root, "^(Test|TestAlias)$")
        log = "Running Ceph test suite: api\n--- PASS: Test (0.00s)\n--- PASS: TestAlias (0.00s)\n"
        self.assertTrue(results.verify(request, log)["passed"])
        self.assertIn("api/TestAlias: MISSING", results.verify(request, log.replace("--- PASS: TestAlias (0.00s)\n", ""))["problems"])
        self.assertIn("api/Test: MISSING", results.verify(request, log.replace("--- PASS: Test (0.00s)\n", ""))["problems"])

    def test_broad_snapshot_needs_no_listing(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(results.snapshot(root, ""), {"selector": "", "expected": []})
            with self.assertRaises(ValueError):
                results.snapshot(root, "^TestAny$")

    def test_compression_catalog_selection_requires_its_actual_api_parent_pass(self):
        selector = "^TestCephMsgrCompressionOfferIntegration$"
        rows = [row for row in plan.load_catalog() if row["params"].get("test_run") == selector]
        self.assertEqual(len(rows), 1)
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


    def test_workflow_copies_the_gate_and_its_local_imports_before_tested_code(self):
        # The gate runs from $RUNNER_TEMP, so every repository module it imports
        # must be copied from the trusted checkout beside it.
        text = plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        tested_checkout = text.index("ref: ${{ inputs.ref }}")
        source = Path(results.__file__).read_text(encoding="utf-8")
        local = [name for name in re.findall(r"(?m)^import (\w+)$", source) if (plan.ROOT / "tools" / (name + ".py")).exists()]
        self.assertIn("go_test_list", local)
        for name in ["native_test_results"] + local:
            copy = 'cp .trusted-native/tools/%s.py "$RUNNER_TEMP/%s.py"' % (name, name)
            self.assertLess(text.index(copy), tested_checkout, name)


if __name__ == "__main__":
    unittest.main()
