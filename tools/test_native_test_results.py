import re
import tempfile
from pathlib import Path
import unittest

import native_test_results as results
import scenario_plan as plan
from test_go_test_list import SHAPES, TWICE, module, tests_file


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

    def test_sequence_check_failure_fails_the_test_before_it_and_leaves_the_rest_missing(self):
        request = {"selector": "^TestA$|^TestB$", "expected": [["api", "TestA"], ["api", "TestB"]]}
        passed = ("Running Ceph test suite: api\n--- PASS: TestA (1.00s)\n"
                  "Fixture start state after api/TestA: passed (1 polls, 300 ms)\n"
                  "Running Ceph test suite: api\n--- PASS: TestB (1.00s)\n"
                  "Fixture start state after api/TestB: passed (1 polls, 300 ms)\n")
        self.assertTrue(results.verify(request, passed)["passed"])
        stopped = ("Running Ceph test suite: api\n--- PASS: TestA (1.00s)\n"
                   "Fixture start state after api/TestA: FAILED: config: mon/mon_allow_pool_delete set, absent at start\n")
        result = results.verify(request, stopped)
        self.assertFalse(result["passed"])
        self.assertIn("api/TestA: the fixture start-state check after it failed: config: mon/mon_allow_pool_delete set, absent at start",
                      result["problems"])
        self.assertIn("api/TestB: MISSING", result["problems"])
        # A failed check after the last test fails the gate as well.
        last = passed.replace("after api/TestB: passed (1 polls, 300 ms)", "after api/TestB: FAILED: osd-flags: OSDMap flags set [noout]")
        self.assertFalse(results.verify(request, last)["passed"])

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

    def test_snapshot_refuses_a_selected_name_listed_twice(self):
        # The reviewer's case: internal and external TestActual in the API
        # suite. Without the refusal, a log of the internal test alone has one
        # PASS for api/TestActual and would satisfy the request.
        root = module(self, TWICE)
        (root / "cephmsgr").mkdir()
        (root / "cephmsgr" / "client_test.go").write_text(tests_file("func TestActual(t *testing.T) {}", package="cephmsgr"),
                                                          encoding="utf-8")
        for selector in ("^TestActual$", "^Test", "Actual"):
            with self.subTest(selector), self.assertRaisesRegex(ValueError, "TestActual is listed 2 times for integration"):
                results.snapshot(root, selector)
        output = root / "expected.json"
        self.assertEqual(results.main(["snapshot", "--root", str(root), "--selector", "^TestActual$",
                                       "--output", str(output)]), 1)
        self.assertFalse(output.exists())
        # The repeated name not selected, and the same name once per suite, are fine.
        self.assertEqual(results.snapshot(root, "^TestOther$")["expected"], [["api", "TestOther"]])

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
