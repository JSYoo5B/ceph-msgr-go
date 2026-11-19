import re
import subprocess
import tempfile
from pathlib import Path
import unittest

import native_test_results as results
import rgw_native_gate as gate
import scenario_plan as plan
from test_go_test_list import SHAPES, TWICE, module, tests_file
from test_rgw_native_gate import stream as rgw_stream


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
        # A sequence snapshot requires a passed check after every test: a
        # fixture that ran the union without checks cannot pass.
        sequenced = dict(request, sequence=True)
        self.assertTrue(results.verify(sequenced, passed)["passed"])
        unchecked = passed.replace("Fixture start state after api/TestA: passed (1 polls, 300 ms)\n", "")
        self.assertTrue(results.verify(request, unchecked)["passed"])
        self.assertIn("api/TestA: no passed fixture start-state check after it", results.verify(sequenced, unchecked)["problems"])
        with self.assertRaises(ValueError):
            results.snapshot(".", "", sequence=True)
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
        self.assertIn('native_terminal.py" --directory "$RUNNER_TEMP"', text)


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


def git(root, *arguments):
    return subprocess.run(["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@example.invalid"] + list(arguments),
                          check=True, capture_output=True, text=True).stdout.strip()


class RoutedRGWResultTests(unittest.TestCase):
    """The rgw-fixture backend of rows with rgw_native=1."""

    def checkout(self, names=(gate.CHECKPOINT, gate.MULTISTRIPE)):
        """A committed Git tree whose fixture module declares names; returns (root, HEAD)."""
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        root = Path(holder.name)
        fixture = root / results.RGW_SUITE[1]
        fixture.mkdir(parents=True)
        (fixture / "go.mod").write_text("module example.invalid/fixture\n\ngo 1.24\n", encoding="utf-8")
        (fixture / "main.go").write_text("package main\n\nfunc main() {}\n", encoding="utf-8")
        bodies = ["func %s(t *testing.T) {}" % name for name in names] + ["func TestRGWNativeHelper(t *testing.T) {}"]
        (fixture / "rgw_test.go").write_text(tests_file(*bodies, package="main"), encoding="utf-8")
        git(root, "init", "-q")
        git(root, "add", ".")
        git(root, "commit", "-q", "-m", "fixture")
        return root, git(root, "rev-parse", "HEAD")

    def stream_path(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        return str(Path(holder.name) / "rgw-native.jsonl")

    def request(self, test=gate.CHECKPOINT):
        return {"selector": "^%s$" % test, "expected": [["fixture", test]], "backend": results.RGW_BACKEND,
                "revision": "a" * 40, "stream": "/nonexistent/rgw-native.jsonl"}

    def log(self, test=gate.CHECKPOINT, result="PASS"):
        return "Running Ceph test suite: fixture\n=== RUN   %s\n--- %s: %s (33.39s)\nPASS\n" % (test, result, test)

    def test_snapshot_binds_one_known_checkpoint_the_revision_and_a_new_stream(self):
        root, head = self.checkout()
        stream = self.stream_path()
        for test in (gate.CHECKPOINT, gate.MULTISTRIPE):
            request = results.snapshot(root, "^%s$" % test, backend=results.RGW_BACKEND, revision=head, stream=stream)
            self.assertEqual(request, {"selector": "^%s$" % test, "expected": [["fixture", test]],
                                       "backend": results.RGW_BACKEND, "revision": head, "stream": stream})

    def test_snapshot_refuses_unsupported_backends_tests_and_shapes(self):
        root, head = self.checkout()
        stream = self.stream_path()
        refused = {
            "unknown backend": dict(selector="^%s$" % gate.CHECKPOINT, backend="rgw", revision=head, stream=stream),
            "unknown test": dict(selector="^TestRGWNativeHelper$", backend=results.RGW_BACKEND, revision=head, stream=stream),
            "unanchored test": dict(selector=gate.CHECKPOINT, backend=results.RGW_BACKEND, revision=head, stream=stream),
            "both tests": dict(selector="^%s$|^%s$" % (gate.CHECKPOINT, gate.MULTISTRIPE), backend=results.RGW_BACKEND,
                               revision=head, stream=stream),
            "broad selector": dict(selector="", backend=results.RGW_BACKEND, revision=head, stream=stream),
            "sequence": dict(selector="^%s$" % gate.CHECKPOINT, sequence=True, backend=results.RGW_BACKEND,
                             revision=head, stream=stream),
            "missing revision": dict(selector="^%s$" % gate.CHECKPOINT, backend=results.RGW_BACKEND, stream=stream),
            "wrong revision": dict(selector="^%s$" % gate.CHECKPOINT, backend=results.RGW_BACKEND, revision="b" * 40,
                                   stream=stream),
            "short revision": dict(selector="^%s$" % gate.CHECKPOINT, backend=results.RGW_BACKEND, revision=head[:12],
                                   stream=stream),
            "relative stream": dict(selector="^%s$" % gate.CHECKPOINT, backend=results.RGW_BACKEND, revision=head,
                                    stream="rgw-native.jsonl"),
            "revision without backend": dict(selector="^%s$" % gate.CHECKPOINT, revision=head),
        }
        for name, arguments in refused.items():
            with self.subTest(name), self.assertRaises(ValueError):
                results.snapshot(root, **arguments)

    def test_snapshot_refuses_a_changed_fixture_module_an_old_stream_and_a_missing_test(self):
        root, head = self.checkout()
        arguments = dict(backend=results.RGW_BACKEND, revision=head)
        selector = "^%s$" % gate.CHECKPOINT
        untracked = root / results.RGW_SUITE[1] / "extra_test.go"
        untracked.write_text("package main\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "differs from the tested revision"):
            results.snapshot(root, selector, stream=self.stream_path(), **arguments)
        untracked.unlink()
        stream = self.stream_path()
        Path(stream).write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "already exists"):
            results.snapshot(root, selector, stream=stream, **arguments)
        root, head = self.checkout(names=(gate.MULTISTRIPE,))
        with self.assertRaisesRegex(ValueError, "does not list"):
            results.snapshot(root, selector, backend=results.RGW_BACKEND, revision=head, stream=self.stream_path())

    def test_verdict_requires_the_text_pass_and_the_rgw_gate(self):
        for test in (gate.CHECKPOINT, gate.MULTISTRIPE):
            with self.subTest(test):
                verdict = results.verify(self.request(test), self.log(test), stream=rgw_stream(test), head="a" * 40)
                self.assertTrue(verdict["passed"], verdict["problems"])
                self.assertEqual(verdict["observed"], [{"suite": "fixture", "test": test, "result": "PASS"}])
                self.assertTrue(verdict["rgw_gate"]["passed"])
                self.assertEqual(verdict["backend"], results.RGW_BACKEND)

    def test_verdict_refuses_skipped_missing_duplicate_and_foreign_targets(self):
        test = gate.CHECKPOINT
        good = rgw_stream(test)
        skipped = rgw_stream(test, passes=0, extra=[{"Action": "skip", "Test": test}], line="", cleanup=False)
        cases = {
            "skipped stream and log": (self.log(test, "SKIP"), skipped, "a" * 40),
            "skipped stream": (self.log(test), skipped, "a" * 40),
            "missing log result": ("Running Ceph test suite: fixture\nPASS\n", good, "a" * 40),
            "missing stream": (self.log(test), None, "a" * 40),
            "duplicate log result": (self.log(test) + "--- PASS: %s (1.00s)\n" % test, good, "a" * 40),
            "duplicate stream pass": (self.log(test), rgw_stream(test, passes=2), "a" * 40),
            "other checkpoint": (self.log(gate.MULTISTRIPE), rgw_stream(gate.MULTISTRIPE), "a" * 40),
            "product suite result": (self.log(test) + "Running Ceph test suite: api\n--- PASS: TestCephAIntegration (1.00s)\n",
                                     good, "a" * 40),
            "foreign package": (self.log(test), rgw_stream(test, terminal="example.org/unrelated"), "a" * 40),
            "no cleanup record": (self.log(test), rgw_stream(test, cleanup=False), "a" * 40),
            "revision changed": (self.log(test), good, "b" * 40),
            "not a test2json stream": (self.log(test), "go: downloading\n", "a" * 40),
        }
        for name, (log, stream, head) in cases.items():
            with self.subTest(name):
                verdict = results.verify(self.request(test), log, stream=stream, head=head)
                self.assertFalse(verdict["passed"])

    def test_verdict_reads_the_stream_file_and_refuses_unknown_backends(self):
        request = dict(self.request(), stream=self.stream_path())
        self.assertFalse(results.verify(request, self.log())["passed"])
        Path(request["stream"]).write_text(rgw_stream(gate.CHECKPOINT), encoding="utf-8")
        self.assertTrue(results.verify(request, self.log())["passed"])
        self.assertFalse(results.verify(dict(request, backend="rgw"), self.log())["passed"])

    def test_product_requests_refuse_fixture_results(self):
        request = {"selector": "^TestRequired$", "expected": [["api", "TestRequired"]]}
        text = "Running Ceph test suite: api\n--- PASS: TestRequired (1.00s)\n"
        self.assertTrue(results.verify(request, text)["passed"])
        self.assertNotIn("backend", results.verify(request, text))
        verdict = results.verify(request, text + self.log())
        self.assertFalse(verdict["passed"])
        self.assertIn("fixture/%s: a fixture-module result in a product-suite request" % gate.CHECKPOINT, verdict["problems"])

    def test_cli_rechecks_the_revision_after_the_run(self):
        root, head = self.checkout()
        stream = self.stream_path()
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        scratch = Path(holder.name)
        expected = scratch / "expected.json"
        self.assertEqual(results.main(["snapshot", "--root", str(root), "--selector", "^%s$" % gate.CHECKPOINT,
                                       "--backend", results.RGW_BACKEND, "--revision", head, "--stream", stream,
                                       "--output", str(expected)]), 0)
        Path(stream).write_text(rgw_stream(gate.CHECKPOINT), encoding="utf-8")
        (scratch / "log.txt").write_text(self.log(), encoding="utf-8")
        arguments = ["verify", "--expected", str(expected), "--log", str(scratch / "log.txt"),
                     "--output", str(scratch / "tests.json"), "--root", str(root)]
        self.assertEqual(results.main(arguments), 0)
        git(root, "commit", "-q", "--allow-empty", "-m", "moved")
        self.assertEqual(results.main(arguments), 1)


if __name__ == "__main__":
    unittest.main()
