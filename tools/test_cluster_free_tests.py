import os
import re
import subprocess
from pathlib import Path
import unittest

import cluster_free_tests as tests
from test_go_test_list import SHAPES, TWICE, module, tests_file

ROOT = Path(__file__).resolve().parent.parent


class ClusterFreeTestsTests(unittest.TestCase):
    def fixture(self, *bodies):
        return module(self, {"case%d_test.go" % index: tests_file(body) for index, body in enumerate(bodies)})

    def run_go(self, root, selector):
        """The verbose log of Go actually running the selected tests."""
        return subprocess.run(["go", "test", "-mod=readonly", "-count=1", "-v", "-run", selector, "./integration"],
                              cwd=root, env=dict(os.environ, GOWORK="off", CGO_ENABLED="0"),
                              capture_output=True, text=True, timeout=600, check=True).stdout

    def test_repository_tests_are_classified_by_the_name_rule(self):
        self.assertEqual(tests.check(ROOT), [])
        pattern = re.compile(tests.selector(ROOT))
        for name in tests.declared(ROOT):
            self.assertEqual(bool(pattern.fullmatch(name)), not tests.NATIVE.fullmatch(name), name)

    def test_name_rule_classifies_without_a_list(self):
        bodies = ("func TestCephNativeIntegration(t *testing.T) {}", "func TestCephFSHelper(t *testing.T) {}",
                  "func TestHelper(t *testing.T) {}")
        root = self.fixture(*bodies)
        self.assertEqual(tests.check(root), [])
        # A TestCeph-prefixed helper without the Integration suffix is cluster-free.
        self.assertEqual(tests.cluster_free(root), ["TestCephFSHelper", "TestHelper"])
        added = self.fixture(*bodies + ("func TestNew(t *testing.T) {}",))
        self.assertEqual(tests.cluster_free(added), ["TestCephFSHelper", "TestHelper", "TestNew"])

    def test_go_discovery_shapes_reach_the_selector_and_the_gate(self):
        native = tests_file("func TestCephNativeIntegration(t *testing.T) {}")
        root = module(self, dict(SHAPES, **{"native_test.go": native}))
        # Literal expectation, not the helper's own output: the bare Test, the
        # aliased import and the blank parameter are tests; TestMain, the
        # method, the lowercase helper and the ignored file are not.
        self.assertEqual(tests.cluster_free(root), ["Test", "TestAlias", "TestX"])
        selector = tests.selector(root)
        self.assertEqual(selector, "^(Test|TestAlias|TestX)$")
        log = self.run_go(root, selector)
        self.assertEqual(tests.verify(root, log), [])
        # The reviewer's counterexamples: a log without the aliased-import test
        # or without the bare Test fails the gate.
        for name in ("TestAlias", "Test"):
            missing = re.sub(r"(?m)^--- PASS: %s \(.*\)\n" % name, "", log)
            self.assertNotEqual(missing, log, name)
            self.assertIn("%s: MISSING" % name, tests.verify(root, missing))
        self.assertIn("test outside the cluster-free set ran: TestCephNativeIntegration",
                      tests.verify(root, log + "\n--- PASS: TestCephNativeIntegration (0.00s)\n"))
        self.assertIn("test outside the cluster-free set ran: TestIgnored",
                      tests.verify(root, log + "\n--- PASS: TestIgnored (0.00s)\n"))

    def test_a_name_listed_twice_is_refused_before_any_result(self):
        # The reviewer's case: internal and external TestActual. Go runs both
        # under one name; a log of only the internal file has one PASS.
        root = module(self, TWICE)
        env = dict(os.environ, GOWORK="off", CGO_ENABLED="0")
        internal = subprocess.run(["go", "test", "-mod=readonly", "-count=1", "-v", "-run", "^(TestActual|TestOther)$",
                                   "./integration/internal_test.go", "./integration/doc.go"],
                                  cwd=root, env=env, capture_output=True, text=True, timeout=600, check=True).stdout
        self.assertEqual(re.findall(r"(?m)^--- PASS: (\w+) ", internal), ["TestActual", "TestOther"])
        full = self.run_go(root, "^(TestActual|TestOther)$")
        self.assertEqual(re.findall(r"(?m)^--- PASS: (\w+) ", full).count("TestActual"), 2)
        for log in (internal, full, ""):
            with self.assertRaisesRegex(ValueError, "TestActual is listed 2 times for integration"):
                tests.verify(root, log)
        with self.assertRaisesRegex(ValueError, "TestActual is listed 2 times"):
            tests.check(root)
        path = root / "go-test.log"
        path.write_text(internal, encoding="utf-8")
        for command in (["check"], ["selector"], ["verify", "--log", str(path)]):
            self.assertEqual(tests.main(["--root", str(root)] + command), 1, command)
        # A native name listed twice is not cluster-free and does not block the gate.
        native = module(self, {"internal_test.go": tests_file("func TestCephXIntegration(t *testing.T) {}",
                                                              "func TestOther(t *testing.T) {}"),
                               "external_test.go": tests_file("func TestCephXIntegration(t *testing.T) {}",
                                                              package="integration_test")})
        self.assertEqual(tests.cluster_free(native), ["TestOther"])
        self.assertEqual(tests.verify(native, "--- PASS: TestOther (0.00s)\n"), [])

    def test_unlistable_and_empty_sets_fail(self):
        duplicate = self.fixture("func TestHelper(t *testing.T) {}", "func TestHelper(t *testing.T) {}")
        with self.assertRaises(ValueError):
            tests.check(duplicate)
        self.assertEqual(tests.main(["--root", str(duplicate), "check"]), 1)
        native_only = self.fixture("func TestCephOnlyIntegration(t *testing.T) {}")
        self.assertIn("no cluster-free top-level test", tests.check(native_only))
        self.assertIn("no cluster-free top-level test", tests.verify(native_only, "PASS\n"))

    def test_selector_is_anchored_to_cluster_free_names(self):
        root = self.fixture("func TestA(t *testing.T) {}", "func TestAB(t *testing.T) {}")
        pattern = re.compile(tests.selector(root))
        self.assertTrue(pattern.search("TestA") and pattern.search("TestAB"))
        self.assertIsNone(pattern.search("TestABC"))

    def test_gate_requires_a_top_level_pass_for_every_cluster_free_test(self):
        root = self.fixture("func TestA(t *testing.T) {}", "func TestB(t *testing.T) {}",
                            "func TestCephNativeIntegration(t *testing.T) {}")
        passing = "--- PASS: TestA (0.00s)\n--- PASS: TestB (0.01s)\nPASS\nok  \tpkg\t0.1s\n"
        self.assertEqual(tests.verify(root, passing), [])
        for text in ("PASS\n",
                     passing.replace("--- PASS: TestB (0.01s)\n", ""),
                     passing.replace("PASS: TestB", "SKIP: TestB"),
                     passing.replace("PASS: TestB", "FAIL: TestB"),
                     passing.replace("--- PASS: TestB", "    --- PASS: TestB"),
                     passing.replace("--- PASS: TestB (0.01s)", "--- PASS: TestB ("),
                     passing + "--- PASS: TestA (0.00s)\n",
                     passing + "--- PASS: TestCephNativeIntegration (0.00s)\n",
                     passing + "--- PASS: TestUndeclared (0.00s)\n"):
            self.assertNotEqual(tests.verify(root, text), [], text)

    def test_command_line_exit_status(self):
        root = self.fixture("func TestA(t *testing.T) {}")
        log = root / "go-test.log"
        log.write_text("--- PASS: TestA (0.00s)\n", encoding="utf-8")
        self.assertEqual(tests.main(["--root", str(root), "check"]), 0)
        self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(log)]), 0)
        for text in ("--- SKIP: TestA (0.00s)\n", "--- FAIL: TestA (0.00s)\n", "ok  \tpkg\t0.1s\n"):
            log.write_text(text, encoding="utf-8")
            self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(log)]), 1, text)
        self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(root / "absent")]), 1)


if __name__ == "__main__":
    unittest.main()
