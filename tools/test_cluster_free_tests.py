import re
import tempfile
from pathlib import Path
import unittest

import cluster_free_tests as tests

ROOT = Path(__file__).resolve().parent.parent


class ClusterFreeTestsTests(unittest.TestCase):
    def fixture(self, sources):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "integration").mkdir()
        for index, body in enumerate(sources):
            (root / "integration" / ("case%d_test.go" % index)).write_text(
                "package integration_test\n\n" + body, encoding="utf-8")
        return root

    def test_repository_tests_are_classified_by_the_name_rule(self):
        self.assertEqual(tests.check(ROOT), [])
        pattern = re.compile(tests.selector(ROOT))
        for name in tests.declared(ROOT):
            self.assertEqual(bool(pattern.fullmatch(name)), not tests.NATIVE.fullmatch(name), name)

    def test_name_rule_classifies_without_a_list(self):
        source = ("func TestCephNativeIntegration(t *testing.T) {}\nfunc TestCephFSHelper(t *testing.T) {}\n"
                  "func TestHelper(t *testing.T) {}\nfunc (s suite) TestMethod(t *testing.T) {}\n")
        root = self.fixture([source])
        self.assertEqual(tests.check(root), [])
        # A TestCeph-prefixed helper without the Integration suffix is cluster-free.
        self.assertEqual(tests.cluster_free(root), ["TestCephFSHelper", "TestHelper"])
        added = self.fixture([source + "func TestNew(t *testing.T) {}\n"])
        self.assertEqual(tests.cluster_free(added), ["TestCephFSHelper", "TestHelper", "TestNew"])

    def test_duplicate_and_empty_sets_fail(self):
        cases = [
            ("duplicate top-level", ["func TestHelper(t *testing.T) {}\n", "func TestHelper(t *testing.T) {}\n"]),
            ("no cluster-free", ["func TestCephOnlyIntegration(t *testing.T) {}\n"]),
        ]
        for want, sources in cases:
            root = self.fixture(sources)
            problems = tests.check(root)
            self.assertTrue(any(want in problem for problem in problems), (want, problems))
            self.assertTrue(any(want in problem for problem in tests.verify(root, "PASS\n")), want)

    def test_selector_is_anchored_to_cluster_free_names(self):
        root = self.fixture(["func TestA(t *testing.T) {}\nfunc TestAB(t *testing.T) {}\n"])
        pattern = re.compile(tests.selector(root))
        self.assertTrue(pattern.search("TestA") and pattern.search("TestAB"))
        self.assertIsNone(pattern.search("TestABC"))

    def test_gate_requires_a_top_level_pass_for_every_cluster_free_test(self):
        root = self.fixture(["func TestA(t *testing.T) {}\nfunc TestB(t *testing.T) {}\n"
                             "func TestCephNativeIntegration(t *testing.T) {}\n"])
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
        root = self.fixture(["func TestA(t *testing.T) {}\n"])
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
