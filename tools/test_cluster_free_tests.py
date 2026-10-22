import re
import tempfile
from pathlib import Path
import unittest

import cluster_free_tests as tests

ROOT = Path(__file__).resolve().parent.parent


class ClusterFreeTestsTests(unittest.TestCase):
    def fixture(self, sources, listed):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "integration").mkdir()
        for index, body in enumerate(sources):
            (root / "integration" / ("case%d_test.go" % index)).write_text(
                "package integration_test\n\n" + body, encoding="utf-8")
        (root / tests.INVENTORY).write_text(
            "# header comment\n\n" + "".join(name + "\n" for name in listed), encoding="utf-8")
        return root

    def test_repository_inventory_classifies_every_top_level_test(self):
        self.assertEqual(tests.check(ROOT), [])
        pattern = re.compile(tests.selector(ROOT))
        for name in tests.declared(ROOT):
            self.assertEqual(bool(pattern.fullmatch(name)), not name.startswith("TestCeph"), name)

    def test_unlisted_stale_native_unsorted_and_duplicate_entries_fail(self):
        source = "func TestCephNative(t *testing.T) {}\nfunc TestHelper(t *testing.T) {}\n"
        self.assertEqual(tests.check(self.fixture([source], ["TestHelper"])), [])
        cases = {
            "unclassified": ([source + "func TestNew(t *testing.T) {}\n"], ["TestHelper"]),
            "no top-level test": ([source], ["TestGone", "TestHelper"]),
            "native fixture test": ([source], ["TestCephNative", "TestHelper"]),
            "not sorted": ([source + "func TestA(t *testing.T) {}\n"], ["TestHelper", "TestA"]),
            "duplicate inventory": ([source], ["TestHelper", "TestHelper"]),
            "duplicate top-level": ([source, "func TestHelper(t *testing.T) {}\n"], ["TestHelper"]),
            "empty": (["func TestCephOnly(t *testing.T) {}\n"], []),
        }
        for want, (sources, listed) in cases.items():
            problems = tests.check(self.fixture(sources, listed))
            self.assertTrue(any(want in problem for problem in problems), (want, problems))

    def test_selector_is_anchored_to_listed_names(self):
        root = self.fixture(["func TestA(t *testing.T) {}\nfunc TestAB(t *testing.T) {}\n"], ["TestA", "TestAB"])
        pattern = re.compile(tests.selector(root))
        self.assertTrue(pattern.search("TestA") and pattern.search("TestAB"))
        self.assertIsNone(pattern.search("TestABC"))

    def test_gate_requires_a_top_level_pass_for_every_listed_test(self):
        root = self.fixture(["func TestA(t *testing.T) {}\nfunc TestB(t *testing.T) {}\n"], ["TestA", "TestB"])
        passing = "--- PASS: TestA (0.00s)\n--- PASS: TestB (0.01s)\nPASS\nok  \tpkg\t0.1s\n"
        self.assertEqual(tests.verify(root, passing), [])
        for text in ("PASS\n",
                     passing.replace("PASS: TestB", "SKIP: TestB"),
                     passing.replace("PASS: TestB", "FAIL: TestB"),
                     passing.replace("--- PASS: TestB", "    --- PASS: TestB"),
                     passing.replace("--- PASS: TestB (0.01s)", "--- PASS: TestB ("),
                     passing + "--- PASS: TestA (0.00s)\n",
                     passing + "--- PASS: TestCephNative (0.00s)\n"):
            self.assertNotEqual(tests.verify(root, text), [], text)

    def test_command_line_exit_status(self):
        root = self.fixture(["func TestA(t *testing.T) {}\n"], ["TestA"])
        log = root / "go-test.log"
        log.write_text("--- PASS: TestA (0.00s)\n", encoding="utf-8")
        self.assertEqual(tests.main(["--root", str(root), "check"]), 0)
        self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(log)]), 0)
        log.write_text("--- SKIP: TestA (0.00s)\n", encoding="utf-8")
        self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(log)]), 1)
        self.assertEqual(tests.main(["--root", str(root), "verify", "--log", str(root / "absent")]), 1)


if __name__ == "__main__":
    unittest.main()
