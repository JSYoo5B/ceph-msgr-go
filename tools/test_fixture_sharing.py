import subprocess
import tempfile
import unittest
from pathlib import Path

import fixture_sharing as sharing
import scenario_catalog

HEADER = "package integration\n\nimport \"testing\"\n\n"


def declared(body, path="integration/a_test.go"):
    return sharing.test_declarations({path: HEADER + body})


class TestDeclarationTests(unittest.TestCase):
    def test_every_native_test_is_inventoried_with_its_last_doc_line(self):
        tests, problems = sharing.test_declarations({
            "integration/a_test.go": HEADER + "// TestCephAIntegration reads.\n//\n//fixture:shareable reads MON maps only\n"
                                     "func TestCephAIntegration(t *testing.T) {}\n\n// TestOther is cluster-free.\n"
                                     "func TestOther(t *testing.T) {}\n",
            "cephmsgr/b_test.go": HEADER + "//fixture:isolated stops mon.a and never restarts it\n"
                                  "func TestCephAIntegration(t *testing.T) {}\n",
            "integration/fixture/r_test.go": HEADER + "//fixture:isolated stops the checkpoint OSD\n"
                                             "func TestRGWNativeRIntegration(t *testing.T) {}\n"
                                             "func TestCephNotNativeHereIntegration(t *testing.T) {}\n",
            "integration/helper.go": "//fixture:shareable not a test file\n",
            "tools/x_test.go": "//fixture:shareable not a suite\nfunc TestCephXIntegration(t *testing.T) {}\n"})
        self.assertEqual(problems, [])
        self.assertEqual(tests, {
            "integration/TestCephAIntegration": {"value": "shareable", "reason": "reads MON maps only",
                                                 "source": "integration/a_test.go"},
            "cephmsgr/TestCephAIntegration": {"value": "isolated", "reason": "stops mon.a and never restarts it",
                                              "source": "cephmsgr/b_test.go"},
            "fixture/TestRGWNativeRIntegration": {"value": "isolated", "reason": "stops the checkpoint OSD",
                                                  "source": "integration/fixture/r_test.go"}})

    def test_missing_repeated_invalid_and_misplaced_declarations_are_refused(self):
        native = "func TestCephAIntegration(t *testing.T) {}\n"
        for name, (body, want) in {
            "missing": ("// TestCephAIntegration does things.\n" + native,
                        "integration/TestCephAIntegration (integration/a_test.go) declares no fixture sharing"),
            "repeated": ("//fixture:shareable reads only\n//fixture:isolated stops osd.0\n" + native,
                         "declares fixture sharing 2 times"),
            "not last": ("//fixture:shareable reads only\n// More text.\n" + native, "is not the last line of its doc comment"),
            "misspelled value": ("//fixture:sharable reads only\n" + native, "invalid fixture sharing declaration"),
            "empty reason": ("//fixture:isolated\n" + native, "invalid fixture sharing declaration"),
            "reason with a final period": ("//fixture:isolated stops osd.0.\n" + native, "without a final period"),
            "non-ASCII reason": ("//fixture:isolated stops osd.0 — then restarts\n" + native, "printable ASCII"),
            "long reason": ("//fixture:isolated " + "x" * (sharing.MAX_REASON + 1) + "\n" + native, "at most 120 bytes"),
            "not on the doc comment": ("//fixture:shareable reads only\n\n" + native,
                                       "integration/a_test.go:5: fixture sharing declaration '//fixture:shareable reads only'"
                                       " is not on a native test's doc comment"),
            "on a helper": ("//fixture:shareable reads only\nfunc helper() {}\n\n//fixture:shareable reads only\n" + native,
                            "is not on a native test's doc comment"),
            "on a method": ("//fixture:shareable reads only\nfunc (s suite) TestCephBIntegration(t *testing.T) {}\n\n"
                            "//fixture:shareable reads only\n" + native, "is not on a native test's doc comment"),
            "inside a test": ("//fixture:shareable reads only\nfunc TestCephAIntegration(t *testing.T) {\n\t//fixture:isolated stops osd.0\n}\n",
                              "is not on a native test's doc comment"),
        }.items():
            with self.subTest(name):
                tests, problems = declared(body)
                self.assertTrue(any(want in problem for problem in problems), problems)
                self.assertIn("integration/TestCephAIntegration", tests, "the native test is not inventoried")
        tests, problems = sharing.test_declarations({
            "integration/a_test.go": HEADER + "//fixture:shareable a\nfunc TestCephAIntegration(t *testing.T) {}\n",
            "integration/b_test.go": HEADER + "//fixture:shareable b\nfunc TestCephAIntegration(t *testing.T) {}\n"})
        self.assertTrue(any("duplicate native test integration/TestCephAIntegration" in problem for problem in problems), problems)

    def test_a_routed_checkpoint_is_its_own_suite(self):
        tests, _ = sharing.test_declarations({
            "integration/a_test.go": HEADER + "//fixture:shareable r\nfunc TestCephAIntegration(t *testing.T) {}\n",
            "integration/fixture/r_test.go": HEADER + "//fixture:isolated r\nfunc TestRGWNativeRIntegration(t *testing.T) {}\n"})
        self.assertEqual(sharing.selection_value("Integration$", False, tests)[0], "shareable")
        self.assertEqual(sharing.selection_value("Integration$", True, tests)[0], "isolated")


class RowValueTests(unittest.TestCase):
    TESTS = {
        "integration/TestCephReadIntegration": {"value": "shareable"},
        "cephmsgr/TestCephReadIntegration": {"value": "shareable"},
        "integration/TestCephOtherIntegration": {"value": "shareable"},
        "integration/TestCephKillIntegration": {"value": "isolated"},
        "integration/TestCephNewIntegration": {"value": None},
    }

    def test_every_row_is_derived_from_its_effective_selector(self):
        rows = [{"id": sid} for sid in ("read", "pair", "mixed", "new", "none", "broad", "invalid", "unreported")]
        selectors = {"read": (False, "^TestCephReadIntegration$"), "pair": (False, "^TestCeph(Read|Other)Integration$"),
                     "mixed": (False, "^TestCeph(Read|Kill)Integration$"), "new": (False, "^TestCephNewIntegration$"),
                     "none": (False, "^TestCephMissingIntegration$"), "broad": (False, "^TestCeph"),
                     "invalid": (False, "^(TestCeph")}
        values, problems = sharing.row_values(rows, selectors, self.TESTS)
        self.assertEqual(values, {"read": "shareable", "pair": "shareable", "mixed": "isolated"})
        self.assertEqual(problems, [
            "new: no fixture sharing value: it selects integration/TestCephNewIntegration, which declares no valid fixture sharing",
            "none: no fixture sharing value: its effective selector '^TestCephMissingIntegration$' selects no native test",
            "broad: no fixture sharing value: it selects integration/TestCephNewIntegration, which declares no valid fixture sharing",
            "invalid: no fixture sharing value: its effective selector '^(TestCeph' selects no native test",
            "unreported: no fixture sharing value: the fixture reports no effective selector for it"])
        self.assertEqual(sharing.value_line("mixed", "^TestCeph(Read|Kill)Integration$", False, self.TESTS),
                         "mixed isolated by 1 of 3 selected tests, first integration/TestCephKillIntegration")
        self.assertEqual(sharing.value_line("read", "^TestCephReadIntegration$", False, self.TESTS), "read shareable")

    def test_the_planner_groups_by_the_same_rule(self):
        self.assertTrue(sharing.shareable_row({"test_run": "^TestCephReadIntegration$"}, self.TESTS))
        for params in ({}, {"test_run": "^TestCeph(Read|Kill)Integration$"}, {"test_run": "^TestCephNewIntegration$"},
                       {"test_run": "^TestCephMissingIntegration$"}, {"test_run": "("},
                       {"test_run": "^TestCephReadIntegration$", "rgw_native": "1"}):
            with self.subTest(params=params):
                self.assertFalse(sharing.shareable_row(params, self.TESTS))
        # One name declared in two packages needs both to be shareable.
        tests = dict(self.TESTS, **{"cephmsgr/TestCephReadIntegration": {"value": "isolated"}})
        self.assertFalse(sharing.shareable_row({"test_run": "^TestCephReadIntegration$"}, tests))


class SourceTests(unittest.TestCase):
    def test_revision_sources_read_only_the_suite_test_blobs(self):
        source = HEADER + "//fixture:shareable r\nfunc TestCephAIntegration(t *testing.T) {}\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = lambda *args: subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
            run("init", "-q")
            for path, text in {"integration/a_test.go": source, "integration/helper.go": "package integration\n",
                               "integration/fixture/x_test.go": "package main\n", "integration/fixture/x.go": "package main\n",
                               "integration/fixture/sub/y_test.go": "package sub\n", "tools/z_test.go": "package z\n",
                               "cephmsgr/b_test.go": "package cephmsgr_test\n"}.items():
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                # Bytes, so that Windows newline translation does not change
                # the blobs the test compares.
                (root / path).write_bytes(text.encode("utf-8"))
            run("add", ".")
            run("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "x")
            sha = run("rev-parse", "HEAD").stdout.decode().strip()
            (root / "integration/a_test.go").write_text("changed after the commit\n")
            sources = sharing.revision_sources(sha, root)
            want = ["cephmsgr/b_test.go", "integration/a_test.go", "integration/fixture/x_test.go"]
            self.assertEqual(sorted(sources), want)
            self.assertEqual(sources["integration/a_test.go"], source)
            self.assertEqual(sorted(sharing.tree_sources(root)), want)
            with self.assertRaises(scenario_catalog.CatalogError):
                sharing.revision_sources("HEAD", root)


if __name__ == "__main__":
    unittest.main()
