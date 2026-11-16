import unittest

import native_path_coverage as coverage
from test_go_test_list import SHAPES, WRONG_FILE, module


class NativePathCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = (coverage.ROOT / coverage.COVERAGE).read_text()
        cls.tests = coverage.go_tests(coverage.ROOT)

    def check(self, text):
        return coverage.check(text=text, tests=self.tests)

    def row(self, key):
        return next(line for line in self.text.splitlines() if line.startswith("| " + key + " |"))

    def test_current_map_is_consistent(self):
        self.assertEqual(self.check(self.text), [])

    def test_every_spec_path_has_a_section_with_rows(self):
        areas = {area for area, _ in coverage.coverage_rows(self.text)}
        for label in coverage.spec_paths(coverage.ROOT):
            self.assertIn(label, areas)
        text = self.text.replace("### Service-induced paths", "### Services")
        self.assertIn("SPEC path without coverage section: Service-induced paths", self.check(text))

    def test_go_tests_include_both_integration_packages(self):
        self.assertIn("TestCephMonPingIntegration", self.tests)
        self.assertIn("TestCephServiceKeyEpochIntegration", self.tests)

    def test_plain_test_names_resolve_against_the_tests_go_lists(self):
        tests = coverage.go_tests(module(self, SHAPES), modules=(".",))
        # Literal expectation: the bare Test, the aliased-import test and the
        # blank parameter are tests; TestMain, the method, the lowercase helper
        # and the build-constrained test are not.
        self.assertEqual(tests, {"Test", "TestAlias", "TestX"})
        for token in ("TestAlias", "TestX/subtest", "^TestAl.*$"):
            self.assertEqual(coverage.token_problems(token, tests, set(), set(), set()), [], token)
        for token in ("TestMain", "TestY", "TestIgnored"):
            self.assertEqual(coverage.token_problems(token, tests, set(), set(), set()),
                             ["missing top-level Go test " + token])

    def test_test_link_must_reach_the_declaring_file(self):
        root = module(self, dict(SHAPES, **WRONG_FILE))
        package = (root / "integration").resolve()
        self.assertEqual(coverage.test_link_problems(root, "TestActual", package / "right_test.go"), [])
        self.assertEqual(coverage.test_link_problems(root, "TestAlias", package / "alias_test.go"), [])
        for name, file in (("TestActual", "wrong_test.go"), ("TestIgnored", "ignored_test.go"),
                           ("TestY", "shapes_test.go"), ("TestAlias", "shapes_test.go")):
            self.assertEqual(coverage.test_link_problems(root, name, package / file),
                             ["link integration/%s does not reach the file declaring top-level Go test %s" % (file, name)])

    def test_test_links_in_the_map_are_checked(self):
        row = self.row("MSG-1")
        right = row.replace(" |", " [TestCephMonPingIntegration](../../integration/mon_ping_test.go) |", 1)
        self.assertEqual(self.check(self.text.replace(row, right)), [])
        wrong = row.replace(" |", " [`TestCephMonPingIntegration`](../../integration/commands_test.go) |", 1)
        self.assertIn("link integration/commands_test.go does not reach the file declaring top-level Go test "
                      "TestCephMonPingIntegration", self.check(self.text.replace(row, wrong)))

    def test_unknown_scenario_group_and_suite_are_reported(self):
        text = self.text.replace("`mon-mgr.aes256k.ip4.expiry-1`", "`mon-mgr.aes256k.ip4.expiry-2`")
        self.assertIn("unknown scenario mon-mgr.aes256k.ip4.expiry-2", self.check(text))
        text = self.text.replace("`group:mds-fresh`", "`group:mds-stale`")
        self.assertIn("unknown catalog group group:mds-stale", self.check(text))
        text = self.text.replace("`suite:race`", "`suite:races`")
        self.assertIn("unknown check suite suite:races", self.check(text))

    def test_scenario_with_unknown_prefix_is_reported(self):
        text = self.text.replace("`mon-mgr.aes256k.ip4.expiry-1`", "`mon-mgx.aes256k.ip4.expiry-1`")
        self.assertIn("unknown scenario mon-mgx.aes256k.ip4.expiry-1", self.check(text))

    def test_scenarios_of_a_removed_group_are_reported(self):
        data = coverage.scenario_catalog.load()
        data["scenarios"] = [row for row in data["scenarios"] if row["group"] != "mon-mgr"]
        problems = coverage.check(text=self.text, tests=self.tests, catalog=coverage.catalog_from(data))
        self.assertIn("unknown scenario mon-mgr.aes256k.ip4.expiry-1", problems)
        self.assertIn("unknown catalog group group:mon-mgr", problems)

    def test_test_names_and_selectors_must_resolve(self):
        text = self.text.replace("`TestCephMonPingIntegration`", "`TestCephMonPongIntegration`")
        self.assertIn("missing top-level Go test TestCephMonPongIntegration", self.check(text))
        text = self.text + "\n`^TestCephNoSuchThing$`\n"
        self.assertIn("selector matches no Go test ^TestCephNoSuchThing$", self.check(text))
        self.assertEqual(self.check(self.text + "\n`^TestCephMonPing.*$`\n"), [])

    def test_upstream_receipts_need_a_known_revision(self):
        text = self.text.replace("ctg@v0.4.0:TestOSDHeartbeatFailure", "ctg@v0.5.0:TestOSDHeartbeatFailure")
        self.assertIn("malformed upstream receipt ctg@v0.5.0:TestOSDHeartbeatFailure", self.check(text))

    def test_record_links_must_exist(self):
        text = self.text.replace("(OSD_SOAK_VALIDATION.md)", "(OSD_SOAK_MISSING.md)")
        self.assertIn("missing or non-repository file OSD_SOAK_MISSING.md", self.check(text))
        text = self.text + "\n[outside](../../../outside.md)\n"
        self.assertIn("missing or non-repository file ../../../outside.md", self.check(text))

    def test_rows_need_a_known_class_and_unique_ids(self):
        row = self.row("MSG-1")
        text = self.text.replace(row, row.replace("`released`", "`covered`"))
        self.assertIn("row MSG-1: unknown class `covered`", self.check(text))
        text = self.text.replace(row, row + "\n" + row)
        self.assertIn("duplicate row MSG-1", self.check(text))
        text = self.text.replace(row, "| MSG-1 | short |")
        self.assertIn("row MSG-1: expected 8 nonempty columns", self.check(text))

    def test_class_counts_come_from_the_rows(self):
        counts = coverage.class_counts(self.text)
        self.assertEqual(set(counts), set(coverage.CLASSES))
        self.assertEqual(sum(counts.values()), len(list(coverage.coverage_rows(self.text))))
        row = self.row("MSG-1")
        moved = coverage.class_counts(self.text.replace(row, row.replace("`released`", "`proposed`")))
        self.assertEqual(moved["released"], counts["released"] - 1)
        self.assertEqual(moved["proposed"], counts["proposed"] + 1)

    def test_released_rows_need_local_evidence(self):
        row = self.row("MSG-1")
        cells = coverage.split_row(row)
        cells[6] = "upstream receipt only"
        text = self.text.replace(row, "| " + " | ".join(cells) + " |")
        self.assertIn("row MSG-1: released row needs local evidence", self.check(text))

if __name__ == "__main__":
    unittest.main()
