import json
import unittest

import native_path_coverage as coverage


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
        data = json.loads((coverage.ROOT / coverage.CATALOG).read_text())
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
        text = self.text.replace("ctg@main:TestOSDHeartbeatFailure", "ctg@v0.4.0:TestOSDHeartbeatFailure")
        self.assertIn("malformed upstream receipt ctg@v0.4.0:TestOSDHeartbeatFailure", self.check(text))

    def test_record_links_must_exist(self):
        text = self.text.replace("(OSD_SOAK_VALIDATION.md)", "(OSD_SOAK_MISSING.md)")
        self.assertIn("missing or non-repository file OSD_SOAK_MISSING.md", self.check(text))
        text = self.text + "\n[outside](../../../outside.md)\n"
        self.assertIn("missing or non-repository file ../../../outside.md", self.check(text))

    def test_rows_need_a_known_class_unique_ids_and_matching_summary(self):
        row = self.row("MSG-1")
        text = self.text.replace(row, row.replace("`released`", "`covered`"))
        problems = self.check(text)
        self.assertIn("row MSG-1: unknown class `covered`", problems)
        self.assertTrue(any(problem.startswith("summary released: table says") for problem in problems))
        text = self.text.replace(row, row + "\n" + row)
        self.assertIn("duplicate row MSG-1", self.check(text))
        text = self.text.replace(row, "| MSG-1 | short |")
        self.assertIn("row MSG-1: expected 8 nonempty columns", self.check(text))

    def test_unmerged_pull_requests_are_not_capabilities(self):
        # Self-contained: list a synthetic open PR and cite it from a row, so the
        # check does not depend on which pull requests are open today.
        row = self.row("MDS-11")
        heading = "## Open pull requests\n"
        text = self.text.replace(heading, heading + "\n| Pull request | Scope | Status |\n"
                                 "| --- | --- | --- |\n| #999 | synthetic | open PR, unmerged |\n")
        cited = text.replace(row, row.replace("issue #10", "issue #10; open PR #999"))
        self.assertEqual(self.check(cited), [])
        cited = cited.replace("`proposed` | Not defined | Not defined | Not defined | issue #10; open PR #999",
                              "`released` | Not defined | Not defined | Not defined | issue #10; open PR #999")
        self.assertIn("row MDS-11: unmerged PR #999 cited as released", self.check(cited))


if __name__ == "__main__":
    unittest.main()
