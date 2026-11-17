import unittest

import native_path_coverage as coverage
from test_go_test_list import REPEATED, SHAPES, WRONG_FILE, module

DIRECTORY = coverage.COVERAGE.as_posix()


def where(name):
    return DIRECTORY + "/" + name


class NativePathCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.documents = coverage.read_documents()
        cls.tests = coverage.go_tests(coverage.ROOT)

    def check(self, documents):
        return coverage.check(documents=documents, tests=self.tests)

    def holder(self, text):
        """The file that holds text."""
        names = [name for name, body in self.documents.items() if text in (body or "")]
        self.assertTrue(names, text)
        return names[0]

    def edit(self, name, old, new):
        if name is None:
            name = self.holder(old)
        documents = dict(self.documents)
        self.assertTrue(old in documents[name], "%s not in %s" % (old, name))
        documents[name] = documents[name].replace(old, new)
        return documents

    def with_text(self, name, text):
        return dict(self.documents, **{name: text})

    def row(self, key):
        """(file name, line) of the row with ID key."""
        for name, text in self.documents.items():
            for line in (text or "").splitlines():
                if line.startswith("| " + key + " |"):
                    return name, line
        raise AssertionError("no row " + key)

    def assertProblem(self, problems, problem):
        self.assertIn(problem, problems)

    def test_current_map_is_consistent(self):
        self.assertEqual(self.check(self.documents), [])

    def test_every_spec_path_has_an_area_file_with_rows(self):
        areas = {label for _, label, rows in coverage.compose(self.documents)[0] if rows}
        for label in coverage.spec_paths(coverage.ROOT):
            self.assertIn(label, areas)
        documents = self.edit("SVC.md", "# Service-induced paths\n", "# Services\n")
        self.assertProblem(self.check(documents), "SPEC path without an area file with rows: Service-induced paths")

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

    def test_repeated_init_functions_do_not_hide_a_linked_test(self):
        root = module(self, REPEATED)
        package = (root / "integration").resolve()
        self.assertEqual(coverage.test_link_problems(root, "TestActual", package / "right_test.go"), [])
        self.assertEqual(coverage.test_link_problems(root, "TestActual", package / "wrong_test.go"),
                         ["link integration/wrong_test.go does not reach the file declaring top-level Go test TestActual"])

    def test_test_links_in_every_file_are_checked(self):
        name, row = self.row("MSG-1")
        right = row.replace("| MSG-1 | ", "| MSG-1 | [TestCephMonPingIntegration](../../mon_ping_test.go) ", 1)
        self.assertEqual(self.check(self.edit(name, row, right)), [])
        wrong = row.replace("| MSG-1 | ", "| MSG-1 | [`TestCephMonPingIntegration`](../../commands_test.go) ", 1)
        self.assertProblem(self.check(self.edit(name, row, wrong)),
                           where(name) + ": link integration/commands_test.go does not reach the file declaring "
                           "top-level Go test TestCephMonPingIntegration")
        index = self.documents[coverage.INDEX] + "\n[`TestCephMonPingIntegration`](../../commands_test.go)\n"
        self.assertProblem(self.check(self.with_text(coverage.INDEX, index)),
                           where(coverage.INDEX) + ": link integration/commands_test.go does not reach the file "
                           "declaring top-level Go test TestCephMonPingIntegration")

    def test_unknown_scenario_group_and_suite_are_reported(self):
        for old, new, problem in (
                ("`mon-mgr.aes256k.ip4.expiry-1`", "`mon-mgr.aes256k.ip4.expiry-2`",
                 "unknown scenario mon-mgr.aes256k.ip4.expiry-2"),
                ("`group:mds-fresh`", "`group:mds-stale`", "unknown catalog group group:mds-stale"),
                ("`suite:race`", "`suite:races`", "unknown check suite suite:races")):
            name = self.holder(old)
            self.assertProblem(self.check(self.edit(name, old, new)), where(name) + ": " + problem)

    def test_scenario_with_unknown_prefix_is_reported(self):
        name = self.holder("`mon-mgr.aes256k.ip4.expiry-1`")
        documents = self.edit(name, "`mon-mgr.aes256k.ip4.expiry-1`", "`mon-mgx.aes256k.ip4.expiry-1`")
        self.assertProblem(self.check(documents), where(name) + ": unknown scenario mon-mgx.aes256k.ip4.expiry-1")

    def test_scenarios_of_a_removed_group_are_reported(self):
        data = coverage.scenario_catalog.load()
        data["scenarios"] = [row for row in data["scenarios"] if row["group"] != "mon-mgr"]
        problems = coverage.check(documents=self.documents, tests=self.tests, catalog=coverage.catalog_from(data))
        name = self.holder("`mon-mgr.aes256k.ip4.expiry-1`")
        self.assertProblem(problems, where(name) + ": unknown scenario mon-mgr.aes256k.ip4.expiry-1")
        self.assertTrue(any(p.endswith(": unknown catalog group group:mon-mgr") for p in problems), problems)

    def test_test_names_and_selectors_must_resolve(self):
        name = self.holder("`TestCephMonPingIntegration`")
        documents = self.edit(name, "`TestCephMonPingIntegration`", "`TestCephMonPongIntegration`")
        self.assertProblem(self.check(documents), where(name) + ": missing top-level Go test TestCephMonPongIntegration")
        text = self.documents["OSD.md"] + "\n`^TestCephNoSuchThing$`\n"
        self.assertProblem(self.check(self.with_text("OSD.md", text)),
                           where("OSD.md") + ": selector matches no Go test ^TestCephNoSuchThing$")
        text = self.documents[coverage.INDEX] + "\n`^TestCephMonPing.*$`\n"
        self.assertEqual(self.check(self.with_text(coverage.INDEX, text)), [])

    def test_upstream_receipts_need_a_known_revision(self):
        name = self.holder("ctg@v0.4.0:TestOSDHeartbeatFailure")
        documents = self.edit(name, "ctg@v0.4.0:TestOSDHeartbeatFailure", "ctg@v0.5.0:TestOSDHeartbeatFailure")
        self.assertProblem(self.check(documents),
                           where(name) + ": malformed upstream receipt ctg@v0.5.0:TestOSDHeartbeatFailure")

    def test_record_links_must_exist(self):
        name = self.holder("(../OSD_SOAK_VALIDATION.md)")
        documents = self.edit(name, "(../OSD_SOAK_VALIDATION.md)", "(../OSD_SOAK_MISSING.md)")
        self.assertProblem(self.check(documents), where(name) + ": missing or non-repository file ../OSD_SOAK_MISSING.md")
        text = self.documents[coverage.INDEX] + "\n[outside](../../../../outside.md)\n"
        self.assertProblem(self.check(self.with_text(coverage.INDEX, text)),
                           where(coverage.INDEX) + ": missing or non-repository file ../../../../outside.md")

    def test_rows_need_a_known_class_unique_ids_and_columns(self):
        name, row = self.row("MSG-1")
        self.assertProblem(self.check(self.edit(name, row, row.replace("`released`", "`covered`"))),
                           "row MSG-1: unknown class `covered`")
        self.assertProblem(self.check(self.edit(name, row, row + "\n" + row)),
                           "duplicate row MSG-1: in %s and %s" % (where(name), where(name)))
        self.assertProblem(self.check(self.edit(name, row, "| MSG-1 | short |")),
                           "row MSG-1: expected 8 nonempty columns")

    def test_class_counts_come_from_the_rows(self):
        counts = coverage.class_counts(self.documents)
        self.assertEqual(set(counts), set(coverage.CLASSES))
        rows = sum(len(rows) for _, _, rows in coverage.compose(self.documents)[0])
        self.assertEqual(sum(counts.values()), rows)
        name, row = self.row("MSG-1")
        moved = coverage.class_counts(self.edit(name, row, row.replace("`released`", "`proposed`")))
        self.assertEqual(moved["released"], counts["released"] - 1)
        self.assertEqual(moved["proposed"], counts["proposed"] + 1)

    def test_released_rows_need_local_evidence(self):
        name, row = self.row("MSG-1")
        cells = coverage.split_row(row)
        cells[6] = "upstream receipt only"
        self.assertProblem(self.check(self.edit(name, row, "| " + " | ".join(cells) + " |")),
                           "row MSG-1: released row needs local evidence")

if __name__ == "__main__":
    unittest.main()
