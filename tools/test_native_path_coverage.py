import posixpath
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import native_path_coverage as coverage
from test_go_test_list import REPEATED, SHAPES, WRONG_FILE, module

DIRECTORY = coverage.COVERAGE.as_posix()
LEGACY = "integration/fixture/NATIVE_PATH_COVERAGE.md"


def where(name):
    return DIRECTORY + "/" + name


def synthetic_row(prefix):
    return ("| %s-900 | Synthetic path | `proposed` | Not defined | Not defined | Not defined | issue #176 "
            "| not in catalog |" % prefix)


def row_file(key, row=None, note="Updated on 2026-11-17 for issue #177. New row."):
    """The text of the row file of key, as an author writes it."""
    prefix = key.split("-", 1)[0]
    row = row or synthetic_row(prefix).replace("| %s-900 |" % prefix, "| %s |" % key, 1)
    return ("# %s\n\nThis file holds one `%s` row of the [native path coverage map](../README.md).\n\n"
            "## Rows\n\n| %s |\n%s|\n%s\n\n## Updates\n\n%s\n"
            % (key, prefix, " | ".join(coverage.HEADER), "| --- " * len(coverage.HEADER), row, note))


def add_row(text, row):
    """Append row to the end of the file's coverage table."""
    lines = text.splitlines()
    at = lines.index("| " + " | ".join(coverage.HEADER) + " |") + 2
    while at < len(lines) and lines[at].startswith("|"):
        at += 1
    return "\n".join(lines[:at] + [row] + lines[at:]) + "\n"


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=CI test", "-c", "user.email=ci@example.invalid",
                           "-c", "core.autocrlf=false", *args],
                          cwd=cwd, capture_output=True, text=True, timeout=60)


def relink(cells, directory):
    """Rewrite every relative link of the cells to a repository path."""
    def sub(match):
        label, target = match.groups()
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            return match.group(0)
        path, sep, anchor = target.partition("#")
        resolved = posixpath.normpath(posixpath.join(directory, path)) + ("/" if path.endswith("/") else "")
        return "[%s](%s%s%s)" % (label, resolved, sep, anchor)
    return [coverage.LINK.sub(sub, cell) for cell in cells]


def legacy_rows(text):
    """(area, cells) of every row of the single-file map, by its ### headings."""
    sections = re.split(r"(?m)^(#{2,3} .*)$", text)
    rows = []
    for heading, body in zip(sections[1::2], sections[2::2]):
        if heading.startswith("### "):
            rows.extend((heading[4:].strip(), relink(cells, "integration/fixture"))
                        for cells in coverage.table_rows(body))
    return rows


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

    def test_index_lists_every_area_file_in_order(self):
        names = coverage.area_files(self.documents[coverage.INDEX])
        self.assertEqual(names, ["MSG.md", "MM.md", "OSD.md", "MDS.md", "RBD.md", "D2D.md", "SVC.md"])
        # The other entries are row files of these areas.
        self.assertEqual(sorted(names + [coverage.INDEX]), sorted(name for name in self.documents if "/" not in name))
        for name in self.documents:
            if "/" in name:
                self.assertIn(name.split("/", 1)[0] + ".md", names)

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

    def test_rows_need_a_known_class_and_columns(self):
        name, row = self.row("MSG-1")
        self.assertProblem(self.check(self.edit(name, row, row.replace("`released`", "`covered`"))),
                           "row MSG-1: unknown class `covered`")
        self.assertProblem(self.check(self.edit(name, row, "| MSG-1 | short |")),
                           "row MSG-1: expected 8 nonempty columns")

    def test_duplicate_row_ids_are_refused_within_and_across_files(self):
        name, row = self.row("MSG-1")
        self.assertProblem(self.check(self.edit(name, row, row + "\n" + row)),
                           "duplicate row MSG-1: in %s and %s" % (where(name), where(name)))
        _, other = self.row("MM-1")
        moved = other.replace("| MM-1 |", "| MSG-1 |", 1)
        problems = self.check(self.edit("MM.md", other, other + "\n" + moved))
        self.assertProblem(problems, "duplicate row MSG-1: in %s and %s" % (where("MSG.md"), where("MM.md")))

    def test_a_row_in_the_wrong_area_file_is_refused(self):
        name, row = self.row("MSG-1")
        documents = self.edit(name, row + "\n", "")
        documents["MM.md"] = add_row(documents["MM.md"], row)
        self.assertEqual(self.check(documents), ["row MSG-1: in %s, whose rows are MM-<number>" % where("MM.md")])
        for key in ("MM1", "MM-01", "mm-1", "MM-1a"):
            with self.subTest(key=key):
                _, line = self.row("MM-1")
                documents = self.edit("MM.md", line, line.replace("| MM-1 |", "| %s |" % key, 1))
                self.assertProblem(self.check(documents), "row %s: in %s, whose rows are MM-<number>" % (key, where("MM.md")))

    def test_an_unlisted_or_irregular_entry_is_refused(self):
        documents = dict(self.documents, **{"NOTES.md": self.documents["SVC.md"]})
        problems = self.check(documents)
        self.assertProblem(problems, "unlisted file " + where("NOTES.md"))
        documents = dict(self.documents, **{"drafts": None})
        self.assertEqual(self.check(documents), ["not a regular UTF-8 text file " + where("drafts")])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / coverage.COVERAGE / "drafts").mkdir(parents=True)
            (root / coverage.COVERAGE / "README.md").write_text("# Map\r\n\r\ntext\r\n", encoding="utf-8")
            (root / coverage.COVERAGE / ".DS_Store").write_bytes(b"\x00\x00\x00\x01Bud1\xff\xfe")
            self.assertEqual(coverage.read_documents(root),
                             {".DS_Store": None, "README.md": "# Map\n\ntext\n", "drafts": None})

    def test_index_must_list_existing_area_files_and_hold_no_rows(self):
        documents = dict(self.documents)
        del documents["OSD.md"]
        self.assertProblem(self.check(documents), "missing area file " + where("OSD.md"))
        documents = self.edit(coverage.INDEX, "[SVC.md](SVC.md)", "SVC.md")
        problems = self.check(documents)
        self.assertProblem(problems, "unlisted file " + where("SVC.md"))
        self.assertProblem(problems, "SPEC path without an area file with rows: Service-induced paths")
        documents = self.edit(coverage.INDEX, "[SVC.md](SVC.md)", "[SVC.md](SVC.md), [svc.md](svc.md)")
        self.assertProblem(self.check(documents), "area file %s: name is not <PREFIX>.md" % where("svc.md"))
        index = add_row(self.documents[coverage.INDEX] + "\n" + "| " + " | ".join(coverage.HEADER) + " |\n"
                        + "| --- " * len(coverage.HEADER) + "|\n", synthetic_row("SVC"))
        self.assertProblem(self.check(self.with_text(coverage.INDEX, index)),
                           "row SVC-900: in the index %s, not in an area file" % where(coverage.INDEX))
        documents = self.edit(coverage.INDEX, "## Area files\n", "## Areas\n")
        self.assertEqual(self.check(documents)[0], where(coverage.INDEX) + ": missing Area files section")
        self.assertEqual(self.check({k: v for k, v in self.documents.items() if k != coverage.INDEX})[0],
                         "missing index " + where(coverage.INDEX))
        documents = self.edit("SVC.md", "# Service-induced paths\n", "Service-induced paths\n")
        self.assertProblem(self.check(documents), "area file %s: first line is not a # title" % where("SVC.md"))

    def test_area_files_keep_rows_in_their_rows_section(self):
        documents = self.edit("SVC.md", "\n## Updates\n", "\n## Updates\n\n| " + " | ".join(coverage.HEADER)
                              + " |\n" + "| --- " * len(coverage.HEADER) + "|\n" + synthetic_row("SVC") + "\n")
        self.assertEqual(self.check(documents),
                         ["row SVC-900: outside the Rows section of " + where("SVC.md")])
        documents = self.edit("SVC.md", "## Rows\n", "## Table\n")
        self.assertProblem(self.check(documents), "area file %s: missing Rows section" % where("SVC.md"))

    def test_area_titles_and_prefixes_match_the_index(self):
        documents = self.edit("SVC.md", "# Service-induced paths\n", "# Daemon-to-daemon\n")
        self.assertProblem(self.check(documents),
                           "area file %s: title Daemon-to-daemon and prefix SVC differ from the index line "
                           "SVC | Service-induced paths" % where("SVC.md"))
        documents = self.edit(coverage.INDEX, "| `SVC` | Service-induced paths |", "| `SRV` | Service-induced paths |")
        self.assertEqual(self.check(documents),
                         ["area file %s: title Service-induced paths and prefix SVC differ from the index line "
                          "SRV | Service-induced paths" % where("SVC.md")])
        documents = self.edit(coverage.INDEX, "| `SVC` | Service-induced paths | [SVC.md](SVC.md) |\n",
                              "\nAlso [SVC.md](SVC.md).\n")
        self.assertEqual(self.check(documents),
                         ["area file %s: no Prefix | Path | Area file line in the index" % where("SVC.md")])

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

    def test_a_row_file_adds_one_row_to_its_area(self):
        """A new row is a file of its own under its area's directory."""
        counts = coverage.class_counts(self.documents)
        for name in coverage.area_files(self.documents[coverage.INDEX]):
            prefix = name[:-3]
            with self.subTest(area=prefix):
                key = prefix + "-synthetic-path"
                documents = self.with_text(prefix + "/" + key + ".md", row_file(key))
                self.assertEqual(self.check(documents), [])
                self.assertEqual(coverage.class_counts(documents)["proposed"], counts["proposed"] + 1)
                areas = coverage.compose(documents)[0]
                self.assertIn((prefix + "/" + key + ".md", areas[[n for n, _, _ in areas].index(name)][1],
                               [coverage.split_row(synthetic_row(prefix).replace(prefix + "-900", key))]), areas)

    def test_a_row_file_is_refused_unless_name_title_and_row_agree(self):
        good = row_file("RBD-synthetic-path")
        for name, problem in (
                ("RBD/RBD-11.md", "row file %s: name is not RBD/RBD-<name>.md"),
                ("RBD/OSD-synthetic-path.md", "row file %s: name is not RBD/RBD-<name>.md"),
                ("RBD/RBD-Synthetic.md", "row file %s: name is not RBD/RBD-<name>.md"),
                ("RBD/RBD-synthetic--path.md", "row file %s: name is not RBD/RBD-<name>.md"),
                ("RBD/RBD-synthetic-path.txt", "row file %s: name is not RBD/RBD-<name>.md"),
                ("NOTES/NOTES-synthetic-path.md", "unlisted file %s"),
                ("rbd/rbd-synthetic-path.md", "unlisted file %s")):
            with self.subTest(name=name):
                self.assertProblem(self.check(self.with_text(name, good)), problem % where(name))
        name = "RBD/RBD-synthetic-path.md"
        self.assertEqual(self.check(self.with_text(name, good.replace("# RBD-synthetic-path\n", "# RBD underlying I/O\n"))),
                         ["row file %s: first line is not the title # RBD-synthetic-path" % where(name)])
        self.assertEqual(self.check(self.with_text(name, good.replace("## Rows\n", "## Table\n"))),
                         ["row file %s: missing Rows section" % where(name)])
        other = good.replace("| RBD-synthetic-path |", "| RBD-other-path |")
        self.assertEqual(self.check(self.with_text(name, other)),
                         ["row file %s: its Rows section must hold the one row RBD-synthetic-path, not RBD-other-path" % where(name)])
        row = synthetic_row("RBD").replace("RBD-900", "RBD-synthetic-path")
        self.assertEqual(self.check(self.with_text(name, good.replace(row, row + "\n" + row.replace("synthetic-path", "second-path")))),
                         ["row file %s: its Rows section must hold the one row RBD-synthetic-path, not RBD-synthetic-path, RBD-second-path"
                          % where(name)])
        self.assertProblem(self.check(self.with_text(name, good.replace(row, "| " + " | ".join(coverage.HEADER) + " |"))),
                           "row file %s: its Rows section must hold the one row RBD-synthetic-path, not none" % where(name))
        moved = good.replace("\n## Updates\n", "\n## Updates\n\n| " + " | ".join(coverage.HEADER) + " |\n"
                             + "| --- " * len(coverage.HEADER) + "|\n" + row.replace("synthetic-path", "second-path") + "\n")
        self.assertEqual(self.check(self.with_text(name, moved)),
                         ["row RBD-second-path: outside the Rows section of " + where(name)])
        self.assertEqual(self.check(self.with_text("RBD/drafts", None)), ["not a regular UTF-8 text file " + where("RBD/drafts")])
        # A numbered row stays in the area table: a number in a row file, or
        # a name in an area table, is refused.
        _, line = self.row("RBD-1")
        documents = self.edit("RBD.md", line, line.replace("| RBD-1 |", "| RBD-first |", 1))
        self.assertProblem(self.check(documents), "row RBD-first: in %s, whose rows are RBD-<number>" % where("RBD.md"))

    def test_row_file_links_are_relative_to_the_row_file(self):
        name = "RBD/RBD-synthetic-path.md"
        right = row_file("RBD-synthetic-path", note="See [the base record](../../RBD_BASE_FIXTURE.md).")
        self.assertEqual(self.check(self.with_text(name, right)), [])
        wrong = right.replace("(../../RBD_BASE_FIXTURE.md)", "(../RBD_BASE_FIXTURE.md)")
        self.assertEqual(self.check(self.with_text(name, wrong)),
                         [where(name) + ": missing or non-repository file ../RBD_BASE_FIXTURE.md"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / coverage.COVERAGE
            (root / "RBD" / "nested").mkdir(parents=True)
            (root / "empty").mkdir()
            (root / "RBD" / "RBD-a.md").write_text("# RBD-a\n", encoding="utf-8")
            self.assertEqual(coverage.read_documents(Path(directory)),
                             {"RBD/RBD-a.md": "# RBD-a\n", "RBD/nested": None, "empty": None})

    def test_a_row_added_to_one_area_edits_that_area_file_only(self):
        """No count, index line or list elsewhere has to change with a row."""
        for name in coverage.area_files(self.documents[coverage.INDEX]):
            with self.subTest(area=name):
                documents = self.with_text(name, add_row(self.documents[name], synthetic_row(name[:-3])))
                self.assertEqual([n for n in documents if documents[n] != self.documents[n]], [name])
                self.assertEqual(self.check(documents), [])
                counts = coverage.class_counts(documents)
                self.assertEqual(counts["proposed"], coverage.class_counts(self.documents)["proposed"] + 1)


class MergeTests(unittest.TestCase):
    """Two branches add a row each; Git composes them and the map stays valid."""

    @classmethod
    def setUpClass(cls):
        cls.documents = coverage.read_documents()
        cls.tests = coverage.go_tests(coverage.ROOT)

    def run_git(self, *args):
        result = git(self.root, *args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def write(self, name, text):
        path = self.root / coverage.COVERAGE / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")

    def branch(self, name, area):
        self.run_git("checkout", "-q", "-b", name, "base")
        path = self.root / coverage.COVERAGE / area
        self.write(area, add_row(path.read_text(encoding="utf-8"), synthetic_row(area[:-3])
                                 .replace("-900 |", "-90%d |" % (len(name)))))
        self.run_git("commit", "-q", "-am", name)
        return self.run_git("diff", "--name-only", "base", name).split()

    def test_rows_of_two_areas_merge_without_a_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            self.root = Path(directory)
            (self.root / coverage.COVERAGE).mkdir(parents=True)
            for name, text in self.documents.items():
                self.write(name, text)
            self.run_git("init", "-q")
            self.run_git("add", "-A")
            self.run_git("commit", "-q", "-m", "base")
            self.run_git("branch", "base")
            self.assertEqual(self.branch("a", "OSD.md"), [where("OSD.md")])
            self.assertEqual(self.branch("bb", "MDS.md"), [where("MDS.md")])
            self.run_git("checkout", "-q", "a")
            self.run_git("merge", "-q", "--no-edit", "bb")
            merged = coverage.read_documents(self.root)
            self.assertEqual(coverage.check(documents=merged, tests=self.tests), [])
            keys = [cells[0] for _, _, rows in coverage.compose(merged)[0] for cells in rows]
            self.assertIn("OSD-901", keys)
            self.assertIn("MDS-902", keys)

    def test_row_files_of_one_area_merge_without_a_conflict(self):
        """Two authors from one base each add a row to one area: each names
        its own row, so neither file nor number is shared."""
        with tempfile.TemporaryDirectory() as directory:
            self.root = Path(directory)
            (self.root / coverage.COVERAGE).mkdir(parents=True)
            for name, text in self.documents.items():
                (self.root / coverage.COVERAGE / name).parent.mkdir(parents=True, exist_ok=True)
                self.write(name, text)
            self.run_git("init", "-q")
            self.run_git("add", "-A")
            self.run_git("commit", "-q", "-m", "base")
            self.run_git("branch", "base")
            added = {}
            for branch, key in (("a", "RBD-first-path"), ("b", "RBD-second-path")):
                self.run_git("checkout", "-q", "-b", branch, "base")
                (self.root / coverage.COVERAGE / "RBD").mkdir(exist_ok=True)
                self.write("RBD/%s.md" % key, row_file(key))
                self.run_git("add", "-A")
                self.run_git("commit", "-q", "-m", branch)
                added[branch] = self.run_git("diff", "--name-only", "base", branch).split()
            self.assertEqual(added, {"a": [where("RBD/RBD-first-path.md")], "b": [where("RBD/RBD-second-path.md")]})
            self.run_git("checkout", "-q", "a")
            self.run_git("merge", "-q", "--no-edit", "b")
            merged = coverage.read_documents(self.root)
            self.assertEqual(coverage.check(documents=merged, tests=self.tests), [])
            keys = [cells[0] for _, _, rows in coverage.compose(merged)[0] for cells in rows]
            self.assertEqual([key for key in keys if key.startswith("RBD-") and not key[4:].isdigit()],
                             ["RBD-first-path", "RBD-second-path"])


class SplitTests(unittest.TestCase):
    """The commit that split the single-file map kept every row."""

    def test_split_rows_equal_the_single_file_rows(self):
        root = coverage.ROOT
        added = git(root, "log", "--format=%H", "--diff-filter=A", "--", where(coverage.INDEX))
        commits = added.stdout.split() if added.returncode == 0 else []
        if not commits:
            self.skipTest("no Git history of " + where(coverage.INDEX))
        split = commits[-1]
        parent = git(root, "rev-parse", "--verify", "--quiet", split + "^")
        if parent.returncode != 0:
            self.skipTest("the parent of the split commit is not in this checkout (shallow clone)")
        parent = parent.stdout.strip()
        old = git(root, "show", "%s:%s" % (parent, LEGACY))
        self.assertEqual(old.returncode, 0, old.stderr)
        self.assertNotEqual(git(root, "cat-file", "-e", "%s:%s" % (split, LEGACY)).returncode, 0,
                            "the split commit still has the single file")
        names = git(root, "ls-tree", "--name-only", split, DIRECTORY + "/")
        self.assertEqual(names.returncode, 0, names.stderr)
        documents = {}
        for path in names.stdout.split():
            shown = git(root, "show", "%s:%s" % (split, path))
            self.assertEqual(shown.returncode, 0, shown.stderr)
            documents[posixpath.basename(path)] = shown.stdout
        areas, problems = coverage.compose(documents)
        self.assertEqual(problems, [])
        composed = [(label, relink(cells, DIRECTORY)) for _, label, rows in areas for cells in rows]
        expected = legacy_rows(old.stdout)
        self.assertGreater(len(expected), 0)
        self.assertEqual(composed, expected)


if __name__ == "__main__":
    unittest.main()
