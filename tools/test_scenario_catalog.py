import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import scenario_catalog as catalog
import scenario_plan


def row(sid, group):
    return {"id": sid, "group": group, "params": {"key": "aes", "service": "aes", "family": "4"}}


def plan(path, selector):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = scenario_plan.main(["plan", "--catalog", str(path), "--select", selector])
    return code, out.getvalue(), err.getvalue()


class FragmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name) / "catalog.d"
        self.dir.mkdir()
        self.write(catalog.INDEX, {"version": 1, "fixed_jobs": [{"id": "race", "job": "race"}]})
        self.write("osd.json", {"scenarios": [row("osd.b", "osd"), row("osd.a", "osd")]})
        self.write("mon-mgr.json", {"scenarios": [row("mon-mgr.a", "mon-mgr")]})

    def write(self, name, data):
        (self.dir / name).write_text(data if isinstance(data, str) else json.dumps(data))

    def refused(self, text):
        with self.assertRaises(catalog.CatalogError) as raised:
            catalog.load(self.dir)
        self.assertIn(text, str(raised.exception))

    def test_composes_in_file_name_order_keeping_row_order(self):
        document = catalog.load(self.dir)
        self.assertEqual([r["id"] for r in document["scenarios"]], ["mon-mgr.a", "osd.b", "osd.a"])
        self.assertEqual(document["version"], 1)
        self.assertEqual(document["fixed_jobs"], [{"id": "race", "job": "race"}])

    def test_refuses_duplicate_identifier_across_fragments(self):
        self.write("rbd.json", {"scenarios": [row("osd.a", "rbd")]})
        self.refused("duplicate identifier osd.a in osd.json and rbd.json")

    def test_refuses_duplicate_identifier_within_a_fragment(self):
        self.write("rbd.json", {"scenarios": [row("rbd.a", "rbd"), row("rbd.a", "rbd")]})
        self.refused("duplicate identifier rbd.a")

    def test_refuses_row_in_the_wrong_groups_file(self):
        self.write("rbd.json", {"scenarios": [row("rbd.a", "osd")]})
        self.refused("rbd.json: rbd.a belongs to group 'osd', not 'rbd'")
        self.write("rbd.json", {"scenarios": [{"id": "rbd.a", "params": {}}]})
        self.refused("belongs to group None")

    def test_refuses_unknown_files_and_entries(self):
        for name in ("README.md", "osd_x.json", "osd.json.orig", "_other.json", ".hidden.json"):
            with self.subTest(name=name):
                self.write(name, "{}")
                self.refused("unknown catalog file " + name)
                (self.dir / name).unlink()
        (self.dir / "nested").mkdir()
        self.refused("unknown catalog entry nested")
        (self.dir / "nested").rmdir()
        (self.dir / "link.json").symlink_to(self.dir / "osd.json")
        self.refused("unknown catalog entry link.json")

    def test_refuses_malformed_index_and_fragments(self):
        self.write("rbd.json", {"scenarios": []})
        self.refused("rbd.json has no scenarios")
        self.write("rbd.json", {"scenarios": [row("rbd.a", "rbd")], "group": "rbd"})
        self.refused("rbd.json has unknown keys: group")
        self.write("rbd.json", "[")
        self.refused("rbd.json:")
        (self.dir / "rbd.json").unlink()
        self.write(catalog.INDEX, {"version": 2, "fixed_jobs": []})
        self.refused("unsupported catalog version 2")
        self.write(catalog.INDEX, {"version": 1, "scenarios": []})
        self.refused("_catalog.json has unknown keys: scenarios")
        (self.dir / catalog.INDEX).unlink()
        self.refused("catalog directory has no _catalog.json")

    def test_single_file_is_still_accepted(self):
        legacy = Path(self.tmp.name) / "catalog.json"
        legacy.write_text(json.dumps(catalog.load(self.dir)))
        self.assertEqual(catalog.load(legacy), catalog.load(self.dir))
        legacy.write_text(json.dumps({"version": 2, "scenarios": []}))
        with self.assertRaises(catalog.CatalogError):
            catalog.load(legacy)

    def test_planner_accepts_the_fragment_directory(self):
        code, out, _ = plan(self.dir, "group:osd")
        self.assertEqual(code, 0)
        self.assertEqual([r["scenario"] for r in json.loads(out)["include"]], ["osd.b", "osd.a"])
        self.write("rbd.json", {"scenarios": [row("osd.a", "rbd")]})
        code, _, err = plan(self.dir, "group:osd")
        self.assertEqual(code, 2)
        self.assertIn("duplicate identifier osd.a", err)


class RepositoryCatalogTests(unittest.TestCase):
    def test_fragments_are_canonical_and_split_back_unchanged(self):
        document = catalog.load()
        files = {path.name: path.read_text(encoding="utf-8") for path in catalog.CATALOG_DIR.iterdir()}
        self.assertEqual(catalog.split(document), files)

    def test_adding_a_row_edits_exactly_one_fragment(self):
        document = catalog.load()
        before = catalog.split(document)
        for group in sorted({r["group"] for r in document["scenarios"]}):
            with self.subTest(group=group):
                added = dict(document, scenarios=document["scenarios"] + [row(group + ".new", group)])
                after = catalog.split(added)
                self.assertEqual(sorted(name for name in after if after[name] != before.get(name)), [group + ".json"])

    def test_single_file_and_fragments_plan_identically(self):
        """The trusted planner gives the same answer for a revision with the
        single file as for one with the same rows in fragments."""
        document = catalog.load()
        with tempfile.TemporaryDirectory() as directory:
            legacy = Path(directory) / "catalog.json"
            legacy.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
            selectors = ["all"] + ["group:" + g for g in sorted({r["group"] for r in document["scenarios"]})]
            selectors += [r["id"] for r in document["scenarios"]] + [j["id"] for j in document["fixed_jobs"]]
            for selector in selectors:
                with self.subTest(selector=selector):
                    fragmented = plan(catalog.CATALOG_DIR, selector)
                    self.assertEqual(fragmented[0], 0, fragmented[2])
                    self.assertEqual(plan(legacy, selector), fragmented)


class ExtractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q")
        self.scenarios = self.root / ".github" / "scenarios"
        self.scenarios.mkdir(parents=True)
        self.document = {"version": 1, "scenarios": [row("osd.a", "osd"), row("mon-mgr.a", "mon-mgr")],
                         "fixed_jobs": [{"id": "race", "job": "race"}]}
        self.count = 0

    def git(self, *args):
        return subprocess.check_output(["git", "-c", "user.name=CI test", "-c", "user.email=ci@example.invalid",
                                        "-c", "commit.gpgsign=false", *args],
                                       cwd=self.root, stderr=subprocess.PIPE).decode().strip()

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", "fixture")
        return self.git("rev-parse", "HEAD")

    def fragments(self):
        directory = self.scenarios / "catalog.d"
        directory.mkdir(exist_ok=True)
        for name, text in catalog.split(self.document).items():
            (directory / name).write_text(text)
        return directory

    def extract(self, revision):
        self.count += 1
        output = Path(self.tmp.name) / ("out%d" % self.count)
        return catalog.extract(revision, output, git_dir=self.root)

    def refused(self, revision, text):
        with self.assertRaises(catalog.CatalogError) as raised:
            self.extract(revision)
        self.assertIn(text, str(raised.exception))

    def test_single_file_revision_plans(self):
        (self.scenarios / "catalog.json").write_text(json.dumps(self.document))
        path = self.extract(self.commit())
        self.assertEqual(path.name, "catalog.json")
        code, out, _ = plan(path, "group:osd")
        self.assertEqual((code, [r["scenario"] for r in json.loads(out)["include"]]), (0, ["osd.a"]))

    def test_fragment_revision_plans_from_objects_not_the_worktree(self):
        self.fragments()
        revision = self.commit()
        (self.scenarios / "catalog.d" / "osd.json").unlink()  # the worktree is not read
        path = self.extract(revision)
        self.assertEqual(sorted(p.name for p in path.iterdir()), ["_catalog.json", "mon-mgr.json", "osd.json"])
        code, out, _ = plan(path, "osd.a")
        self.assertEqual((code, [r["scenario"] for r in json.loads(out)["include"]]), (0, ["osd.a"]))
        self.assertEqual(plan(path, "all")[1], json.dumps(scenario_plan.matrix(
            catalog.load(path)["scenarios"] + scenario_plan.fixed_entries(self.document)), separators=(",", ":")) + "\n")

    def test_refuses_missing_ambiguous_and_irregular_layouts(self):
        self.refused(self.commit(), "the tested revision has no scenario catalog")
        directory = self.fragments()
        (self.scenarios / "catalog.json").write_text(json.dumps(self.document))
        self.refused(self.commit(), "both catalog.d and catalog.json")
        (self.scenarios / "catalog.json").unlink()
        (directory / "notes.txt").write_text("x")
        self.refused(self.commit(), "unknown catalog file notes.txt")
        (directory / "notes.txt").unlink()
        (directory / "link.json").symlink_to("osd.json")
        self.refused(self.commit(), "catalog entry link.json is not a regular file")
        (directory / "link.json").unlink()
        (directory / "sub").mkdir()
        (directory / "sub" / "x.json").write_text("{}")
        self.refused(self.commit(), "unknown catalog file sub")
        self.refused("main", "revision must be a full commit SHA")
        self.refused("0" * 40, "git cat-file failed")

    def test_cli_prints_the_path(self):
        self.fragments()
        revision = self.commit()
        out, err = io.StringIO(), io.StringIO()
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = catalog.main(["extract", "--revision", revision, "--output", str(Path(self.tmp.name) / "cli")])
        finally:
            os.chdir(cwd)
        self.assertEqual(code, 0, err.getvalue())
        self.assertEqual(out.getvalue().strip(), str(Path(self.tmp.name) / "cli" / "catalog.d"))


if __name__ == "__main__":
    unittest.main()
