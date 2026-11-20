import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import scenario_catalog as catalog
import scenario_declarations
import scenario_plan

WORKFLOW = "jobs:\n  unit:\n    if: contains(fromJSON(inputs.matrix).fixed, 'unit')\n    runs-on: x\n"


def declaring(*rows, test="TestCephXIntegration"):
    return "package integration_test\n\n%s\n//fixture:shareable x\nfunc %s(t *testing.T) {}\n" % ("\n".join(rows), test)


class RepositoryCatalogTests(unittest.TestCase):
    def test_rows_are_the_declarations_in_planning_order(self):
        document = catalog.load()
        rows = document["scenarios"]
        self.assertEqual(rows, scenario_declarations.discover(scenario_declarations.tree_sources(catalog.ROOT))[0])
        self.assertEqual([row["id"] for row in rows], sorted((row["id"] for row in rows),
                                                             key=lambda sid: scenario_declarations.planning_key({"id": sid})))
        for row in rows:
            self.assertEqual(row["group"], row["id"].split(".", 1)[0])
        self.assertEqual(scenario_declarations.stray_declarations(catalog.ROOT), [])

    def test_fixed_checks_are_the_selectable_platform_jobs(self):
        text = (catalog.ROOT / catalog.PLATFORM_WORKFLOW).read_text(encoding="utf-8")
        jobs = catalog.load()["fixed_jobs"]
        self.assertTrue(jobs)
        self.assertEqual(jobs, [{"id": job["id"], "job": job["id"]} for job in jobs])
        self.assertEqual(text.count("contains(fromJSON(inputs.matrix).fixed, "), len(jobs))
        self.assertEqual(catalog.fixed_jobs(WORKFLOW), [{"id": "unit", "job": "unit"}])
        with self.assertRaises(catalog.CatalogError) as raised:
            catalog.fixed_jobs(WORKFLOW.replace("fixed, 'unit'", "fixed, 'race'"))
        self.assertIn("platform job unit is selected as fixed check 'race'", str(raised.exception))

    def test_no_catalog_file_is_tracked(self):
        for path in catalog.RETIRED:
            self.assertFalse((catalog.ROOT / path).exists(), path)


class TreeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.write(".github/workflows/platform-checks.yml", WORKFLOW)
        self.write("integration/x_test.go", declaring("//scenario:row osd.b key=aes", "//scenario:suite osd.a key=aes"))

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_loads_the_declared_rows_and_the_fixed_checks(self):
        document = catalog.load(self.root)
        self.assertEqual(document, {"scenarios": [
            {"id": "osd.a", "group": "osd", "params": {"key": "aes"}},
            {"id": "osd.b", "group": "osd", "params": {"key": "aes", "test_run": "^TestCephXIntegration$"}},
        ], "fixed_jobs": [{"id": "unit", "job": "unit"}]})

    def test_refuses_refused_declarations(self):
        self.write("cephmsgr/y_test.go", declaring("//scenario:row osd.b key=aes", test="TestCephYIntegration"))
        with self.assertRaises(catalog.CatalogError) as raised:
            catalog.load(self.root)
        self.assertIn("duplicate scenario identifier osd.b", str(raised.exception))

    def test_refuses_the_catalog_files_of_the_earlier_layout(self):
        for name in (".github/scenarios/catalog.d/_catalog.json", ".github/scenarios/catalog.json"):
            with self.subTest(name=name):
                self.write(name, "{}")
                with self.assertRaises(catalog.CatalogError) as raised:
                    catalog.load(self.root)
                self.assertIn("rows are declared beside their tests; remove this catalog file", str(raised.exception))
                (self.root / name).unlink()


class RevisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q")

    def git(self, *args):
        return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false",
                               *args], cwd=self.root, check=True, capture_output=True).stdout.decode().strip()

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def commit(self):
        self.git("add", "-A", ".")
        self.git("commit", "-qm", "x", "--allow-empty")
        return self.git("rev-parse", "HEAD")

    def plan(self, *args):
        out, err, cwd = io.StringIO(), io.StringIO(), os.getcwd()
        os.chdir(self.root)
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = scenario_plan.main(list(args))
        finally:
            os.chdir(cwd)
        return code, out.getvalue(), err.getvalue()

    def test_plans_a_revision_from_its_objects_not_the_worktree(self):
        self.write("integration/x_test.go", declaring("//scenario:row osd.a key=aes service=aes family=4"))
        self.write("integration/helper.go", "//scenario:row osd.helper key=aes\n")
        self.write("internal/x_test.go", declaring("//scenario:row osd.internal key=aes service=aes family=4"))
        revision = self.commit()
        self.write("integration/x_test.go", declaring("//scenario:row osd.changed key=aes service=aes family=4"))
        document = catalog.load_revision(revision, self.root)
        self.assertEqual([row["id"] for row in document["scenarios"]], ["osd.a"])
        self.assertEqual(document["fixed_jobs"], catalog.load()["fixed_jobs"])
        code, out, err = self.plan("plan", "--revision", revision, "--select", "osd.a")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["include"], [{"key": "aes", "service": "aes", "family": "4",
                                                       "test_run": "^TestCephXIntegration$", "scenario": "osd.a"}])
        code, out, _ = self.plan("list", "--revision", revision)
        self.assertEqual(out.split()[0], "osd.a")
        code, _, err = self.plan("plan", "--revision", revision, "--select", "osd.changed")
        self.assertEqual(code, 2)
        self.assertIn("unknown scenario 'osd.changed'", err)

    def test_refuses_a_revision_with_the_earlier_catalog_or_bad_declarations(self):
        self.write("integration/x_test.go", declaring("//scenario:row osd.a key=aes service=aes family=4"))
        self.write(".github/scenarios/catalog.d/_catalog.json", "{}")
        revision = self.commit()
        code, _, err = self.plan("plan", "--revision", revision, "--select", "all")
        self.assertEqual(code, 2)
        self.assertIn("the tested revision has .github/scenarios/catalog.d from before rows were declared beside their tests;"
                      " merge the default branch", err)
        self.git("rm", "-rq", ".github")
        self.write("integration/x_test.go", declaring("//scenario:row osd.a key", "//scenario:row osd.a key=aes"))
        code, _, err = self.plan("plan", "--revision", self.commit(), "--select", "all")
        self.assertEqual(code, 2)
        self.assertIn("integration/x_test.go:3: osd.a: malformed parameter 'key'", err)
        for revision, message in (("main", "revision must be a full commit SHA"), ("0" * 40, "git cat-file failed")):
            with self.assertRaises(catalog.CatalogError) as raised:
                catalog.load_revision(revision, self.root)
            self.assertIn(message, str(raised.exception))

    def test_ci_command_plans_the_revision_with_the_trusted_planner(self):
        text = (catalog.ROOT / ".github/workflows/ci-command.yml").read_text(encoding="utf-8")
        step = text.split("- name: Resolve the selectors against the tested revision", 1)[1].split("- name:", 1)[0]
        self.assertIn('python3 tools/scenario_plan.py plan --revision "$SHA"', step)
        self.assertNotIn("git show", step)
        self.assertNotIn("checkout", step)


if __name__ == "__main__":
    unittest.main()
