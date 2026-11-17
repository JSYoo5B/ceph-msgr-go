"""Two pull requests that add native tests merge without registry conflicts.

This is the acceptance test of issue #170 (issue #177). It copies the tracked
files of this checkout into a temporary Git repository, commits them as a
base and makes six branches from it. Each branch adds what the section
"Adding a native test" of .github/scenarios/README.md tells an author to add,
and only that:

- a native test TestCeph<Name>Integration with its //fixture: sharing line
  and a cluster-free helper test, in a new integration/<name>_test.go;
- its catalog row, appended to its group's fragment in catalog.d;
- the fixture capability the row declares, in a new integration/fixture file
  (defineCapability, or defineExclusiveCapability on a fixture with its own
  default selection);
- a coverage row with a dated note in the area's coverage file;
- the row's own checks, in a new tools/test_ci_rows_<group>_<name>.py.

Case A merges an OSD branch with an MDS branch, which must merge without a
textual conflict in any file. Case B merges two RBD branches, which may
conflict only in the RBD catalog fragment and the RBD coverage file that both
edit; the conflict is resolved by keeping both sides' rows (a composition that
refuses anything but rows added on both sides), and the result must pass.
Case C does the same for two controls of the four-OSD erasure topology, the
most frequent kind of new row, in the OSD fragment and coverage file.

Each merged tree then passes the repository's own validators, run from that
tree: scenario_plan.py validate (catalog, selectors, the fixture's admission
and sharing of every row), the cluster-free listing tools and the cluster-free
tests with their result gate, native_path_coverage.py, every test of the
fixture module and every tools/test_ci_rows_* module. A validator that needs a
total, a global list or a test-name admission list fails on a merged tree,
because no branch edits one; a branch that did edit one would conflict in a
file outside the area.

The test needs git and the Go toolchain of the fixture module, as the other
tools tests do. It runs no cluster. Every merged tree is validated in the same
directory, so the Go build cache serves the unchanged packages.
"""
import difflib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / ".github" / "scenarios" / "README.md"
SECTION = "## Adding a native test"
TIMEOUT = 1800

BASE_PARAMS = {
    "key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
    "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
}
ID_PROFILE = "aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6"


class Author:
    """One branch's additions: one native test, one helper, one row, one
    capability, one coverage row and the row's own checks."""

    def __init__(self, name, group, area, opt_ins, exclusive, requires, test=None, base=None, id_profile=ID_PROFILE, also=()):
        self.name = name                      # e.g. OSD, RBDA
        self.snake = "merge_proof_" + name.lower()
        self.slug = "merge-proof-" + name.lower()
        self.group, self.area = group, area
        self.test = test or "TestCephMergeProof%sIntegration" % name
        self.helper = "TestMergeProof%sHelper" % name
        self.capability = self.slug
        self.selector = "^%s$" % self.test
        self.id = "%s.%s.t-mergeproof%s" % (group, id_profile, name.lower())
        self.params = dict(base or BASE_PARAMS, **opt_ins)
        # The declared list: this author's capability and any existing one
        # its topology requires, sorted as the catalog requires.
        self.declared = ",".join(sorted((self.capability,) + tuple(also)))
        self.params.update(test_run=self.selector, capabilities=self.declared)
        self.exclusive, self.requires = exclusive, requires
        self.number = None                    # coverage row number, set per base

    # The files, as an author writes them.

    def native_test(self):
        return ("package integration_test\n\n"
                "import \"testing\"\n\n"
                "// %s is a native test of the merge proof (tools/test_merge_proof.py).\n"
                "// It exists only in that test's temporary repository.\n"
                "//\n"
                "//fixture:shareable changes nothing outside the temporary repository of the merge proof\n"
                "func %s(t *testing.T) {\n"
                "\tt.Skip(\"merge proof test; selected only in the merge proof's temporary repository\")\n"
                "}\n\n"
                "// %s is the cluster-free helper test of the merge proof.\n"
                "func %s(t *testing.T) {\n"
                "\tif selector := %s; selector[0] != '^' {\n"
                "\t\tt.Fatal(selector)\n"
                "\t}\n"
                "}\n" % (self.test, self.test, self.helper, self.helper, json.dumps(self.selector)))

    def capability_source(self):
        variable = "capMergeProof" + self.name
        if self.exclusive:
            call = "defineExclusiveCapability(%s, %s," % (self.exclusive, json.dumps(self.capability))
        else:
            call = "defineCapability(%s," % json.dumps(self.capability)
        return ("package main\n\n"
                "// %s admits the mode of the merge proof's test %s.\n"
                "var %s = %s\n"
                "\t\"admit the merge proof's mode for one test\",\n"
                "\t%s, nil)\n\n"
                "// mergeProof%sMode is the mode that %s admits.\n"
                "func mergeProof%sMode(p profile) bool { return p.admits(%s) }\n"
                % (variable, self.test, variable, call, json.dumps(self.requires), self.name, variable, self.name, variable))

    def row_checks(self):
        return ('"""Admission checks for the merge proof\'s %s row."""\n'
                "import re\n"
                "import unittest\n\n"
                "import ci_row_catalog\n\n\n"
                "class MergeProof%sRowsTest(ci_row_catalog.RowsTestCase):\n"
                "    def test_row_selects_only_its_test_with_its_capability(self):\n"
                "        rows = [row for row in self.rows if row.get(\"test_run\") == %r]\n"
                "        self.assertEqual(len(rows), 1)\n"
                "        self.assertEqual(rows[0][\"capabilities\"], %r)\n"
                "        selector = re.compile(rows[0][\"test_run\"])\n"
                "        self.assertEqual({name for name in self.declared if selector.search(name)}, {%r})\n\n\n"
                "if __name__ == \"__main__\":\n"
                "    unittest.main()\n" % (self.name, self.name, self.selector, self.declared, self.test))

    def coverage_row(self):
        return ("| %s-%d | Merge proof path %s | `released` | Merge proof fixture | Merge proof observation "
                "| Merge proof cleanup | `%s`, `%s` | dedicated row |"
                % (self.area, self.number, self.name, self.id, self.test))

    def coverage_note(self):
        return ("Updated by the merge proof %s. %s-%d is new, with one dedicated catalog row. No class changed."
                % (self.name, self.area, self.number))

    def paths(self):
        return {
            "test": "integration/%s_test.go" % self.snake,
            "fragment": ".github/scenarios/catalog.d/%s.json" % self.group,
            "capability": "integration/fixture/%s.go" % self.snake,
            "coverage": "integration/fixture/coverage/%s.md" % self.area,
            "checks": "tools/test_ci_rows_%s_%s.py" % (self.group.replace("-", "_"), self.snake),
        }


AUTHORS = {
    "osd": Author("OSD", "osd", "OSD", {}, None, "OSD=1 OSD_STORE=bluestore OSD_REPLICAS=1 OSD_PROFILE=legacy-v6"),
    "mds": Author("MDS", "mds-cephfs", "MDS", {"mds_fresh": "1", "mds_cephfs": "1"}, "fixtureMDSCephFS", "MDS_FRESH=1 MDS_CEPHFS=1"),
    "rbda": Author("RBDA", "rbd", "RBD", {"rbd": "1"}, "fixtureRBD", "RBD=1"),
    "rbdb": Author("RBDB", "rbd", "RBD", {"rbd": "1"}, "fixtureRBD", "RBD=1"),
}
# Two controls of the four-OSD erasure topology, the most frequent kind of
# new row: each its own mode capability beside osd-erasure-width.
ERASURE_PARAMS = {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container",
                  "osd": "1", "osd_store": "bluestore", "osd_replicas": "4", "osd_profile": "replicated-pg-v8"}
for _key in ("eca", "ecb"):
    AUTHORS[_key] = Author(_key.upper(), "osd", "OSD", {}, None, "OSD=1 OSD_STORE=bluestore OSD_REPLICAS=4",
                           test="TestCephOSDErasureMergeProof%sIntegration" % _key.upper(), base=ERASURE_PARAMS,
                           id_profile="aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8",
                           also=("osd-erasure-width",))


# Text edits that keep each file's line ending.

def newline(text):
    return "\r\n" if "\r\n" in text else "\n"


def append_rows(text, rows):
    """Append catalog rows to a fragment as an author does: after its last row."""
    nl = newline(text)
    match = re.search(r"\n  \}\r?\n \]\r?\n\}(\r?\n)?\Z", text)
    if not match:
        raise AssertionError("catalog fragment does not end in its last row")
    blocks = []
    for row in rows:
        body = json.dumps(row, indent=1, ensure_ascii=False).split("\n")
        blocks.append(nl.join("  " + line for line in body))
    tail = nl + "  }," + nl + ("," + nl).join(blocks) + nl + " ]" + nl + "}" + (match.group(1) or "")
    return text[:match.start()] + tail


def coverage_with(text, row, note):
    """Add a row to the area file's Rows table and a dated note under Updates."""
    nl = newline(text)
    lines = text.split(nl)
    end = lines.index("## Rows")
    while not lines[end].startswith("|"):
        end += 1
    while end < len(lines) and lines[end].startswith("|"):
        end += 1
    lines.insert(end, row)
    if "## Updates" in lines:
        at = lines.index("## Updates") + 1
        while at < len(lines) and not lines[at].startswith("## "):
            at += 1
        while lines[at - 1] == "":
            at -= 1
        lines[at:at] = ["", note]
    else:
        while lines and lines[-1] == "":
            lines.pop()
        lines += ["", "## Updates", "", note, ""]
    return nl.join(lines)


def row_numbers(text, area):
    return [int(number) for number in re.findall(r"(?m)^\| %s-(\d+) \|" % re.escape(area), text)]


# Conflict resolution that keeps the rows of both sides and refuses anything else.

def compose_fragment(base, ours, theirs):
    rows = [json.loads(text)["scenarios"] for text in (base, ours, theirs)]
    for side in rows[1:]:
        if side[:len(rows[0])] != rows[0]:
            raise AssertionError("a side changed rows of the base fragment; not a composition of added rows")
    added = rows[2][len(rows[0]):]
    if {row["id"] for row in added} & {row["id"] for row in rows[1]}:
        raise AssertionError("both sides add a row with one identifier")
    composed = append_rows(ours, added)
    if json.loads(composed)["scenarios"] != rows[1] + added:
        raise AssertionError("composed fragment does not hold both sides' rows")
    return composed


def compose_insertions(base, ours, theirs):
    """Both sides' added lines at their base positions, ours first; any other
    change than an insertion refuses the composition."""
    nl = newline(ours)
    base_lines = base.split(nl)
    inserted = []
    for side in (ours, theirs):
        lines, at = side.split(nl), {}
        for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, base_lines, lines, autojunk=False).get_opcodes():
            if op == "insert":
                at[i1] = lines[j1:j2]
            elif op != "equal":
                raise AssertionError("a side changed base lines; not a composition of added lines")
        inserted.append(at)
    result = []
    for index in range(len(base_lines) + 1):
        for at in inserted:
            result += at.get(index, [])
        if index < len(base_lines):
            result.append(base_lines[index])
    return nl.join(result)


class Repository:
    """A temporary Git repository isolated from the user's and the system's
    Git configuration."""

    def __init__(self, path):
        self.path = path
        config = path.parent / "gitconfig"
        config.write_text("", encoding="utf-8")
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=str(config), GIT_CONFIG_NOSYSTEM="1",
                        GIT_TERMINAL_PROMPT="0", LC_ALL="C", PYTHONDONTWRITEBYTECODE="1")
        for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY"):
            self.env.pop(name, None)

    def run(self, *args, check=True, cwd=None):
        result = subprocess.run(list(args), cwd=str(cwd or self.path), env=self.env, capture_output=True,
                                text=True, encoding="utf-8", errors="replace", timeout=TIMEOUT)
        if check and result.returncode != 0:
            raise AssertionError("%s failed with status %d:\n%s\n%s" % (" ".join(args), result.returncode,
                                                                        result.stdout[-4000:], result.stderr[-4000:]))
        return result

    def git(self, *args, check=True):
        return self.run("git", "-c", "user.name=Merge proof", "-c", "user.email=merge-proof@example.invalid",
                        "-c", "core.autocrlf=false", "-c", "commit.gpgsign=false", "-c", "rerere.enabled=false",
                        *args, check=check)

    def read(self, relative):
        return (self.path / relative).read_bytes().decode("utf-8")

    def write(self, relative, text):
        target = self.path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(text.encode("utf-8"))


def copy_tracked(source, target):
    listing = subprocess.run(["git", "-C", str(source), "ls-files", "-z"], capture_output=True, timeout=120)
    if listing.returncode != 0:
        raise AssertionError("git cannot list the tracked files of %s: %s" % (source, listing.stderr.decode(errors="replace")))
    count = 0
    for name in listing.stdout.decode("utf-8").split("\0"):
        origin = source / name
        if not name or not origin.is_file() or origin.is_symlink():
            continue
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, destination)
        count += 1
    return count


def remove_tree(path):
    def writable(function, name, _):
        os.chmod(name, stat.S_IWRITE)
        function(name)
    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=writable)
    else:
        shutil.rmtree(path, onerror=writable)


def readme_patterns(text=None):
    """The file patterns of the README section "Adding a native test"."""
    text = README.read_text(encoding="utf-8") if text is None else text
    if SECTION not in text:
        raise AssertionError("the scenario README has no section %r" % SECTION)
    body = text.split(SECTION, 1)[1].split("\n## ", 1)[0]
    patterns = re.findall(r"(?m)^- `([^`]+)`", body)
    # A <placeholder> is one path component part; the rest is literal.
    return [(pattern, re.compile("^%s$" % "[A-Za-z0-9_.-]+".join(map(re.escape, re.split(r"<[^<>/]+>", pattern)))))
            for pattern in patterns]


class MergeProofTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("git") is None or shutil.which("go") is None:
            raise AssertionError("the merge proof needs git and go on PATH")
        cls.scratch = Path(tempfile.mkdtemp(prefix="merge-proof-"))
        try:
            cls.repo = Repository(cls.scratch / "repo")
            cls.repo.path.mkdir()
            cls.copied = copy_tracked(ROOT, cls.repo.path)
            cls.repo.git("init", "-q")
            cls.repo.git("checkout", "-q", "-b", "base")
            cls.repo.git("add", "--all", ".")
            cls.repo.git("commit", "-q", "--no-verify", "-m", "base")
            cls.base_ids = cls.repo.run(sys.executable, "tools/scenario_plan.py", "list").stdout.split()
            cls.base_rows = sum(len(json.loads(path.read_bytes().decode("utf-8"))["scenarios"])
                                for path in (cls.repo.path / ".github" / "scenarios" / "catalog.d").glob("*.json")
                                if not path.name.startswith("_"))
            cls.branches = {}
            for area in ("OSD", "MDS", "RBD"):
                taken = max(row_numbers(cls.repo.read("integration/fixture/coverage/%s.md" % area), area))
                for author in AUTHORS.values():
                    if author.area == area:
                        taken += 1
                        author.number = taken
            for key, author in AUTHORS.items():
                cls.branches[key] = cls.author_branch(author)
        except BaseException:
            remove_tree(cls.scratch)
            raise

    @classmethod
    def tearDownClass(cls):
        remove_tree(cls.scratch)

    @classmethod
    def author_branch(cls, author):
        repo, paths = cls.repo, author.paths()
        repo.git("checkout", "-q", "-f", "-b", "add-" + author.name.lower(), "base")
        repo.write(paths["test"], author.native_test())
        repo.write(paths["capability"], author.capability_source())
        repo.write(paths["checks"], author.row_checks())
        repo.write(paths["fragment"], append_rows(repo.read(paths["fragment"]),
                                                  [{"id": author.id, "group": author.group, "params": author.params}]))
        repo.write(paths["coverage"], coverage_with(repo.read(paths["coverage"]), author.coverage_row(), author.coverage_note()))
        repo.git("add", "--", *paths.values())
        repo.git("commit", "-q", "--no-verify", "-m", "add " + author.test)
        return "add-" + author.name.lower()

    def changed(self, branch):
        return set(self.repo.git("diff", "--name-only", "base", branch).stdout.split())

    def merge(self, ours, theirs):
        """Merge theirs into ours; return the unmerged paths."""
        self.repo.git("checkout", "-q", "-f", ours)
        self.repo.git("merge", "--no-ff", "--no-edit", theirs, check=False)
        return set(self.repo.git("diff", "--name-only", "--diff-filter=U").stdout.split())

    def stage(self, relative, number):
        return self.repo.git("show", ":%d:%s" % (number, relative)).stdout

    def validate(self, authors):
        """Run the validators of the merged tree in it and check they saw the
        added rows, tests and capabilities."""
        repo, python = self.repo, sys.executable
        added = [author.id for author in authors]
        listed = repo.run(python, "tools/scenario_plan.py", "list").stdout.split()
        self.assertEqual(sorted(sid for sid in listed if sid not in self.base_ids), sorted(added))
        validate = repo.run(python, "tools/scenario_plan.py", "validate", check=False)
        self.assertEqual(validate.returncode, 0, "scenario_plan.py validate refused the merged tree:\n" + validate.stderr[-6000:])
        rows = int(re.search(r"catalog valid: (\d+) scenarios", validate.stdout).group(1))
        self.assertEqual(rows, self.base_rows + len(authors))
        for author in authors:
            plan = json.loads(repo.run(python, "tools/scenario_plan.py", "plan", "--select", author.id).stdout)
            self.assertEqual([(row["scenario"], row["capabilities"]) for row in plan["include"]], [(author.id, author.declared)])

        repo.run(python, "tools/cluster_free_tests.py", "check")
        selector = repo.run(python, "tools/cluster_free_tests.py", "selector").stdout.strip()
        names = set(selector[2:-2].split("|"))
        for author in authors:
            self.assertIn(author.helper, names)
            self.assertNotIn(author.test, names)
        env = dict(repo.env, GOWORK="off", CGO_ENABLED="0")
        run = subprocess.run(["go", "test", "-mod=readonly", "-count=1", "-v", "-run", selector, "./integration"],
                             cwd=str(repo.path), env=env, capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=TIMEOUT)
        log = self.scratch / "cluster-free.log"
        log.write_text(run.stdout, encoding="utf-8")
        self.assertEqual(run.returncode, 0, "cluster-free tests failed:\n" + (run.stdout + run.stderr)[-6000:])
        repo.run(python, "tools/cluster_free_tests.py", "verify", "--log", str(log))

        coverage = repo.run(python, "tools/native_path_coverage.py", check=False)
        self.assertEqual(coverage.returncode, 0, "native_path_coverage.py refused the merged tree:\n" + coverage.stderr[-6000:])
        released = int(re.search(r"released (\d+)", coverage.stdout).group(1))
        self.assertEqual(released, self.base_released + len(authors))

        fixture = subprocess.run(["go", "test", "-mod=readonly", "-count=1", "-v", "./..."], cwd=str(repo.path / "integration" / "fixture"),
                                 env=env, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=TIMEOUT)
        self.assertEqual(fixture.returncode, 0, "fixture module tests failed:\n" + (fixture.stdout + fixture.stderr)[-6000:])
        for test in ("TestFixtureCapabilityDefinitions", "TestCatalogRowsAreAdmittedByTheFixture",
                     "TestCatalogRowsDeclareFixtureSharing", "TestCICatalogInventory",
                     "TestExclusiveCapabilityAdmitsOneTestOfItsOwnFixture"):
            self.assertRegex(fixture.stdout, r"(?m)^--- PASS: %s \(" % test)

        checks = repo.run(python, "-m", "unittest", "discover", "-s", "tools", "-p", "test_ci_rows_*.py", "-v", check=False)
        self.assertEqual(checks.returncode, 0, "row checks failed:\n" + checks.stderr[-6000:])
        for author in authors:
            self.assertRegex(checks.stderr, r"test_row_selects_only_its_test_with_its_capability \(test_ci_rows_[a-z_]+%s\.MergeProof%sRowsTest"
                             % (author.snake, author.name))

    def test_branches_edit_only_the_files_the_readme_names(self):
        patterns = readme_patterns(self.repo.read(".github/scenarios/README.md"))
        self.assertTrue(patterns)
        for key, branch in self.branches.items():
            changed = self.changed(branch)
            self.assertEqual(changed, set(AUTHORS[key].paths().values()))
            for path in changed:
                self.assertTrue([pattern for pattern, compiled in patterns if compiled.match(path)],
                                "%s edits %s, which the README section does not name" % (branch, path))
            for pattern, compiled in patterns:
                self.assertTrue([path for path in changed if compiled.match(path)], "%s edits no %s" % (branch, pattern))

    def test_merges_compose_and_validate(self):
        # Case A and Case B run in this order in one test so that both merged
        # trees are validated in one directory and share the build cache.
        self.repo.git("checkout", "-q", "-f", "base")
        self.base_released = int(re.search(r"released (\d+)", self.repo.run(sys.executable, "tools/native_path_coverage.py").stdout).group(1))

        # Case A: different areas, no textual conflict in any file.
        unmerged = self.merge(self.branches["osd"], self.branches["mds"])
        self.assertEqual(unmerged, set(), "two branches of different areas conflict")
        self.assertEqual(len(self.repo.git("rev-list", "--parents", "-n", "1", "HEAD").stdout.split()), 3, "no merge commit")
        self.validate([AUTHORS["osd"], AUTHORS["mds"]])

        # Case B: one area; a conflict only in the area's fragment and
        # coverage file, resolved by keeping both rows.
        self.same_area("rbda", "rbdb")
        # Case C: two controls of the four-OSD erasure topology in one area.
        self.same_area("eca", "ecb")

    def same_area(self, first, second):
        ours, theirs = AUTHORS[first], AUTHORS[second]
        allowed = {ours.paths()["fragment"], ours.paths()["coverage"]}
        unmerged = self.merge(self.branches[first], self.branches[second])
        self.assertTrue(unmerged <= allowed, "conflicts outside the area's fragment and coverage file: %s" % sorted(unmerged - allowed))
        for relative in unmerged:
            stages = [self.stage(relative, number) for number in (1, 2, 3)]
            compose = compose_fragment if relative.endswith(".json") else compose_insertions
            self.repo.write(relative, compose(*stages))
            self.repo.git("add", "--", relative)
        self.repo.git("commit", "-q", "--no-verify", "--no-edit")
        for relative in allowed:
            text = self.repo.read(relative)
            for author in (ours, theirs):
                self.assertIn(author.id if relative.endswith(".json") else author.coverage_row(), text)
        self.validate([ours, theirs])


class CompositionTest(unittest.TestCase):
    """The conflict resolution keeps both sides' rows and refuses anything else."""

    FRAGMENT = '{\n "scenarios": [\n  {\n   "id": "a",\n   "group": "g",\n   "params": {}\n  }\n ]\n}\n'

    def test_fragment_composition_keeps_both_added_rows(self):
        ours = append_rows(self.FRAGMENT, [{"id": "b", "group": "g", "params": {}}])
        theirs = append_rows(self.FRAGMENT, [{"id": "c", "group": "g", "params": {}}])
        composed = compose_fragment(self.FRAGMENT, ours, theirs)
        self.assertEqual([row["id"] for row in json.loads(composed)["scenarios"]], ["a", "b", "c"])
        crlf = self.FRAGMENT.replace("\n", "\r\n")
        self.assertNotIn("\n", append_rows(crlf, [{"id": "b", "group": "g", "params": {}}]).replace("\r\n", ""))
        with self.assertRaisesRegex(AssertionError, "changed rows of the base"):
            compose_fragment(self.FRAGMENT, ours, theirs.replace('"a"', '"z"'))
        with self.assertRaisesRegex(AssertionError, "one identifier"):
            compose_fragment(self.FRAGMENT, ours, ours)

    def test_line_composition_refuses_a_changed_line(self):
        base = "# T\n\n| A-1 |\n\n## Updates\n\nnote 1\n"
        ours = base.replace("| A-1 |\n", "| A-1 |\n| A-2 |\n") + "\nnote 2\n"
        theirs = base.replace("| A-1 |\n", "| A-1 |\n| A-3 |\n") + "\nnote 3\n"
        self.assertEqual(compose_insertions(base, ours, theirs),
                         "# T\n\n| A-1 |\n| A-2 |\n| A-3 |\n\n## Updates\n\nnote 1\n\nnote 2\n\nnote 3\n")
        with self.assertRaisesRegex(AssertionError, "changed base lines"):
            compose_insertions(base, ours, theirs.replace("note 1", "note one"))

    def test_readme_section_names_the_files(self):
        patterns = [pattern for pattern, _ in readme_patterns()]
        self.assertEqual(len(patterns), len(Author("X", "g", "A", {}, None, "").paths()), patterns)


if __name__ == "__main__":
    unittest.main()
