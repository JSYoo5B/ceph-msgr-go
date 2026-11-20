"""Every workflow step that lists Go tests runs under a Go that can list them.

The Python tools ask Go which tests exist (go_test_list.py, fuzz_targets.py),
so a step that runs them needs an installed Go at least as new as the highest
`go` directive of the modules they list; with GOTOOLCHAIN=local nothing
switches toolchains for it. This test reads the workflows and go.mod files,
finds each step that runs the tools suite or a listing tool, takes the Go that
the last setup-go before it installs (every matrix value), and compares.
"""
import ast
import fnmatch
import itertools
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"
WORKFLOWS = ROOT / ".github" / "workflows"

# Tools that run Go on repository modules: {module name: (subcommands that
# run Go, or None for every invocation; module directories Go loads)}. The
# guard test below keeps this table equal to the tools that call Go.
LISTERS = {
    "go_test_list": (None, ("tools/quality",)),  # declarations run tools/quality/testdecls
    "cluster_free_tests": (None, (".",)),
    "native_test_results": (("snapshot",), (".",)),
    # validate and sharing list the suites and run the fixture's
    # TestCatalogRowsAreAdmittedByTheFixture.
    "scenario_plan": (("validate", "sharing"), (".", "integration/fixture")),
    "native_path_coverage": (None, None),  # its MODULES, read below
    "fuzz_targets": (None, (".",)),
    "product_dependencies": (None, (".",)),
    "workflow_lint": (None, (".",)),  # pinned Go tool under the product toolchain
    "windows_interop": (None, (".", "integration/fixture")),
}
SUITE = re.compile(r"-m unittest discover -s tools(?: -p '([^']+)')?")


def modules_of(name):
    subcommands, modules = LISTERS[name]
    if modules is None:
        import native_path_coverage
        modules = native_path_coverage.MODULES
    return modules


def go_directive(module):
    text = (ROOT / module / "go.mod").read_text(encoding="utf-8")
    match = re.search(r"(?m)^go (\d+)\.(\d+)(?:\.(\d+))?\s*$", text)
    if not match:
        raise AssertionError("no go directive in %s/go.mod" % module)
    return tuple(int(part or 0) for part in match.groups())


def installed(version):
    """A setup-go version spec as a comparable tuple; x is any patch."""
    match = re.fullmatch(r"(\d+)\.(\d+)(?:\.(\d+|x))?", version)
    if not match:
        raise AssertionError("unsupported go-version %r" % version)
    major, minor, patch = match.groups()
    return int(major), int(minor), float("inf") if patch in (None, "x") else int(patch)


def local_imports(name, seen=None):
    """The tools modules `name` imports, transitively (parsed, not grepped)."""
    seen = set() if seen is None else seen
    if name in seen or not (TOOLS / (name + ".py")).is_file():
        return seen
    seen.add(name)
    tree = ast.parse((TOOLS / (name + ".py")).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names = [node.module]
        for imported in names:
            local_imports(imported.split(".")[0], seen)
    return seen


def suite_modules(pattern):
    modules = set()
    for path in TOOLS.glob("*.py"):
        if fnmatch.fnmatch(path.name, pattern):
            for name in local_imports(path.stem) & set(LISTERS):
                modules.update(modules_of(name))
    return modules


def jobs(text):
    """[(job name, job text)] of a workflow."""
    match = re.search(r"(?m)^jobs:\n", text)
    if not match:
        return []
    parts = re.split(r"(?m)^  ([A-Za-z0-9_-]+):[ \t]*\n", text[match.end():])
    return list(zip(parts[1::2], parts[2::2]))


def steps(job):
    return re.split(r"(?m)^      - ", job)[1:]


def matrix_values(job, key):
    """Every value of matrix key: an inline list or `include` entries."""
    match = re.search(r"(?ms)^    strategy:\n(.*?)(?=^    \S|\Z)", job)
    strategy = match.group(1) if match else ""
    listed = re.search(r"(?m)^        %s: (\[.*\])\s*$" % re.escape(key), strategy)
    if listed:
        return ast.literal_eval(listed.group(1))
    included = re.findall(r"(?m)^ {10}(?:- |  )%s: (.+?)\s*$" % re.escape(key), strategy)
    if not included:
        raise AssertionError("no matrix values for %s" % key)
    return [value.strip("'\"") for value in included]


def go_versions(job, step):
    match = re.search(r"(?m)^\s+go-version: (.+?)\s*$", step)
    if not match:
        raise AssertionError("setup-go without go-version")
    value = match.group(1).strip("'\"")
    expression = re.fullmatch(r"\$\{\{ matrix\.(\w+) \}\}", value)
    return matrix_values(job, expression.group(1)) if expression else [value]


def step_requirements(step):
    """(what, module directories) for each Go listing a step runs."""
    found = []
    for match in SUITE.finditer(step):
        modules = suite_modules(match.group(1) or "test*.py")
        if modules:
            found.append(("tools suite %s" % (match.group(1) or "test*.py"), modules))
    # Tools run from the checkout or from a trusted copy in $RUNNER_TEMP.
    for match in re.finditer(r"(?:tools/|\$RUNNER_TEMP/)(\w+)\.py\"?((?:[ \t]+[\w-]+)?)", step):
        name, argument = match.group(1), match.group(2).strip()
        if name in LISTERS and name != "go_test_list":
            subcommands, _ = LISTERS[name]
            if subcommands is None or argument in subcommands:
                found.append(("%s %s" % (name, argument), set(modules_of(name))))
    return found


def problems(name, text):
    """Steps of the workflow `name` whose installed Go cannot list their modules."""
    result = []
    for job_name, job in jobs(text):
        versions = None
        for step in steps(job):
            if re.search(r"(?m)uses: actions/setup-go@", step):
                versions = go_versions(job, step)
            for what, modules in step_requirements(step):
                required = max(go_directive(module) for module in modules)
                if versions is None:
                    result.append("%s %s: %s runs without setup-go" % (name, job_name, what))
                    continue
                for version in versions:
                    if installed(version) < required:
                        result.append("%s %s: %s needs Go %s, runs with Go %s"
                                      % (name, job_name, what, ".".join(map(str, required)), version))
    return result


def active_go(text, job_name, command):
    """setup-go versions in effect at the first step of a job running command."""
    job = dict(jobs(text))[job_name]
    versions = None
    for step in steps(job):
        if "uses: actions/setup-go@" in step:
            versions = go_versions(job, step)
        elif command in step:
            return versions
    raise AssertionError("%s has no step running %s" % (job_name, command))


OLD_PLATFORM_UNIT = """jobs:
  unit:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        go: ['1.25.0', '1.27.x']
    env:
      GOTOOLCHAIN: local
    steps:
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version: ${{ matrix.go }}
      - name: Test CI diagnostic tools
        run: |
          "$DIAGNOSTIC_PYTHON" -m unittest discover -s tools -p 'test_*.py'
"""


class WorkflowToolchainTests(unittest.TestCase):
    def test_every_listing_step_has_a_go_that_can_list_its_modules(self):
        checked = 0
        for path in sorted(WORKFLOWS.glob("*.yml")):
            text = path.read_text(encoding="utf-8")
            checked += sum(len(step_requirements(step)) for _, job in jobs(text) for step in steps(job))
            self.assertEqual(problems(path.name, text), [])
        self.assertGreaterEqual(checked, 8)

    def test_the_suite_lists_the_fixture_module(self):
        # The highest directive the suite needs is the fixture's Go 1.26.
        self.assertIn("integration/fixture", suite_modules("test_*.py"))
        self.assertEqual(max(go_directive(module) for module in suite_modules("test_*.py")),
                         go_directive("integration/fixture"))
        self.assertEqual(suite_modules("test_rgw_native_gate*.py"), set())

    def test_old_platform_unit_layout_is_refused(self):
        # The layout of platform-checks.yml before this check: the whole suite
        # under the matrix Go with GOTOOLCHAIN=local.
        self.assertEqual(problems("platform-checks.yml", OLD_PLATFORM_UNIT),
                         ["platform-checks.yml unit: tools suite test_*.py needs Go 1.26.0, runs with Go 1.25.0"])
        bare = OLD_PLATFORM_UNIT.replace("go-version: ${{ matrix.go }}", "go-version: '1.27.x'").replace(
            "      - uses: actions/setup-go", "      - uses: actions/checkout")
        self.assertEqual(problems("x.yml", bare), ["x.yml unit: tools suite test_*.py runs without setup-go"])
        validate = "jobs:\n  plan:\n    steps:\n      - uses: actions/setup-go@v\n        with:\n          go-version: '1.24.x'\n" \
                   "      - run: python3 tools/scenario_plan.py validate\n      - run: python3 tools/scenario_plan.py plan --select all\n"
        self.assertEqual(problems("p.yml", validate),
                         ["p.yml plan: scenario_plan validate needs Go 1.26.0, runs with Go 1.24.x"])
        # The periodic plan job of this branch before main's fixture admission
        # merged: validate under the product's Go 1.25.0 cannot run the fixture tests.
        for command in ("validate", "sharing"):
            product = validate.replace("'1.24.x'", "'1.25.0'").replace("scenario_plan.py validate", "scenario_plan.py " + command)
            self.assertEqual(problems("p.yml", product),
                             ["p.yml plan: scenario_plan %s needs Go 1.26.0, runs with Go 1.25.0" % command])

    def test_product_steps_keep_the_compatibility_go(self):
        platform = (WORKFLOWS / "platform-checks.yml").read_text(encoding="utf-8")
        self.assertEqual(active_go(platform, "unit", "go test -json -fullpath ./..."), ["1.25.0", "1.27.x"])
        basic = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
        self.assertEqual(active_go(basic, "basic", "go build -mod=readonly ./..."), ["1.25.0"])
        self.assertEqual(go_directive("."), (1, 25, 0))

    def test_listers_table_names_every_tool_that_calls_go(self):
        calling = set()
        for path in TOOLS.glob("*.py"):
            if path.name.startswith("test_"):
                continue
            text = path.read_text(encoding="utf-8")
            if re.search(r"go_test_list\.(?:list_tests|package_tests|selected_tests|test_files|declares|declarations)\b"
                         r"|\[\"go\"|_go\(", text):
                calling.add(path.stem)
        self.assertEqual(calling, set(LISTERS))


if __name__ == "__main__":
    unittest.main()
