#!/usr/bin/env python3
"""Validate the native interoperability scenario catalog and plan a selection.

The catalog (composed by scenario_catalog.py) is the discoverable list of the
real-daemon scenarios and selectable platform check suites. Each native entry
has a stable identifier, group and workflow matrix parameters. This tool
validates the catalog against the reusable check jobs, the tests it selects and
the fixture, and prints the selected matrix with an explanation. Every catalog
row is in the default coverage (`all`); there is no separate list of required
rows. `validate` resolves test selectors against the tests Go lists for the
native suites (go_test_list.py compiles them and executes no test body) and runs
the fixture's own admission of every row (one cluster-free Go test of
integration/fixture, which needs the fixture module's Go toolchain); it starts
no cluster. `list`, `plan` and `env` read local files only.

  scenario_plan.py validate               invariants, fixed jobs, selector targets, documented
                                          reproductions, fixture admission of every row
  scenario_plan.py list                   one identifier per line
  scenario_plan.py plan --select S        matrix JSON on stdout, explanation on stderr
  scenario_plan.py env ID                 shell exports of the native workflow's environment for one row
  scenario_plan.py sharing                every row's fixture sharing value and the default coverage's
                                          jobs once shareable rows of one configuration share a fixture

`list`, `plan` and `env` accept --catalog PATH to read another revision's
catalog (a pull request's vocabulary) while running this trusted script. PATH
is a fragment directory, or the earlier single catalog file of a pull request
opened before the split.

A selector S is "all", "group:<group>" or a scenario identifier; several can be
given. An unknown selector is an error, never an empty or conservative match.

A row may declare the fixture capabilities its test needs in the parameter
"capabilities": a comma-separated, sorted list of names that the fixture
defines with defineCapability calls in integration/fixture. Every reading of
the catalog refuses a capability on a row whose test_run is not exactly one
anchored test (^TestName$). Whether the fixture defines a declared capability
and admits it on the row's topology is the fixture's own decision: validate
passes every row, as the native workflow forwards it, through the fixture's
loadProfile (TestCatalogRowsAreAdmittedByTheFixture) and reports each row the
fixture refuses, so the planner keeps no copy of the fixture's rules. plan
reads another revision's catalog and leaves that check to that revision's own
validate and to its fixture, which refuses the same rows before it starts a
cluster.

`env ID` prints one `export NAME=value` line for every variable the native
workflow binds to a matrix parameter, with the workflow's fallbacks, so a
documented reproduction runs a row as CI does:

  (
    env_lines=$(python3 tools/scenario_plan.py env ID) &&
    eval "$env_lines" &&
    sh integration/run.sh
  )

`env` exits non-zero and prints nothing on stdout for an unknown row. The
assignment keeps that status, which `eval "$(...)"` would replace with eval's
own success, so the && chain never starts the runner with another row's
environment, and the subshell keeps one row's environment from the next.

Every native test declares whether it may share a fixture with the tests
after it, beside its function (//fixture:shareable <reason> or
//fixture:isolated <reason>; integration/fixture/FIXTURE_SHARING.md). A row is
shareable only when every test its effective selector selects is. The fixture
derives each row's value (TestCatalogRowsDeclareFixtureSharing); validate and
sharing run that test and refuse a row without a value, a test without a
declaration and a misplaced declaration. A fixture configuration is a row's
parameters without test_run. sharing lists, per configuration, the shareable
rows that one fixture can run and the isolated rows that keep their own; it
plans nothing, and the default and periodic coverage still run one job per row.

validate refuses a documented `env ID` of an unknown row, a documented `env`
that is not this checked form, and a hand-written reproduction that selects
the exact test of a row with capabilities but does not declare them.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import go_test_list
import scenario_catalog

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / scenario_catalog.CATALOG
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CHECKS_WORKFLOW = ROOT / ".github" / "workflows" / "platform-checks.yml"
NATIVE_WORKFLOW = ROOT / ".github" / "workflows" / "native-scenarios.yml"
REQUIRED_PARAMS = ("key", "service", "family")
SUPPORTED_RELEASE = re.compile(r"^20\.2\.\d+$")
FIXTURE = ROOT / "integration" / "fixture"
# The fixture's exactTestSelector and capabilityNamePattern (capability.go).
EXACT_TEST = re.compile(r"\^Test[A-Za-z0-9_]+\$")
CAPABILITY_NAME = re.compile(r"[a-z][a-z0-9]*(-[a-z0-9]+)*")
# The fixture test that passes every catalog row through loadProfile, and the
# mark of each problem line it reports (capability_test.go).
ADMISSION_TEST = "TestCatalogRowsAreAdmittedByTheFixture"
ADMISSION_MARK = "catalog admission: "
# The fixture test that derives every row's fixture sharing value, the mark of
# each problem line and the mark of each row's value (sharing_test.go).
SHARING_TEST = "TestCatalogRowsDeclareFixtureSharing"
SHARING_MARK = "fixture sharing: "
SHARING_ROW_MARK = "fixture sharing row: "
SHARING_VALUES = ("shareable", "isolated")
# A documented assignment of one exact test selector, and a documented
# reproduction through `env` (reproduction_problems).
DOCUMENTED_RUN = re.compile(r"CEPH_MSGR_TEST_RUN=['\"]?(\^Test[A-Za-z0-9_]+\$)")
DOCUMENTED_CAPABILITIES = re.compile(r"CEPH_MSGR_TEST_CAPABILITIES=['\"]?([a-z0-9,-]+)")
DOCUMENTED_ENV = re.compile(r"scenario_plan\.py env ([^\s\"'`)]+)")
# The checked form of a documented `env` (env_recipe_problems): the lookup is
# assigned on its own line inside a subshell, and its status reaches the runner
# through an && chain.
CHECKED_ENV = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=\$\(python3 tools/scenario_plan\.py env [^\s)]+\) &&$")
SUBSTITUTED_ENV = re.compile(r"\$\(\s*(?:python3\s+)?\S*scenario_plan\.py env |`python3\s+\S*scenario_plan\.py env ")
DOCUMENTED_RUNNER = re.compile(r"integration/run\.sh|integration/fixture run ")
WORKFLOW_BINDING = re.compile(r"(?m)^ +(CEPH_[A-Z0-9_]+): \$\{\{ matrix\.([a-z_]+)(?: \|\| '([^']*)')? \}\}\s*$")


class PlanError(Exception):
    pass


def load_catalog_document(path=CATALOG):
    try:
        return scenario_catalog.load(path)
    except scenario_catalog.CatalogError as error:
        raise PlanError(str(error)) from None


def load_catalog(path=CATALOG):
    return load_catalog_document(path)["scenarios"]


def validate_catalog(scenarios):
    """Return a list of problems; empty when the catalog is consistent."""
    problems, seen = [], set()
    # This file and workflow belong to the trusted calling revision, even
    # when the catalog is read from a newer pull-request commit.
    supported = set(re.findall(r"(?m)^ +CEPH_[A-Z0-9_]+: \$\{\{ matrix\.([a-z_]+)",
                               NATIVE_WORKFLOW.read_text(encoding="utf-8")))
    supported.add("release")  # Display/validated family; image selects the daemon.
    if not scenarios:
        problems.append("catalog has no scenarios")
    for index, entry in enumerate(scenarios):
        sid = entry.get("id")
        if not isinstance(sid, str) or not sid:
            problems.append("entry %d has no identifier" % index)
            continue
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,159}", sid) or sid == "all":
            problems.append("entry %d has an invalid identifier" % index)
        if sid in seen:
            problems.append("duplicate identifier %s" % sid)
        seen.add(sid)
        if not entry.get("group"):
            problems.append("%s has no group" % sid)
        params = entry.get("params")
        if not isinstance(params, dict) or not params:
            problems.append("%s has no parameters" % sid)
            continue
        for name in sorted(params.keys() - supported):
            problems.append("%s: trusted native runner does not support parameter %s" % (sid, name))
        for name in REQUIRED_PARAMS:
            if not params.get(name):
                problems.append("%s lacks required parameter %s" % (sid, name))
        release = params.get("release")
        if release is not None and not SUPPORTED_RELEASE.match(release):
            problems.append("%s names unsupported release %s" % (sid, release))
        if "test_run" in params:
            pattern = params["test_run"]
            if not pattern:
                problems.append("%s has an empty test selector" % sid)
            else:
                if "/" in pattern:
                    problems.append("%s: only top-level test selectors are supported" % sid)
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append("%s has an invalid test selector: %s" % (sid, error))
        if "capabilities" in params:
            problems.extend(declaration_problems(sid, params))
    return problems


def declaration_problems(sid, params):
    """A capability admits a fixture mode for exactly one anchored test."""
    value = params["capabilities"]
    names = value.split(",") if isinstance(value, str) else None
    if not names or not all(CAPABILITY_NAME.fullmatch(name) for name in names):
        return ["%s has an invalid capability list %r" % (sid, value)]
    problems = []
    if names != sorted(set(names)):
        problems.append("%s: capabilities must be sorted and unique" % sid)
    selector = params.get("test_run")
    if not isinstance(selector, str) or not EXACT_TEST.fullmatch(selector):
        problems.append("%s: capabilities %s require a test_run of exactly one anchored test (^TestName$), not %r"
                        % (sid, value, selector))
    return problems


def workflow_bindings(text=None):
    """{environment variable: (matrix parameter, fallback)} of the native job."""
    if text is None:
        text = NATIVE_WORKFLOW.read_text(encoding="utf-8")
    return {name: (param, fallback) for name, param, fallback in WORKFLOW_BINDING.findall(text)}


def row_environment(params, bindings):
    """The environment the native workflow gives one row, in binding order."""
    return [(name, params.get(param) or fallback) for name, (param, fallback) in bindings.items()]


def markdown_chunks(text):
    """The fenced code blocks and the paragraphs of a Markdown text."""
    chunks, current, fenced = [], [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            if current:
                chunks.append("\n".join(current))
            current, fenced = [], not fenced
        elif not fenced and not line.strip():
            if current:
                chunks.append("\n".join(current))
            current = []
        else:
            current.append(line)
    if current:
        chunks.append("\n".join(current))
    return chunks


def documentation_files(root=ROOT):
    """The Markdown files of the tree, outside .git."""
    return sorted(path for path in Path(root).rglob("*.md") if ".git" not in path.relative_to(root).parts)


def env_recipe_problems(name, chunk):
    """A documented `env` lookup cannot start a runner after it failed.

    `eval "$(python3 tools/scenario_plan.py env ID)"` evaluates an empty string
    when the lookup fails and succeeds, so the next line starts the runner
    with whatever environment the shell already had (under `sh -e` as well).
    Every line that substitutes an `env` lookup is therefore the
    assignment CHECKED_ENV on the line after one that opens a subshell "(",
    followed by `eval "$VAR"` and the runner in one && chain, with the
    subshell closed on the line after the runner."""
    problems = []
    lines = [line.strip() for line in chunk.splitlines()]
    for index, line in enumerate(lines):
        if "scenario_plan.py env " not in line:
            continue
        checked = CHECKED_ENV.match(line)
        if not checked:
            if SUBSTITUTED_ENV.search(line):
                problems.append("%s: unchecked env lookup %r; assign it, then chain eval and the runner with &&"
                                % (name, line))
            continue
        variable = checked.group(1)
        if index == 0 or not lines[index - 1].endswith("("):
            problems.append("%s: env lookup %r does not open a subshell" % (name, line))
        evaluated, runner = False, None
        for later in range(index + 1, len(lines)):
            if lines[later] == 'eval "$%s" &&' % variable:
                evaluated = True
            if DOCUMENTED_RUNNER.search(lines[later]):
                runner = later
                break
            if not (lines[later].endswith("&&") or lines[later].endswith("\\")):
                break
        if runner is None:
            problems.append("%s: env lookup %r does not reach its runner through an && chain" % (name, line))
        elif not evaluated:
            problems.append('%s: env lookup %r is not evaluated by eval "$%s" &&' % (name, line, variable))
        elif runner + 1 >= len(lines) or not lines[runner + 1].startswith(")"):
            problems.append("%s: env lookup %r leaves its subshell open after the runner" % (name, line))
    return problems


def reproduction_problems(documents, scenarios):
    """A documented reproduction runs a catalog row as the fixture admits it.

    documents is [(name, text)]. A reproduction through `scenario_plan.py env
    ID` names a row of the catalog. A hand-written environment that selects
    the exact test of a row declaring capabilities declares the same
    capabilities in the same code block or paragraph; without them the fixture
    refuses the run before it starts a cluster. An `env` lookup has the
    checked form of env_recipe_problems."""
    ids = {entry["id"] for entry in scenarios}
    declared = {}
    for entry in scenarios:
        params = entry.get("params", {})
        if params.get("capabilities"):
            declared.setdefault(params.get("test_run"), set()).add(params["capabilities"])
    problems = []
    for name, text in documents:
        for chunk in markdown_chunks(text):
            problems += env_recipe_problems(name, chunk)
            for sid in DOCUMENTED_ENV.findall(chunk):
                if sid not in ids and sid != "ID":
                    problems.append("%s: reproduction names unknown scenario %s" % (name, sid))
            given = set(DOCUMENTED_CAPABILITIES.findall(chunk))
            for selector in DOCUMENTED_RUN.findall(chunk):
                wanted = declared.get(selector)
                if wanted and not given & wanted:
                    problems.append("%s: reproduction of %s lacks CEPH_MSGR_TEST_CAPABILITIES=%s"
                                    % (name, selector, " or ".join(sorted(wanted))))
    return problems


def fixture_admission_problems(run=subprocess.run, directory=FIXTURE):
    """The rows of this tree's catalog that its fixture refuses.

    Runs the fixture's TestCatalogRowsAreAdmittedByTheFixture, which passes
    every row through loadProfile with the native workflow's bindings and
    reports each refusal on a line marked ADMISSION_MARK. A check that does
    not run, does not pass and reports no row is a problem as well, never a
    silent pass."""
    command = ["go", "test", "-mod=readonly", "-count=1", "-json", "-run", "^%s$" % ADMISSION_TEST, "."]
    try:
        result = run(command, cwd=str(directory), env=dict(os.environ, GOWORK="off"),
                     capture_output=True, text=True, timeout=1200)
    except (OSError, subprocess.SubprocessError) as error:
        return ["the fixture admission check (%s) did not run: %s" % (ADMISSION_TEST, error)]
    problems, actions = [], []
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("Test") != ADMISSION_TEST:
            continue
        if event.get("Action") in ("pass", "fail", "skip"):
            actions.append(event["Action"])
        output = event.get("Output") or ""
        if ADMISSION_MARK in output:
            problems.append(output.split(ADMISSION_MARK, 1)[1].rstrip())
    if result.returncode == 0 and actions == ["pass"] and not problems:
        return []
    if not problems:
        tail = " | ".join((result.stderr or result.stdout).strip().splitlines()[-5:])
        problems.append("the fixture admission check (%s) reported %s with exit status %d and no row: %s"
                        % (ADMISSION_TEST, ",".join(actions) or "no result", result.returncode, tail))
    return problems


def fixture_sharing(run=subprocess.run, directory=FIXTURE):
    """(problems, {row: value}) as this tree's fixture derives them.

    Runs the fixture's TestCatalogRowsDeclareFixtureSharing, which reads the
    declaration beside every native test and derives each row's value from
    the tests the row's effective selector selects. A problem line is marked
    SHARING_MARK, a row's value SHARING_ROW_MARK. A check that does not run or
    does not pass is a problem as well, never a silent pass."""
    command = ["go", "test", "-mod=readonly", "-count=1", "-json", "-run", "^%s$" % SHARING_TEST, "."]
    try:
        result = run(command, cwd=str(directory), env=dict(os.environ, GOWORK="off"),
                     capture_output=True, text=True, timeout=1200)
    except (OSError, subprocess.SubprocessError) as error:
        return ["the fixture sharing check (%s) did not run: %s" % (SHARING_TEST, error)], {}
    problems, values, actions = [], {}, []
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("Test") != SHARING_TEST:
            continue
        if event.get("Action") in ("pass", "fail", "skip"):
            actions.append(event["Action"])
        output = event.get("Output") or ""
        if SHARING_MARK in output:
            problems.append(output.split(SHARING_MARK, 1)[1].rstrip())
        elif SHARING_ROW_MARK in output:
            fields = output.split(SHARING_ROW_MARK, 1)[1].split()
            if len(fields) < 2 or fields[1] not in SHARING_VALUES or fields[0] in values:
                problems.append("the fixture sharing check reported an unreadable row line %r" % output.strip())
            else:
                values[fields[0]] = fields[1]
    if result.returncode == 0 and actions == ["pass"] and not problems:
        return [], values
    if not problems:
        tail = " | ".join((result.stderr or result.stdout).strip().splitlines()[-5:])
        problems.append("the fixture sharing check (%s) reported %s with exit status %d and no problem line: %s"
                        % (SHARING_TEST, ",".join(actions) or "no result", result.returncode, tail))
    return problems, values


def sharing_problems(scenarios, values):
    """Every row has exactly one fixture sharing value and no value names an
    unknown row: the planner refuses a row without one."""
    ids = [entry["id"] for entry in scenarios]
    problems = ["%s has no fixture sharing value" % sid for sid in ids if values.get(sid) not in SHARING_VALUES]
    problems += ["fixture sharing value for unknown row %s" % sid for sid in sorted(set(values) - set(ids))]
    return problems


def configuration(params):
    """A row's fixture configuration: its parameters without test_run."""
    return {name: value for name, value in params.items() if name != "test_run"}


def sharing_groups(scenarios, values):
    """[(configuration, shareable ids, isolated ids)] in catalog order.

    The shareable rows of one configuration can run on one fixture; every
    isolated row keeps a fixture of its own."""
    groups, order = {}, []
    for entry in scenarios:
        key = json.dumps(configuration(entry["params"]), sort_keys=True)
        if key not in groups:
            groups[key] = (configuration(entry["params"]), [], [])
            order.append(key)
        groups[key][1 if values[entry["id"]] == "shareable" else 2].append(entry["id"])
    return [groups[key] for key in order]


def grouped_jobs(groups):
    """Jobs of a coverage once each configuration's shareable rows share one
    fixture: one per configuration with a shareable row, one per isolated row."""
    return sum((1 if shareable else 0) + len(isolated) for _, shareable, isolated in groups)


def fixed_job_problems(document, workflow_text):
    """Selectable suites must exist in the reusable platform workflow."""
    problems = []
    seen = {entry.get("id") for entry in document.get("scenarios", [])}
    jobs = document.get("fixed_jobs", [])
    if not jobs:
        problems.append("catalog lists no fixed jobs")
    for job in jobs:
        name = job.get("job", "")
        if job.get("id") != name:
            problems.append("fixed check identifier must equal its job name: %r" % job.get("id"))
        if job.get("id") in seen:
            problems.append("duplicate fixed job %s" % job.get("id"))
        seen.add(job.get("id"))
        if not re.search(r"(?m)^  %s:\s*$" % re.escape(name), workflow_text):
            problems.append("fixed job %r is not defined in the workflow" % name)
    return problems


SUITE_PACKAGES = ("integration", "cephmsgr")


def suite_tests(root=ROOT):
    """{package: top-level tests} that Go lists for both native suites.

    The native runner applies one selector to the API suite (integration) and
    the client suite (cephmsgr), as the result gate does."""
    return go_test_list.list_tests(root, ["./" + package for package in SUITE_PACKAGES])


def integration_tests(root=ROOT):
    """Names of the top-level Go tests of both native suites."""
    return set().union(*suite_tests(root).values())


def selector_problems(scenarios, tests):
    """A test selector that matches no test would silently run nothing."""
    problems = []
    for entry in scenarios:
        pattern = entry["params"].get("test_run")
        if pattern and not any(re.search(pattern, name) for name in tests):
            problems.append("%s: selector %s matches no integration test" % (entry["id"], pattern))
    return problems


def select(scenarios, selectors):
    """Return (selected entries in catalog order, explanation lines)."""
    if not selectors:
        raise PlanError("no selector given")
    by_id = {entry["id"]: entry for entry in scenarios}
    groups = {entry["group"] for entry in scenarios}
    chosen, explanation = set(), []
    for selector in selectors:
        if selector == "all":
            matched = set(by_id)
            explanation.append("all: %d scenarios" % len(matched))
        elif selector.startswith("group:"):
            group = selector[len("group:") :]
            if group not in groups:
                raise PlanError("unknown group %r (known: %s)" % (group, ", ".join(sorted(groups))))
            matched = {sid for sid, entry in by_id.items() if entry["group"] == group}
            explanation.append("%s: %d scenarios" % (selector, len(matched)))
        elif selector in by_id:
            matched = {selector}
            explanation.append("%s: 1 scenario" % selector)
        else:
            raise PlanError("unknown scenario %r" % selector)
        chosen |= matched
    return [entry for entry in scenarios if entry["id"] in chosen], explanation


def matrix(entries):
    return {
        "include": [dict(entry["params"], scenario=entry["id"])
                    for entry in entries if "check" not in entry["params"]],
        "fixed": [entry["params"]["check"] for entry in entries if "check" in entry["params"]],
    }


def fixed_entries(document):
    return [{"id": job["id"], "group": "checks", "params": {"check": job["id"]}}
            for job in document.get("fixed_jobs", [])]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    listing = sub.add_parser("list")
    listing.add_argument("--catalog", default=str(CATALOG))
    plan = sub.add_parser("plan")
    plan.add_argument("--select", action="append", required=True)
    plan.add_argument("--catalog", default=str(CATALOG))
    sub.add_parser("sharing")
    env = sub.add_parser("env")
    env.add_argument("id")
    env.add_argument("--catalog", default=str(CATALOG))
    args = parser.parse_args(argv)
    try:
        scenarios = load_catalog(getattr(args, "catalog", CATALOG))
        if args.command == "validate":
            document = load_catalog_document()
            problems = (validate_catalog(scenarios)
                        + fixed_job_problems(document, CHECKS_WORKFLOW.read_text(encoding="utf-8"))
                        + selector_problems(scenarios, integration_tests())
                        + reproduction_problems([(str(path.relative_to(ROOT)), path.read_text(encoding="utf-8"))
                                                 for path in documentation_files()], scenarios)
                        + fixture_admission_problems())
            shared, values = fixture_sharing()
            problems += shared + sharing_problems(scenarios, values)
            for problem in problems:
                print(problem, file=sys.stderr)
            if problems:
                return 1
            print("catalog valid: %d scenarios, all in default coverage, selecting existing tests, admitted by the fixture,"
                  " each with a fixture sharing value" % len(scenarios))
        elif args.command == "sharing":
            problems, values = fixture_sharing()
            problems += sharing_problems(scenarios, values)
            if problems:
                for problem in problems:
                    print(problem, file=sys.stderr)
                return 1
            groups = sharing_groups(scenarios, values)
            for params, shareable, isolated in groups:
                print("configuration %s" % json.dumps(params, sort_keys=True))
                for sid in shareable:
                    print("  shareable %s" % sid)
                for sid in isolated:
                    print("  isolated  %s" % sid)
            shareable_rows = sum(len(group[1]) for group in groups)
            print("%d rows: %d shareable, %d isolated; %d configurations, %d with more than one shareable row"
                  % (len(scenarios), shareable_rows, len(scenarios) - shareable_rows, len(groups),
                     sum(1 for group in groups if len(group[1]) > 1)))
            print("default coverage: %d jobs, %d once shareable rows of one configuration share a fixture"
                  % (len(scenarios), grouped_jobs(groups)))
        elif args.command == "list":
            for entry in scenarios + fixed_entries(load_catalog_document(args.catalog)):
                print(entry["id"])
        elif args.command == "env":
            problems = validate_catalog(scenarios)
            if problems:
                raise PlanError("; ".join(problems))
            rows = [entry for entry in scenarios if entry["id"] == args.id]
            if not rows:
                raise PlanError("unknown scenario %r" % args.id)
            for name, value in row_environment(rows[0]["params"], workflow_bindings()):
                print("export %s=%s" % (name, shlex.quote(value)))
        else:
            problems = validate_catalog(scenarios)
            document = load_catalog_document(args.catalog)
            if "fixed_jobs" in document:
                problems += fixed_job_problems(document, CHECKS_WORKFLOW.read_text(encoding="utf-8"))
            if problems:
                raise PlanError("; ".join(problems))
            vocabulary = scenarios + fixed_entries(document)
            entries, explanation = select(vocabulary, args.select)
            for line in explanation:
                print(line, file=sys.stderr)
            print("selected %d of %d scenarios/checks" % (len(entries), len(vocabulary)), file=sys.stderr)
            print(json.dumps(matrix(entries), separators=(",", ":")))
    except PlanError as error:
        print("scenario_plan: %s" % error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
