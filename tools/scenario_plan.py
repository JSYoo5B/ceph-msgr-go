#!/usr/bin/env python3
"""Validate the native interoperability scenario catalog and plan a selection.

The catalog (scenario_catalog.py) is the discoverable set of the real-daemon
scenarios and selectable platform check suites. Each native row is declared
beside the test it selects (//scenario:row or //scenario:suite,
scenario_declarations.py) with a stable identifier, its group (the identifier
before the first dot) and its workflow matrix parameters; the fixed checks are
the selectable jobs of the reusable platform workflow. This tool validates the
catalog against those jobs, the tests it selects and the fixture, and prints
the selected matrix with an explanation. Every row is in the default coverage
(`all`); there is no separate list of required rows. Rows are planned in
planning order (scenario_declarations.planning_key): the rows that predate the
declarations in their recorded order, then later rows by identifier. `validate` resolves test selectors against the tests Go
lists for the native suites (go_test_list.py compiles them and executes no
test body) and passes every row it read to the fixture's admission (one
cluster-free Go test of integration/fixture, which needs the fixture module's
Go toolchain); the declarations have this one reader, and the fixture takes
its rows from it (`rows`, CEPH_MSGR_PLANNED_ROWS); it starts no cluster. `list`, `plan`
and `env` read local files and Git objects only.

  scenario_plan.py validate               invariants, fixed jobs, selector targets, documented
                                          reproductions, fixture admission of every row
  scenario_plan.py list                   one identifier per line
  scenario_plan.py plan --select S        matrix JSON on stdout, explanation on stderr
  scenario_plan.py env ID [ID ...]        shell exports of the native workflow's environment for one row,
                                          or for the one job that several rows of one fixture share
  scenario_plan.py sharing                every row's fixture sharing value and the jobs of the default
                                          coverage
  scenario_plan.py rows                   every row as JSON ([{"id", "params"}]), the one reading of the
                                          declarations that the fixture's catalog tests use

`list`, `plan` and `env` accept --revision SHA to read another revision's
declarations (a pull request's vocabulary) from its Git objects, without a
checkout, while running this trusted script; the fixture sharing declarations
that group rows come from the same objects, and the fixed checks from this
tree's workflow.

A selector S is "all", "group:<group>" or a scenario identifier; several can be
given. An unknown selector is an error, never an empty or conservative match.

A row may declare the fixture capabilities its test needs in the parameter
"capabilities": a comma-separated, sorted list of names that the fixture
defines with defineCapability (or defineExclusiveCapability) calls in
integration/fixture. Every reading of the catalog refuses a capability on a
row whose test_run is not exactly one anchored test (^TestName$). Whether the fixture defines a declared capability
and admits it on the row's topology is the fixture's own decision: validate
passes every row, as the native workflow forwards it, through the fixture's
loadProfile (TestCatalogRowsAreAdmittedByTheFixture) and reports each row the
fixture refuses, so the planner keeps no copy of the fixture's rules. plan
reads another revision's catalog and leaves that check to that revision's own
validate and to its fixture, which refuses the same rows before it starts a
cluster.

`env ID` prints one `export NAME=value` line for every fixture input the
native workflow forwards for the row's job (native_env.py, with its
fallbacks), so a documented reproduction runs a row as CI does:

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
shareable only when every test its effective selector selects is. The
declarations have one reader, fixture_sharing.py; the fixture reports each
row's effective selector (its own default when the row has no test_run) from
the admission test, and fixture_sharing.row_values derives the row's value.
validate and sharing refuse a row without a value, a test without a
declaration, a misplaced declaration and a native test that Go does not list.
A fixture configuration is a row's parameters without test_run.

plan groups the selected rows: the shareable rows of one configuration with
an explicit test_run, in planning order, run as one job of at most
MAX_SEQUENCE_ROWS rows, a fixture sequence (integration/fixture/sequence.go).
Every other selected row, and a configuration with one such row selected,
keeps a job of its own, exactly as before; a request of one row is therefore
never grouped. The plan reads the declarations with the same reader, as text,
because it may plan another revision, and groups by a row's test_run;
validate requires the fixture's effective selector of every row with an
explicit test_run to be that test_run. A sequence
job carries the rows' selectors in test_sequence (CEPH_MSGR_TEST_SEQUENCE),
their union in test_run, the row identifiers in scenarios and its first row in
scenario. sharing lists, per configuration, the shareable and isolated rows
and the default coverage's job count.

validate refuses a documented `env ID` of an unknown row, a documented `env`
that is not this checked form, and a hand-written reproduction that selects
the exact test of a row with capabilities but does not declare them. It also
refuses a scenario declaration outside the native suites' test files and the
catalog files of the earlier layout. A further check is a module of its own in
tools/scenario_checks (see that package), which validate runs in name order;
adding one edits no function, list or test class of this file.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

import fixture_sharing as sharing_sources
import go_test_list
import native_env
import rgw_native_gate
import scenario_catalog
import scenario_checks
import scenario_declarations

ROOT = Path(__file__).resolve().parent.parent
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
# The mark of the line on which the same test reports an admitted row's
# effective selector: "<id> <routed 0|1> <selector>" (capability_test.go).
SELECTOR_ROW_MARK = "catalog selector row: "
SHARING_VALUES = sharing_sources.VALUES
# A documented assignment of one exact test selector, and a documented
# reproduction through `env` (reproduction_problems).
DOCUMENTED_RUN = re.compile(r"CEPH_MSGR_TEST_RUN=['\"]?(\^Test[A-Za-z0-9_]+\$)")
DOCUMENTED_CAPABILITIES = re.compile(r"CEPH_MSGR_TEST_CAPABILITIES=['\"]?([a-z0-9,-]+)")
DOCUMENTED_ENV = re.compile(r"scenario_plan\.py env ((?:[^\s\"'`)]+)(?: [^\s\"'`)-][^\s\"'`)]*)*)")
# The checked form of a documented `env` (env_recipe_problems): the lookup is
# assigned on its own line inside a subshell, and its status reaches the runner
# through an && chain.
CHECKED_ENV = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=\$\(python3 tools/scenario_plan\.py env [^\s)]+(?: [^\s)]+)*\) &&$")
SUBSTITUTED_ENV = re.compile(r"\$\(\s*(?:python3\s+)?\S*scenario_plan\.py env |`python3\s+\S*scenario_plan\.py env ")
DOCUMENTED_RUNNER = re.compile(r"integration/run\.sh|integration/fixture run ")
# The most rows one fixture sequence runs; a larger group is split in order.
MAX_SEQUENCE_ROWS = 8
# Matrix parameters only the planner sets, for a fixture sequence.
SEQUENCE_PARAMS = ("test_sequence", "scenarios", "sequence_rows")


class PlanError(Exception):
    pass


def load_catalog_document(revision=None):
    """The catalog of this tree, or of a revision's Git objects."""
    try:
        if revision:
            return scenario_catalog.load_revision(revision, Path.cwd(), ROOT)
        return scenario_catalog.load(ROOT)
    except scenario_catalog.CatalogError as error:
        raise PlanError(str(error)) from None


def load_catalog(revision=None):
    return load_catalog_document(revision)["scenarios"]


def validate_catalog(scenarios):
    """Return a list of problems; empty when the catalog is consistent."""
    problems, seen = [], set()
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
        # The trusted revision's forwarding (native_env.py) decides which
        # names a row may set, even for a newer pull-request catalog; which
        # inputs exist is the fixture's admission (fixture_admission).
        for name in sorted(params):
            problem = native_env.parameter_problem(name)
            if problem:
                problems.append("%s: trusted native runner does not support parameter %s: %s" % (sid, name, problem))
        for name in SEQUENCE_PARAMS:
            if name in params:
                problems.append("%s: parameter %s is set by the planner, not by a row" % (sid, name))
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
                if re.search(r"\s", pattern):
                    problems.append("%s: a test selector has no white space" % sid)
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append("%s has an invalid test selector: %s" % (sid, error))
        if "capabilities" in params:
            problems.extend(declaration_problems(sid, params))
        problems.extend(routing_problems(sid, params))
    return problems


# The row parameter that routes a row to one RGW checkpoint of the fixture
# module (integration/fixture/rgw_native_route.go) instead of the
# controller's fixture, and the closed set of tests it may route to: the
# tests the trusted RGW result gate verifies.
ROUTING_PARAM = "rgw_native"
ROUTED_SUITE = "integration/fixture"


def routed(params):
    """Whether a row runs an RGW checkpoint of the fixture module."""
    return params.get(ROUTING_PARAM) == "1"


def routing_problems(sid, params):
    """A routed row names exactly one known RGW checkpoint and nothing else.

    The trusted planner checks this on every catalog it reads, a pull
    request's included, so a row can never route the native workflow to
    another test of the fixture module or to a union of checkpoints."""
    if ROUTING_PARAM not in params:
        return []
    value = params[ROUTING_PARAM]
    if value not in ("0", "1"):
        return ["%s: %s must be 0 or 1, not %r" % (sid, ROUTING_PARAM, value)]
    if value == "0":
        return []
    allowed = ["^%s$" % name for name in rgw_native_gate.TESTS]
    problems = []
    if params.get("test_run") not in allowed:
        problems.append("%s: %s=1 requires a test_run of exactly one of %s, not %r"
                        % (sid, ROUTING_PARAM, ", ".join(allowed), params.get("test_run")))
    if params.get("capabilities"):
        problems.append("%s: %s=1 admits no capabilities" % (sid, ROUTING_PARAM))
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
            for listed in DOCUMENTED_ENV.findall(chunk):
                for sid in listed.split():
                    if sid not in ids and sid != "ID":
                        problems.append("%s: reproduction names unknown scenario %s" % (name, sid))
            given = set(DOCUMENTED_CAPABILITIES.findall(chunk))
            for selector in DOCUMENTED_RUN.findall(chunk):
                wanted = declared.get(selector)
                if wanted and not given & wanted:
                    problems.append("%s: reproduction of %s lacks CEPH_MSGR_TEST_CAPABILITIES=%s"
                                    % (name, selector, " or ".join(sorted(wanted))))
    return problems


def planned_rows(scenarios):
    """The rows as the fixture reads them: [{"id", "params"}] in plan order."""
    return [{"id": row["id"], "params": row["params"]} for row in scenarios]


def fixture_admission(run=subprocess.run, directory=FIXTURE, planned=(), rows=()):
    """(problems, {row: (routed, effective selector)}): the rows of this
    tree's catalog that its fixture refuses, and the effective selector of
    every row it admits, on lines marked SELECTOR_ROW_MARK.

    Runs the fixture's TestCatalogRowsAreAdmittedByTheFixture on rows (the
    rows this planner read, in CEPH_MSGR_PLANNED_ROWS), which passes every
    row through loadProfile
    with the native workflow's bindings and reports each refusal on a line
    marked ADMISSION_MARK. A check that does not run, does not pass and
    reports no row is a problem as well, never a silent pass. planned are
    matrix entries of fixture sequences the planner makes; the same test
    passes each of them through loadProfile."""
    command = ["go", "test", "-mod=readonly", "-count=1", "-json", "-run", "^%s$" % ADMISSION_TEST, "."]
    with tempfile.TemporaryDirectory() as scratch:
        jobs = Path(scratch) / "planned-jobs.json"
        jobs.write_text(json.dumps(list(planned)), encoding="utf-8")
        declared = Path(scratch) / "planned-rows.json"
        declared.write_text(json.dumps(planned_rows(rows)), encoding="utf-8")
        try:
            result = run(command, cwd=str(directory), env=dict(os.environ, GOWORK="off", CEPH_MSGR_PLANNED_JOBS=str(jobs),
                                                               CEPH_MSGR_PLANNED_ROWS=str(declared)),
                         capture_output=True, text=True, timeout=1200)
        except (OSError, subprocess.SubprocessError) as error:
            return ["the fixture admission check (%s) did not run: %s" % (ADMISSION_TEST, error)], {}
    problems, selectors, actions = [], {}, []
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
        elif SELECTOR_ROW_MARK in output:
            fields = output.split(SELECTOR_ROW_MARK, 1)[1].split()
            if len(fields) != 3 or fields[1] not in ("0", "1") or fields[0] in selectors:
                problems.append("the fixture admission check reported an unreadable selector line %r" % output.strip())
            else:
                selectors[fields[0]] = (fields[1] == "1", fields[2])
    if result.returncode == 0 and actions == ["pass"] and not problems:
        return [], selectors
    if not problems:
        tail = " | ".join((result.stderr or result.stdout).strip().splitlines()[-5:])
        problems.append("the fixture admission check (%s) reported %s with exit status %d and no row: %s"
                        % (ADMISSION_TEST, ",".join(actions) or "no result", result.returncode, tail))
    return problems, selectors


def fixture_values(scenarios, selectors, tests, listing, routed_listing):
    """(problems, {row: value}): every row's fixture sharing value, derived
    from the fixture's effective selectors and the //fixture: declarations
    (fixture_sharing.py, their one reader), and the inventory check.

    tests is fixture_sharing.test_declarations of this tree. A native test
    that the reader finds but Go does not list (a file the build excludes),
    or that Go lists but the reader does not find, is a problem: the reader
    must see exactly the tests that exist."""
    problems = []
    read = {key.split("/", 1)[1] for key in tests if not key.startswith("fixture/")}
    listed = {name for names in listing.values() for name in names
              if sharing_sources.SUITES[0][2].fullmatch(name)}
    routed_read = {key.split("/", 1)[1] for key in tests if key.startswith("fixture/")}
    routed = {name for names in (routed_listing or {}).values() for name in names
              if sharing_sources.SUITES[2][2].fullmatch(name)}
    for name in sorted(read - listed):
        problems.append("native test %s is declared in a file Go does not list (build-excluded?)" % name)
    for name in sorted(listed - read):
        problems.append("native test %s is listed by Go but not found by the fixture sharing reader" % name)
    for name in sorted(routed_read ^ routed):
        problems.append("RGW checkpoint %s: Go and the fixture sharing reader disagree on whether it exists" % name)
    values, derived = sharing_sources.row_values(scenarios, selectors, tests)
    for entry in scenarios:
        selector = entry["params"].get("test_run")
        if selector and entry["id"] in selectors and selectors[entry["id"]] != (entry["params"].get("rgw_native") == "1", selector):
            problems.append("%s: the fixture's effective selector %r is not the row's test_run %r, by which plan groups it"
                            % (entry["id"], selectors[entry["id"]][1], selector))
    return problems + derived, values


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


def jobs(entries, declared):
    """[[entry]] in order: the shareable rows of one configuration with an
    explicit test_run (fixture_sharing.shareable_row) form one job of at most
    MAX_SEQUENCE_ROWS rows, placed where its first row is; every other row is
    a job of its own. declared is fixture_sharing.test_declarations of the
    planned sources; None means unknown sources, and then nothing is grouped."""
    result, open_jobs = [], {}
    for entry in entries:
        params = entry["params"]
        if declared is None or "check" in params or not sharing_sources.shareable_row(params, declared):
            result.append([entry])
            continue
        key = json.dumps(configuration(params), sort_keys=True)
        job = open_jobs.get(key)
        if job is None or len(job) == MAX_SEQUENCE_ROWS:
            job = open_jobs[key] = []
            result.append(job)
        job.append(entry)
    return result


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
    """{package: {top-level test: count}} that Go lists for both native suites.

    The native runner applies one selector to the API suite (integration) and
    the client suite (cephmsgr), as the result gate does."""
    return go_test_list.list_tests(root, ["./" + package for package in SUITE_PACKAGES])


def integration_tests(root=ROOT):
    """Names of the top-level Go tests of both native suites, each once."""
    return set().union(*suite_tests(root).values())


def routed_tests(root=ROOT):
    """{".": {top-level test: count}} that Go lists for the fixture module's
    root package, where the routed RGW checkpoints are declared."""
    return go_test_list.list_tests(Path(root) / ROUTED_SUITE, ["."])


def selector_problems(scenarios, listing, routed_listing=None):
    """A row's test selector must select at least one listed test, each of
    them listed once.

    `listing` is suite_tests(). A selector that matches no test would silently
    run nothing. A selected name that Go lists more than once in one suite
    package (the internal and the external test package both declare it)
    cannot be selected alone, and the result gate refuses it before the run
    (go_test_list.selected_tests), so the row is refused here already.

    A routed row (rgw_native=1) runs its one test in the fixture module
    instead; `routed_listing` is routed_tests(), and the row's test must be
    listed there exactly once. A routed row is never checked against the
    product suites, nor any other row against the fixture module."""
    problems = []
    for entry in scenarios:
        pattern = entry["params"].get("test_run")
        if routed(entry["params"]):
            names = (routed_listing or {}).get(".", {})
            name = pattern[1:-1] if pattern else ""
            if names.get(name) != 1:
                problems.append("%s: routed selector %s is listed %d times in %s, not once"
                                % (entry["id"], pattern, names.get(name, 0), ROUTED_SUITE))
            continue
        if not pattern:
            continue
        def chosen(package, name):
            return bool(re.search(pattern, name))
        if not any(chosen(package, name) for package, names in listing.items() for name in names):
            problems.append("%s: selector %s matches no integration test" % (entry["id"], pattern))
        for repeat in go_test_list.repeated(listing, chosen):
            problems.append("%s: selector %s selects %s" % (entry["id"], pattern, go_test_list.repeated_message(*repeat)))
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


def job_parameters(job):
    """The matrix entry of one job: a row's parameters, or a fixture sequence."""
    if len(job) == 1:
        return dict(job[0]["params"], scenario=job[0]["id"])
    runs = [entry["params"]["test_run"] for entry in job]
    return dict(configuration(job[0]["params"]), scenario=job[0]["id"], scenarios=" ".join(entry["id"] for entry in job),
                sequence_rows=str(len(job)), test_run="|".join(runs), test_sequence=" ".join(runs))


def matrix(entries, declared=None):
    native = [entry for entry in entries if "check" not in entry["params"]]
    return {
        "include": [job_parameters(job) for job in jobs(native, declared)],
        "fixed": [entry["params"]["check"] for entry in entries if "check" in entry["params"]],
    }


def plan_sources(revision=None):
    """The native tests' fixture sharing declarations plan and env group by:
    this tree's, or a revision's from its Git objects."""
    if revision:
        try:
            return sharing_sources.test_declarations(sharing_sources.revision_sources(revision, Path.cwd()))[0]
        except scenario_catalog.CatalogError as error:
            raise PlanError("test sources of %s: %s" % (revision, error)) from None
    return sharing_sources.test_declarations(sharing_sources.tree_sources(ROOT))[0]


def fixed_entries(document):
    return [{"id": job["id"], "group": "checks", "params": {"check": job["id"]}}
            for job in document.get("fixed_jobs", [])]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    listing = sub.add_parser("list")
    listing.add_argument("--revision")
    plan = sub.add_parser("plan")
    plan.add_argument("--select", action="append", required=True)
    plan.add_argument("--revision")
    sub.add_parser("sharing")
    sub.add_parser("rows")
    env = sub.add_parser("env")
    env.add_argument("id", nargs="+")
    env.add_argument("--revision")
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            problems = scenario_declarations.stray_declarations(ROOT)
            try:
                document = scenario_catalog.load(ROOT)
            except scenario_catalog.CatalogError as error:
                for problem in problems + str(error).splitlines():
                    print(problem, file=sys.stderr)
                return 1
            scenarios = document["scenarios"]
            listing, routed_listing = suite_tests(), routed_tests()
            tests, declared = sharing_sources.test_declarations(sharing_sources.tree_sources(ROOT))
            admitted, selectors = fixture_admission(planned=[job for job in matrix(
                scenarios, tests)["include"] if "test_sequence" in job], rows=scenarios)
            shared, values = fixture_values(scenarios, selectors, tests, listing, routed_listing)
            problems += (validate_catalog(scenarios)
                         + fixed_job_problems(document, CHECKS_WORKFLOW.read_text(encoding="utf-8"))
                         + selector_problems(scenarios, listing, routed_listing)
                         + reproduction_problems([(str(path.relative_to(ROOT)), path.read_text(encoding="utf-8"))
                                                  for path in documentation_files()], scenarios)
                         + admitted + declared + shared + sharing_problems(scenarios, values))
            # A new check is a module of tools/scenario_checks, not a call here.
            problems += scenario_checks.run(scenario_checks.ScenarioContext(
                root=ROOT, document=document, scenarios=scenarios, listing=listing, routed_listing=routed_listing,
                sharing=tests, selectors=selectors, values=values))
            for problem in problems:
                print(problem, file=sys.stderr)
            if problems:
                return 1
            print("catalog valid: %d scenarios, all in default coverage, selecting existing tests, admitted by the fixture,"
                  " each with a fixture sharing value" % len(scenarios))
        elif args.command == "rows":
            print(json.dumps(planned_rows(load_catalog())))
        elif args.command == "sharing":
            scenarios = load_catalog()
            tests, problems = sharing_sources.test_declarations(sharing_sources.tree_sources(ROOT))
            admitted, selectors = fixture_admission(rows=scenarios)
            shared, values = fixture_values(scenarios, selectors, tests, suite_tests(), routed_tests())
            problems += admitted + shared + sharing_problems(scenarios, values)
            if problems:
                for problem in problems:
                    print(problem, file=sys.stderr)
                return 1
            groups = sharing_groups(scenarios, values)
            planned = jobs(scenarios, tests)
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
            sequences = [job for job in planned if len(job) > 1]
            print("default coverage: %d rows in %d jobs, %d of them fixture sequences of %s rows"
                  % (len(scenarios), len(planned), len(sequences), "+".join(str(len(job)) for job in sequences) or "no"))
        elif args.command == "list":
            document = load_catalog_document(args.revision)
            for entry in document["scenarios"] + fixed_entries(document):
                print(entry["id"])
        elif args.command == "env":
            scenarios = load_catalog(args.revision)
            problems = validate_catalog(scenarios)
            if problems:
                raise PlanError("; ".join(problems))
            by_id = {entry["id"]: entry for entry in scenarios}
            for sid in args.id:
                if sid not in by_id:
                    raise PlanError("unknown scenario %r" % sid)
            if len(set(args.id)) != len(args.id):
                raise PlanError("a scenario is named twice")
            planned = jobs([entry for entry in scenarios if entry["id"] in args.id], plan_sources(args.revision))
            if len(planned) != 1:
                raise PlanError("the rows %s do not form one fixture sequence (%d jobs)" % (" ".join(args.id), len(planned)))
            try:
                forwarded = native_env.environment(job_parameters(planned[0]))
            except native_env.ForwardingError as error:
                raise PlanError(str(error)) from None
            for name, value in forwarded.items():
                print("export %s=%s" % (name, shlex.quote(value)))
        else:
            document = load_catalog_document(args.revision)
            scenarios = document["scenarios"]
            problems = validate_catalog(scenarios) + fixed_job_problems(document, CHECKS_WORKFLOW.read_text(encoding="utf-8"))
            if problems:
                raise PlanError("; ".join(problems))
            vocabulary = scenarios + fixed_entries(document)
            entries, explanation = select(vocabulary, args.select)
            for line in explanation:
                print(line, file=sys.stderr)
            print("selected %d of %d scenarios/checks" % (len(entries), len(vocabulary)), file=sys.stderr)
            planned = matrix(entries, plan_sources(args.revision))
            sequences = [job for job in planned["include"] if "test_sequence" in job]
            for job in sequences:
                print("fixture sequence of %s rows: %s" % (job["sequence_rows"], job["scenarios"]), file=sys.stderr)
            print("%d native jobs" % len(planned["include"]), file=sys.stderr)
            print(json.dumps(planned, separators=(",", ":")))
    except PlanError as error:
        print("scenario_plan: %s" % error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
