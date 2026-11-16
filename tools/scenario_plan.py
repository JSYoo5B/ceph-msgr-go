#!/usr/bin/env python3
"""Validate the native interoperability scenario catalog and plan a selection.

The catalog (composed by scenario_catalog.py) is the discoverable list of the
real-daemon scenarios and selectable platform check suites. Each native entry
has a stable identifier, group and workflow matrix parameters. This tool
validates the catalog against the mandatory list and reusable check jobs and prints the
selected matrix with an explanation. It reads local files only and executes no
test.

  scenario_plan.py validate               invariants, mandatory list, fixed jobs, selector targets,
                                          fixture capability vocabulary
  scenario_plan.py list                   one identifier per line
  scenario_plan.py plan --select S        matrix JSON on stdout, explanation on stderr

Both `list` and `plan` accept --catalog PATH to plan against another revision's
catalog (a pull request's vocabulary) while running this trusted script. PATH
is a fragment directory, or the earlier single catalog file of a pull request
opened before the split.

A selector S is "all", "group:<group>" or a scenario identifier; several can be
given. An unknown selector is an error, never an empty or conservative match.

A row may declare the fixture capabilities its test needs in the parameter
"capabilities": a comma-separated, sorted list of names that the fixture
defines with defineCapability calls in integration/fixture. Every reading of
the catalog refuses a capability on a row whose test_run is not exactly one
anchored test (^TestName$). validate also refuses a capability that the
fixture source of the same tree does not define, and one whose requirements
(the defineCapability requirement terms) the row's parameters do not meet.
plan reads another revision's catalog and leaves those two checks to that
revision's own validate and to its fixture, which refuses the same
declarations before it starts a cluster.
"""
import argparse
import json
import re
import sys
from pathlib import Path

import scenario_catalog

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / scenario_catalog.CATALOG
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CHECKS_WORKFLOW = ROOT / ".github" / "workflows" / "platform-checks.yml"
NATIVE_WORKFLOW = ROOT / ".github" / "workflows" / "native-scenarios.yml"
MANDATORY = ROOT / ".github" / "scenarios" / "mandatory.txt"
REQUIRED_PARAMS = ("key", "service", "family")
SUPPORTED_RELEASE = re.compile(r"^20\.2\.\d+$")
FIXTURE = ROOT / "integration" / "fixture"
# The fixture's exactTestSelector and capabilityNamePattern (capability.go).
EXACT_TEST = re.compile(r"\^Test[A-Za-z0-9_]+\$")
CAPABILITY_NAME = re.compile(r"[a-z][a-z0-9]*(-[a-z0-9]+)*")
# defineCapability(name, summary, requires, extra): the name, summary and
# requirements are plain string literals, which
# TestFixtureCapabilityVocabularyIsTheSourceDeclarations enforces with this
# same expression.
CAPABILITY_DEFINITION = re.compile(r'\bdefineCapability\(\s*"([^"\\]*)",\s*"(?:[^"\\]|\\.)*",\s*"([^"\\]*)"')
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


def fixture_capabilities(directory=FIXTURE):
    """[(name, requirement terms)] the fixture defines, from its non-test source."""
    found = []
    for path in sorted(Path(directory).glob("*.go")):
        if not path.name.endswith("_test.go"):
            found.extend((name, requires.split()) for name, requires
                         in CAPABILITY_DEFINITION.findall(path.read_text(encoding="utf-8")))
    return found


def workflow_bindings(text=None):
    """{environment variable: (matrix parameter, fallback)} of the native job."""
    if text is None:
        text = NATIVE_WORKFLOW.read_text(encoding="utf-8")
    return {name: (param, fallback) for name, param, fallback in WORKFLOW_BINDING.findall(text)}


def requirement_problem(sid, name, term, declared, params, bindings):
    if term[0] == "+":
        return None if term[1:] in declared else "%s: capability %s requires capability %s" % (sid, name, term[1:])
    if term[0] == "!":
        return None if term[1:] not in declared else "%s: capability %s excludes capability %s" % (sid, name, term[1:])
    variable, _, want = term.partition("=")
    binding = bindings.get("CEPH_MSGR_TEST_" + variable)
    if binding is None:
        return "%s: capability %s requires CEPH_MSGR_TEST_%s, which the native workflow does not forward" % (sid, name, variable)
    param, fallback = binding
    got = params.get(param) or fallback
    if got != want:
        return "%s: capability %s requires %s=%r, the row has %r" % (sid, name, param, want, got)
    return None


def capability_problems(scenarios, vocabulary, bindings):
    """Every declared capability is defined, and the row meets its requirements."""
    problems = []
    names = [name for name, _ in vocabulary]
    if len(set(names)) != len(names):
        problems.append("the fixture defines a capability twice")
    requirements = dict(vocabulary)
    for entry in scenarios:
        params = entry.get("params", {})
        value = params.get("capabilities")
        if not isinstance(value, str):
            continue
        declared = set(value.split(","))
        for name in value.split(","):
            if name not in requirements:
                problems.append("%s declares capability %s, which the fixture does not define" % (entry["id"], name))
                continue
            for term in requirements[name]:
                problem = requirement_problem(entry["id"], name, term, declared, params, bindings)
                if problem:
                    problems.append(problem)
    return problems


def mandatory_problems(scenarios, mandatory):
    """The mandatory list names every scenario the default coverage must run.

    Removing or renaming a scenario therefore needs an explicit edit of both
    files; a scenario cannot be dropped from the catalog silently."""
    ids = [entry["id"] for entry in scenarios if isinstance(entry.get("id"), str)]
    problems = []
    for sid in mandatory:
        if sid not in ids:
            problems.append("mandatory scenario missing from catalog: %s" % sid)
    for sid in ids:
        if sid not in mandatory:
            problems.append("catalog scenario not in the mandatory list: %s" % sid)
    if len(set(mandatory)) != len(mandatory):
        problems.append("mandatory list has duplicates")
    return problems


def load_mandatory(path=MANDATORY):
    return [line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


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


def integration_tests(directories=(ROOT / "integration", ROOT / "cephmsgr")):
    """Names of the top-level Go tests of both native suites.

    The native runner applies one selector to the API suite (integration) and
    the client suite (cephmsgr), as the result gate does."""
    names = set()
    for directory in directories:
        for path in sorted(Path(directory).glob("*_test.go")):
            names.update(re.findall(r"(?m)^func (Test[A-Za-z0-9_]*)\(", path.read_text(encoding="utf-8")))
    return names


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
    args = parser.parse_args(argv)
    try:
        scenarios = load_catalog(getattr(args, "catalog", CATALOG))
        if args.command == "validate":
            document = load_catalog_document()
            problems = (validate_catalog(scenarios) + mandatory_problems(scenarios, load_mandatory())
                        + fixed_job_problems(document, CHECKS_WORKFLOW.read_text(encoding="utf-8"))
                        + selector_problems(scenarios, integration_tests())
                        + capability_problems(scenarios, fixture_capabilities(), workflow_bindings()))
            for problem in problems:
                print(problem, file=sys.stderr)
            if problems:
                return 1
            print("catalog valid: %d scenarios, all mandatory, selecting existing tests" % len(scenarios))
        elif args.command == "list":
            for entry in scenarios + fixed_entries(load_catalog_document(args.catalog)):
                print(entry["id"])
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
