#!/usr/bin/env python3
"""Validate the native interoperability scenario catalog and plan a selection.

The catalog (.github/scenarios/catalog.json) is the discoverable list of the
real-daemon scenarios. Each entry has a stable identifier, a group and the
parameters the workflow matrix row uses. This tool validates the catalog,
compares it with the static matrix of .github/workflows/ci.yml, and prints the
selected matrix with an explanation. It reads local files only and executes no
test.

  scenario_plan.py validate           catalog invariants, workflow parity, selector targets
  scenario_plan.py list               one identifier per line
  scenario_plan.py plan --select S    matrix JSON on stdout, explanation on stderr

A selector S is "all", "group:<group>" or a scenario identifier; several can be
given. An unknown selector is an error, never an empty or conservative match.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / ".github" / "scenarios" / "catalog.json"
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
REQUIRED_PARAMS = ("key", "service", "family")
SUPPORTED_RELEASE = re.compile(r"^20\.2\.\d+$")


class PlanError(Exception):
    pass


def load_catalog(path=CATALOG):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("version") != 1:
        raise PlanError("unsupported catalog version %r" % data.get("version"))
    return data["scenarios"]


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
        if sid in seen:
            problems.append("duplicate identifier %s" % sid)
        seen.add(sid)
        if not entry.get("group"):
            problems.append("%s has no group" % sid)
        params = entry.get("params")
        if not isinstance(params, dict) or not params:
            problems.append("%s has no parameters" % sid)
            continue
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
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append("%s has an invalid test selector: %s" % (sid, error))
    return problems


def _split_flow(body):
    parts, current, quote = [], "", None
    for char in body:
        if quote:
            current += char
            if char == quote:
                quote = None
        elif char in "'\"":
            quote = char
            current += char
        elif char == ",":
            parts.append(current)
            current = ""
        else:
            current += char
    parts.append(current)
    return parts


def workflow_matrix(path=WORKFLOW):
    """Parse the one-line flow rows of the ceph job's static matrix."""
    text = Path(path).read_text(encoding="utf-8")
    job = text.index("\n  ceph:\n")
    start = text.index("        include:\n", job) + len("        include:\n")
    end = text.index("    steps:", start)
    rows = []
    for line in text[start:end].splitlines():
        line = line.strip()
        if not line.startswith("- {"):
            continue
        row = {}
        for part in _split_flow(line[3 : line.rindex("}")]):
            name, value = part.split(":", 1)
            value = value.strip()
            if value[:1] in "'\"":
                value = value[1:-1]
            row[name.strip()] = value
        rows.append(row)
    return rows


def parity_problems(scenarios, rows):
    """Compare catalog parameters with the workflow rows, in both directions."""
    wanted = [json.dumps(entry["params"], sort_keys=True) for entry in scenarios]
    actual = [json.dumps(row, sort_keys=True) for row in rows]
    problems = []
    for row in actual:
        if row not in wanted:
            problems.append("workflow row not in catalog: %s" % row)
    for row in wanted:
        if row not in actual:
            problems.append("catalog scenario not in workflow: %s" % row)
    if len(wanted) != len(actual):
        problems.append("catalog has %d scenarios, workflow %d rows" % (len(wanted), len(actual)))
    return problems


def integration_tests(directory=ROOT / "integration"):
    """Names of the top-level Go tests the integration package defines."""
    names = set()
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
    return {"include": [dict(entry["params"], scenario=entry["id"]) for entry in entries]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    sub.add_parser("list")
    plan = sub.add_parser("plan")
    plan.add_argument("--select", action="append", required=True)
    args = parser.parse_args(argv)
    try:
        scenarios = load_catalog()
        if args.command == "validate":
            problems = (validate_catalog(scenarios) + parity_problems(scenarios, workflow_matrix())
                        + selector_problems(scenarios, integration_tests()))
            for problem in problems:
                print(problem, file=sys.stderr)
            if problems:
                return 1
            print("catalog valid: %d scenarios match the workflow matrix and select existing tests" % len(scenarios))
        elif args.command == "list":
            for entry in scenarios:
                print(entry["id"])
        else:
            problems = validate_catalog(scenarios)
            if problems:
                raise PlanError("; ".join(problems))
            entries, explanation = select(scenarios, args.select)
            for line in explanation:
                print(line, file=sys.stderr)
            print("selected %d of %d scenarios" % (len(entries), len(scenarios)), file=sys.stderr)
            print(json.dumps(matrix(entries), separators=(",", ":")))
    except PlanError as error:
        print("scenario_plan: %s" % error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
