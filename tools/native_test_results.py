#!/usr/bin/env python3
"""Require native test execution, independently of the test process exit code.

Explicit selectors require every matching top-level test to pass. An empty
secondary suite is allowed. Broad fixture defaults may intentionally skip tests;
they require at least one pass and retain all observed skips without claiming
that every test ran. Only complete, unindented Go verbose result lines count.
The selectable top-level tests are the ones Go lists for each suite package
(tools/go_test_list.py), not a scan of the source text. An explicit selector
that selects a name Go lists more than once in one suite package (the internal
and the external test package both declare it) is refused by `snapshot`, before
any run: `-run` runs both under that name and a result line cannot tell which
one it reports.

A fixture sequence (several rows' tests on one fixture, run one test at a
time) reports the start-state check after each test on a line
"Fixture start state after <suite>/<Test>: FAILED: ..." when it fails; that
test then fails the gate with the check's conditions, and the tests the
sequence did not reach are missing.
"""
import argparse
import json
from pathlib import Path
import re
import sys

import go_test_list

SUITES = (("api", "integration"), ("client", "cephmsgr"))


def snapshot(root, selector):
    if "/" in selector:
        raise ValueError("only top-level test selectors are supported")
    pattern = re.compile(selector) if selector else None
    expected = []
    if pattern:
        # A broad default selects no explicit test, so it needs no listing.
        selected = go_test_list.selected_tests(root, ["./" + directory for _, directory in SUITES],
                                               lambda package, name: bool(pattern.search(name)))
        for suite, directory in SUITES:
            expected.extend([suite, name] for name in sorted(selected[directory]))
    if selector and not expected:
        raise ValueError("explicit selector matches no top-level tests")
    return {"selector": selector, "expected": expected}


def verify(request, text):
    suite, observed, problems = None, {}, []
    for line in text.splitlines():
        if line in ("Running Ceph test suite: api", "Running Ceph test suite: client"):
            suite = line.rsplit(" ", 1)[1]
        match = re.fullmatch(r"--- (PASS|FAIL|SKIP): (Test\w*) \([^\r\n]*\)", line)
        if match and suite:
            key = (suite, match[2])
            if key in observed:
                problems.append("duplicate result: %s/%s" % key)
            observed[key] = match[1]
        check = re.fullmatch(r"Fixture start state after (api|client)/(Test\w*): FAILED: ([^\r\n]*)", line)
        if check:
            problems.append("%s/%s: the fixture start-state check after it failed: %s" % (check[1], check[2], check[3]))
    for key in map(tuple, request["expected"]):
        if observed.get(key) != "PASS":
            problems.append("%s/%s: %s" % (*key, observed.get(key, "MISSING")))
    if not any(state == "PASS" for state in observed.values()):
        problems.append("no top-level native test passed")
    if "FAIL" in observed.values():
        problems.append("a top-level native test failed")
    return {"selector": request["selector"], "expected": request["expected"],
            "observed": [{"suite": key[0], "test": key[1], "result": state}
                         for key, state in sorted(observed.items())],
            "problems": problems, "passed": not problems}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("snapshot")
    capture.add_argument("--selector", default="")
    capture.add_argument("--root", default=".")
    check = sub.add_parser("verify")
    check.add_argument("--expected", required=True)
    check.add_argument("--log", required=True)
    for command in (capture, check):
        command.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "snapshot":
            result = snapshot(args.root, args.selector)
        else:
            result = verify(json.loads(Path(args.expected).read_text(encoding="utf-8")),
                            Path(args.log).read_text(encoding="utf-8", errors="replace"))
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if args.command == "verify" and not result["passed"]:
            print("native test result gate: " + "; ".join(result["problems"]), file=sys.stderr)
            return 1
    except (ValueError, OSError) as error:
        print("native test result gate: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
