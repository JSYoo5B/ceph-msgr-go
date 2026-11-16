#!/usr/bin/env python3
"""Select and gate the cluster-free tests of the integration package.

The integration package mixes TestCeph...Integration native fixture tests, which
stay opt-in, with helper/decoder tests that need no cluster (including
TestCephFS... consumer-helper tests). The name rule classifies every top-level
test from the source, discovered as `go test` discovers it (see FUNC below): a TestCeph...Integration name is a native fixture test
and every other top-level test is cluster-free, so there is no list to edit.
`check` refuses duplicate top-level names and an empty cluster-free set.
`selector` prints the anchored -run pattern for exactly the cluster-free tests.
`verify` requires every cluster-free test to report an unindented top-level PASS
line; a skip, a missing result, a failure or a test outside the cluster-free set
(a native fixture test or a name the source does not declare) fails the gate
even when the process exited zero.
"""
import argparse
from pathlib import Path
import re
import sys

NATIVE = re.compile(r"TestCeph\w*Integration")


# A top-level function that `go test` runs as a test: no receiver, a name that is
# `Test` alone or `Test` followed by a character that is not a lowercase letter,
# and one *testing.T parameter. `TestMain(m *testing.M)` is the package harness,
# not a test, and reports no result of its own.
FUNC = re.compile(r"(?m)^func (Test\w*)\s*\(([^)]*)\)")
TEST_NAME = re.compile(r"Test(?:[^a-z]\w*)?")
TESTING_T = re.compile(r"\s*(?:\w+\s+)?\*testing\.T\s*")


def is_go_test(name, params):
    return bool(TEST_NAME.fullmatch(name)) and bool(TESTING_T.fullmatch(params))


def declared(root):
    names = []
    for path in sorted((Path(root) / "integration").glob("*_test.go")):
        for name, params in FUNC.findall(path.read_text(encoding="utf-8")):
            if is_go_test(name, params):
                names.append(name)
    return names


def cluster_free(root):
    """Every declared top-level test that is not a native fixture test, sorted."""
    return sorted({name for name in declared(root) if not NATIVE.fullmatch(name)})


def check(root):
    problems = []
    names = declared(root)
    for name in sorted({name for name in names if names.count(name) > 1}):
        problems.append("duplicate top-level test: %s" % name)
    if not cluster_free(root):
        problems.append("no cluster-free top-level test")
    return problems


def selector(root):
    return "^(%s)$" % "|".join(cluster_free(root))


def verify(root, text):
    observed, problems = {}, check(root)
    for line in text.splitlines():
        match = re.fullmatch(r"--- (PASS|FAIL|SKIP): (Test\w*) \([^\r\n]*\)", line)
        if match:
            if match[2] in observed:
                problems.append("duplicate result: %s" % match[2])
            observed[match[2]] = match[1]
    expected = cluster_free(root)
    for name in expected:
        if observed.get(name) != "PASS":
            problems.append("%s: %s" % (name, observed.get(name, "MISSING")))
    for name in sorted(set(observed) - set(expected)):
        problems.append("test outside the cluster-free set ran: %s" % name)
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    sub.add_parser("selector")
    gate = sub.add_parser("verify")
    gate.add_argument("--log", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "selector":
            problems = check(args.root)
            if not problems:
                print(selector(args.root))
        elif args.command == "check":
            problems = check(args.root)
        else:
            problems = verify(args.root, Path(args.log).read_text(encoding="utf-8", errors="replace"))
    except (ValueError, OSError) as error:
        problems = [str(error)]
    if problems:
        print("cluster-free integration tests: " + "; ".join(problems), file=sys.stderr)
        return 1
    if args.command == "verify":
        print("cluster-free integration tests: %d passed" % len(cluster_free(args.root)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
