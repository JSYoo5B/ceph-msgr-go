#!/usr/bin/env python3
"""Select and gate the cluster-free tests of the integration package.

The integration package mixes TestCeph* native fixture tests, which stay opt-in,
with helper/decoder tests that need no cluster. integration/cluster_free_tests.txt
lists the latter. `check` requires every top-level test to be either TestCeph* or
listed, and every listed name to exist. `selector` prints the anchored -run
pattern for exactly the listed tests. `verify` requires every listed test to
report an unindented top-level PASS line; a skip, a missing result, a failure
or an unlisted test fails the gate even when the process exited zero.
"""
import argparse
from pathlib import Path
import re
import sys

INVENTORY = Path("integration") / "cluster_free_tests.txt"
NATIVE_PREFIX = "TestCeph"


def declared(root):
    names = []
    for path in sorted((Path(root) / "integration").glob("*_test.go")):
        names.extend(re.findall(r"(?m)^func (Test\w+)\s*\(", path.read_text(encoding="utf-8")))
    return names


def inventory(root):
    lines = (Path(root) / INVENTORY).read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]


def check(root):
    problems = []
    names, listed = declared(root), inventory(root)
    for name in sorted({name for name in names if names.count(name) > 1}):
        problems.append("duplicate top-level test: %s" % name)
    for name in sorted({name for name in listed if listed.count(name) > 1}):
        problems.append("duplicate inventory entry: %s" % name)
    if listed != sorted(listed):
        problems.append("inventory is not sorted")
    for name in listed:
        if not re.fullmatch(r"Test\w+", name):
            problems.append("invalid inventory entry: %r" % name)
        elif name.startswith(NATIVE_PREFIX):
            problems.append("native fixture test listed as cluster-free: %s" % name)
        elif name not in names:
            problems.append("inventory entry has no top-level test: %s" % name)
    for name in sorted(set(names) - set(listed)):
        if not name.startswith(NATIVE_PREFIX):
            problems.append("unclassified top-level test (list it or rename it TestCeph*): %s" % name)
    if not listed:
        problems.append("inventory is empty")
    return problems


def selector(root):
    return "^(%s)$" % "|".join(inventory(root))


def verify(root, text):
    observed, problems = {}, []
    for line in text.splitlines():
        match = re.fullmatch(r"--- (PASS|FAIL|SKIP): (Test\w+) \([^\r\n]*\)", line)
        if match:
            if match[2] in observed:
                problems.append("duplicate result: %s" % match[2])
            observed[match[2]] = match[1]
    listed = inventory(root)
    for name in listed:
        if observed.get(name) != "PASS":
            problems.append("%s: %s" % (name, observed.get(name, "MISSING")))
    for name in sorted(set(observed) - set(listed)):
        problems.append("unlisted test ran: %s" % name)
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
        print("cluster-free integration tests: %d passed" % len(inventory(args.root)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
