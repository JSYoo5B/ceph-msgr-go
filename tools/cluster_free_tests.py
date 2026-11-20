#!/usr/bin/env python3
"""Select and gate the cluster-free tests of the integration package.

The integration package mixes TestCeph...Integration native fixture tests, which
stay opt-in, with helper/decoder tests that need no cluster (including
TestCephFS... consumer-helper tests). The top-level tests are the ones Go lists
for ./integration (tools/go_test_list.py), so build constraints, import aliases,
methods and TestMain follow Go's own rules. A TestCeph...Integration name is a
native fixture test and every other listed test is cluster-free, so there is no
list to edit.
`check` refuses a package Go cannot list, an empty cluster-free set and a
cluster-free name that Go lists more than once (the internal and the external
test package both declare it): `-run` would run both under one name and a
result line could not tell which one passed, so the gate refuses the name
before it reads any result.
`selector` prints the anchored -run pattern for exactly the cluster-free tests.
`verify` requires every cluster-free test to report an unindented top-level PASS
line; a skip, a missing result, a failure or a test outside the cluster-free set
(a native fixture test or a name Go does not list) fails the gate even when the
process exited zero.
"""
import argparse
from pathlib import Path
import re
import sys

import go_test_list

# The Go listings this tool runs: (subcommands that run Go, or None for every
# invocation; the module directories Go loads). tools/test_workflow_toolchains.py
# reads this declaration to require a Go that can list those modules.
GO_LISTING = (None, (".",))

# ASCII \w, as the fixture's Go native-name rule (ci_catalog_test.go): a name
# Go does not treat as native must be cluster-free.
NATIVE = re.compile(r"TestCeph\w*Integration", re.ASCII)


def declared(root):
    """The top-level tests Go lists for the integration package, sorted, each once."""
    return sorted(go_test_list.package_tests(root, "integration"))


def cluster_free(root):
    """Every listed top-level test that is not a native fixture test, sorted.

    Raises go_test_list.ListError when Go lists one of them more than once."""
    return sorted(go_test_list.selected_tests(root, ("./integration",),
                                              lambda package, name: not NATIVE.fullmatch(name))["integration"])


def check(root):
    problems = []
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
