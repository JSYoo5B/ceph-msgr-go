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
sequence did not reach are missing. A snapshot taken with --sequence
(the job forwards CEPH_MSGR_TEST_SEQUENCE) also requires a passed check line
after every expected test, so a fixture that ran the union selector without
the checks between tests cannot pass.

A row with rgw_native=1 runs one RGW checkpoint of the nested fixture module
instead of the product suites (integration/fixture/rgw_native_route.go). Its
snapshot names the backend (--backend rgw-fixture), the tested revision and
the test2json stream file the controller creates. The snapshot refuses any
selector other than exactly one known checkpoint (tools/rgw_native_gate.py),
a checkpoint Go does not list exactly once in the fixture package, a
checkout whose HEAD is not the tested revision or whose fixture module has a
modified or untracked file, and a stream file that already exists. Its
verdict requires the checkpoint's PASS line under "Running Ceph test suite:
fixture", no result of any other test or suite, and the RGW gate's verdict on
the stream: exactly one PASS of that test and a PASS of the fixture package,
no skip or failure, its receipt and its independent cleanup record, all
attributed to the fixture package. A fixture result in a request of the
product suites is a problem as well.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import go_test_list
import rgw_native_gate

SUITES = (("api", "integration"), ("client", "cephmsgr"))
# The backend of a routed row: its suite name, the fixture package directory
# and the closed set of tests it may run.
RGW_BACKEND = "rgw-fixture"
RGW_SUITE = ("fixture", "integration/fixture")
REVISION = re.compile(r"[0-9a-f]{40}")


def _git(root, *arguments):
    try:
        result = subprocess.run(["git", "-C", str(root)] + list(arguments), capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("git %s failed: %s" % (" ".join(arguments), error)) from None
    if result.returncode:
        raise ValueError("git %s failed: %s" % (" ".join(arguments), result.stderr.strip()[-500:]))
    return result.stdout


def rgw_snapshot(root, selector, revision, stream):
    """The request of a routed RGW checkpoint (see the module description)."""
    names = [name for name in rgw_native_gate.TESTS if selector == "^%s$" % name]
    if len(names) != 1:
        raise ValueError("the %s backend runs exactly one of %s, not %r"
                         % (RGW_BACKEND, ", ".join("^%s$" % name for name in rgw_native_gate.TESTS), selector))
    name = names[0]
    if not revision or not REVISION.fullmatch(revision):
        raise ValueError("the %s backend needs the tested revision as 40 lowercase hex digits" % RGW_BACKEND)
    head = _git(root, "rev-parse", "HEAD").strip()
    if head != revision:
        raise ValueError("the checkout is %s, not the tested revision %s" % (head, revision))
    changed = _git(root, "status", "--porcelain", "--untracked-files=all", "--", RGW_SUITE[1])
    if changed.strip():
        raise ValueError("the fixture module differs from the tested revision: %s"
                         % " | ".join(changed.strip().splitlines()[:5]))
    if not stream or not os.path.isabs(stream):
        raise ValueError("the %s backend needs the absolute path of its test2json stream" % RGW_BACKEND)
    if os.path.lexists(stream):
        raise ValueError("the test2json stream %s already exists; it would not be this run's" % stream)
    selected = go_test_list.selected_tests(Path(root) / RGW_SUITE[1], ["."], lambda package, test: test == name)
    if sorted(selected.get(".", ())) != [name]:
        raise ValueError("Go does not list %s in %s" % (name, RGW_SUITE[1]))
    return {"selector": selector, "expected": [[RGW_SUITE[0], name]], "backend": RGW_BACKEND,
            "revision": revision, "stream": stream}


def snapshot(root, selector, sequence=False, backend="", revision=None, stream=None):
    if backend == RGW_BACKEND:
        if sequence:
            raise ValueError("an RGW checkpoint runs alone, never in a fixture sequence")
        return rgw_snapshot(root, selector, revision, stream)
    if backend:
        raise ValueError("unsupported backend %r" % backend)
    if revision is not None or stream is not None:
        raise ValueError("a revision and a stream belong to the %s backend" % RGW_BACKEND)
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
    if sequence and not selector:
        raise ValueError("a fixture sequence needs an explicit selector")
    request = {"selector": selector, "expected": expected}
    if sequence:
        request["sequence"] = True
    return request


def verify(request, text, stream=None, head=None):
    """The verdict of one run's log text. For a routed RGW request, stream is
    the test2json text (read from the request's file when None) and head the
    checkout's revision after the run (unchecked when None)."""
    suite, observed, problems, passed_checks = None, {}, [], set()
    backend = request.get("backend")
    for line in text.splitlines():
        if line in ("Running Ceph test suite: api", "Running Ceph test suite: client",
                    "Running Ceph test suite: " + RGW_SUITE[0]):
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
        checked = re.fullmatch(r"Fixture start state after (api|client)/(Test\w*): passed \([^\r\n]*\)", line)
        if checked:
            passed_checks.add((checked[1], checked[2]))
    for key in map(tuple, request["expected"]):
        if observed.get(key) != "PASS":
            problems.append("%s/%s: %s" % (*key, observed.get(key, "MISSING")))
    if request.get("sequence"):
        for key in map(tuple, request["expected"]):
            if observed.get(key) == "PASS" and key not in passed_checks:
                problems.append("%s/%s: no passed fixture start-state check after it" % key)
    if not any(state == "PASS" for state in observed.values()):
        problems.append("no top-level native test passed")
    if "FAIL" in observed.values():
        problems.append("a top-level native test failed")
    expected = set(map(tuple, request["expected"]))
    rgw = None
    if backend == RGW_BACKEND:
        for key in sorted(set(observed) - expected):
            problems.append("%s/%s: not the routed checkpoint" % key)
        rgw, stream_problems = rgw_verdict(request, stream, head)
        problems.extend(stream_problems)
    elif backend:
        problems.append("unsupported backend %r" % backend)
    else:
        for key in sorted(key for key in observed if key[0] == RGW_SUITE[0]):
            problems.append("%s/%s: a fixture-module result in a product-suite request" % key)
    result = {"selector": request["selector"], "expected": request["expected"],
              "observed": [{"suite": key[0], "test": key[1], "result": state}
                           for key, state in sorted(observed.items())],
              "problems": problems, "passed": not problems}
    if backend:
        result.update(backend=backend, revision=request.get("revision"), rgw_gate=rgw)
    return result


def rgw_verdict(request, stream, head):
    """(the RGW gate's summary or None, problems) of a routed request."""
    problems = []
    if head is not None and head != request.get("revision"):
        problems.append("the checkout is %s after the run, not the tested revision %s" % (head, request.get("revision")))
    if stream is None:
        try:
            stream = Path(request["stream"]).read_text(encoding="utf-8", errors="replace")
        except (KeyError, OSError) as error:
            return None, problems + ["no test2json stream of the routed checkpoint: %s" % error]
    try:
        summary = rgw_native_gate.verify(stream, request["expected"][0][1])
    except (ValueError, IndexError) as error:
        return None, problems + ["RGW gate: %s" % error]
    return summary, problems + ["RGW gate: %s" % problem for problem in summary["problems"]]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("snapshot")
    capture.add_argument("--selector", default="")
    capture.add_argument("--root", default=".")
    capture.add_argument("--sequence", action="store_true",
                         help="the job is a fixture sequence: require a passed start-state check after every test")
    capture.add_argument("--backend", choices=(RGW_BACKEND,),
                         help="the row runs one RGW checkpoint of the fixture module (rgw_native=1)")
    capture.add_argument("--revision", help="the tested revision (%s backend)" % RGW_BACKEND)
    capture.add_argument("--stream", help="the test2json stream file the run creates (%s backend)" % RGW_BACKEND)
    check = sub.add_parser("verify")
    check.add_argument("--expected", required=True)
    check.add_argument("--log", required=True)
    check.add_argument("--root", default=".", help="the checkout whose HEAD a routed request rechecks")
    for command in (capture, check):
        command.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "snapshot":
            result = snapshot(args.root, args.selector, args.sequence, args.backend or "", args.revision, args.stream)
        else:
            request = json.loads(Path(args.expected).read_text(encoding="utf-8"))
            head = _git(args.root, "rev-parse", "HEAD").strip() if request.get("backend") else None
            result = verify(request, Path(args.log).read_text(encoding="utf-8", errors="replace"), head=head)
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
