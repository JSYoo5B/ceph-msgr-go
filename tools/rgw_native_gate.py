#!/usr/bin/env python3
"""Require an actual native RGW fixture PASS from retained `go test -json` output.

The RGW checkpoints in integration/fixture skip unless an explicit environment
variable is set, so a zero exit status is not acceptance. This gate reads the
test2json stream and requires exactly one PASS of the selected top-level test,
a package PASS, no failed or skipped test, the independent owned-resource
cleanup record and the selected test's own receipt. Every event must name the
fixture module's package: a result, receipt or package PASS attributed to any
other package, or to none, is a problem and is never counted.

Each checkpoint's receipt check lives in its own module
rgw_native_gate_<name>.py beside this file, which declares the checkpoint's
TEST name, the MARKER that starts its receipt line and check(line, gate). The
gate imports every such module from its own directory only, so the set of
checkpoints it verifies (TESTS) has no list to edit, and a copy of the gate
never imports another revision's modules from the import path.

It runs no test, starts no fixture and makes no network request. On an
authorized Linux Docker runner, never against an existing cluster:

    CEPH_MSGR_TEST_RGW_NATIVE=1 go -C integration/fixture test -json \
      -run '^TestRGWNativeMultiStripeIntegration$' -count=1 -timeout=20m . > run.jsonl
    python3 tools/rgw_native_gate.py --jsonl run.jsonl \
      --test TestRGWNativeMultiStripeIntegration --output gate.json
"""
import argparse
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

# The module path in integration/fixture/go.mod; every checkpoint lives in its root package.
PACKAGE = "github.com/JSYoo5B/ceph-msgr-go/integration/fixture"
SOURCE = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE_DIGEST = "6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
# Match rgwNativeCallMethods in integration/fixture, including its eight-call bound.
CALL_METHOD = re.compile(r"(?:\[|,)call ([a-z_]+\.[a-z_]+)(?:[ ,\]]|$)")
CLEANUP = re.compile(r"RGW_NATIVE_CLEANUP independent inspection confirms containers=([1-9][0-9]*) volumes=([1-9][0-9]*) network=removed")
# The checkpoint names the fixture routes (rgwNativeCheckpoint in
# integration/fixture/rgw_native_route.go) and the module of each one's check.
CHECKPOINT_NAME = re.compile(r"TestRGWNative\w*Integration")
CHECKPOINT_MODULE = re.compile(r"rgw_native_gate_[a-z][a-z0-9_]*")


def call_methods(description):
    """The class methods of the first eight CALL tokens of description."""
    return CALL_METHOD.findall(description)[:8]


def release_problems(receipt):
    """A JSON receipt names the pinned Tentacle source and image."""
    if receipt.get("source") != SOURCE or not str(receipt.get("image", "")).endswith("@sha256:" + IMAGE_DIGEST):
        return ["receipt release identity differs from the pinned Tentacle image"]
    return []


def json_receipt(line, marker):
    """(the JSON object of a receipt line or None, problems)."""
    try:
        receipt = json.loads(line)
    except ValueError:
        return None, ["%s receipt is not JSON" % marker.strip()]
    if not isinstance(receipt, dict):
        return None, ["%s receipt is not a JSON object" % marker.strip()]
    return receipt, []


def load_checkpoints(directory=Path(__file__).resolve().parent):
    """{test name: module} of every rgw_native_gate_<name>.py in directory.

    A module must declare a TEST that is a routed checkpoint name, a MARKER
    and check(line, gate); two modules may not declare the same test or
    marker."""
    checkpoints, markers = {}, set()
    for path in sorted(directory.glob("rgw_native_gate_*.py")):
        if not CHECKPOINT_MODULE.fullmatch(path.stem):
            raise ValueError("RGW checkpoint module name is not rgw_native_gate_<name>.py: %s" % path.name)
        module = sys.modules.get(path.stem)
        if module is None or Path(getattr(module, "__file__", "")).resolve() != path.resolve():
            # Never a same-named module of another directory on the import path.
            spec = importlib.util.spec_from_file_location(path.stem, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            sys.modules.setdefault(path.stem, module)
        test, marker = getattr(module, "TEST", None), getattr(module, "MARKER", None)
        if not isinstance(test, str) or not CHECKPOINT_NAME.fullmatch(test):
            raise ValueError("%s declares no TEST named TestRGWNative...Integration" % path.name)
        if not isinstance(marker, str) or not re.fullmatch(r"RGW_NATIVE_[A-Z0-9_]+ ", marker) or not callable(getattr(module, "check", None)):
            raise ValueError("%s declares no MARKER 'RGW_NATIVE_<NAME> ' and check(line, gate)" % path.name)
        if test in checkpoints or marker in markers:
            raise ValueError("%s declares a checkpoint or marker another module declares" % path.name)
        checkpoints[test] = module
        markers.add(marker)
    return checkpoints


CHECKPOINTS = load_checkpoints()
TESTS = tuple(sorted(CHECKPOINTS))


def parse_stream(text):
    rows = []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            raise ValueError("line %d is not a test2json event" % number)
        if not isinstance(row, dict):
            raise ValueError("line %d is not a test2json event" % number)
        rows.append(row)
    return rows


def verify(text, test):
    if test not in CHECKPOINTS:
        raise ValueError("unsupported test selector")
    checkpoint = CHECKPOINTS[test]
    rows = parse_stream(text)
    problems, passes, package_pass, output, foreign = [], 0, False, [], []
    for row in rows:
        action, name = row.get("Action"), row.get("Test")
        if row.get("Package") != PACKAGE:
            package = row.get("Package")
            label = package if isinstance(package, str) and package else "<none>"
            if label not in foreign:
                foreign.append(label)
                problems.append("event outside the fixture package: %s" % label)
            continue
        if name and name != test and not name.startswith(test + "/"):
            problems.append("unexpected test in the stream: %s" % name)
            continue
        if name == test:
            if action == "pass":
                passes += 1
            elif action in ("fail", "skip"):
                problems.append("selected test reported %s" % action)
        elif name:
            if action in ("fail", "skip"):
                problems.append("subtest %s reported %s" % (name, action))
        elif action == "pass":
            package_pass = True
        elif action in ("fail", "skip"):
            problems.append("package reported %s" % action)
        if action == "output" and name == test:
            output.append(row.get("Output", ""))
    if passes != 1:
        problems.append("the selected test must PASS exactly once, saw %d" % passes)
    if not package_pass:
        problems.append("no PASS of package %s" % PACKAGE)
    text_output = "".join(output)
    cleanup = CLEANUP.search(text_output)
    if not cleanup:
        problems.append("missing independent owned-resource removal record")
    marker = checkpoint.MARKER
    receipts = [line.split(marker, 1)[1] for line in text_output.splitlines() if marker in line]
    details = {}
    if len(receipts) != 1:
        problems.append("expected exactly one %s receipt, saw %d" % (marker.strip(), len(receipts)))
    else:
        receipt_problems, details = checkpoint.check(receipts[0], sys.modules[__name__])
        problems.extend(receipt_problems)
    summary = {"test": test, "package": PACKAGE, "passed": not problems, "problems": problems,
               "cleanup": {"containers": int(cleanup[1]), "volumes": int(cleanup[2])} if cleanup else None,
               "receipt_sha256": hashlib.sha256(receipts[0].encode()).hexdigest() if len(receipts) == 1 else None}
    summary.update(details)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", required=True)
    parser.add_argument("--test", required=True, choices=TESTS)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        summary = verify(Path(args.jsonl).read_text(encoding="utf-8", errors="replace"), args.test)
        Path(args.output).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    except (ValueError, OSError) as error:
        print("RGW native gate: %s" % error, file=sys.stderr)
        return 1
    if not summary["passed"]:
        print("RGW native gate: " + "; ".join(summary["problems"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
