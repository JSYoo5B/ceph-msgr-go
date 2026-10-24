#!/usr/bin/env python3
"""Require an actual native RGW fixture PASS from retained `go test -json` output.

The RGW checkpoints in integration/fixture skip unless an explicit environment
variable is set, so a zero exit status is not acceptance. This gate reads the
test2json stream and requires exactly one PASS of the selected top-level test,
a package PASS, no failed or skipped test, the independent owned-resource
cleanup record and the selected test's own receipt. For the multi-stripe
checkpoint it also recomputes the deterministic payload hash and checks the
retained layout arithmetic, so a receipt cannot pass on its own claims alone.

It runs no test, starts no fixture and makes no network request.
"""
import argparse
import hashlib
import json
import re
import struct
import sys
from pathlib import Path

CHECKPOINT = "TestRGWNativeCheckpointIntegration"
MULTISTRIPE = "TestRGWNativeMultiStripeIntegration"
TESTS = (CHECKPOINT, MULTISTRIPE)
MULTISTRIPE_SIZE = 6 << 20
SOURCE = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE_DIGEST = "6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
CLEANUP = re.compile(r"RGW_NATIVE_CLEANUP independent inspection confirms containers=([1-9][0-9]*) volumes=([1-9][0-9]*) network=removed")


def large_payload(size):
    """Mirror rgwNativeLargePayload in integration/fixture."""
    out = bytearray()
    while len(out) < size:
        out += hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + struct.pack(">Q", len(out) // 32)).digest()
    return bytes(out[:size])


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


def check_multistripe(receipt):
    problems = []
    if receipt.get("bytes") != MULTISTRIPE_SIZE:
        problems.append("receipt object size is not %d" % MULTISTRIPE_SIZE)
    expected = hashlib.sha256(large_payload(MULTISTRIPE_SIZE)).hexdigest()
    if receipt.get("sha256") != expected:
        problems.append("receipt payload hash differs from the deterministic input")
    if receipt.get("source") != SOURCE or not str(receipt.get("image", "")).endswith("@sha256:" + IMAGE_DIGEST):
        problems.append("receipt release identity differs from the pinned Tentacle image")
    stripes = receipt.get("stripes")
    if not isinstance(stripes, list) or len(stripes) < 2 or len(stripes) > 64:
        return problems + ["layout has fewer than two byte-bearing objects"]
    covered, seen = 0, set()
    for position, stripe in enumerate(stripes):
        if not isinstance(stripe, dict) or stripe.get("index") != position or stripe.get("offset") != covered:
            problems.append("layout has a gap, overlap or out-of-order stripe at %d" % position)
            break
        size = stripe.get("size")
        if not isinstance(size, int) or size <= 0 or stripe.get("received_write_bytes") != size or not stripe.get("received_writes"):
            problems.append("stripe %d has no gateway-attributed WRITE reception equal to its size" % position)
        if stripe.get("oid") in seen:
            problems.append("layout repeats a raw object")
        seen.add(stripe.get("oid"))
        covered += size if isinstance(size, int) else 0
    if covered != MULTISTRIPE_SIZE:
        problems.append("layout covers %d bytes" % covered)
    for field in ("put_reception", "delete_head_remove", "delete_gc_enqueue"):
        if not isinstance(receipt.get(field), list) or not receipt[field]:
            problems.append("receipt has no %s" % field)
    put = [op.get("tid") for op in receipt.get("put_reception", []) if isinstance(op, dict) and isinstance(op.get("tid"), int)]
    for field, tokens in (("delete_head_remove", ("call rgw.obj_remove",)),
                          ("delete_gc_enqueue", ("rgw_gc.rgw_gc_queue_enqueue", "call rgw.gc_set_entry"))):
        for op in receipt.get(field, []):
            description = op.get("description", "") if isinstance(op, dict) else ""
            tid = op.get("tid") if isinstance(op, dict) else None
            if not put or not isinstance(tid, int) or tid <= max(put) or not any(token in description for token in tokens):
                problems.append("%s entry is not a DELETE-phase request with its expected method" % field)
    tails = len(stripes) - (0 if receipt.get("head_sentinel") else 1)
    if tails < 1 or receipt.get("gc_removed_tails") != tails:
        problems.append("receipt does not account for every removed GC tail")
    if not isinstance(receipt.get("gc_chains"), int) or receipt["gc_chains"] < 1:
        problems.append("receipt has no GC chain")
    return problems


def verify(text, test):
    if test not in TESTS:
        raise ValueError("unsupported test selector")
    rows = parse_stream(text)
    problems, passes, package_pass, output = [], 0, False, []
    for row in rows:
        action, name = row.get("Action"), row.get("Test")
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
        problems.append("no package PASS")
    text_output = "".join(output)
    cleanup = CLEANUP.search(text_output)
    if not cleanup:
        problems.append("missing independent owned-resource removal record")
    marker = "RGW_NATIVE_MULTISTRIPE " if test == MULTISTRIPE else "RGW_NATIVE_CHECKPOINT "
    receipts = [line.split(marker, 1)[1] for line in text_output.splitlines() if marker in line]
    receipt = None
    if len(receipts) != 1:
        problems.append("expected exactly one %s receipt, saw %d" % (marker.strip(), len(receipts)))
    elif test == MULTISTRIPE:
        try:
            receipt = json.loads(receipts[0])
        except ValueError:
            problems.append("multi-stripe receipt is not JSON")
        else:
            problems.extend(check_multistripe(receipt))
    else:
        receipt = {"line_sha256": hashlib.sha256(receipts[0].encode()).hexdigest()}
        if "object_index_user_cleanup=confirmed" not in receipts[0] or SOURCE not in receipts[0]:
            problems.append("checkpoint receipt lacks its confirmed cleanup or pinned source")
    summary = {"test": test, "passed": not problems, "problems": problems,
               "cleanup": {"containers": int(cleanup[1]), "volumes": int(cleanup[2])} if cleanup else None,
               "receipt_sha256": hashlib.sha256(receipts[0].encode()).hexdigest() if len(receipts) == 1 else None,
               "stripes": len(receipt.get("stripes", [])) if isinstance(receipt, dict) and test == MULTISTRIPE else None}
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
