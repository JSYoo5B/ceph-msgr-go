#!/usr/bin/env python3
"""Require an actual native RGW fixture PASS from retained `go test -json` output.

The RGW checkpoints in integration/fixture skip unless an explicit environment
variable is set, so a zero exit status is not acceptance. This gate reads the
test2json stream and requires exactly one PASS of the selected top-level test,
a package PASS, no failed or skipped test, the independent owned-resource
cleanup record and the selected test's own receipt. Every event must name the
fixture module's package: a result, receipt or package PASS attributed to any
other package, or to none, is a problem and is never counted. For the multi-stripe
checkpoint it also recomputes the deterministic payload hash and checks the
retained layout arithmetic, so a receipt cannot pass on its own claims alone.
For the backend-fault checkpoint it recomputes the three deterministic object
hashes and checks the retained fault record: the OSD down and back up in later
epochs with a new Messenger nonce, every overlapping request classified as a
success, gateway-attributed reception at the restarted OSD in its new map
epochs and after the gateway's pre-fault TIDs, the gateway's watch reconnects,
and a new manager the gateway reconnected and reported to.

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
BACKEND_FAULT = "TestRGWNativeBackendFaultIntegration"
TESTS = (CHECKPOINT, MULTISTRIPE, BACKEND_FAULT)
MARKERS = {CHECKPOINT: "RGW_NATIVE_CHECKPOINT ", MULTISTRIPE: "RGW_NATIVE_MULTISTRIPE ",
           BACKEND_FAULT: "RGW_NATIVE_BACKEND_FAULT "}
# The module path in integration/fixture/go.mod; both tests live in its root package.
PACKAGE = "github.com/JSYoo5B/ceph-msgr-go/integration/fixture"
MULTISTRIPE_SIZE = 6 << 20
SOURCE = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE_DIGEST = "6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
# Match rgwNativeCallMethods in integration/fixture, including its eight-call bound.
CALL_METHOD = re.compile(r"(?:\[|,)call ([a-z_]+\.[a-z_]+)(?:[ ,\]]|$)")
# Match rgwNativeWatchReconnect and rgwNativeWrites in integration/fixture.
WATCH_RECONNECT = re.compile(r"(?:\[|,)watch reconnect cookie [0-9]+ gen [1-9][0-9]*(?:[ ,\]]|$)")
WRITE = re.compile(r"(?:\[|,)(?:write|writefull)(?:[ ,\]]|$)")
# The objects of the backend-fault checkpoint and the (multiplier, offset) of
# rgwNativeFaultPayload that builds each one's 1,031 bytes.
FAULT_OBJECTS = {"before-fault": (13, 7), "during-osd-fault": (29, 11), "during-mgr-fault": (31, 3)}
FAULT_SIZE = 1031
CLEANUP = re.compile(r"RGW_NATIVE_CLEANUP independent inspection confirms containers=([1-9][0-9]*) volumes=([1-9][0-9]*) network=removed")


def large_payload(size):
    """Mirror rgwNativeLargePayload in integration/fixture."""
    out = bytearray()
    while len(out) < size:
        out += hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + struct.pack(">Q", len(out) // 32)).digest()
    return bytes(out[:size])


def fault_payload(multiplier, offset, size=FAULT_SIZE):
    """Mirror rgwNativeFaultPayload in integration/fixture."""
    return bytes((i * multiplier + offset) & 0xff for i in range(size))


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
    for field, methods in (("delete_head_remove", ("rgw.obj_remove",)),
                           ("delete_gc_enqueue", ("rgw_gc.rgw_gc_queue_enqueue", "rgw.gc_set_entry"))):
        for op in receipt.get(field, []):
            description = op.get("description", "") if isinstance(op, dict) else ""
            tid = op.get("tid") if isinstance(op, dict) else None
            observed_methods = CALL_METHOD.findall(description)[:8]
            if not put or not isinstance(tid, int) or tid <= max(put) or not any(method in observed_methods for method in methods):
                problems.append("%s entry is not a DELETE-phase request with its expected method" % field)
    tails = len(stripes) - (0 if receipt.get("head_sentinel") else 1)
    if tails < 1 or receipt.get("gc_removed_tails") != tails:
        problems.append("receipt does not account for every removed GC tail")
    if not isinstance(receipt.get("gc_chains"), int) or receipt["gc_chains"] < 1:
        problems.append("receipt has no GC chain")
    return problems


def _ops(value):
    return [op for op in value if isinstance(op, dict)] if isinstance(value, list) else []


def _clients(value, generation):
    """Whether value is a nonempty list of the gateway's own client names."""
    return isinstance(value, list) and bool(value) and all(client in generation for client in value)


def _calls(op, method):
    return method in CALL_METHOD.findall(op.get("description", ""))[:8]


def check_backend_fault(receipt):
    problems = []
    if receipt.get("source") != SOURCE or not str(receipt.get("image", "")).endswith("@sha256:" + IMAGE_DIGEST):
        problems.append("receipt release identity differs from the pinned Tentacle image")
    hashes = receipt.get("sha256")
    expected = {key: hashlib.sha256(fault_payload(*args)).hexdigest() for key, args in FAULT_OBJECTS.items()}
    if receipt.get("bytes") != FAULT_SIZE or hashes != expected:
        problems.append("receipt object hashes differ from the deterministic inputs")
    generation = {entry.get("client") for entry in _ops(receipt.get("gateway_generation"))}
    if not generation:
        problems.append("receipt has no gateway generation")
    before_fault = receipt.get("before_fault") if isinstance(receipt.get("before_fault"), dict) else {}
    last = before_fault.get("last_tids") if isinstance(before_fault.get("last_tids"), dict) else {}
    if not _ops(before_fault.get("put_reception")) or not last:
        problems.append("receipt has no gateway-attributed reception before the fault")
    osd = receipt.get("osd") if isinstance(receipt.get("osd"), dict) else {}
    states = [osd.get(name) if isinstance(osd.get(name), dict) else {} for name in ("before", "down", "after")]
    before, down, after = states
    up_from = after.get("up_from")
    if not (before.get("up") is True and down.get("up") is False and after.get("up") is True
            and isinstance(before.get("epoch"), int) and isinstance(down.get("epoch"), int)
            and isinstance(up_from, int) and before["epoch"] < down["epoch"] < up_from):
        problems.append("OSD record does not show it up, then down, then up again in later epochs")
    if not before.get("nonce") or before.get("nonce") == after.get("nonce"):
        problems.append("restarted OSD kept its Messenger nonce")
    outcomes = _ops(osd.get("outcomes"))
    if len(outcomes) != 2 or any(o.get("class") != "succeeded" or o.get("status") != 200 for o in outcomes):
        problems.append("a request that overlapped the OSD outage is not classified as a success")
    if osd.get("pending_at_restart") is not True:
        problems.append("the overlapping requests were not outstanding when the OSD was started")
    put, get, watches = _ops(osd.get("put_reception")), _ops(osd.get("get_reception")), _ops(osd.get("watch_reconnect"))
    if not (any(WRITE.search(op.get("description", "")) for op in put)
            and any(_calls(op, "rgw.bucket_prepare_op") for op in put)
            and any(_calls(op, "rgw.bucket_complete_op") for op in put)):
        problems.append("restarted OSD has no complete gateway PUT reception")
    if not get:
        problems.append("restarted OSD has no gateway GET reception")
    if not watches or not all(WATCH_RECONNECT.search(op.get("description", "")) for op in watches):
        problems.append("restarted OSD has no gateway watch reconnect")
    for op in put + get + watches:
        client, tid, epoch = op.get("client"), op.get("tid"), op.get("map_epoch")
        if client not in generation or not isinstance(tid, int) or not isinstance(epoch, int):
            problems.append("reception entry is not attributed to the gateway")
        elif not isinstance(up_from, int) or epoch < up_from:
            problems.append("%s TID %d carries an epoch before the OSD returned" % (client, tid))
        elif client in last and (not isinstance(last[client], int) or tid <= last[client]):
            problems.append("%s TID %d is not after the gateway's pre-fault TIDs" % (client, tid))
    if not _clients(osd.get("gateway_connected"), generation):
        problems.append("no gateway Messenger connection to the restarted OSD")
    mgr = receipt.get("mgr") if isinstance(receipt.get("mgr"), dict) else {}
    old, new = [mgr.get(name) if isinstance(mgr.get(name), dict) else {} for name in ("before", "after")]
    if not (old.get("available") is True and new.get("available") is True and old.get("gid") and new.get("gid")
            and old["gid"] != new["gid"] and old.get("nonce") and new.get("nonce") and old["nonce"] != new["nonce"]):
        problems.append("manager record does not show a new active manager process")
    outcome = mgr.get("outcome") if isinstance(mgr.get("outcome"), dict) else {}
    if outcome.get("class") != "succeeded" or outcome.get("status") != 200:
        problems.append("the request during the manager outage is not classified as a success")
    if not _clients(mgr.get("gateway_connected"), generation) or mgr.get("status_reported") is not True:
        problems.append("the gateway did not reconnect and report to the restarted manager")
    gids = mgr.get("service_gids")
    if not isinstance(gids, list) or len(gids) != 1 or "client.%s" % gids[0] not in generation:
        problems.append("the service map rgw daemon is not the gateway")
    if receipt.get("native_readback") != "confirmed" or receipt.get("object_index_user_cleanup") != "confirmed":
        problems.append("receipt lacks its native readback or confirmed cleanup")
    return problems


def verify(text, test):
    if test not in TESTS:
        raise ValueError("unsupported test selector")
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
    marker = MARKERS[test]
    receipts = [line.split(marker, 1)[1] for line in text_output.splitlines() if marker in line]
    receipt = None
    if len(receipts) != 1:
        problems.append("expected exactly one %s receipt, saw %d" % (marker.strip(), len(receipts)))
    elif test in (MULTISTRIPE, BACKEND_FAULT):
        try:
            receipt = json.loads(receipts[0])
        except ValueError:
            problems.append("%s receipt is not JSON" % marker.strip())
        else:
            if not isinstance(receipt, dict):
                problems.append("%s receipt is not a JSON object" % marker.strip())
                receipt = None
            elif test == MULTISTRIPE:
                problems.extend(check_multistripe(receipt))
            else:
                problems.extend(check_backend_fault(receipt))
    else:
        receipt = {"line_sha256": hashlib.sha256(receipts[0].encode()).hexdigest()}
        if "object_index_user_cleanup=confirmed" not in receipts[0] or SOURCE not in receipts[0]:
            problems.append("checkpoint receipt lacks its confirmed cleanup or pinned source")
    summary = {"test": test, "package": PACKAGE, "passed": not problems, "problems": problems,
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
