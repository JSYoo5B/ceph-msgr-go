"""Receipt check of TestRGWNativeMultiStripeIntegration for tools/rgw_native_gate.py.

It recomputes the deterministic payload hash and checks the retained layout
arithmetic, so a receipt cannot pass on its own claims alone: the stripes
cover the object without gap or overlap, each with a gateway-attributed WRITE
reception of its size, and the DELETE phase removes the head and enqueues
every tail for garbage collection after the PUT.
"""
import hashlib
import struct

TEST = "TestRGWNativeMultiStripeIntegration"
MARKER = "RGW_NATIVE_MULTISTRIPE "
MULTISTRIPE_SIZE = 6 << 20


def large_payload(size):
    """Mirror rgwNativeLargePayload in integration/fixture."""
    out = bytearray()
    while len(out) < size:
        out += hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + struct.pack(">Q", len(out) // 32)).digest()
    return bytes(out[:size])


def check(line, gate):
    """(problems, summary fields) of the one receipt line."""
    receipt, problems = gate.json_receipt(line, MARKER)
    if receipt is None:
        return problems, {}
    return check_receipt(receipt, gate), {"stripes": len(receipt.get("stripes", []))}


def check_receipt(receipt, gate, size=MULTISTRIPE_SIZE):
    problems = []
    if receipt.get("bytes") != size:
        problems.append("receipt object size is not %d" % size)
    expected = hashlib.sha256(large_payload(size)).hexdigest()
    if receipt.get("sha256") != expected:
        problems.append("receipt payload hash differs from the deterministic input")
    problems.extend(gate.release_problems(receipt))
    stripes = receipt.get("stripes")
    if not isinstance(stripes, list) or len(stripes) < 2 or len(stripes) > 64:
        return problems + ["layout has fewer than two byte-bearing objects"]
    expected_size = size
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
    if covered != expected_size:
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
            observed_methods = gate.call_methods(description)
            if not put or not isinstance(tid, int) or tid <= max(put) or not any(method in observed_methods for method in methods):
                problems.append("%s entry is not a DELETE-phase request with its expected method" % field)
    tails = len(stripes) - (0 if receipt.get("head_sentinel") else 1)
    if tails < 1 or receipt.get("gc_removed_tails") != tails:
        problems.append("receipt does not account for every removed GC tail")
    if not isinstance(receipt.get("gc_chains"), int) or receipt["gc_chains"] < 1:
        problems.append("receipt has no GC chain")
    return problems
