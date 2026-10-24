import hashlib
import json
import unittest

import rgw_native_gate as gate

SMALL = gate.CHECKPOINT
MULTI = gate.MULTISTRIPE
IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:" + gate.IMAGE_DIGEST
CLEANUP = "RGW_NATIVE_CLEANUP independent inspection confirms containers=4 volumes=1 network=removed"


def receipt(**changes):
    sizes = [4 << 20, 2 << 20]
    stripes, offset = [], 0
    for index, size in enumerate(sizes):
        stripes.append({"index": index, "offset": offset, "size": size, "oid": "m_o%d" % index,
                        "received_writes": 1, "received_write_bytes": size})
        offset += size
    value = {"image": IMAGE, "source": gate.SOURCE, "bytes": gate.MULTISTRIPE_SIZE,
             "sha256": hashlib.sha256(gate.large_payload(gate.MULTISTRIPE_SIZE)).hexdigest(),
             "stripes": stripes, "head_sentinel": None, "gc_chains": 1, "gc_removed_tails": 1,
             "put_reception": [{}], "delete_head_remove": [{}], "delete_gc_enqueue": [{}]}
    value.update(changes)
    return value


def stream(test, *, passes=1, package=True, extra=(), cleanup=True, line=None):
    rows = []
    if line is None:
        line = ("RGW_NATIVE_MULTISTRIPE " + json.dumps(receipt())) if test == MULTI else \
            "RGW_NATIVE_CHECKPOINT image=%s source=%s object_index_user_cleanup=confirmed" % (IMAGE, gate.SOURCE)
    if line:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:1: " + line + "\n"})
    if cleanup:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:2: " + CLEANUP + "\n"})
    rows += [{"Action": "pass", "Test": test}] * passes
    rows += list(extra)
    if package:
        rows.append({"Action": "pass", "Package": "p"})
    return "\n".join(json.dumps(row) for row in rows) + "\n"


class RGWNativeGateTest(unittest.TestCase):
    def test_accepts_actual_passes(self):
        for test in (SMALL, MULTI):
            result = gate.verify(stream(test), test)
            self.assertTrue(result["passed"], result["problems"])
            self.assertEqual(result["cleanup"], {"containers": 4, "volumes": 1})
        self.assertEqual(gate.verify(stream(MULTI), MULTI)["stripes"], 2)

    def test_default_skip_is_not_acceptance(self):
        skipped = stream(MULTI, passes=0, extra=[{"Action": "skip", "Test": MULTI}], line="", cleanup=False)
        result = gate.verify(skipped, MULTI)
        self.assertFalse(result["passed"])
        self.assertTrue(any("skip" in problem for problem in result["problems"]))

    def test_rejects_missing_duplicate_or_foreign_results(self):
        self.assertFalse(gate.verify(stream(MULTI, passes=0), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, passes=2), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, package=False), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, extra=[{"Action": "fail", "Test": MULTI}]), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, extra=[{"Action": "pass", "Test": SMALL}]), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, extra=[{"Action": "fail", "Test": MULTI + "/sub"}]), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, cleanup=False), MULTI)["passed"])
        self.assertFalse(gate.verify(stream(MULTI, extra=[{"Action": "fail"}]), MULTI)["passed"])

    def test_rejects_receipts_that_contradict_the_layout_arithmetic(self):
        short = receipt()
        short["stripes"][1]["size"] -= 1
        gap = receipt()
        gap["stripes"][1]["offset"] += 1
        unreceived = receipt()
        unreceived["stripes"][1]["received_write_bytes"] = 1
        single = receipt()
        single["stripes"] = single["stripes"][:1]
        for name, bad in {
            "short": short, "gap": gap, "unreceived": unreceived, "single": single,
            "hash": receipt(sha256="0" * 64), "size": receipt(bytes=1), "source": receipt(source="x"),
            "image": receipt(image="quay.io/ceph/ceph:v20.2.4"), "no delete": receipt(delete_head_remove=[]),
            "no gc enqueue": receipt(delete_gc_enqueue=[]), "no chain": receipt(gc_chains=0),
            "tails": receipt(gc_removed_tails=2),
        }.items():
            line = "RGW_NATIVE_MULTISTRIPE " + json.dumps(bad)
            self.assertFalse(gate.verify(stream(MULTI, line=line), MULTI)["passed"], name)

    def test_head_sentinel_counts_every_stripe_as_a_tail(self):
        bad = receipt(head_sentinel="m_head", gc_removed_tails=2)
        result = gate.verify(stream(MULTI, line="RGW_NATIVE_MULTISTRIPE " + json.dumps(bad)), MULTI)
        self.assertTrue(result["passed"], result["problems"])

    def test_rejects_missing_or_duplicate_receipt_and_garbage(self):
        self.assertFalse(gate.verify(stream(MULTI, line=""), MULTI)["passed"])
        doubled = stream(MULTI) + stream(MULTI)
        self.assertFalse(gate.verify(doubled, MULTI)["passed"])
        with self.assertRaises(ValueError):
            gate.verify("not json\n", MULTI)
        with self.assertRaises(ValueError):
            gate.verify("[]\n", MULTI)
        with self.assertRaises(ValueError):
            gate.verify(stream(MULTI), "TestSomethingElse")

    def test_payload_matches_the_documented_construction(self):
        payload = gate.large_payload(100)
        self.assertEqual(len(payload), 100)
        self.assertEqual(payload[:32], hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + bytes(8)).digest())
        self.assertEqual(payload[32:64], hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + bytes(7) + b"\x01").digest())


if __name__ == "__main__":
    unittest.main()
