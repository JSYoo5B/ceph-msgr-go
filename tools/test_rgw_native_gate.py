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
             "put_reception": [{"tid": 10}],
             "delete_head_remove": [{"tid": 20, "description": "osd_op(x [call rgw.obj_remove in=13b])"}],
             "delete_gc_enqueue": [{"tid": 21, "description": "osd_op(x [call rgw_gc.rgw_gc_queue_enqueue in=9b])"}]}
    value.update(changes)
    return value


FAULT = gate.BACKEND_FAULT


def fault_receipt(**changes):
    """A backend-fault receipt with the shape TestRGWNativeBackendFaultIntegration logs."""
    def op(tid, epoch, description, obj, client="client.12"):
        return {"client": client, "tid": tid, "map_epoch": epoch, "description": description, "object": obj}
    head, index = "m.1_", ".dir.b.1.7"
    value = {
        "image": IMAGE, "source": gate.SOURCE, "bytes": gate.FAULT_SIZE,
        "sha256": {key: hashlib.sha256(gate.fault_payload(*args)).hexdigest() for key, args in gate.FAULT_OBJECTS.items()},
        "gateway_generation": [{"client": "client.11", "nonce": 1}, {"client": "client.12", "nonce": 2}],
        "before_fault": {"put_reception": [op(799, 20, "[stat,call rgw.bucket_prepare_op in=20b]", index),
                                           op(800, 20, "[create,writefull 0~1031]", head + "before-fault"),
                                           op(801, 21, "[stat,call rgw.bucket_complete_op in=21b]", index)],
                         "last_tids": {"client.11": 10, "client.12": 811}},
        "osd": {
            "before": {"epoch": 21, "up": True, "up_from": 6, "nonce": 5},
            "down": {"epoch": 22, "up": False, "up_from": 6, "nonce": 5},
            "after": {"epoch": 24, "up": True, "up_from": 24, "nonce": 6},
            "pending_at_restart": True,
            "outcomes": [{"request": "PUT during-osd-fault", "class": "succeeded", "status": 200},
                         {"request": "GET before-fault", "class": "succeeded", "status": 200}],
            "put_reception": [op(814, 24, "[stat,call rgw.guard_bucket_resharding in=26b,call rgw.bucket_prepare_op in=20b]", index),
                              op(825, 25, "[create,writefull 0~1031,setxattr user.rgw.etag (32) in=13b]", head + "during-osd-fault"),
                              op(827, 25, "[stat,call rgw.guard_bucket_resharding in=26b,call rgw.bucket_complete_op in=21b]", index)],
            "get_reception": [op(813, 24, "[call,getxattrs,stat,read 0~1031]", head + "before-fault")],
            "watch_reconnect": [op(815, 24, "osd_op(client.12.0:815 4.0 4:f9:::notify.0:head [watch reconnect cookie 1 gen 1] snapc 0=[] e24)", "notify.0")],
            "gateway_connected": ["client.12"],
        },
        "mgr": {
            "before": {"epoch": 4, "gid": 4106, "nonce": 7, "available": True},
            "after": {"epoch": 6, "gid": 4337, "nonce": 8, "available": True},
            "outcome": {"request": "PUT during-mgr-fault", "class": "succeeded", "status": 200},
            "gateway_connected": ["client.11", "client.12"], "status_reported": True, "service_gids": [12],
        },
        "native_readback": "confirmed", "object_index_user_cleanup": "confirmed",
    }
    for path, replacement in changes.items():
        target = value
        keys = path.split(".")
        for key in keys[:-1]:
            target = target[key]
        if replacement is DELETE:
            del target[keys[-1]]
        else:
            target[keys[-1]] = replacement
    return value


DELETE = object()


def stream(test, *, passes=1, package=True, extra=(), cleanup=True, line=None, owner=gate.PACKAGE, terminal=gate.PACKAGE):
    """Build a test2json stream; owner names the package of every test event, terminal that of the package PASS."""
    rows = []
    if line is None:
        if test == MULTI:
            line = "RGW_NATIVE_MULTISTRIPE " + json.dumps(receipt())
        elif test == FAULT:
            line = "RGW_NATIVE_BACKEND_FAULT " + json.dumps(fault_receipt())
        else:
            line = "RGW_NATIVE_CHECKPOINT image=%s source=%s object_index_user_cleanup=confirmed" % (IMAGE, gate.SOURCE)
    if line:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:1: " + line + "\n"})
    if cleanup:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:2: " + CLEANUP + "\n"})
    rows += [{"Action": "pass", "Test": test}] * passes
    rows += list(extra)
    rows = [dict(row) if "Package" in row else dict(row, Package=owner) for row in rows]
    if package:
        rows.append({"Action": "pass", "Package": terminal})
    return "\n".join(json.dumps(row) for row in rows) + "\n"


class RGWNativeGateTest(unittest.TestCase):
    def test_accepts_actual_passes(self):
        for test in (SMALL, MULTI, FAULT):
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

    def test_binds_every_result_to_the_fixture_package(self):
        other = "example.org/unrelated"
        for test in (SMALL, MULTI, FAULT):
            with self.subTest(test=test, case="foreign terminal package PASS"):
                result = gate.verify(stream(test, terminal=other), test)
                self.assertFalse(result["passed"])
                self.assertIn("event outside the fixture package: " + other, result["problems"])
                self.assertIn("no PASS of package " + gate.PACKAGE, result["problems"])
            with self.subTest(test=test, case="foreign test events"):
                result = gate.verify(stream(test, owner=other), test)
                self.assertFalse(result["passed"])
                self.assertTrue(any("must PASS exactly once, saw 0" in problem for problem in result["problems"]), result)
                self.assertTrue(any("receipt, saw 0" in problem for problem in result["problems"]), result)
                self.assertIsNone(result["cleanup"])
            with self.subTest(test=test, case="foreign package PASS beside the fixture's own"):
                result = gate.verify(stream(test, extra=[{"Action": "pass", "Package": other}]), test)
                self.assertEqual(result["problems"], ["event outside the fixture package: " + other])
            with self.subTest(test=test, case="event without a package"):
                text = stream(test) + json.dumps({"Action": "pass"}) + "\n"
                self.assertEqual(gate.verify(text, test)["problems"], ["event outside the fixture package: <none>"])
            with self.subTest(test=test, case="selected test PASS without a package"):
                text = stream(test, passes=0) + json.dumps({"Action": "pass", "Test": test}) + "\n"
                self.assertFalse(gate.verify(text, test)["passed"])
            with self.subTest(test=test, case="foreign failure"):
                result = gate.verify(stream(test, extra=[{"Action": "fail", "Package": other}]), test)
                self.assertFalse(result["passed"])
            self.assertEqual(gate.verify(stream(test), test)["package"], gate.PACKAGE)

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
            "startup gc init": receipt(delete_gc_enqueue=[{"tid": 5, "description": "osd_op(x [call rgw_gc.rgw_gc_queue_init in=23b])"}]),
            "gc enqueue before PUT": receipt(delete_gc_enqueue=[{"tid": 9, "description": "[call rgw_gc.rgw_gc_queue_enqueue]"}]),
            "wrong delete method": receipt(delete_head_remove=[{"tid": 20, "description": "[call rgw.bucket_complete_op]"}]),
        }.items():
            line = "RGW_NATIVE_MULTISTRIPE " + json.dumps(bad)
            self.assertFalse(gate.verify(stream(MULTI, line=line), MULTI)["passed"], name)

    def test_delete_methods_require_complete_call_tokens(self):
        cases = (
            ("delete_head_remove", "rgw.obj_remove"),
            ("delete_gc_enqueue", "rgw_gc.rgw_gc_queue_enqueue"),
            ("delete_gc_enqueue", "rgw.gc_set_entry"),
        )
        for field, method in cases:
            for description in (
                "osd_op(x [call %s in=9b])" % method,
                "[create,call version.check_conds in=18b,call %s in=9b]" % method,
            ):
                with self.subTest(field=field, description=description):
                    value = receipt(**{field: [{"tid": 21, "description": description}]})
                    result = gate.verify(stream(MULTI, line="RGW_NATIVE_MULTISTRIPE " + json.dumps(value)), MULTI)
                    self.assertTrue(result["passed"], result["problems"])
            for description in (
                "[call %s_extra in=9b]" % method,
                "[call prefix_%s in=9b]" % method,
                "[setxattr user.%s (1)]" % method,
                "[read %s in=9b]" % method,
                "[not_call %s in=9b]" % method,
                "[call %s.extra in=9b]" % method,
                "[call %s9 in=9b]" % method,
                "[" + ",".join(["call other.method in=1b"] * 8 + ["call %s in=9b" % method]) + "]",
            ):
                with self.subTest(field=field, description=description):
                    value = receipt(**{field: [{"tid": 21, "description": description}]})
                    result = gate.verify(stream(MULTI, line="RGW_NATIVE_MULTISTRIPE " + json.dumps(value)), MULTI)
                    self.assertFalse(result["passed"])
                    self.assertTrue(any(field + " entry" in problem for problem in result["problems"]), result)

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

    def test_backend_fault_receipt_must_show_the_fault_and_its_recovery(self):
        def fault_line(**changes):
            return "RGW_NATIVE_BACKEND_FAULT " + json.dumps(fault_receipt(**changes))
        osd = fault_receipt()["osd"]
        cases = {
            "payload hash": {"sha256.during-osd-fault": "0" * 64},
            "missing object": {"sha256.during-mgr-fault": DELETE},
            "size": {"bytes": 1030},
            "source": {"source": "x"},
            "image": {"image": "quay.io/ceph/ceph:v20.2.4"},
            "no pre-fault reception": {"before_fault.put_reception": []},
            "OSD never down": {"osd.down.up": True},
            "down before stop": {"osd.down.epoch": 21},
            "up from the old epoch": {"osd.after.up_from": 22},
            "same OSD nonce": {"osd.after.nonce": 5},
            "PUT failed with status": {"osd.outcomes": [dict(osd["outcomes"][0], **{"class": "failed_status", "status": 503}), osd["outcomes"][1]]},
            "GET uncertain": {"osd.outcomes": [osd["outcomes"][0], {"request": "GET before-fault", "class": "uncertain"}]},
            "one outcome": {"osd.outcomes": osd["outcomes"][:1]},
            "returned before restart": {"osd.pending_at_restart": False},
            "no head write": {"osd.put_reception": [osd["put_reception"][0], osd["put_reception"][2]]},
            "prepare suffix": {"osd.put_reception": [dict(osd["put_reception"][0], description="[call rgw.bucket_prepare_op_x in=1b]")] + osd["put_reception"][1:]},
            "no GET reception": {"osd.get_reception": []},
            # The review mutation: a valid client, epoch and TID whose
            # operation is not a read of the whole head.
            "GET entry is a writefull": {"osd.get_reception": [dict(osd["get_reception"][0], description="[create,writefull 0~1031]")]},
            "GET entry has no read": {"osd.get_reception": [dict(osd["get_reception"][0], description="[call,getxattrs,stat]")]},
            "GET entry is a class read": {"osd.get_reception": [dict(osd["get_reception"][0], description="[call rgw.read 0~1031]")]},
            "GET entry is a sparse read": {"osd.get_reception": [dict(osd["get_reception"][0], description="[sparse-read 0~1031]")]},
            "GET read is short": {"osd.get_reception": [dict(osd["get_reception"][0], description="[read 0~1030]")]},
            "GET read at another offset": {"osd.get_reception": [dict(osd["get_reception"][0], description="[read 1~1031]")]},
            "GET read of another object": {"osd.get_reception": [dict(osd["get_reception"][0], object="m.1_during-osd-fault")]},
            "GET reads plus a write": {"osd.get_reception": osd["get_reception"] + [dict(osd["get_reception"][0], description="[writefull 0~1031]")]},
            "PUT write on another object": {"osd.put_reception": [osd["put_reception"][0], dict(osd["put_reception"][1], object="m.1_before-fault"), osd["put_reception"][2]]},
            "PUT reception with an extra entry": {"osd.put_reception": osd["put_reception"] + [dict(osd["put_reception"][0], description="[read 0~1031]")]},
            "pre-fault PUT incomplete": {"before_fault.put_reception": fault_receipt()["before_fault"]["put_reception"][:1]},
            "watch reconnect on another object": {"osd.watch_reconnect": [dict(osd["watch_reconnect"][0], object="m.1_before-fault")]},
            "no watch reconnect": {"osd.watch_reconnect": []},
            "fresh watch, not a reconnect": {"osd.watch_reconnect": [dict(osd["watch_reconnect"][0], description="[watch watch cookie 1]")]},
            "reception before the OSD returned": {"osd.get_reception": [dict(osd["get_reception"][0], map_epoch=22)]},
            "reception of a pre-fault TID": {"osd.get_reception": [dict(osd["get_reception"][0], tid=811)]},
            "reception of another client": {"osd.get_reception": [dict(osd["get_reception"][0], client="client.99")]},
            "no OSD connection": {"osd.gateway_connected": []},
            "OSD connection of another client": {"osd.gateway_connected": ["client.99"]},
            "same manager": {"mgr.after.gid": 4106},
            "same manager nonce": {"mgr.after.nonce": 7},
            "manager unavailable": {"mgr.after.available": False},
            "PUT during manager outage uncertain": {"mgr.outcome": {"request": "PUT during-mgr-fault", "class": "uncertain"}},
            "no manager reconnection": {"mgr.gateway_connected": []},
            "no status report": {"mgr.status_reported": False},
            "service daemon is not the gateway": {"mgr.service_gids": [99]},
            "no native readback": {"native_readback": "skipped"},
            "no cleanup": {"object_index_user_cleanup": DELETE},
        }
        for name, changes in cases.items():
            with self.subTest(name):
                result = gate.verify(stream(FAULT, line=fault_line(**changes)), FAULT)
                self.assertFalse(result["passed"], name)
        for line in ("RGW_NATIVE_BACKEND_FAULT not json", "RGW_NATIVE_BACKEND_FAULT []",
                     "RGW_NATIVE_MULTISTRIPE " + json.dumps(receipt())):
            with self.subTest(line=line[:40]):
                self.assertFalse(gate.verify(stream(FAULT, line=line), FAULT)["passed"])
        # A backend-fault receipt never completes another checkpoint.
        self.assertFalse(gate.verify(stream(MULTI, line=fault_line()), MULTI)["passed"])

    def test_fault_payloads_match_the_fixture_construction(self):
        self.assertEqual(hashlib.sha256(gate.fault_payload(13, 7)).hexdigest(),
                         "f5ae8589d38de5ff5e7d3214338b062b60e477067a16e3efdf26ae28cbfa3eab")
        self.assertEqual(gate.fault_payload(29, 11)[:3], bytes([11, 40, 69]))

    def test_payload_matches_the_documented_construction(self):
        payload = gate.large_payload(100)
        self.assertEqual(len(payload), 100)
        self.assertEqual(payload[:32], hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + bytes(8)).digest())
        self.assertEqual(payload[32:64], hashlib.sha256(b"ceph-msgr-go rgw multistripe v1:" + bytes(7) + b"\x01").digest())


if __name__ == "__main__":
    unittest.main()
