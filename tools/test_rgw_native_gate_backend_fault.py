"""Receipt tests of TestRGWNativeBackendFaultIntegration (tools/rgw_native_gate_backend_fault.py)."""
import hashlib
import json
import unittest

import rgw_native_gate as gate
import rgw_native_gate_backend_fault as fault
from test_rgw_native_gate import IMAGE, stream
from test_rgw_native_gate_multi_stripe import MULTI, SAMPLE as multi_stripe_sample

FAULT = fault.TEST


def fault_receipt(**changes):
    """A backend-fault receipt with the shape TestRGWNativeBackendFaultIntegration logs."""
    def op(tid, epoch, description, obj, client="client.12"):
        return {"client": client, "tid": tid, "map_epoch": epoch, "description": description, "object": obj}
    head, index = "m.1_", ".dir.b.1.7"
    value = {
        "image": IMAGE, "source": gate.SOURCE, "bytes": fault.FAULT_SIZE,
        "sha256": {key: hashlib.sha256(fault.fault_payload(*args)).hexdigest() for key, args in fault.FAULT_OBJECTS.items()},
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
SAMPLE = fault.MARKER + json.dumps(fault_receipt())


class BackendFaultReceiptTest(unittest.TestCase):
    def test_backend_fault_receipt_must_show_the_fault_and_its_recovery(self):
        def fault_line(**changes):
            return fault.MARKER + json.dumps(fault_receipt(**changes))
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
        for line in (fault.MARKER + "not json", fault.MARKER + "[]",
                     multi_stripe_sample):
            with self.subTest(line=line[:40]):
                self.assertFalse(gate.verify(stream(FAULT, line=line), FAULT)["passed"])
        # A backend-fault receipt never completes another checkpoint.
        self.assertFalse(gate.verify(stream(MULTI, line=fault_line()), MULTI)["passed"])

    def test_fault_payloads_match_the_fixture_construction(self):
        self.assertEqual(hashlib.sha256(fault.fault_payload(13, 7)).hexdigest(),
                         "f5ae8589d38de5ff5e7d3214338b062b60e477067a16e3efdf26ae28cbfa3eab")
        self.assertEqual(fault.fault_payload(29, 11)[:3], bytes([11, 40, 69]))


if __name__ == "__main__":
    unittest.main()
