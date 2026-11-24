"""Fail-closed evidence joins for the combined RGW fault checkpoint."""
import copy
import hashlib
import rgw_native_gate_multi_stripe as multi
import json
import unittest

import rgw_native_gate as gate
import rgw_native_gate_multi_stripe_fault as fault
from test_rgw_native_gate import stream
from test_rgw_native_gate_multi_stripe import receipt as multistripe


def receipt():
    value = multistripe(bytes=12 << 20, sha256=hashlib.sha256(multi.large_payload(12 << 20)).hexdigest())
    value["stripes"][1].update(size=8 << 20, received_write_bytes=8 << 20)
    op = {"client": "client.21", "tid": 10, "object": "m_o1", "map_epoch": 4,
          "description": "osd_op(x [write 0~2097152])", "events": ["reached_pg"]}
    value["put_reception"] = [op]
    value["gateway_generation"] = [{"client": "client.21", "nonce": 100}]
    value["osd_fault"] = {
        "before": {"up": True, "epoch": 4, "nonce": 1000},
        "down": {"up": False, "epoch": 5, "nonce": 1000},
        "after": {"up": True, "epoch": 6, "up_from": 6, "nonce": 2000},
        "prefix_bytes": 11 << 20, "pending_at_restart": True, "overlap_window_seconds": 5,
        "pre_fault_writes": [copy.deepcopy(op)],
        "outcome": {"request": "PUT multistripe", "class": "succeeded", "status": 200}}
    value["mgr_takeover"] = {
        "before": {"available": True, "epoch": 10, "active_name": "a", "active_gid": 11,
                   "standbys": [{"name": "b", "gid": 12}],
                   "active_addrs": {"addrvec": [{"addr": "192.0.2.1:6800", "nonce": 50}]}},
        "after": {"available": True, "epoch": 11, "active_name": "b", "active_gid": 12,
                  "active_addrs": {"addrvec": [{"addr": "192.0.2.2:6800", "nonce": 51}]}},
        "gateway_connected_before": ["client.21"], "gateway_connected": ["client.21"], "former_active_stopped": True}
    return value


SAMPLE = fault.MARKER + json.dumps(receipt())


class MultiStripeFaultGateTest(unittest.TestCase):
    def test_accepts_complete_joined_evidence(self):
        result = gate.verify(stream(fault.TEST), fault.TEST)
        self.assertTrue(result["passed"], result["problems"])

    def test_rejects_independently_missing_or_contradictory_fault_evidence(self):
        cases = [
            (("osd_fault", "down", "up"), True),
            (("osd_fault", "after", "nonce"), 1000),
            (("osd_fault", "after", "up_from"), 5),
            (("osd_fault", "prefix_bytes"), 6 << 20),
            (("osd_fault", "pending_at_restart"), False),
            (("osd_fault", "overlap_window_seconds"), 0),
            (("osd_fault", "outcome", "class"), "uncertain"),
            (("osd_fault", "pre_fault_writes"), []),
            (("osd_fault", "pre_fault_writes", 0, "client"), "client.99"),
            (("osd_fault", "pre_fault_writes", 0, "object"), "foreign"),
            (("osd_fault", "pre_fault_writes", 0, "events"), []),
            (("osd_fault", "pre_fault_writes", 0, "map_epoch"), 6),
            (("mgr_takeover", "after", "active_name"), "a"),
            (("mgr_takeover", "after", "active_gid"), 13),
            (("mgr_takeover", "before", "standbys"), []),
            (("mgr_takeover", "after", "active_addrs", "addrvec"), []),
            (("mgr_takeover", "after", "active_addrs", "addrvec", 0, "nonce"), 50),
            (("mgr_takeover", "former_active_stopped"), False),
            (("mgr_takeover", "gateway_connected"), ["client.99"]),
            (("mgr_takeover", "gateway_connected"), []),
            (("mgr_takeover", "gateway_connected_before"), []),
            (("osd_fault", "pre_fault_writes", 0, "description"), "[read 0~1]"),
            (("gc_removed_tails",), 0),
            (("sha256",), "0" * 64),
        ]
        for path, replacement in cases:
            with self.subTest(path=path):
                value = receipt()
                target = value
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = replacement
                result = gate.verify(stream(fault.TEST, line=fault.MARKER + json.dumps(value)), fault.TEST)
                self.assertFalse(result["passed"], result)
