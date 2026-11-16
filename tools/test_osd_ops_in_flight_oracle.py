import base64
import hashlib
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_ops_in_flight_oracle as ops

# Synthetic, compact (non-pretty) JSON in the field order of the pinned
# OpTracker::dump_ops_in_flight, TrackedOp::dump and OpRequest::_dump
# (src/common/TrackedOp.cc:236-282, 505-520, src/osd/OpRequest.cc:53-89) with
# the top-level section name dropped (src/common/Formatter.cc:207-226).
# Identities, objects, addresses and times are invented; no daemon produced
# these bytes. A delayed op reports its delay as its flag point
# (src/osd/OpRequest.h:116-126).
HELD_EVENTS = ["initiated", "throttled", "header_read", "all_read", "dispatched", "queued_for_pg", "reached_pg", "op must wait for map"]
ENTITY = {"type": "client", "num": 4171, "inc": 21}
PLACEMENT = {"epoch": 29, "pgid": "3.7", "primary_id": 3, "osd_id": 3, "acting": [3, 2, 1, 0], "up": [3, 2, 1, 0],
             "pg_shard": 0, "address": "v2:192.0.2.10:6800/1"}


def op(reqid="client.4171.21:5", pgid="3.7s0", epoch=1029, flag_point="op must wait for map", events=HELD_EVENTS, client="client.4171", tid=5, info=True):
    data = {"flag_point": flag_point}
    if info:
        data["client_info"] = {"client": client, "client_addr": "192.0.2.20:0/3", "tid": tid}
    data["events"] = [{"event": e, "time": "2026-11-16T00:00:00.000000+0000", "duration": 0.0} for e in events]
    return {"description": "osd_op(%s %s 3:e0000000:::ec-held-request:head [append 0~13,setxattr _ec-held (3)] snapc 0=[] "
                           "ondisk+write+known_if_redirected+supports_pool_eio e%d)" % (reqid, pgid, epoch),
            "initiated_at": "2026-11-16T00:00:00.000000+0000", "age": 1.5, "duration": 1.5, "continuous": False, "type_data": data}


def subop():
    """An op of another kind, which the tracker dumps without client_info."""
    return {"description": "osd_repop(client.4180.0:9 3.1 e29/27)", "initiated_at": "2026-11-16T00:00:00.000000+0000", "age": 0.1,
            "duration": 0.1, "continuous": False, "type_data": {"flag_point": "commit sent; apply or cleanup",
                                                                "events": [{"event": "initiated", "time": "t", "duration": 0.0}]}}


def dump(items, count=None):
    return json.dumps({"ops": items, "num_ops": len(items) if count is None else count}, separators=(",", ":")).encode()


class ParseTest(unittest.TestCase):
    def test_reports_the_ops_of_the_exact_client_entity(self):
        parsed = ops.parse_ops(dump([subop(), op(), op("client.4171.20:5", tid=5), op("client.41710.21:6", client="client.41710", tid=6)]))
        found = ops.client_ops(parsed, 4171, 21)
        self.assertEqual(found, [{"index": 1, "reqid": "client.4171.21:5", "tid": 5, "pgid": "3.7s0", "epoch": 1029, "flag_point": "op must wait for map",
                                  "events": HELD_EVENTS, "description": parsed[1]["description"]}])
        # Near misses: another incarnation and a longer entity number.
        self.assertEqual([o["reqid"] for o in ops.client_ops(parsed, 4171, 20)], ["client.4171.20:5"])
        self.assertEqual(ops.client_ops(parsed, 417, 21), [])
        self.assertEqual(ops.client_ops(parsed, 4171, 2), [])

    def test_undecoded_op(self):
        # Before do_op an op prints its raw PG and "(undecoded)"
        # (src/messages/MOSDOp.h:622-641).
        item = dict(op(), description="osd_op(client.4171.21:5 3.7s0 3.d6c2a1e7 (undecoded) ondisk+write+returnvec e1025)")
        found = ops.client_ops(ops.parse_ops(dump([item])), 4171, 21)
        self.assertEqual([(o["tid"], o["pgid"], o["epoch"]) for o in found], [(5, "3.7s0", 1025)])

    def test_replicated_pg_and_empty_dump(self):
        parsed = ops.parse_ops(dump([op(pgid="2.1f")]))
        self.assertEqual(ops.client_ops(parsed, 4171, 21)[0]["pgid"], "2.1f")
        self.assertEqual(ops.parse_ops(dump([])), [])

    def test_refuses_untrusted_shapes(self):
        bad = {
            "not JSON": b"{",
            "truncated": dump([op()])[:-3],
            "empty": b"",
            "extra top-level key": json.dumps({"ops": [], "num_ops": 0, "x": 1}).encode(),
            "missing num_ops": json.dumps({"ops": []}).encode(),
            "num_ops differs": dump([op()], count=2),
            "duplicate key": b'{"ops":[],"num_ops":0,"num_ops":0}',
            "op without type_data": dump([{k: v for k, v in op().items() if k != "type_data"}]),
            "op with an extra field": dump([dict(op(), extra=1)]),
            "type_data extra field": dump([dict(op(), type_data=dict(op()["type_data"], x=1))]),
            "event shape": dump([dict(op(), type_data=dict(op()["type_data"], events=[{"event": "initiated"}]))]),
            "client_info shape": dump([dict(op(), type_data=dict(op()["type_data"], client_info={"client": "client.4171"}))]),
        }
        for name, raw in bad.items():
            with self.subTest(name):
                with self.assertRaises(ops.OpsError):
                    ops.parse_ops(raw)
        with self.assertRaises(ops.OpsError):
            ops.parse_ops(b" " * (ops.MAX_OPS_BYTES + 1))

    def test_refuses_an_inconsistent_op_of_the_target_client(self):
        bad = {
            "no client_info": op(info=False),
            "client_info of another entity": op(client="client.4172"),
            "client_info of another tid": op(tid=6),
            "unparsable identity": op(reqid="client.4171.21:x"),
            "no epoch": dict(op(), description="osd_op(client.4171.21:5 3.7s0 [append 0~13])"),
            "unparsable PG": op(pgid="3.7s"),
        }
        for name, item in bad.items():
            with self.subTest(name):
                with self.assertRaises(ops.OpsError):
                    ops.client_ops(ops.parse_ops(dump([item])), 4171, 21)


class SnapshotTest(unittest.TestCase):
    def test_complete_receipt(self):
        raw = dump([op()])
        receipt = ops.snapshot(PLACEMENT, dict(PLACEMENT), 0, raw, "", ENTITY)
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["num_ops"], receipt["client"], len(receipt["ops"]), receipt["absence_established"]), (1, "client.4171.21", 1, False))
        self.assertEqual((receipt["bytes"], receipt["sha256"], base64.b64decode(receipt["raw"])), (len(raw), hashlib.sha256(raw).hexdigest(), raw))

    def test_fails_closed(self):
        moved = dict(PLACEMENT, epoch=30)
        cases = {
            "refused": ops.snapshot(PLACEMENT, PLACEMENT, -22, b"", "op_tracker tracking is not enabled now", ENTITY),
            "oversized": ops.snapshot(PLACEMENT, PLACEMENT, 0, b"", "", ENTITY, oversized_length=ops.MAX_OPS_BYTES + 1),
            "placement moved": ops.snapshot(PLACEMENT, moved, 0, dump([op()]), "", ENTITY),
            "unparsable": ops.snapshot(PLACEMENT, PLACEMENT, 0, b"{", "", ENTITY),
            "inconsistent target op": ops.snapshot(PLACEMENT, PLACEMENT, 0, dump([op(tid=6)]), "", ENTITY),
        }
        for name, receipt in cases.items():
            with self.subTest(name):
                self.assertFalse(receipt["complete"])
                self.assertTrue(receipt["failures"])
        # A moved placement still reports what the tracker held.
        self.assertEqual(len(cases["placement moved"]["ops"]), 1)

    def test_refuses_invalid_entities(self):
        for entity in ({"type": "osd", "num": 1, "inc": 0}, {"type": "client", "num": 0, "inc": 0}, {"type": "client", "num": 1},
                       {"type": "client", "num": 1, "inc": 1 << 31}, {"type": "client", "num": True, "inc": 0},
                       {"type": "client", "num": 1, "inc": 0, "tid": 1}):
            with self.subTest(entity):
                with self.assertRaises(ValueError):
                    ops.client_entity(entity)


if __name__ == "__main__":
    unittest.main()
