#!/usr/bin/env python3
"""Object-scope backoff recipe regressions; mocks do not establish daemon acceptance."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import osd_object_backoff_oracle as oracle

FSID = "12345678-1234-1234-1234-123456789abc"
PREFIX = "fixture-native-omapw-"
MAIN = PREFIX + "main-" + "a" * 32
ABSENT = PREFIX + "absent-" + "b" * 32
METADATA = dict(pool="ceph-msgr-osd-fixture", pool_id=2, object="fixture", profile=oracle.PROFILES[0],
                replicas=3, min_size=2, pool_pg_num=8, osd_id=1)
MANIFEST = dict(osd_profile=oracle.PROFILES[0], osd_replicas="3", osd_store="bluestore", osd="1", fsid=FSID)
OWNERSHIP = dict(schema_version=1, source_commit=oracle.SOURCE_COMMIT, pool=METADATA["pool"], pool_id=2,
                 fixture_object="fixture", namespace="", objects=[MAIN, ABSENT])


def osd_dump(flags="sortbitwise", up=(1, 1, 1)):
    return dict(fsid=FSID, epoch=40, flags=flags, osds=[dict(osd=i, up=u, **{"in": 1}) for i, u in enumerate(up)])


class ObjectBackoffOracleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)
        (self.out / "osd-oracle.json").write_text(json.dumps(METADATA))
        (self.out / "fixture-manifest.json").write_text(json.dumps(MANIFEST))
        (self.out / "daemon-version").write_text("ceph version 20.2.4 (" + oracle.SOURCE_COMMIT + ") tentacle")
        (self.out / (oracle.writes.OWNED + ".json")).write_text(json.dumps(OWNERSHIP))

    def tearDown(self):
        self.tmp.cleanup()

    def test_owned_requires_three_bluestore_replicas(self):
        self.assertEqual(oracle.owned(Path("/root"), self.out)[2], [MAIN, ABSENT])
        for field, value in (("osd_replicas", "1"), ("osd_store", "memstore"), ("osd_profile", "legacy-v6")):
            (self.out / "fixture-manifest.json").write_text(json.dumps(dict(MANIFEST, **{field: value})))
            with self.assertRaises(ValueError):
                oracle.owned(Path("/root"), self.out)

    def test_objects_and_osds_are_closed(self):
        self.assertEqual(oracle.owned_object([MAIN], MAIN), MAIN)
        with self.assertRaises(ValueError):
            oracle.owned_object([MAIN], "fixture")
        for value in (-1, 3, True, "1"):
            with self.assertRaises(ValueError):
                oracle.osd_id(value)
        with self.assertRaises(ValueError):
            oracle.payload("")

    def test_parse_native_objecter_log(self):
        name = ABSENT
        text = "\n".join([
            "2026-10-16T01:00:00.100+0000 ffff 15 client.4500.objecter _send_op 1 to 2.3 on osd.1",
            "2026-10-16T01:00:00.200+0000 ffff 10 client.4500.objecter handle_osd_backoff osd_backoff(2.3 block id 1 "
            "[2:c0000000:::%s:head,2:c0000000:::%s:head) e45)" % (name, name),
            "2026-10-16T01:00:06.000+0000 ffff 10 client.4500.objecter handle_osd_backoff osd_backoff(2.3 unblock id 1 "
            "[2:c0000000:::%s:head,2:c0000000:::%s:head) e45)" % (name, name),
            "2026-10-16T01:00:06.001+0000 ffff 15 client.4500.objecter _send_op 1 to 2.3 on osd.1",
            "unrelated line"])
        parsed = oracle.parse_log(text, name)
        self.assertEqual([b["op"] for b in parsed["backoffs"]], ["block", "unblock"])
        self.assertTrue(all(b["single_object"] and b["begin_has_object"] and b["epoch"] == 45 for b in parsed["backoffs"]))
        self.assertEqual([(s["tid"], s["osd"]) for s in parsed["sends"]], [(1, 1), (1, 1)])

    def test_ack_history_matches_only_object_bounds(self):
        bounds = "[2:c0000000:::%s:head,2:c0000000:::%s:head)" % (ABSENT, ABSENT)
        history = {"ops": [
            {"description": "osd_backoff(2.3 ack-block id 1 %s e45)" % bounds,
             "type_data": {"client_info": {"client": "client.42"}, "events": [{"event": "reached_pg"}]}},
            {"description": "osd_backoff(2.3 ack-block id 2 [2:00000000::::head,2:20000000::::head) e45)",
             "type_data": {"client_info": {"client": "client.42"}, "events": [{"event": "reached_pg"}]}},
            {"description": "osd_op(client.42.0:1 2.3 2:c0000000:::%s:head [append 0~3])" % ABSENT}]}
        found = oracle.parse_ack_history(history, "2.3", ABSENT)
        self.assertEqual(len(found), 1)
        self.assertEqual((found[0]["id"], found[0]["map_epoch"], found[0]["client"], found[0]["reached_pg"],
                          found[0]["single_object"]), (1, 45, "client.42", True, True))
        self.assertEqual(oracle.parse_ack_history(history, "2.4", ABSENT), [])

    def test_restore_reinstates_only_recorded_flags_and_configuration(self):
        state = dict(schema_version=1, source_commit=oracle.SOURCE_COMMIT, flags=["noout", "sortbitwise"],
                     config={"global/osd_heartbeat_grace": "30"}, restored=False, native=None)
        oracle.save_state(self.out, state)
        calls = []

        def ceph(root, args, json_output=True, timeout=0):
            calls.append(tuple(args))
            if args[:2] == ["osd", "dump"]:
                return osd_dump("noout,sortbitwise")
            if args[:2] == ["pg", "stat"]:
                return {"pg_summary": {"num_pg_by_state": [{"name": "active+clean", "num": 9}]}}
            if args[:2] == ["config", "dump"]:
                return [{"section": "global", "name": "osd_heartbeat_grace", "value": "30"}]
            return None
        with patch.object(oracle, "ceph", ceph):
            result = oracle.restore(Path("/root"), self.out, FSID)
        self.assertTrue(result["restored"])
        self.assertNotIn(("osd", "unset", "noout"), calls)
        self.assertIn(("osd", "unset", "norecover"), calls)
        self.assertIn(("osd", "unset", "nobackfill"), calls)
        self.assertIn(("config", "set", "global", "osd_heartbeat_grace", "30"), calls)
        self.assertIn(("config", "rm", "osd", "osd_backoff_on_degraded"), calls)
        self.assertTrue(oracle.load_state(self.out)["restored"])

    def test_prepare_refuses_unrestored_scope_and_down_osd(self):
        oracle.save_state(self.out, dict(restored=False))
        with self.assertRaises(ValueError):
            oracle.prepare(Path("/root"), self.out, METADATA, FSID, [MAIN], {"objects": [MAIN]})
        oracle.save_state(self.out, dict(restored=True))
        with patch.object(oracle, "ceph", lambda root, args, json_output=True, timeout=0: osd_dump(up=(1, 0, 1))):
            with self.assertRaises(ValueError):
                oracle.prepare(Path("/root"), self.out, METADATA, FSID, [MAIN], {"objects": [MAIN]})


if __name__ == "__main__":
    unittest.main()
