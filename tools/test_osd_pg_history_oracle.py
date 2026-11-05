import base64
import hashlib
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pg_history_oracle as history
import osd_pool_oracle as oracle

# Synthetic, compact (non-pretty) JSON in the field order of the pinned
# pg_log_t::dump, pg_log_entry_t::dump and pg_log_dup_t::dump
# (src/osd/osd_types.cc:5096-5118, 5234-5246, 5355-5373) with the top-level
# section name dropped (src/common/Formatter.cc:207-226). Identities, objects
# and versions are invented; no daemon produced these bytes.
ENTRY = ('{"op":"modify","object":"3:b47ce0a1:::ec-history-calibration:head","version":"%s","prior_version":"%s",'
         '"reqid":"%s","extra_reqids":[%s],"mtime":"2023-11-14T22:13:20.000000+0000","return_code":0,'
         '"written_shards":"{0,1,2,3}","mod_desc":{"object_mod_desc":{"can_local_rollback":false,'
         '"rollback_info_completed":false,"ops":[]}},"clean_regions":{"object_clean_regions":{"clean_offsets":"[]",'
         '"clean_omap":true,"new_object":false}}}')
EXTRA = '{"reqid":"%s","user_version":"%d","return_code":0}'
DUP = '{"reqid":"%s","version":"%s","user_version":"%d","return_code":"0"}'
KNOWN = "client.4171.7:2"
SEED = "client.4171.0:1"


def dump(entries, dups=(), head="29'4", tail="29'1"):
    return ('{"pg_log_t":{"head":"%s","tail":"%s","log":[%s],"dups":[%s]}}'
            % (head, tail, ",".join(entries), ",".join(dups))).encode()


def captured():
    """Ordinary entries, one extra_reqid on a later entry and one duplicate."""
    return dump([
        ENTRY % ("29'2", "29'1", SEED, ""),
        ENTRY % ("29'3", "29'2", KNOWN, ""),
        ENTRY % ("29'4", "29'3", "client.4180.3:9", EXTRA % ("client.4190.5:11", 3)),
    ], dups=[DUP % ("client.4199.2:6", "29'1", 1)])


def target(text):
    kind, num, inc, tid = history.parse_reqid(text)
    return {"type": kind, "num": num, "inc": inc, "tid": tid}


PLACEMENT = {"epoch": 29, "pgid": "3.1", "primary_id": 0, "osd_id": 0, "acting": [0, 2, 3, 1], "up": [0, 2, 3, 1],
             "pg_shard": 0, "address": "v2:192.0.2.10:6800/1"}


class ParseTest(unittest.TestCase):
    def test_finds_identities_in_each_history_tier(self):
        parsed = history.parse_log(captured())
        self.assertEqual((parsed["head"], parsed["tail"], len(parsed["log"]), len(parsed["dups"])), ((29, 4), (29, 1), 3, 1))
        log = history.locate(parsed, history.parse_reqid(KNOWN))
        self.assertEqual(log, [{"tier": "log", "index": 1, "version": "29'3", "op": "modify",
                                "object": "3:b47ce0a1:::ec-history-calibration:head", "return_code": 0}])
        extra = history.locate(parsed, history.parse_reqid("client.4190.5:11"))
        self.assertEqual([(e["tier"], e["index"], e["extra_index"], e["user_version"]) for e in extra], [("extra_reqids", 2, 0, 3)])
        dup = history.locate(parsed, history.parse_reqid("client.4199.2:6"))
        self.assertEqual(dup, [{"tier": "dups", "index": 0, "version": "29'1"}])

    def test_every_wrong_identity_field_misses(self):
        # Mutation check: each field of the full key matters, and a TID or an
        # entity alone does not match.
        parsed = history.parse_log(captured())
        for wrong in ("osd.4171.7:2", "client.4172.7:2", "client.4171.8:2", "client.4171.-7:2", "client.4171.7:3",
                      "client.4171.0:2", "client.?.7:2", "client.4190.5:12", "client.4190.6:11", "client.4199.2:7",
                      "client.4198.2:6", "client.4199.3:6", "osd.4199.2:6", "client.4191.5:11", "osd.4190.5:11"):
            with self.subTest(wrong=wrong):
                self.assertEqual(history.locate(parsed, history.parse_reqid(wrong)), [])

    def test_extra_reqids_and_dups_are_searched_separately(self):
        parsed = history.parse_log(captured())
        parsed_without_extra = history.parse_log(dump([
            ENTRY % ("29'2", "29'1", SEED, ""), ENTRY % ("29'3", "29'2", KNOWN, ""),
            ENTRY % ("29'4", "29'3", "client.4180.3:9", "")], dups=[DUP % ("client.4199.2:6", "29'1", 1)]))
        identity = history.parse_reqid("client.4190.5:11")
        self.assertEqual(len(history.locate(parsed, identity)), 1)
        self.assertEqual(history.locate(parsed_without_extra, identity), [])
        no_dups = history.parse_log(dump([ENTRY % ("29'2", "29'1", SEED, ""), ENTRY % ("29'3", "29'2", KNOWN, ""),
                                          ENTRY % ("29'4", "29'3", "client.4180.3:9", "")]))
        self.assertEqual(history.locate(no_dups, history.parse_reqid("client.4199.2:6")), [])

    def test_identity_text_formats(self):
        self.assertEqual(history.parse_reqid("client.?.0:5"), ("client", None, 0, 5))
        self.assertEqual(history.parse_reqid("unknown.0.0:0"), ("unknown", 0, 0, 0))
        self.assertEqual(history.parse_reqid("client.12.-1:18446744073709551615"), ("client", 12, -1, (1 << 64) - 1))
        self.assertEqual(history.reqid_text(("client", 4171, 7, 2)), KNOWN)
        for bad in ("client.4171.7", "client.4171:2", "client.04171.7:2", "client.4171.7:2 ", "pool.1.0:1",
                    "client.4171.2147483648:1", "client.4171.7:18446744073709551616", 7, None):
            with self.subTest(bad=bad), self.assertRaises(history.HistoryError):
                history.parse_reqid(bad)

    def test_truncated_reply_fails_closed(self):
        raw = captured()
        for cut in (1, 2, 10, len(raw) // 2, len(raw) - 1):
            with self.subTest(cut=cut), self.assertRaises(history.HistoryError):
                history.parse_log(raw[:-cut])

    def test_incomplete_or_reshaped_documents_fail_closed(self):
        entries = [ENTRY % ("29'2", "29'1", SEED, ""), ENTRY % ("29'3", "29'2", KNOWN, ""),
                   ENTRY % ("29'4", "29'3", "client.4180.3:9", "")]
        cases = {
            "valid JSON that stops before head": dump(entries[:2]),
            "version not increasing": dump([entries[1], entries[0], entries[2]]),
            "entry at the tail": dump([ENTRY % ("29'1", "29'0", SEED, "")] + entries[1:]),
            "dup newer than tail": dump(entries, dups=[DUP % ("client.4199.2:6", "29'2", 1)]),
            "missing dups": b'{"pg_log_t":{"head":"29\'1","tail":"29\'1","log":[]}}',
            "extra section": b'{"pg_log_t":{"head":"29\'1","tail":"29\'1","log":[],"dups":[],"more":1}}',
            "named outer section": b'{"op_log":{"pg_log_t":{"head":"29\'1","tail":"29\'1","log":[],"dups":[]}}}',
            "duplicate key": b'{"pg_log_t":{"head":"29\'1","head":"29\'1","tail":"29\'1","log":[],"dups":[]}}',
            "tail after head": b'{"pg_log_t":{"head":"29\'1","tail":"29\'2","log":[],"dups":[]}}',
            "unparsable reqid": dump(entries[:2] + [ENTRY % ("29'4", "29'3", "client4180.3:9", "")]),
            "unparsable extra reqid": dump(entries[:2] + [ENTRY % ("29'4", "29'3", "client.4180.3:9", EXTRA % ("c.1.1:1", 1))]),
            "entry missing extra_reqids": dump(entries[:2] + [entries[2].replace('"extra_reqids":[],', "")]),
            "unknown entry field": dump(entries[:2] + [entries[2].replace('"op":', '"surprise":1,"op":')]),
            "two documents": captured() + b"{}",
            "not UTF-8": b"\xff" + captured(),
            "empty": b"",
        }
        for name, raw in cases.items():
            with self.subTest(name), self.assertRaises(history.HistoryError):
                history.parse_log(raw)
        with self.assertRaises(history.HistoryError):
            history.parse_log(b" " * (history.MAX_LOG_BYTES + 1))
        # An empty log is complete only when head equals tail.
        self.assertEqual(history.parse_log(b'{"pg_log_t":{"head":"0\'0","tail":"0\'0","log":[],"dups":[]}}')["log"], [])


class SnapshotTest(unittest.TestCase):
    targets = [target(KNOWN), target("client.4171.8:2")]

    def test_complete_snapshot_reports_positions_and_raw_bytes(self):
        raw = captured()
        receipt = history.snapshot(dict(PLACEMENT), dict(PLACEMENT), 0, raw, "", self.targets)
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["bytes"], receipt["sha256"]), (len(raw), hashlib.sha256(raw).hexdigest()))
        self.assertEqual(base64.b64decode(receipt["raw"]), raw)
        self.assertEqual((receipt["head"], receipt["tail"], receipt["log_entries"], receipt["dup_entries"], receipt["extra_reqid_entries"]),
                         ("29'4", "29'1", 3, 1, 1))
        self.assertEqual([len(t["locations"]) for t in receipt["targets"]], [1, 0])
        self.assertEqual(receipt["targets"][0]["reqid"], KNOWN)
        self.assertTrue(receipt["primary_only"])
        self.assertFalse(receipt["absence_established"])

    def test_moved_placement_between_brackets_fails_closed(self):
        for field, value in (("epoch", 30), ("primary_id", 3), ("osd_id", 3), ("acting", [3, 2, 0, 1]),
                             ("pgid", "3.2"), ("pg_shard", 2), ("address", "v2:192.0.2.11:6800/1")):
            with self.subTest(field=field):
                after = dict(PLACEMENT, **{field: value})
                receipt = history.snapshot(dict(PLACEMENT), after, 0, captured(), "", self.targets)
                self.assertFalse(receipt["complete"])
                self.assertIn("placement or epoch changed between the before and after brackets", receipt["failures"])
                # The located identity is still reported, never as a pass.
                self.assertEqual(len(receipt["targets"][0]["locations"]), 1)

    def test_truncated_refused_and_oversized_replies_fail_closed(self):
        truncated = history.snapshot(PLACEMENT, PLACEMENT, 0, captured()[:-3], "", self.targets)
        self.assertFalse(truncated["complete"])
        self.assertNotIn("targets", truncated)
        refused = history.snapshot(PLACEMENT, PLACEMENT, -11, b"", "not primary for pgid 3.1", self.targets)
        self.assertFalse(refused["complete"])
        self.assertIn("-11", refused["failures"][0])
        self.assertNotIn("targets", refused)
        oversized = history.snapshot(PLACEMENT, PLACEMENT, 0, b"", "", self.targets, oversized_length=history.MAX_LOG_BYTES + 1)
        self.assertFalse(oversized["complete"])
        self.assertEqual(oversized["bytes"], history.MAX_LOG_BYTES + 1)
        self.assertNotIn("raw", oversized)

    def test_targets_are_validated(self):
        for bad in ([], [target(KNOWN)] * 9, [{"type": "client", "num": 1, "inc": 0}],
                    [{"type": "unknown", "num": 1, "inc": 0, "tid": 1}], [{"type": "client", "num": -1, "inc": 0, "tid": 1}],
                    [{"type": "client", "num": 1, "inc": 1 << 31, "tid": 1}], [{"type": "client", "num": 1, "inc": 0, "tid": True}]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                history.snapshot(PLACEMENT, PLACEMENT, 0, captured(), "", bad)


class FakeHistoryRados:
    def __init__(self, code, raw):
        self.code, self.raw, self.commands, self.lib = code, raw, [], None

    def dump(self, cluster):
        return {"pools": [{"pool_name": "ceph-msgr-owned-pool-1", "pool": 3}]}

    def osd(self, cluster, osd, command, limit):
        self.commands.append((osd, command, limit))
        return self.code, self.raw, "", None


class ModeTest(unittest.TestCase):
    def request(self, **extra):
        return dict({"slot": 1, "object": "ec-history-calibration", "targets": [target(KNOWN)]}, **extra)

    def test_brackets_the_primary_log_command(self):
        rados = FakeHistoryRados(0, captured())
        calls = []

        def place(command, pool, name, pool_id, namespace=""):
            calls.append((pool, name, pool_id, namespace))
            return dict(PLACEMENT)

        with patch.object(oracle.mutation, "collect_placement", side_effect=place):
            result = oracle.mode_pghistory(rados, None, self.request(), None)["history"]
        self.assertEqual(len(calls), 2)
        self.assertEqual(rados.commands, [(0, {"prefix": "pg", "pgid": "3.1", "cmd": "log", "format": "json"}, history.MAX_LOG_BYTES)])
        self.assertTrue(result["complete"])
        self.assertEqual((result["osd"], result["pgid"]), (0, "3.1"))

    def test_a_primary_change_during_the_command_is_reported_incomplete(self):
        rados = FakeHistoryRados(0, captured())
        observations = iter([dict(PLACEMENT), dict(PLACEMENT, epoch=31, primary_id=3, osd_id=3)])
        with patch.object(oracle.mutation, "collect_placement", side_effect=lambda *a, **k: next(observations)):
            result = oracle.mode_pghistory(rados, None, self.request(), None)["history"]
        self.assertFalse(result["complete"])

    def test_invalid_requests_are_refused_before_native_contact(self):
        rados = FakeHistoryRados(0, captured())
        for request in (self.request(targets=[]), self.request(extra=1), self.request(targets=[{"type": "client"}])):
            with self.subTest(request=request), self.assertRaises(ValueError):
                oracle.mode_pghistory(rados, None, request, None)
        self.assertEqual(rados.commands, [])


if __name__ == "__main__":
    unittest.main()
