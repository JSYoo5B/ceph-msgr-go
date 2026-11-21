import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_subwrites_oracle as subwrites


def op(description, events=("initiated", "queued_for_pg", "reached_pg", "sub_op_started", "sub_op_committed", "done")):
    return {"description": description, "type_data": {"flag_point": "commit sent; apply or cleanup",
                                                      "events": [{"event": e, "time": "t", "duration": 0} for e in events]}}


# Synthetic op history laid out like the pinned OpTracker dump and
# MOSDECSubOpWrite print; no daemon produced it.
REQID = "client.4171.91:5"
WRITE = "MOSDECSubOpWrite(3.6s2 31/29 ECSubWrite(tid=12, reqid=client.4171.91:5, at_version=31'4, trim_to=0'0, pg_committed_to=31'3))"


class SubWrites(unittest.TestCase):
    def test_committed_sub_write_of_one_request(self):
        found = subwrites.sub_writes({"size": 20, "duration": 600, "ops": [
            op(WRITE),
            op(WRITE.replace("client.4171.91:5", "client.4171.91:6")),
            op(WRITE.replace("3.6s2", "3.5s2")),
            op("osd_op(client.4171.91:5 3.6s0 ...)"),
            op("MOSDECSubOpWriteReply(3.6s0 31/29 ECSubWriteReply(tid=12, last_complete=31'4, committed=1, applied=1))"),
        ]}, "3.6", REQID)
        self.assertEqual(found, [{"pgid": "3.6", "shard": 2, "map_epoch": 31, "min_epoch": 29, "tid": 12, "reqid": REQID,
                                  "at_version": "31'4", "trim_to": "0'0", "pg_committed_to": "31'3", "backfill_or_async_recovery": False, "committed": True, "done": True,
                                  "events": ["initiated", "queued_for_pg", "reached_pg", "sub_op_started", "sub_op_committed", "done"]}])

    def test_roll_forward_sub_write(self):
        nop = "MOSDECSubOpWrite(3.6s2 31/29 ECSubWrite(tid=13, reqid=client.4171.91:5, at_version=0'0, trim_to=0'0, pg_committed_to=31'4))"
        found = subwrites.sub_writes({"ops": [op(WRITE), op(nop)]}, "3.6", REQID)
        self.assertEqual([(f["tid"], f["at_version"], f["pg_committed_to"]) for f in found], [(12, "31'4", "31'3"), (13, "0'0", "31'4")])

    def test_flags_and_uncommitted(self):
        flagged = WRITE.replace("pg_committed_to=31'3)", "pg_committed_to=31'3, has_updated_hit_set_history, backfill_or_async_recovery)")
        found = subwrites.sub_writes({"ops": [op(flagged, events=("initiated", "sub_op_started", "done")),
                                              op(WRITE, events=("initiated", "sub_op_committed", "sub_op_started", "done"))]}, "3.6", REQID)
        self.assertEqual([(f["backfill_or_async_recovery"], f["committed"]) for f in found], [(True, False), (False, False)])

    def test_unparsable_sub_write_of_the_request_is_refused(self):
        with self.assertRaises(ValueError):
            subwrites.sub_writes({"ops": [op(WRITE.replace("at_version=31'4", "at_version=?"))]}, "3.6", REQID)
        # Another request's unparsable sub-write is not inspected.
        self.assertEqual(subwrites.sub_writes({"ops": [op(WRITE.replace("client.4171.91:5", "client.1.0:1").replace("tid=12", "tid=x"))]}, "3.6", REQID), [])

    def test_incomplete_history_of_a_matched_sub_write_is_refused(self):
        def raw(events):
            return {"description": WRITE, "type_data": {"events": events}}
        over = [{"event": "queued_for_pg"}] * subwrites.MAX_EVENTS + [{"event": "done"}]
        for name, entry in {
            "no event list": {"description": WRITE, "type_data": {}},
            "no type_data": {"description": WRITE},
            "empty event list": raw([]),
            "over the event bound": raw(over),
            "malformed event": raw([{"event": "initiated"}, "sub_op_started", {"event": "done"}]),
            "truncated before done": raw([{"event": "sub_op_started"}, {"event": "sub_op_committed"}]),
        }.items():
            with self.subTest(name), self.assertRaises(ValueError):
                subwrites.sub_writes({"ops": [entry]}, "3.6", REQID)

    def test_unexpected_shapes(self):
        self.assertEqual(subwrites.sub_writes({"ops": [{"description": 3}, "x", op(WRITE + " trailing")]}, "3.6", "client.1.0:1"), [])
        for bad in ({}, {"ops": {}}, [], {"ops": [{}] * (subwrites.MAX_OPS + 1)}):
            with self.assertRaises(ValueError):
                subwrites.sub_writes(bad, "3.6", REQID)

    def test_history_retention(self):
        first = {"description": WRITE, "initiated_at": "2026-11-21T01:02:03.000004+0000"}
        second = {"description": "osd_op(client.1.0:1 3.6s0 ...)", "initiated_at": "2026-11-21T01:02:03.000005+0000"}
        found = subwrites.history_retention({"size": 20, "duration": 600, "ops": [first, second]})
        self.assertEqual((found["size"], found["duration"], len(found["ops"])), (20, 600, 2))
        self.assertEqual(len(set(found["ops"])), 2)
        self.assertEqual(found["ops"], subwrites.history_retention({"size": 20, "duration": 600, "ops": [first, second]})["ops"])
        moved = dict(first, initiated_at="2026-11-21T01:02:03.000006+0000")
        self.assertNotEqual(subwrites.history_retention({"size": 20, "duration": 600, "ops": [moved]})["ops"][0], found["ops"][0])
        for bad in ({"ops": []}, {"size": "20", "duration": 600, "ops": []}, {"size": 20, "duration": -1, "ops": []},
                    {"size": 20, "duration": 600, "ops": [{"description": WRITE}]}, {"size": 20, "duration": 600, "ops": [{"initiated_at": "t"}]}):
            with self.subTest(bad), self.assertRaises(ValueError):
                subwrites.history_retention(bad)

    def test_mode_is_registered_and_needs_an_explicit_osd(self):
        self.assertIs(osd_pool_oracle.HANDLERS["subwrites"], subwrites.mode_subwrites)
        reqid = {"type": "client", "num": 4171, "inc": 91, "tid": 5}
        for request in ({"slot": 1, "object": "o", "reqid": reqid}, {"slot": 1, "object": "o", "osd": 2, "reqid": reqid, "pgid": "3.6"},
                        {"slot": 1, "object": "o", "osd": 2, "reqid": {"type": "client", "num": 4171, "inc": 91}}):
            with self.assertRaises(ValueError):
                subwrites.mode_subwrites(None, None, request, None)


if __name__ == "__main__":
    unittest.main()
