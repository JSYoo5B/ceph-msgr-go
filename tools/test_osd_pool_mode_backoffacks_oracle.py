import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_backoffacks_oracle as backoffacks


def op(description, client="client.4721", events=("initiated", "queued_for_pg", "reached_pg", "done")):
    return {"description": description, "type_data": {"client_info": {"client": client, "tid": 0},
                                                      "events": [{"event": e} for e in events]}}


# Synthetic op history laid out like the pinned OpTracker dump; no daemon
# produced it.
PG_WIDE = "osd_backoff(3.7s2 ack-block id 1 [3:e0000000::::0,3:10000000::::head) e31)"
OBJECT = "osd_backoff(3.7s0 ack-block id 4 [3:e1b8d6c3:::ec-object:head,3:e1b8d6c3:::ec-object:head) e35)"


class BackoffAcks(unittest.TestCase):
    def test_pg_wide_and_object_acks_of_one_pg(self):
        found = backoffacks.acks({"ops": [op(PG_WIDE), op(OBJECT, events=("initiated", "done")),
                                          op("osd_op(client.4721.0:7 3.7s0 ...)"),
                                          op("osd_backoff(3.5s0 ack-block id 2 [MIN,MAX) e31)"),
                                          op("osd_backoff(3.7s2 block id 1 [3:e0000000::::0,3:10000000::::head) e31)")]}, "3.7")
        self.assertEqual(found, [
            {"pgid": "3.7", "shard": 2, "id": 1, "begin": "3:e0000000::::0", "end": "3:10000000::::head", "map_epoch": 31,
             "client": "client.4721", "reached_pg": True, "done": True, "events": ["initiated", "queued_for_pg", "reached_pg", "done"]},
            {"pgid": "3.7", "shard": 0, "id": 4, "begin": "3:e1b8d6c3:::ec-object:head", "end": "3:e1b8d6c3:::ec-object:head",
             "map_epoch": 35, "client": "client.4721", "reached_pg": False, "done": True, "events": ["initiated", "done"]}])

    def test_replicated_pg_has_no_shard(self):
        found = backoffacks.acks({"ops": [op("osd_backoff(2.0 ack-block id 9 [MIN,MAX) e5)")]}, "2.0")
        self.assertEqual((found[0]["shard"], found[0]["id"], found[0]["begin"], found[0]["end"]), (-1, 9, "MIN", "MAX"))

    def test_unexpected_shapes(self):
        self.assertEqual(backoffacks.acks({"ops": [{"description": 3}, "x", op(PG_WIDE + " trailing")]}, "3.7"), [])
        for bad in ({}, {"ops": {}}, [], {"ops": [{}] * (backoffacks.MAX_OPS + 1)}):
            with self.assertRaises(ValueError):
                backoffacks.acks(bad, "3.7")

    def test_incomplete_history_of_a_matched_ack_is_refused(self):
        # A negative reached_pg is evidence only from a whole, terminated
        # event list; each truncated or malformed shape must fail instead.
        def raw(events):
            return {"description": PG_WIDE, "type_data": {"client_info": {"client": "client.4721"}, "events": events}}
        over = [{"event": "queued_for_pg"}] * backoffacks.MAX_EVENTS + [{"event": "done"}]
        for name, entry in {
            "no event list": {"description": PG_WIDE, "type_data": {"client_info": {"client": "client.4721"}}},
            "no type_data": {"description": PG_WIDE},
            "event list not a list": raw({"event": "done"}),
            "empty event list": raw([]),
            "over the event bound": raw(over),
            "malformed event": raw([{"event": "initiated"}, "queued_for_pg", {"event": "done"}]),
            "event without a name": raw([{"event": "initiated"}, {}, {"event": "done"}]),
            "truncated before done": raw([{"event": "initiated"}, {"event": "queued_for_pg"}]),
            "done not last": raw([{"event": "done"}, {"event": "queued_for_pg"}]),
        }.items():
            with self.subTest(name), self.assertRaises(ValueError):
                backoffacks.acks({"ops": [entry]}, "3.7")
        # The bound itself is accepted, and an unmatched op is not inspected.
        exact = [{"event": "queued_for_pg"}] * (backoffacks.MAX_EVENTS - 1) + [{"event": "done"}]
        self.assertEqual(len(backoffacks.acks({"ops": [raw(exact)]}, "3.7")[0]["events"]), backoffacks.MAX_EVENTS)
        self.assertEqual(backoffacks.acks({"ops": [{"description": "osd_op(client.1.0:1 3.7s0 ...)"}]}, "3.7"), [])

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["backoffacks"], backoffacks.mode_backoffacks)
        with self.assertRaises(ValueError):
            backoffacks.mode_backoffacks(None, None, {"slot": 1, "object": "o", "pgid": "3.7"}, None)


if __name__ == "__main__":
    unittest.main()
