import copy
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pg_interval_oracle as interval
import osd_pool_oracle as oracle

# Synthetic replies in the field order of the pinned pg query
# (src/osd/PeeringState.cc:4559-4606, src/osd/osd_types.cc:3579-3600 and
# :3693-3738). Epochs, versions and addresses are invented; no daemon
# produced these bytes.
PGID = "3.7"
ACTING = [3, 2, 1, 0]
ORIGIN = {"up": ACTING, "acting": ACTING, "replicas": 4, "min_size": 3, "pool_id": 3, "object": "ec-partial-write-history",
          "osd_id": 3, "primary_id": 3, "epoch": 25, "pg_shard": 0, "pg_temp": False, "nonprimary_shard_count": 1, "pool_type": 3,
          "hash": 4057545319, "namespace": "", "raw_pgid": "3.f1d93a67", "pgid": PGID, "pg_seed": 7, "address": "v2:192.0.2.7:6800/1"}
SETTLED = dict(ORIGIN, epoch=28)
RECORD = [{"id": 1, "from": "25'1", "to": "25'2"}]


def info(spg, last_update="25'2", records=RECORD, records_epoch="25", interval_since=24, started=25):
    return {"pgid": spg, "shard": spg.rpartition("s")[2], "last_update": last_update, "last_complete": last_update, "log_tail": "0'0",
            "last_user_version": 2, "last_backfill": "MAX", "partial_writes_last_complete": records,
            "partial_writes_last_complete_epoch": records_epoch, "purged_snaps": [],
            "history": {"epoch_created": 22, "last_epoch_started": started, "same_up_since": interval_since,
                        "same_interval_since": interval_since, "same_primary_since": 24},
            "stats": {"version": last_update}, "empty": 0, "dne": 0, "incomplete": 0, "last_epoch_started": started}


def query(state="active+clean", epoch=25, up=ACTING, acting=ACTING, interval_since=24, started=25, primary="3.7s0", change=None):
    document = {"snap_trimq": "[]", "snap_trimq_len": 0, "state": state, "epoch": epoch, "up": up, "acting": acting,
                "acting_recovery_backfill": ["0(3)", "1(2)", "2(1)", "3(0)"],
                "info": info(primary, interval_since=interval_since, started=started),
                "peer_info": [dict(info("3.7s%d" % shard, records=[], records_epoch="0", interval_since=interval_since, started=started),
                                   peer="%d(%d)" % (ACTING[shard], shard)) for shard in (3, 2, 1)],
                "recovery_state": [{"name": "Started/Primary/Active"}], "agent_state": {}}
    if change:
        change(document)
    return json.dumps(document, separators=(",", ":")).encode()


def seen(placement=SETTLED, raw=None, code=0, second=None, oversized=None):
    raw = query(epoch=28, interval_since=27, started=28) if raw is None else raw
    return interval.observation(copy.deepcopy(placement), code, raw, "", oversized, copy.deepcopy(placement if second is None else second))


ORIGIN_QUERY = interval.parse_query(query())
# The PG without the member of acting position 1: its interval started at
# epoch 26, one epoch before the placement record.
HOLED = [3, interval.NONE, 1, 0]
ABSENT = dict(ORIGIN, epoch=27, up=HOLED, acting=HOLED)


def absent_query(state="active+undersized+degraded", epoch=27, up=HOLED, acting=HOLED, interval_since=26, started=27, **fields):
    return query(state=state, epoch=epoch, up=up, acting=acting, interval_since=interval_since, started=started, **fields)


def absent(placement=ABSENT, raw=None, code=0, second=None, oversized=None, **fields):
    return seen(placement=placement, raw=absent_query(**fields) if raw is None else raw, code=code, second=second, oversized=oversized)


# The PG without its primary's member: position 0 is empty and the first
# parity shard's OSD, osd.1 on shard 2, leads under a pg_temp. Its interval
# started at epoch 27, one epoch before the placement record.
NO_PRIMARY = [interval.NONE, 2, 1, 0]
REPLACED = dict(ORIGIN, epoch=28, up=NO_PRIMARY, acting=NO_PRIMARY, primary_id=1, osd_id=1, pg_shard=2, pg_temp=True, address="v2:192.0.2.8:6800/3")


def replaced_query(state="active+undersized+degraded", epoch=28, up=NO_PRIMARY, acting=NO_PRIMARY, interval_since=27, started=28, primary="3.7s2", **fields):
    return query(state=state, epoch=epoch, up=up, acting=acting, interval_since=interval_since, started=started, primary=primary, **fields)


def replaced(placement=REPLACED, raw=None, code=0, second=None, oversized=None, **fields):
    return seen(placement=placement, raw=replaced_query(**fields) if raw is None else raw, code=code, second=second, oversized=oversized)


class ParseTest(unittest.TestCase):
    def test_reports_state_sets_interval_and_every_shard_info(self):
        parsed = interval.parse_query(query())
        self.assertEqual((parsed["state"], parsed["epoch"], parsed["up"], parsed["acting"], parsed["same_interval_since"], parsed["last_epoch_started"]),
                         ("active+clean", 25, ACTING, ACTING, 24, 25))
        self.assertEqual(parsed["info"], {"pgid": "3.7s0", "shard": 0, "last_update": "25'2", "last_complete": "25'2", "log_tail": "0'0",
                                          "partial_writes_last_complete": [{"shard": 1, "from": "25'1", "to": "25'2"}],
                                          "partial_writes_last_complete_epoch": 25})
        self.assertEqual([(p["peer"], p["pgid"], p["shard"], p["partial_writes_last_complete"]) for p in parsed["peers"]],
                         [("0(3)", "3.7s3", 3, []), ("1(2)", "3.7s2", 2, []), ("2(1)", "3.7s1", 1, [])])

    def test_only_the_judged_fields_are_required_and_bounded(self):
        # A reply with exactly the judged keys, and the bounds of its lists
        # and names.
        document = json.loads(query())
        minimal = {key: document[key] for key in ("state", "epoch", "up", "acting", "info", "peer_info")}
        self.assertEqual(interval.parse_query(json.dumps(minimal).encode())["state"], "active+clean")

        def changed(**fields):
            return json.dumps(dict(minimal, **fields)).encode()
        peer = minimal["peer_info"][0]
        self.assertEqual(len(interval.parse_query(changed(up=list(range(64)), acting=[(1 << 31) - 1]))["up"]), 64)
        self.assertEqual(len(interval.parse_query(changed(peer_info=[peer] * 64))["peers"]), 64)
        self.assertEqual(interval.parse_query(changed(state="s" * 200))["state"], "s" * 200)
        self.assertEqual(interval.parse_query(changed(peer_info=[dict(peer, peer="p" * 40)]))["peers"][0]["peer"], "p" * 40)
        for name, data in {"65 up members": changed(up=list(range(65))), "an OSD beyond 31 bits": changed(acting=[1 << 31]),
                           "65 peers": changed(peer_info=[peer] * 65), "a state of 201 characters": changed(state="s" * 201),
                           "a peer name of 41 characters": changed(peer_info=[dict(peer, peer="p" * 41)]),
                           "a pgid of 41 characters": changed(info=dict(minimal["info"], pgid="3." + "7" * 39))}.items():
            with self.subTest(name=name), self.assertRaises(interval.HistoryError):
                interval.parse_query(data)

    def test_truncated_or_reshaped_replies_fail_closed(self):
        raw = query()

        def drop(*path):
            def change(document):
                for key in path[:-1]:
                    document = document[key]
                del document[path[-1]]
            return change

        def put(value, *path):
            def change(document):
                for key in path[:-1]:
                    document = document[key]
                document[path[-1]] = value
            return change
        cases = {
            "truncated": raw[:-7], "empty": b"", "two documents": raw + raw, "an array": b"[" + raw + b"]",
            "duplicate key": raw[:-1] + b',"state":"active+clean"}',
            "oversized": raw[:-1] + b" " * interval.MAX_QUERY_BYTES + b"}",
            "no state": query(change=drop("state")), "state is a number": query(change=put(1, "state")), "empty state": query(change=put("", "state")),
            "no epoch": query(change=drop("epoch")), "epoch zero": query(change=put(0, "epoch")), "epoch is a string": query(change=put("25", "epoch")),
            "epoch is a boolean": query(change=put(True, "epoch")), "epoch beyond 32 bits": query(change=put(1 << 32, "epoch")),
            "no up": query(change=drop("up")), "up is empty": query(change=put([], "up")), "up holds a string": query(change=put(["3"], "up")),
            "acting holds a negative": query(change=put([-1], "acting")), "acting is an object": query(change=put({}, "acting")),
            "no info": query(change=drop("info")), "info is an array": query(change=put([], "info")),
            "info without history": query(change=drop("info", "history")), "history without the interval": query(change=drop("info", "history", "same_interval_since")),
            "interval is a string": query(change=put("24", "info", "history", "same_interval_since")),
            "history without the start": query(change=drop("info", "history", "last_epoch_started")),
            "info without a pgid": query(change=drop("info", "pgid")), "info without a shard": query(change=drop("info", "shard")),
            "info shard unparsable": query(change=put("x", "info", "shard")),
            "info last_update unparsable": query(change=put("25.2", "info", "last_update")),
            "info without partial writes": query(change=drop("info", "partial_writes_last_complete")),
            "info record reshaped": query(change=put([{"id": 1, "from": "25'1"}], "info", "partial_writes_last_complete")),
            "no peer_info": query(change=drop("peer_info")), "peer_info is an object": query(change=put({}, "peer_info")),
            "peer without a name": query(change=drop("peer_info", 0, "peer")), "peer is a string": query(change=put("0(3)", "peer_info", 0)),
            "peer without bounds": query(change=drop("peer_info", 2, "last_update")),
        }
        for name, data in cases.items():
            with self.subTest(name=name), self.assertRaises(interval.HistoryError):
                interval.parse_query(data)


class JudgementTest(unittest.TestCase):
    def test_the_original_placement_in_a_newer_clean_interval_is_settled(self):
        self.assertEqual(interval.unsettled(ORIGIN, ORIGIN_QUERY, seen()), [])
        # The interval may start at the bracketing epoch itself, and the
        # primary may already be at a newer map than the placement record.
        self.assertEqual(interval.unsettled(ORIGIN, ORIGIN_QUERY, seen(raw=query(epoch=28, interval_since=28, started=28))), [])
        self.assertEqual(interval.unsettled(ORIGIN, ORIGIN_QUERY, seen(raw=query(epoch=29, interval_since=27, started=28))), [])
        newer = interval.observation(ORIGIN, 0, query(epoch=26), "", None, copy.deepcopy(ORIGIN))
        self.assertEqual(interval.origin_failures(ORIGIN, newer), [])

    def test_every_other_observation_is_unsettled(self):
        later = dict(epoch=28, interval_since=27, started=28)
        cases = {
            "no first placement": (seen(placement="no placement: invalid native placement acting set"), "no placement record"),
            "no second placement": (interval.observation(SETTLED, 0, query(**later), "", None, "no placement: deadline"), "no placement record"),
            "epoch moved during the observation": (seen(second=dict(SETTLED, epoch=29)), "placement or epoch changed during the observation"),
            "the same epoch": (seen(placement=ORIGIN), "the map epoch 25 is not newer than 25"),
            "an older epoch": (seen(placement=dict(ORIGIN, epoch=24)), "the map epoch 24 is not newer than 25"),
            "a pg_temp": (seen(placement=dict(SETTLED, pg_temp=True)), "the PG has a pg_temp"),
            "no pg_temp field": (seen(placement={k: v for k, v in SETTLED.items() if k != "pg_temp"}), "the PG has a pg_temp"),
            "acting of the primary alone": (seen(placement=dict(SETTLED, acting=[3])), "up and acting sets differ"),
            "another acting order": (seen(placement=dict(SETTLED, up=[3, 1, 2, 0], acting=[3, 1, 2, 0])), "placement acting is [3, 1, 2, 0]"),
            "another primary": (seen(placement=dict(SETTLED, primary_id=1, osd_id=1, pg_shard=2)), "placement primary_id is 1"),
            "another shard": (seen(placement=dict(SETTLED, pg_shard=2)), "placement pg_shard is 2"),
            "another PG": (seen(placement=dict(SETTLED, pgid="3.6")), "placement pgid is '3.6'"),
            "another address": (seen(placement=dict(SETTLED, address="v2:192.0.2.7:6801/2")), "placement address is"),
            "another pool": (seen(placement=dict(SETTLED, pool_id=4)), "placement pool_id is 4"),
            "query refused": (seen(code=-11), "pg query returned -11"),
            "query oversized": (seen(oversized=interval.MAX_QUERY_BYTES + 1), "exceeds the bound"),
            "query truncated": (seen(raw=query(**later)[:-3]), "not one complete JSON document"),
            "peering": (seen(raw=query(state="peering", **later)), "the primary reports peering"),
            "remapped": (seen(raw=query(state="active+clean+remapped", **later)), "the primary reports active+clean+remapped"),
            "scrubbing": (seen(raw=query(state="active+clean+scrubbing", **later)), "the primary reports active+clean+scrubbing"),
            "degraded": (seen(raw=query(state="active+undersized+degraded", **later)), "the primary reports active+undersized+degraded"),
            "primary's acting differs": (seen(raw=query(acting=[3, 2147483647, 2147483647, 2147483647], **later)), "the primary's up"),
            "primary's up differs": (seen(raw=query(up=[3, 2, 1], **later)), "the primary's up"),
            "answered for another shard": (seen(raw=query(primary="3.7s2", **later)), "the query was answered for 3.7s2"),
            "answered with another shard ID": (seen(raw=query(change=lambda document: document["info"].update(shard="2"), **later)),
                                               "the query was answered for 3.7s0"),
            "answered for another PG's shard 0": (seen(raw=query(primary="3.6s0", **later)), "the query was answered for 3.6s0"),
            "the same interval": (seen(raw=query(epoch=28, interval_since=24, started=25)), "the interval still started at epoch 24"),
            "an older interval": (seen(raw=query(epoch=28, interval_since=23, started=25)), "the interval still started at epoch 23"),
            "interval after the placement": (seen(raw=query(epoch=29, interval_since=29, started=29)), "the interval started at epoch 29, after the placement's epoch"),
            "not started in its interval": (seen(raw=query(epoch=28, interval_since=27, started=25)), "the PG has not started in its current interval"),
            "primary behind the placement": (seen(raw=query(epoch=27, interval_since=27, started=27)), "the primary is at map epoch 27, behind the placement's epoch"),
        }
        for name, (observed, text) in cases.items():
            with self.subTest(name=name):
                reasons = interval.unsettled(ORIGIN, ORIGIN_QUERY, observed)
                self.assertTrue(any(text in reason for reason in reasons), reasons)

    def test_only_a_clean_unmapped_erasure_coded_pg_is_a_starting_point(self):
        first = interval.observation(ORIGIN, 0, query(), "", None, copy.deepcopy(ORIGIN))
        self.assertEqual(interval.origin_failures(ORIGIN, first), [])
        cases = {
            "placement moved": (ORIGIN, interval.observation(ORIGIN, 0, query(), "", None, dict(ORIGIN, epoch=26)), "changed while the starting point was read"),
            "no second placement": (ORIGIN, interval.observation(ORIGIN, 0, query(), "", None, "no placement: x"), "changed while the starting point was read"),
            "no first placement": (ORIGIN, interval.observation("no placement: x", 0, query(), "", None, copy.deepcopy(ORIGIN)), "changed while the starting point was read"),
            "first placement moved": (ORIGIN, interval.observation(dict(ORIGIN, epoch=26), 0, query(), "", None, copy.deepcopy(ORIGIN)), "changed while the starting point was read"),
            "a pg_temp": (dict(ORIGIN, pg_temp=True), None, "not an erasure-coded PG without a pg_temp"),
            "up differs": (dict(ORIGIN, up=[3, 2, 1]), None, "not an erasure-coded PG without a pg_temp"),
            "replicated": (dict(ORIGIN, pool_type=1, pg_shard=-1), None, "not an erasure-coded PG without a pg_temp"),
            "no shard": (dict(ORIGIN, pg_shard=-1), None, "the primary has no shard in the acting set"),
            "shard beyond the set": (dict(ORIGIN, pg_shard=4), None, "the primary has no shard in the acting set"),
            "query refused": (ORIGIN, interval.observation(ORIGIN, -11, b"", "busy", None, copy.deepcopy(ORIGIN)), "pg query returned -11: busy"),
            "not clean": (ORIGIN, interval.observation(ORIGIN, 0, query(state="active+clean+scrubbing"), "", None, copy.deepcopy(ORIGIN)), "the primary reports active+clean+scrubbing"),
            "primary's sets differ": (ORIGIN, interval.observation(ORIGIN, 0, query(acting=[3, 2, 1]), "", None, copy.deepcopy(ORIGIN)), "the primary reports active+clean with up"),
            "another shard answered": (ORIGIN, interval.observation(ORIGIN, 0, query(primary="3.7s2"), "", None, copy.deepcopy(ORIGIN)), "the query was answered for 3.7s2"),
            "interval after the placement": (ORIGIN, interval.observation(ORIGIN, 0, query(interval_since=26, started=26), "", None, copy.deepcopy(ORIGIN)), "the interval started after the placement's epoch"),
            "primary behind the placement": (ORIGIN, interval.observation(ORIGIN, 0, query(epoch=24), "", None, copy.deepcopy(ORIGIN)), "the primary is at map epoch 24, behind the placement's epoch"),
        }
        for name, (origin, observed, text) in cases.items():
            with self.subTest(name=name):
                if observed is None:
                    observed = interval.observation(origin, 0, query(), "", None, copy.deepcopy(origin))
                reasons = interval.origin_failures(origin, observed)
                self.assertTrue(any(text in reason for reason in reasons), reasons)

    def test_an_active_pg_without_one_nonprimary_member_is_accepted(self):
        self.assertEqual(interval.absent_failures(ORIGIN, 1, 24, absent()), [])
        # The primary may report the PG before it counted its degraded
        # objects, the interval may start at the bracketing epoch itself and
        # the primary may be at a newer map than the placement record.
        for name, reply in {"undersized only": dict(state="active+undersized"), "another token order": dict(state="degraded+undersized+active"),
                            "interval at the placement's epoch": dict(interval_since=27, started=27), "primary ahead": dict(epoch=28)}.items():
            with self.subTest(name=name):
                self.assertEqual(interval.absent_failures(ORIGIN, 1, 24, absent(**reply)), [])
        # Any other position than the primary's may be the empty one.
        for shard in (2, 3):
            holed = ACTING[:shard] + [interval.NONE] + ACTING[shard + 1:]
            self.assertEqual(interval.absent_failures(ORIGIN, shard, 24, absent(placement=dict(ABSENT, up=holed, acting=holed), up=holed, acting=holed)), [])
        # The recorded interval may have started at the recorded epoch.
        self.assertEqual(interval.absent_failures(ORIGIN, 1, 25, absent()), [])

    def test_every_other_observation_is_not_the_pg_without_that_member(self):
        other = [3, 2, interval.NONE, 0]
        two = [3, interval.NONE, interval.NONE, 0]
        cases = {
            "no first placement": (absent(placement="no placement: invalid native placement acting set"), "no placement record"),
            "no second placement": (interval.observation(ABSENT, 0, absent_query(), "", None, "no placement: deadline"), "no placement record"),
            "epoch moved during the observation": (absent(second=dict(ABSENT, epoch=28)), "placement or epoch changed during the observation"),
            "the member is still acting": (absent(placement=SETTLED), "placement up is [3, 2, 1, 0]"),
            "only the acting set has the hole": (absent(placement=dict(ABSENT, up=ACTING)), "placement up is [3, 2, 1, 0]"),
            "only the up set has the hole": (absent(placement=dict(ABSENT, acting=ACTING)), "placement acting is [3, 2, 1, 0]"),
            "another position is empty": (absent(placement=dict(ABSENT, up=other, acting=other)), "placement up is [3, 2, 2147483647, 0]"),
            "two positions are empty": (absent(placement=dict(ABSENT, up=two, acting=two)), "placement acting is [3, 2147483647, 2147483647, 0]"),
            "another OSD took the position": (absent(placement=dict(ABSENT, up=[3, 4, 1, 0], acting=[3, 4, 1, 0])), "placement acting is [3, 4, 1, 0]"),
            "a shorter set": (absent(placement=dict(ABSENT, up=[3, 1, 0], acting=[3, 1, 0])), "placement up is [3, 1, 0]"),
            "the same epoch": (absent(placement=dict(ABSENT, epoch=25)), "the map epoch 25 is not newer than 25"),
            "a pg_temp": (absent(placement=dict(ABSENT, pg_temp=True)), "the PG has a pg_temp"),
            "another primary": (absent(placement=dict(ABSENT, primary_id=1, osd_id=1, pg_shard=2)), "placement primary_id is 1"),
            "another shard": (absent(placement=dict(ABSENT, pg_shard=2)), "placement pg_shard is 2"),
            "another PG": (absent(placement=dict(ABSENT, pgid="3.6")), "placement pgid is '3.6'"),
            "another address": (absent(placement=dict(ABSENT, address="v2:192.0.2.7:6801/2")), "placement address is"),
            "another min_size": (absent(placement=dict(ABSENT, min_size=2)), "placement min_size is 2"),
            "query refused": (absent(code=-11), "pg query returned -11"),
            "query oversized": (absent(oversized=interval.MAX_QUERY_BYTES + 1), "exceeds the bound"),
            "query truncated": (absent(raw=absent_query()[:-3]), "not one complete JSON document"),
            "clean": (absent(state="active+clean"), "the primary reports active+clean"),
            "peering": (absent(state="peering"), "the primary reports peering"),
            "peered below min_size": (absent(state="undersized+degraded+peered"), "the primary reports undersized+degraded+peered"),
            "recovering": (absent(state="active+recovering+undersized+degraded"), "the primary reports active+recovering+undersized+degraded"),
            "remapped": (absent(state="active+undersized+remapped"), "the primary reports active+undersized+remapped"),
            "active alone": (absent(state="active"), "the primary reports active"),
            "degraded without undersized": (absent(state="active+degraded"), "the primary reports active+degraded"),
            "a repeated token": (absent(state="active+undersized+undersized"), "the primary reports active+undersized+undersized"),
            "primary's acting is complete": (absent(acting=ACTING), "the primary's up"),
            "primary's up is complete": (absent(up=ACTING), "the primary's up"),
            "answered for another shard": (absent(primary="3.7s2"), "the query was answered for 3.7s2"),
            "answered with another shard ID": (absent(change=lambda document: document["info"].update(shard="2")), "the query was answered for 3.7s0"),
            "the recorded interval": (absent(interval_since=24, started=25), "the interval still started at epoch 24"),
            "an older interval": (absent(interval_since=23, started=25), "the interval still started at epoch 23"),
            "interval after the placement": (absent(epoch=28, interval_since=28, started=28), "the interval started at epoch 28, after the placement's epoch"),
            "not started in its interval": (absent(interval_since=26, started=25), "the PG has not started in its current interval"),
            "primary behind the placement": (absent(epoch=26), "the primary is at map epoch 26, behind the placement's epoch"),
        }
        for name, (observed, text) in cases.items():
            with self.subTest(name=name):
                reasons = interval.absent_failures(ORIGIN, 1, 24, observed)
                self.assertTrue(any(text in reason for reason in reasons), reasons)
        # The request itself: only a nonprimary position of a complete
        # erasure-coded placement, with an interval start that is not after it.
        for name, (origin, shard, since, text) in {
                "the primary's position": (ORIGIN, 0, 24, "not an acting position other than the primary's"),
                "a position beyond the set": (ORIGIN, 4, 24, "not an acting position"),
                "a negative position": (ORIGIN, -1, 24, "not an acting position"),
                "a boolean position": (ORIGIN, True, 24, "not an acting position"),
                "a position as text": (ORIGIN, "1", 24, "not an acting position"),
                "an interval after the placement": (ORIGIN, 1, 26, "is not an epoch at or before"),
                "interval zero": (ORIGIN, 1, 0, "is not an epoch at or before"),
                "an interval as text": (ORIGIN, 1, "24", "is not an epoch at or before"),
                "a recorded hole": (dict(ORIGIN, up=ABSENT["up"], acting=ABSENT["acting"]), 2, 24, "not a complete erasure-coded acting set"),
                "recorded up differs": (dict(ORIGIN, up=[3, 2, 0, 1]), 1, 24, "not a complete erasure-coded acting set"),
                "a recorded pg_temp": (dict(ORIGIN, pg_temp=True), 1, 24, "not a complete erasure-coded acting set"),
                "replicated": (dict(ORIGIN, pool_type=1), 1, 24, "not a complete erasure-coded acting set"),
                "no recorded shard": (dict(ORIGIN, pg_shard=-1), 1, 24, "not a complete erasure-coded acting set"),
                "no recorded epoch": ({k: v for k, v in ORIGIN.items() if k != "epoch"}, 1, 24, "not a complete erasure-coded acting set"),
                "no record": (None, 1, 24, "not a complete erasure-coded acting set")}.items():
            with self.subTest(name=name):
                reasons = interval.absent_failures(origin, shard, since, absent())
                self.assertEqual(len(reasons), 1)
                self.assertIn(text, reasons[0])

    def test_an_active_pg_under_the_first_member_that_can_lead_is_accepted(self):
        self.assertEqual(interval.replaced_failures(ORIGIN, 24, [1], replaced()), [])
        for name, reply in {"undersized only": dict(state="active+undersized"), "another token order": dict(state="degraded+undersized+active"),
                            "interval at the placement's epoch": dict(interval_since=28, started=28), "primary ahead": dict(epoch=29)}.items():
            with self.subTest(name=name):
                self.assertEqual(interval.replaced_failures(ORIGIN, 24, [1], replaced(**reply)), [])
        # The recorded interval may have started at the recorded epoch.
        self.assertEqual(interval.replaced_failures(ORIGIN, 25, [1], replaced()), [])
        # Without nonprimary shard 2 the primary-first order is the shard
        # order, and shard 1 leads.
        led = dict(REPLACED, primary_id=2, osd_id=2, pg_shard=1)
        self.assertEqual(interval.replaced_failures(ORIGIN, 24, [2], replaced(placement=led, primary="3.7s1")), [])
        self.assertEqual(interval.primary_first(4, [1]), [0, 2, 3, 1])
        self.assertEqual(interval.primary_first(6, [1, 2, 3]), [0, 4, 5, 1, 2, 3])

    def test_every_other_observation_is_not_the_pg_under_that_primary(self):
        def led(osd, shard, **fields):
            return dict(REPLACED, primary_id=osd, osd_id=osd, pg_shard=shard, **fields)
        cases = {
            "no first placement": (replaced(placement="no placement: native target OSD is not up and in"), "no placement record"),
            "no second placement": (interval.observation(REPLACED, 0, replaced_query(), "", None, "no placement: deadline"), "no placement record"),
            "epoch moved during the observation": (replaced(second=dict(REPLACED, epoch=29)), "placement or epoch changed during the observation"),
            "the left-out shard leads": (replaced(placement=led(2, 1, pg_temp=False), primary="3.7s1", state="unknown"), "placement primary_id is 2"),
            "no pg_temp": (replaced(placement=dict(REPLACED, pg_temp=False)), "the PG has no pg_temp"),
            "the second parity shard leads": (replaced(placement=led(0, 3), primary="3.7s3"), "placement primary_id is 0"),
            "the original primary leads": (replaced(placement=dict(ORIGIN, epoch=28, pg_temp=True)), "placement up is [3, 2, 1, 0]"),
            "the OSD of the lead with another shard": (replaced(placement=dict(REPLACED, pg_shard=3)), "placement pg_shard is 3"),
            "the target is not the primary": (replaced(placement=dict(REPLACED, osd_id=0)), "placement osd_id is 0"),
            "the original address": (replaced(placement=dict(REPLACED, address=ORIGIN["address"])), "is not another OSD's"),
            "no address": (replaced(placement=dict(REPLACED, address="")), "is not another OSD's"),
            "the primary still acting": (replaced(placement=dict(REPLACED, up=ACTING, acting=ACTING)), "placement up is [3, 2, 1, 0]"),
            "only the up set has the hole": (replaced(placement=dict(REPLACED, acting=ACTING)), "placement acting is [3, 2, 1, 0]"),
            "another position is empty": (replaced(placement=dict(REPLACED, up=HOLED, acting=HOLED)), "placement up is [3, 2147483647, 1, 0]"),
            "two positions are empty": (replaced(placement=dict(REPLACED, up=[interval.NONE, 2, interval.NONE, 0], acting=[interval.NONE, 2, interval.NONE, 0])),
                                        "placement acting is [2147483647, 2, 2147483647, 0]"),
            "another OSD took the position": (replaced(placement=dict(REPLACED, up=[4, 2, 1, 0], acting=[4, 2, 1, 0])), "placement acting is [4, 2, 1, 0]"),
            "the same epoch": (replaced(placement=dict(REPLACED, epoch=25)), "the map epoch 25 is not newer than 25"),
            "another PG": (replaced(placement=dict(REPLACED, pgid="3.6")), "placement pgid is '3.6'"),
            "another min_size": (replaced(placement=dict(REPLACED, min_size=2)), "placement min_size is 2"),
            "another nonprimary count": (replaced(placement=dict(REPLACED, nonprimary_shard_count=2)), "placement nonprimary_shard_count is 2"),
            "query refused": (replaced(code=-11), "pg query returned -11"),
            "query oversized": (replaced(oversized=interval.MAX_QUERY_BYTES + 1), "exceeds the bound"),
            "query truncated": (replaced(raw=replaced_query()[:-3]), "not one complete JSON document"),
            "unknown": (replaced(state="unknown"), "the primary reports unknown"),
            "peering": (replaced(state="peering"), "the primary reports peering"),
            "clean": (replaced(state="active+clean"), "the primary reports active+clean"),
            "remapped": (replaced(state="active+undersized+degraded+remapped"), "the primary reports active+undersized+degraded+remapped"),
            "peered below min_size": (replaced(state="undersized+degraded+peered"), "the primary reports undersized+degraded+peered"),
            "degraded without undersized": (replaced(state="active+degraded"), "the primary reports active+degraded"),
            "primary's acting is complete": (replaced(acting=ACTING), "the primary's up"),
            "primary's up is complete": (replaced(up=ACTING), "the primary's up"),
            "answered for the old primary's shard": (replaced(primary="3.7s0"), "the query was answered for 3.7s0"),
            "answered with another shard ID": (replaced(change=lambda document: document["info"].update(shard="3")), "the query was answered for 3.7s2"),
            "the recorded interval": (replaced(interval_since=24, started=25), "the interval still started at epoch 24"),
            "interval after the placement": (replaced(epoch=29, interval_since=29, started=29), "the interval started at epoch 29, after the placement's epoch"),
            "not started in its interval": (replaced(interval_since=27, started=25), "the PG has not started in its current interval"),
            "primary behind the placement": (replaced(epoch=27), "the primary is at map epoch 27, behind the placement's epoch"),
        }
        for name, (observed, text) in cases.items():
            with self.subTest(name=name):
                reasons = interval.replaced_failures(ORIGIN, 24, [1], observed)
                self.assertTrue(any(text in reason for reason in reasons), reasons)
        # The request: a complete placement led by its own primary, the
        # pool's nonprimary shards without the primary's, an interval start
        # that is not after the placement.
        for name, (origin, since, nonprimary, text) in {
                "a recorded hole": (dict(ORIGIN, up=HOLED, acting=HOLED), 24, [1], "not a complete erasure-coded acting set"),
                "a recorded pg_temp": (dict(ORIGIN, pg_temp=True), 24, [1], "not a complete erasure-coded acting set"),
                "replicated": (dict(ORIGIN, pool_type=1), 24, [1], "not a complete erasure-coded acting set"),
                "recorded primary off its shard": (dict(ORIGIN, pg_shard=1), 24, [1], "not a complete erasure-coded acting set"),
                "no recorded address": (dict(ORIGIN, address=None), 24, [1], "not a complete erasure-coded acting set"),
                "no record": (None, 24, [1], "not a complete erasure-coded acting set"),
                "no nonprimary shards": (ORIGIN, 24, [], "are not those of the recorded placement"),
                "unreadable nonprimary shards": (ORIGIN, 24, None, "are not those of the recorded placement"),
                "nonprimary shards as text": (ORIGIN, 24, ["1"], "are not those of the recorded placement"),
                "a repeated nonprimary shard": (ORIGIN, 24, [1, 1], "are not those of the recorded placement"),
                "more than the map counts": (ORIGIN, 24, [1, 2], "are not those of the recorded placement"),
                "the primary's own shard": (dict(ORIGIN, nonprimary_shard_count=1), 24, [0], "are not those of the recorded placement"),
                "a shard beyond the set": (ORIGIN, 24, [4], "are not those of the recorded placement"),
                "an interval after the placement": (ORIGIN, 26, [1], "is not an epoch at or before"),
                "interval zero": (ORIGIN, 0, [1], "is not an epoch at or before"),
                "an interval as text": (ORIGIN, "24", [1], "is not an epoch at or before")}.items():
            with self.subTest(name=name):
                reasons = interval.replaced_failures(origin, since, nonprimary, replaced())
                self.assertEqual(len(reasons), 1)
                self.assertIn(text, reasons[0])
        # Only nonprimary shards left: nobody can lead.
        three = dict(ORIGIN, nonprimary_shard_count=3)
        self.assertEqual(interval.replaced_failures(three, 24, [1, 2, 3], replaced(placement=dict(REPLACED, nonprimary_shard_count=3))),
                         ["no member that can lead is left in [2147483647, 2, 1, 0]"])


class FakeRados:
    lib = None
    last_status = ""

    def __init__(self, test):
        self.test, self.sent, self.queries, self.code = test, [], [], 0

    def dump(self, cluster):
        return {"pools": [{"pool_name": "ceph-msgr-owned-pool-2", "pool": 3, "nonprimary_shards": self.test.nonprimary}]}

    def mon(self, cluster, command):
        self.sent.append(command)
        self.last_status = "" if self.code == 0 else "pg currently has no primary"
        return self.code, b""

    def osd(self, cluster, osd, command, limit):
        self.queries.append((osd, command, limit))
        reply = self.test.replies.pop(0) if len(self.test.replies) > 1 else self.test.replies[0]
        return reply if isinstance(reply, tuple) else (0, reply, "", None)


class ModeTest(unittest.TestCase):
    def setUp(self):
        self.now, self.sleeps = 100.0, []
        self.placements, self.replies, self.nonprimary = [ORIGIN], [query()], "{1}"
        self.rados = FakeRados(self)

    def place(self, command, pool, name, pool_id, namespace="", timeout=None):
        self.assertEqual((pool, name, pool_id, namespace, timeout), ("ceph-msgr-owned-pool-2", "ec-partial-write-history", 3, "", 10))
        placement = self.placements.pop(0) if len(self.placements) > 1 else self.placements[0]
        if isinstance(placement, Exception):
            raise placement
        return copy.deepcopy(placement)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def run_mode(self, **request):
        request = dict({"slot": 2, "object": "ec-partial-write-history"}, **request)
        with patch.object(oracle.mutation, "collect_placement", side_effect=self.place):
            return oracle.mode_pginterval(self.rados, None, request, None, clock=lambda: self.now, sleep=self.sleep)["pg_interval"]

    def test_repeer_sends_one_command_and_observes_until_the_pg_settles(self):
        # The starting point takes three placement reads. Then one
        # observation without a routing record, one with the pg_temp of the
        # primary alone, and the settled one.
        transient = dict(ORIGIN, epoch=26, acting=[3], pg_temp=True)
        self.placements = [ORIGIN, ORIGIN, ORIGIN, ValueError("invalid native placement acting set"), ValueError("invalid native placement acting set"),
                           transient, transient, SETTLED]
        self.replies = [query(), (-11, b"", "not primary", None), query(state="peering", epoch=26, interval_since=26, started=25),
                        query(epoch=28, interval_since=27, started=28)]
        receipt = self.run_mode(action="repeer")
        self.assertEqual(self.rados.sent, [{"prefix": "pg repeer", "pgid": PGID}])
        self.assertEqual((receipt["settled"], receipt["failures"], receipt["observations"], receipt["command_sent"], receipt["command_code"]),
                         (True, [], 3, True, 0))
        self.assertEqual(self.sleeps, [oracle.INTERVAL_PAUSE] * 2)
        self.assertEqual((receipt["origin"], receipt["observation"]["before"], receipt["observation"]["after"]), (ORIGIN, SETTLED, SETTLED))
        self.assertEqual((receipt["origin_observation"]["query"]["same_interval_since"], receipt["observation"]["query"]["same_interval_since"]), (24, 27))
        self.assertEqual(self.rados.queries, [(3, {"prefix": "pg", "pgid": PGID, "cmd": "query", "format": "json"}, interval.MAX_QUERY_BYTES)] * 4)

    def test_a_pg_that_is_not_a_clean_starting_point_gets_no_command(self):
        for name, placement, reply in (("scrubbing", ORIGIN, query(state="active+clean+scrubbing")), ("a pg_temp", dict(ORIGIN, pg_temp=True), query()),
                                       ("query refused", ORIGIN, (-11, b"", "busy", None))):
            with self.subTest(name=name):
                self.setUp()
                self.placements, self.replies = [placement], [reply]
                receipt = self.run_mode(action="repeer")
                self.assertEqual((receipt["settled"], receipt["command_sent"], self.rados.sent, receipt["observations"]), (False, False, [], 0))
                self.assertTrue(receipt["failures"])
        # A starting point whose first bracket could not be read gets no
        # command, even when the bracket after the query matches.
        for name, placements in (("no first bracket", [ORIGIN, ValueError("placement unavailable"), ORIGIN, SETTLED]),
                                 ("no second bracket", [ORIGIN, ORIGIN, ValueError("placement unavailable"), SETTLED]),
                                 ("first bracket moved", [ORIGIN, dict(ORIGIN, epoch=26), ORIGIN, SETTLED])):
            with self.subTest(name=name):
                self.setUp()
                self.placements, self.replies = placements, [query(), query(epoch=28, interval_since=27, started=28)]
                receipt = self.run_mode(action="repeer")
                self.assertEqual((receipt["settled"], receipt["command_sent"], self.rados.sent, receipt["observations"]), (False, False, [], 0))
                self.assertTrue(any("changed while the starting point was read" in failure for failure in receipt["failures"]), receipt["failures"])
        self.setUp()
        self.placements = [ValueError("native placement epochs did not converge within read budget")]
        with self.assertRaisesRegex(RuntimeError, "no placement before the command"):
            self.run_mode(action="repeer")
        self.assertEqual(self.rados.sent, [])

    def test_a_refused_command_is_reported_and_not_resent(self):
        self.rados.code = -11
        receipt = self.run_mode(action="repeer")
        self.assertEqual((receipt["settled"], receipt["command_sent"], receipt["command_code"], receipt["observations"]), (False, True, -11, 0))
        self.assertEqual(receipt["failures"], ["pg repeer returned -11: pg currently has no primary"])
        self.assertEqual(len(self.rados.sent), 1)

    def test_a_pg_that_does_not_settle_is_reported_within_the_time_bound(self):
        # The map never moves: every observation repeats the starting point.
        receipt = self.run_mode(action="repeer")
        self.assertEqual((receipt["settled"], receipt["command_sent"], len(self.rados.sent)), (False, True, 1))
        self.assertEqual(receipt["observations"], int(oracle.INTERVAL_SECONDS / oracle.INTERVAL_PAUSE) + 1)
        self.assertIn("the map epoch 25 is not newer than 25", receipt["failures"])
        self.assertIn("the interval still started at epoch 24", receipt["failures"])
        self.assertEqual(self.now, 100.0 + oracle.INTERVAL_SECONDS)

    def test_show_observes_once_without_a_command(self):
        self.placements, self.replies = [SETTLED], [query(epoch=28, interval_since=27, started=28)]
        receipt = self.run_mode(action="show", origin=ORIGIN)
        self.assertEqual((receipt["settled"], receipt["failures"], receipt["observations"], self.rados.sent), (True, [], 1, []))
        self.assertEqual(receipt["observation"]["before"], SETTLED)
        for name, placement, reply, text in (
                ("a pg_temp", dict(SETTLED, pg_temp=True), query(epoch=28), "not an erasure-coded PG without a pg_temp"),
                ("another primary", dict(SETTLED, primary_id=1, osd_id=1, pg_shard=2), query(epoch=28, primary="3.7s2"), "placement primary_id is 1"),
                ("not clean", SETTLED, query(state="active+recovering", epoch=28), "the primary reports active+recovering"),
                ("no placement", ValueError("invalid native placement acting set"), query(), "no placement record")):
            with self.subTest(name=name):
                self.setUp()
                self.placements, self.replies = [placement], [reply]
                receipt = self.run_mode(action="show", origin=ORIGIN)
                self.assertFalse(receipt["settled"])
                self.assertTrue(any(text in failure for failure in receipt["failures"]), receipt["failures"])
                self.assertEqual(self.rados.sent, [])

    def test_absent_observes_once_without_a_command(self):
        self.placements, self.replies = [ABSENT], [absent_query()]
        receipt = self.run_mode(action="absent", origin=ORIGIN, shard=1, since=24)
        self.assertEqual((receipt["action"], receipt["settled"], receipt["failures"], receipt["observations"], self.rados.sent), ("absent", True, [], 1, []))
        self.assertEqual((receipt["observation"]["before"], receipt["observation"]["query"]["same_interval_since"]), (ABSENT, 26))
        self.assertEqual(self.rados.queries, [(3, {"prefix": "pg", "pgid": PGID, "cmd": "query", "format": "json"}, interval.MAX_QUERY_BYTES)])
        for name, placement, reply, request, text in (
                ("the member still acting", ORIGIN, query(), {}, "placement up is [3, 2, 1, 0]"),
                ("clean again", SETTLED, query(epoch=28, interval_since=27, started=28), {}, "the primary reports active+clean"),
                ("no placement", ValueError("native target OSD is not up and in"), absent_query(), {}, "no placement record"),
                ("query refused", ABSENT, (-11, b"", "busy", None), {}, "pg query returned -11: busy"),
                ("another position named", ABSENT, absent_query(), {"shard": 2}, "placement up is [3, 2147483647, 1, 0]"),
                ("the primary's position named", ABSENT, absent_query(), {"shard": 0}, "not an acting position other than the primary's"),
                ("the interval of the observation named", ABSENT, absent_query(), {"since": 26}, "is not an epoch at or before")):
            with self.subTest(name=name):
                self.setUp()
                self.placements, self.replies = [placement], [reply]
                receipt = self.run_mode(**dict({"action": "absent", "origin": ORIGIN, "shard": 1, "since": 24}, **request))
                self.assertFalse(receipt["settled"])
                self.assertTrue(any(text in failure for failure in receipt["failures"]), receipt["failures"])
                self.assertEqual(self.rados.sent, [])

    def test_replaced_observes_once_without_a_command(self):
        self.placements, self.replies = [REPLACED], [replaced_query()]
        receipt = self.run_mode(action="replaced", origin=ORIGIN, since=24)
        self.assertEqual((receipt["action"], receipt["settled"], receipt["failures"], receipt["observations"], self.rados.sent), ("replaced", True, [], 1, []))
        self.assertEqual((receipt["observation"]["before"], receipt["observation"]["query"]["same_interval_since"]), (REPLACED, 27))
        # The query goes to the primary the placement names, not to the
        # recorded one.
        self.assertEqual(self.rados.queries, [(1, {"prefix": "pg", "pgid": PGID, "cmd": "query", "format": "json"}, interval.MAX_QUERY_BYTES)])
        transient = dict(REPLACED, epoch=26, primary_id=2, osd_id=2, pg_shard=1, pg_temp=False)
        for name, placement, reply, nonprimary, request, text in (
                ("the primary still acting", ORIGIN, query(), "{1}", {}, "placement up is [3, 2, 1, 0]"),
                ("the left-out shard leads", transient, replaced_query(state="unknown", epoch=26, interval_since=26, started=25, primary="3.7s1"), "{1}", {},
                 "placement primary_id is 2"),
                ("no placement", ValueError("native target OSD is not up and in"), replaced_query(), "{1}", {}, "no placement record"),
                ("query refused", REPLACED, (-11, b"", "busy", None), "{1}", {}, "pg query returned -11: busy"),
                ("unreadable nonprimary shards", REPLACED, replaced_query(), "1", {}, "are not those of the recorded placement"),
                ("no nonprimary shards", REPLACED, replaced_query(), "{}", {}, "are not those of the recorded placement"),
                ("the interval of the observation named", REPLACED, replaced_query(), "{1}", {"since": 27}, "is not an epoch at or before")):
            with self.subTest(name=name):
                self.setUp()
                self.placements, self.replies, self.nonprimary = [placement], [reply], nonprimary
                receipt = self.run_mode(**dict({"action": "replaced", "origin": ORIGIN, "since": 24}, **request))
                self.assertFalse(receipt["settled"])
                self.assertTrue(any(text in failure for failure in receipt["failures"]), receipt["failures"])
                self.assertEqual(self.rados.sent, [])

    def test_invalid_requests_are_refused_before_native_contact(self):
        for request in ({}, {"action": "out"}, {"action": "repeer", "osd": 1}, {"action": "repeer", "slot": 3}, {"action": "show"},
                        {"action": "show", "origin": {"pgid": PGID}}, {"action": "show", "origin": "3.7"}, {"action": "repeer", "object": ""},
                        {"action": "repeer", "origin": ORIGIN}, {"action": "show", "origin": ORIGIN, "shard": 1},
                        {"action": "show", "origin": ORIGIN, "since": 24}, {"action": "repeer", "shard": 1},
                        {"action": "absent"}, {"action": "absent", "origin": ORIGIN}, {"action": "absent", "origin": ORIGIN, "shard": 1},
                        {"action": "absent", "origin": ORIGIN, "since": 24}, {"action": "absent", "origin": ORIGIN, "shard": "1", "since": 24},
                        {"action": "absent", "origin": ORIGIN, "shard": 1, "since": 24.0},
                        {"action": "absent", "origin": {"pgid": PGID}, "shard": 1, "since": 24},
                        {"action": "absent", "origin": ORIGIN, "shard": 1, "since": 24, "osd": 2},
                        {"action": "replaced"}, {"action": "replaced", "origin": ORIGIN}, {"action": "replaced", "since": 24},
                        {"action": "replaced", "origin": ORIGIN, "since": "24"}, {"action": "replaced", "origin": ORIGIN, "since": 24.0},
                        {"action": "replaced", "origin": {"pgid": PGID}, "since": 24}, {"action": "replaced", "origin": ORIGIN, "since": 24, "shard": 0},
                        {"action": "show", "origin": ORIGIN, "since": 24, "shard": 0}, {"action": "repeer", "since": 24}):
            with self.subTest(request=request), self.assertRaises(ValueError):
                self.run_mode(**request)
        self.assertEqual((self.rados.sent, self.rados.queries), ([], []))


if __name__ == "__main__":
    unittest.main()
