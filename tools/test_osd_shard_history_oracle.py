import base64
import copy
import hashlib
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pg_history_oracle as history
import osd_pool_oracle as oracle
import osd_shard_history_oracle as shard_history

# Synthetic, compact JSON in the field order of the pinned offline reader
# (src/tools/ceph_objectstore_tool.cc:439-480: pg_log_t, then pg_missing_t)
# and of pg_info_t::dump. Identities, objects and versions are invented; no
# daemon produced these bytes.
ENTRY = ('{"op":"modify","object":"%s","version":"%s","prior_version":"0\'0","reqid":"%s","extra_reqids":[%s],'
         '"mtime":"0.000000","return_code":0,"op_returns":[{"rval":0,"bl_length":0}],"written_shards":"{}",'
         '"mod_desc":{"object_mod_desc":{"can_local_rollback":true,"rollback_info_completed":true,"ops":[{"code":"CREATE"}]}},'
         '"clean_regions":{"object_clean_regions":{"clean_offsets":"[41~18446744073709551574]","clean_omap":true,"new_object":false}}}')
EXTRA = '{"reqid":"%s","user_version":"%d"}'
DUP = '{"reqid":"%s","version":"%s","user_version":"%d","return_code":"0"}'
MISSING = '{"missing":[%s],"may_include_deletes":true}'
COPY = "client.4722.9:2"
SOURCE = "client.4722.7:1"
OBJECT = "4:53c99ff5:::ec-copy-history-destination:head"
PGID = "4.2"
ACTING = [1, 3, 2, 0]
PLACEMENT = {"epoch": 28, "pgid": PGID, "primary_id": 1, "osd_id": 1, "acting": ACTING, "up": ACTING, "pg_shard": 0,
             "pg_temp": False, "address": "v2:192.0.2.11:6800/1"}


def log_dump(entries, dups=(), head="28'3", tail="0'0", missing=""):
    return ('{"pg_log_t":{"head":"%s","tail":"%s","log":[%s],"dups":[%s]},"pg_missing_t":%s}'
            % (head, tail, ",".join(entries), ",".join(dups), MISSING % missing)).encode()


def copied(extra=True):
    """A probe entry pair and the copy's entry, with or without the source identity."""
    return log_dump([
        ENTRY % ("4:50ecdf02:::probe:head", "28'1", "client.14617.0:1", ""),
        ENTRY % ("4:50ecdf02:::probe:head", "28'2", "client.14617.0:2", ""),
        ENTRY % (OBJECT, "28'3", COPY, EXTRA % (SOURCE, 1) if extra else ""),
    ])


def info_dump(spg, last_update="28'3", last_complete="28'3", log_tail="0'0", partial=(), partial_epoch="0"):
    return json.dumps({"pgid": spg, "shard": spg.rpartition("s")[2] if "s" in spg else "255", "last_update": last_update, "last_complete": last_complete, "log_tail": log_tail,
                       "last_user_version": 1, "last_backfill": "MAX",
                       "partial_writes_last_complete": [{"id": shard, "from": start, "to": end} for shard, start, end in partial],
                       "partial_writes_last_complete_epoch": partial_epoch, "purged_snaps": [],
                       "history": {"epoch_created": 26, "same_interval_since": 27}, "stats": {"version": last_update},
                       "empty": 0, "dne": 0, "incomplete": 0, "last_epoch_started": 27,
                       "hit_set_history": {"current_last_update": "0'0", "history": []}}, separators=(",", ":")).encode()


def target(text):
    kind, num, inc, tid = history.parse_reqid(text)
    return {"type": kind, "num": num, "inc": inc, "tid": tid}


class Capture:
    """One synthetic capture: the stored bracket, the record and the dump files."""

    def __init__(self, logs=None):
        self.before = json.dumps(PLACEMENT).encode()
        self.after = copy.deepcopy(PLACEMENT)
        self.files = {}
        shards = []
        for index, osd in enumerate(ACTING):
            spg = "%ss%d" % (PGID, index)
            info, log = info_dump(spg), (logs or {}).get(index, copied())
            names = ("osd-shard-history-s%d-info.json" % index, "osd-shard-history-s%d-log.json" % index)
            self.files[names[0]], self.files[names[1]] = info, log
            shards.append({"index": index, "osd": osd, "spg": spg, "reader_isolated": True, "paused": True, "resumed": True,
                           "paused_ms": 800, "store_archive_bytes": 1073762304, "whoami": str(osd), "unsupported_layout": "",
                           "info_exit_code": 0, "info_bytes": len(info), "info_stderr": "", "info_file": names[0],
                           "log_exit_code": 0, "log_bytes": len(log), "log_stderr": "", "log_file": names[1],
                           "reader_removed": True})
        self.record = {"schema_version": 1, "source_commit": shard_history.SOURCE_COMMIT,
                       "before_sha256": hashlib.sha256(self.before).hexdigest(), "pgid": PGID, "acting": list(ACTING),
                       "shards": shards, "complete": True}
        self.targets = [target(COPY), target(SOURCE), target("client.4722.7:2")]

    def set_file(self, index, op, data, recorded=True):
        self.files["osd-shard-history-s%d-%s.json" % (index, op)] = data
        if recorded:
            self.record["shards"][index][op + "_bytes"] = len(data)

    def dumps(self, name, limit):
        data = self.files[name]
        if len(data) > limit:
            raise ValueError("%s exceeds %d bytes" % (name, limit))
        return data

    def snapshot(self, nonprimary=()):
        return shard_history.snapshot(self.before, self.after, json.dumps(self.record).encode(), self.dumps, self.targets, nonprimary)


class SnapshotTest(unittest.TestCase):
    def test_reports_both_linked_identities_on_every_acting_shard(self):
        capture = Capture()
        receipt = capture.snapshot()
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["pgid"], receipt["acting"], receipt["all_acting_shards"], receipt["absence_established"]),
                         (PGID, ACTING, True, False))
        self.assertEqual(receipt["before"], receipt["after"])
        self.assertEqual([(s["index"], s["osd"], s["spg"], s["whoami"]) for s in receipt["shards"]],
                         [(0, 1, "4.2s0", "1"), (1, 3, "4.2s1", "3"), (2, 2, "4.2s2", "2"), (3, 0, "4.2s3", "0")])
        for shard in receipt["shards"]:
            raw = base64.b64decode(shard["raw"])
            self.assertEqual((raw, shard["bytes"], shard["sha256"]), (copied(), len(copied()), hashlib.sha256(copied()).hexdigest()))
            self.assertEqual((shard["head"], shard["tail"], shard["last_update"], shard["last_complete"]), ("28'3", "0'0", "28'3", "28'3"))
            self.assertEqual((shard["log_entries"], shard["extra_reqid_entries"], shard["dup_entries"], shard["missing_entries"]), (3, 1, 0, 0))
            parent, extra, miss = [t["locations"] for t in shard["targets"]]
            self.assertEqual(parent, [{"tier": "log", "index": 2, "version": "28'3", "op": "modify", "object": OBJECT, "return_code": 0}])
            self.assertEqual(extra, [{"tier": "extra_reqids", "index": 2, "extra_index": 0, "version": "28'3", "op": "modify",
                                      "object": OBJECT, "user_version": 1}])
            self.assertEqual(miss, [])
            self.assertEqual([t["reqid"] for t in shard["targets"]], [COPY, SOURCE, "client.4722.7:2"])

    def test_a_shard_without_the_entry_or_without_the_extra_is_complete_and_distinct(self):
        # A shard the primary left out, and one whose entry lacks the nested
        # identity: both are complete observations, neither an absence claim.
        skipped = log_dump([ENTRY % ("4:50ecdf02:::probe:head", "28'1", "client.14617.0:1", ""),
                            ENTRY % ("4:50ecdf02:::probe:head", "28'2", "client.14617.0:2", "")], head="28'2")
        capture = Capture(logs={1: skipped, 2: copied(extra=False)})
        capture.set_file(1, "info", info_dump("4.2s1", last_update="28'2", last_complete="28'2"))
        receipt = capture.snapshot()
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertFalse(receipt["absence_established"])
        positions = [[len(t["locations"]) for t in shard["targets"]] for shard in receipt["shards"]]
        self.assertEqual(positions, [[1, 1, 0], [0, 0, 0], [1, 0, 0], [1, 1, 0]])
        self.assertEqual([s["head"] for s in receipt["shards"]], ["28'3", "28'2", "28'3", "28'3"])

    def test_trimmed_identities_are_found_among_the_duplicates(self):
        trimmed = log_dump([ENTRY % (OBJECT, "28'4", "client.4722.9:9", "")], dups=[DUP % (COPY, "28'3", 2), DUP % (SOURCE, "28'3", 1)],
                           head="28'4", tail="28'3")
        capture = Capture(logs={3: trimmed})
        capture.set_file(3, "info", info_dump("4.2s3", last_update="28'4", last_complete="28'4", log_tail="28'3"))
        shard = capture.snapshot()["shards"][3]
        self.assertTrue(shard["complete"], shard["failures"])
        self.assertEqual([t["locations"] for t in shard["targets"][:2]],
                         [[{"tier": "dups", "index": 0, "version": "28'3"}], [{"tier": "dups", "index": 1, "version": "28'3"}]])

    def test_a_partial_overwrite_reports_written_shards_and_partial_write_records(self):
        # One full write named every shard; one overwrite named shards 0, 2
        # and 3. Shard 1 was left out: its own log ends at the full write.
        full = ENTRY % (OBJECT, "28'3", SOURCE, "")
        patch_entry = (ENTRY % (OBJECT, "28'4", COPY, "")).replace('"written_shards":"{}"', '"written_shards":"{0,2,3}"')
        written = log_dump([full, patch_entry], head="28'4")
        capture = Capture(logs={0: written, 1: log_dump([full]), 2: written, 3: written})
        for index in (0, 2, 3):
            capture.set_file(index, "info", info_dump("4.2s%d" % index, last_update="28'4", last_complete="28'4",
                                                      partial=[(1, "28'3", "28'4")], partial_epoch="28"))
        receipt = capture.snapshot()
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertFalse(receipt["absence_established"])
        self.assertEqual([s["head"] for s in receipt["shards"]], ["28'4", "28'3", "28'4", "28'4"])
        entries = [{"version": "28'3", "written_shards": []}, {"version": "28'4", "written_shards": [0, 2, 3]}]
        self.assertEqual([s["entries"] for s in receipt["shards"]], [entries, entries[:1], entries, entries])
        record = [{"shard": 1, "from": "28'3", "to": "28'4"}]
        self.assertEqual([s["partial_writes_last_complete"] for s in receipt["shards"]], [record, [], record, record])
        self.assertEqual([s["partial_writes_last_complete_epoch"] for s in receipt["shards"]], [28, 0, 28, 28])
        self.assertEqual([s["shard"] for s in receipt["shards"]], [0, 1, 2, 3])
        self.assertEqual([[len(t["locations"]) for t in s["targets"][:2]] for s in receipt["shards"]], [[1, 1], [0, 1], [1, 1], [1, 1]])
        for index, shard in enumerate(receipt["shards"]):
            info = base64.b64decode(shard["info_raw"])
            self.assertEqual((info, shard["info_bytes"], shard["info_sha256"]),
                             (capture.files["osd-shard-history-s%d-info.json" % index], len(info), hashlib.sha256(info).hexdigest()))

    def advanced(self, log=None, info=None, others="28'4"):
        """A partial overwrite at 28'4 that left shard 1 out, captured after a
        later peering: shard 1's info and log head are at 28'4, its last entry
        is the full write at 28'3 and its own info holds the range."""
        full = ENTRY % (OBJECT, "28'3", SOURCE, "")
        patch_entry = (ENTRY % (OBJECT, "28'4", COPY, "")).replace('"written_shards":"{}"', '"written_shards":"{0,2,3}"')
        written = log_dump([full, patch_entry], head=others)
        capture = Capture(logs={0: written, 1: log or log_dump([full], head="28'4"), 2: written, 3: written})
        for index in (0, 2, 3):
            capture.set_file(index, "info", info_dump("4.2s%d" % index, last_update=others, last_complete=others,
                                                      partial=[(1, "28'3", "28'4")], partial_epoch="30"))
        capture.set_file(1, "info", info or info_dump("4.2s1", last_update="28'4", last_complete="28'4",
                                                      partial=[(1, "28'3", "28'4")], partial_epoch="30"))
        return capture

    def test_a_nonprimary_shard_whose_head_passed_its_last_entry_is_reported(self):
        receipt = self.advanced().snapshot(nonprimary=[1])
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual(receipt["nonprimary_shards"], [1])
        self.assertEqual([(s["head"], s["last_entry"], s["head_past_entries"], s["last_update"], s["last_complete"], s["log_entries"])
                          for s in receipt["shards"]],
                         [("28'4", "28'4", False, "28'4", "28'4", 2), ("28'4", "28'3", True, "28'4", "28'4", 1),
                          ("28'4", "28'4", False, "28'4", "28'4", 2), ("28'4", "28'4", False, "28'4", "28'4", 2)])
        self.assertEqual(receipt["shards"][1]["entries"], [{"version": "28'3", "written_shards": []}])
        self.assertEqual([len(t["locations"]) for t in receipt["shards"][1]["targets"][:2]], [0, 1])
        self.assertEqual(receipt["shards"][1]["partial_writes_last_complete"], [{"shard": 1, "from": "28'3", "to": "28'4"}])
        # A record that starts before the last entry spans it as well.
        earlier = self.advanced(info=info_dump("4.2s1", last_update="28'4", last_complete="28'4", partial=[(1, "28'2", "28'4")], partial_epoch="30"))
        self.assertTrue(earlier.snapshot(nonprimary=[1])["complete"])
        # An ordinary shard is unchanged by the nonprimary set.
        self.assertTrue(Capture().snapshot(nonprimary=[1])["complete"])
        self.assertFalse(Capture().snapshot(nonprimary=[1])["shards"][1]["head_past_entries"])

    def test_a_head_past_the_last_entry_is_refused_outside_that_one_shape(self):
        stale = "log: the last log entry is not head"
        full = ENTRY % (OBJECT, "28'3", SOURCE, "")
        # The shape is admitted for the pool's nonprimary shards only.
        for name, nonprimary in {"no nonprimary shard": (), "another nonprimary shard": [2], "a primary-capable data shard": [0]}.items():
            with self.subTest(name=name):
                receipt = self.advanced().snapshot(nonprimary=nonprimary)
                self.assertFalse(receipt["complete"])
                self.assertEqual(receipt["failures"], ["shard 1 (osd.3): " + stale + "; the log array is incomplete"])
                self.assertNotIn("head", receipt["shards"][1])
        # The same shape on the primary's shard or on a parity shard.
        for index in (0, 2, 3):
            with self.subTest(index=index):
                capture = self.advanced()
                capture.set_file(index, "log", log_dump([full], head="28'4"))
                receipt = capture.snapshot(nonprimary=[1])
                self.assertEqual(receipt["failures"], ["shard %d (osd.%d): %s; the log array is incomplete" % (index, ACTING[index], stale)])

        def info(partial, last_update="28'4", last_complete="28'4"):
            return info_dump("4.2s1", last_update=last_update, last_complete=last_complete, partial=partial, partial_epoch="30")
        cases = {
            "no record at all": (None, info([]), "without a partial-write record of this shard"),
            "a record of another shard only": (None, info([(2, "28'3", "28'4")]), "without a partial-write record of this shard"),
            "record ends before the head": (None, info([(1, "28'2", "28'3")]), "partial-write record does not span them"),
            "record ends after the head": (None, info([(1, "28'3", "28'5")]), "partial-write record does not span them"),
            "record starts after the last entry": (log_dump([ENTRY % (OBJECT, "28'2", SOURCE, "")], head="28'4"), info([(1, "28'3", "28'4")]),
                                                   "partial-write record does not span them"),
            "record is the empty range at the head": (None, info([(1, "28'4", "28'4")]), "partial-write record does not span them"),
            "record of a later epoch": (None, info([(1, "28'3", "29'4")]), "partial-write record does not span them"),
            "no entry at all": (log_dump([], head="28'4"), info([(1, "0'0", "28'4")]), "it holds no entry"),
            "an entry newer than the head": (log_dump([full, ENTRY % (OBJECT, "28'5", COPY, "")], head="28'4"), None, stale),
            "entries out of order": (log_dump([full, ENTRY % (OBJECT, "28'3", COPY, "")], head="28'4"), None, "version does not increase"),
            "info behind the log head": (None, info([(1, "28'3", "28'4")], last_update="28'3", last_complete="28'3"),
                                         "the log does not run from the info's log_tail to its last_update"),
            "last_complete behind": (None, info([(1, "28'3", "28'4")], last_complete="28'3"), "the shard's last_complete is behind its last_update"),
            "a missing object": (log_dump([full], head="28'4", missing='{"object":"x"}'), None, "the shard's missing set has 1 entries"),
            "unusable info": (None, info_dump("4.2s0"), "the shard's info is unusable"),
        }
        for name, (log, changed, text) in cases.items():
            with self.subTest(name=name):
                receipt = self.advanced(log=log, info=changed).snapshot(nonprimary=[1])
                self.assertFalse(receipt["complete"] or receipt["absence_established"])
                self.assertTrue(any(failure.startswith("shard 1 (osd.3): ") and text in failure for failure in receipt["failures"]), receipt["failures"])
                self.assertEqual([s["complete"] for s in receipt["shards"]], [True, False, True, True])
        for nonprimary in ([-1], [128], ["1"], [1.0], [True]):
            with self.subTest(nonprimary=nonprimary), self.assertRaises(ValueError):
                self.advanced().snapshot(nonprimary=nonprimary)

    def test_unparsable_written_shards_or_partial_write_records_fail_closed(self):
        entry = ENTRY % (OBJECT, "28'3", COPY, "")
        for name, text in {"unterminated": '"{0,2,3"', "descending": '"{2,0}"', "repeated": '"{0,0}"', "beyond the set": '"{128}"',
                           "bare list": '"0,2,3"', "spaced": '"{0, 2}"', "leading zero": '"{00}"', "trailing comma": '"{0,}"',
                           "an array": "[0,2,3]", "a number": "0", "signed": '"{-1}"', "four digits": '"{1000}"',
                           "trailing text": '"{0,2}x"', "leading text": '"x{0,2}"', "an object": "{}", "null": "null"}.items():
            with self.subTest(written_shards=name):
                capture = Capture()
                capture.set_file(1, "log", log_dump([entry.replace('"written_shards":"{}"', '"written_shards":' + text)]))
                self.refused(capture, "shard 1 (osd.3): log: log entry 0", shard=1)
        capture = Capture()
        capture.set_file(1, "log", log_dump([entry.replace('"written_shards":"{}",', "")]))
        self.refused(capture, "shard 1 (osd.3): log: log entry 0 has an unparsable written_shards None", shard=1)
        capture = Capture()
        capture.set_file(1, "log", log_dump([entry.replace('"written_shards":"{}"', '"written_shards":"{0,127}"')]))
        self.assertEqual(capture.snapshot()["shards"][1]["entries"], [{"version": "28'3", "written_shards": [0, 127]}])

        def edited(change):
            document = json.loads(info_dump("4.2s1", partial=[(1, "28'2", "28'3"), (3, "28'3", "28'3")], partial_epoch="28"))
            change(document)
            return json.dumps(document, separators=(",", ":")).encode()

        def record(change):
            return edited(lambda d: change(d["partial_writes_last_complete"][0]))

        capture = Capture()
        capture.set_file(1, "info", edited(lambda d: None))
        shard = capture.snapshot()["shards"][1]
        self.assertTrue(shard["complete"], shard["failures"])
        self.assertEqual((shard["partial_writes_last_complete"], shard["partial_writes_last_complete_epoch"]),
                         ([{"shard": 1, "from": "28'2", "to": "28'3"}, {"shard": 3, "from": "28'3", "to": "28'3"}], 28))
        cases = {
            "no shard": edited(lambda d: d.pop("shard")),
            "numeric shard": edited(lambda d: d.update(shard=1)),
            "shard beyond the set": edited(lambda d: d.update(shard="128")),
            "shard with a sign": edited(lambda d: d.update(shard="+1")),
            "no shard of a replicated PG": edited(lambda d: d.update(shard="255")),
            "no array": edited(lambda d: d.pop("partial_writes_last_complete")),
            "no epoch": edited(lambda d: d.pop("partial_writes_last_complete_epoch")),
            "array is an object": edited(lambda d: d.update(partial_writes_last_complete={})),
            "numeric epoch": edited(lambda d: d.update(partial_writes_last_complete_epoch=28)),
            "signed epoch": edited(lambda d: d.update(partial_writes_last_complete_epoch="-1")),
            "oversized epoch": edited(lambda d: d.update(partial_writes_last_complete_epoch="1" * 11)),
            "record shard beyond the set": edited(lambda d: d["partial_writes_last_complete"][1].update(id=128)),
            "record is a list": edited(lambda d: d["partial_writes_last_complete"].insert(0, [1, "28'2", "28'3"])),
            "record without to": record(lambda r: r.pop("to")),
            "record with another key": record(lambda r: r.update(epoch=28)),
            "shard as text": record(lambda r: r.update(id="1")),
            "shard as a boolean": record(lambda r: r.update(id=True)),
            "negative shard": record(lambda r: r.update(id=-1)),
            "shard repeated": edited(lambda d: d["partial_writes_last_complete"][1].update(id=1)),
            "shards out of order": record(lambda r: r.update(id=4)),
            "unparsable from": record(lambda r: r.update({"from": "28.2"})),
            "unparsable to": record(lambda r: r.update(to=None)),
            "range ends before it starts": record(lambda r: r.update({"from": "28'4"})),
        }
        for name, data in cases.items():
            with self.subTest(partial_write=name):
                capture = Capture()
                capture.set_file(1, "info", data)
                receipt = self.refused(capture, "shard 1 (osd.3): info: ", shard=1)
                self.assertNotIn("partial_writes_last_complete", receipt["shards"][1])
        # The store returned another shard's collection under the expected name.
        capture = Capture()
        capture.set_file(1, "info", edited(lambda d: d.update(shard="0")))
        receipt = self.refused(capture, "shard 1 (osd.3): the copied collection is shard 0, not acting index 1", shard=1)
        self.assertEqual(receipt["shards"][1]["shard"], 0)

    def refused(self, capture, text, shard=None):
        receipt = capture.snapshot()
        self.assertFalse(receipt["complete"])
        self.assertFalse(receipt["absence_established"])
        self.assertTrue(any(text in failure for failure in receipt["failures"]), receipt["failures"])
        if shard is not None:
            self.assertFalse(receipt["shards"][shard]["complete"])
        return receipt

    def test_moved_placement_fails_closed(self):
        for name, change in {
            "epoch": {"epoch": 29}, "primary": {"primary_id": 3, "osd_id": 3}, "acting order": {"acting": [1, 2, 3, 0]},
            "up": {"up": [1, 3, 2, 4]}, "shard": {"pg_shard": 2}, "pg_temp": {"pg_temp": True},
            "address": {"address": "v2:192.0.2.11:6801/9"}, "pgid": {"pgid": "4.3"}, "new field": {"extra": 1},
        }.items():
            with self.subTest(name=name):
                capture = Capture()
                capture.after.update(change)
                self.refused(capture, "placement or epoch changed between the before and after brackets")
        capture = Capture()
        del capture.after["pg_temp"]
        self.refused(capture, "placement or epoch changed")

    def test_capture_under_another_bracket_or_of_another_pg_fails_closed(self):
        cases = {
            "another first bracket": (lambda c: c.record.update(before_sha256=hashlib.sha256(b"{}").hexdigest()), "not taken under this first bracket"),
            "no bracket digest": (lambda c: c.record.pop("before_sha256"), "not taken under this first bracket"),
            "incomplete capture": (lambda c: c.record.update(complete=False), "the capture is incomplete"),
            "another PG": (lambda c: c.record.update(pgid="4.3"), "the capture is for '4.3'"),
            "another acting order": (lambda c: c.record.update(acting=[3, 1, 2, 0]), "the capture is for '4.2' acting [3, 1, 2, 0]"),
            "a shard short": (lambda c: c.record["shards"].pop(), "the capture has 3 shards for 4 acting members"),
            "a shard too many": (lambda c: c.record["shards"].append(dict(c.record["shards"][0])), "the capture has 5 shards for 4 acting members"),
            "wrong schema": (lambda c: c.record.update(schema_version=2), "unexpected shape or provenance"),
            "another source": (lambda c: c.record.update(source_commit="0" * 40), "unexpected shape or provenance"),
            "shards not a list": (lambda c: c.record.update(shards={}), "unexpected shape or provenance"),
        }
        for name, (change, text) in cases.items():
            with self.subTest(name=name):
                capture = Capture()
                change(capture)
                self.refused(capture, text)

    def test_unusable_brackets_fail_closed(self):
        for name, before, text in (
            ("truncated", json.dumps(PLACEMENT).encode()[:-3], "bracket or capture record"),
            ("empty", b"", "bracket or capture record"),
            ("duplicate key", b'{"pgid":"4.2","pgid":"4.3","acting":[1,3,2,0]}', "bracket or capture record"),
            ("not a placement", b'{"pgid":4,"acting":[1,3,2,0]}', "not a placement record"),
            ("no acting", b'{"pgid":"4.2"}', "not a placement record"),
            ("oversized", b'{"pgid":"4.2","acting":[1,3,2,0],"pad":"' + b"x" * shard_history.MAX_RECORD_BYTES + b'"}', "bracket or capture record"),
        ):
            with self.subTest(name=name):
                capture = Capture()
                capture.before = before
                capture.record["before_sha256"] = hashlib.sha256(before).hexdigest()
                receipt = self.refused(capture, text)
                self.assertEqual(receipt["shards"], [])
        for acting in ([1], [1, 1, 2, 0], [1, "3", 2, 0], [1, 3, 2, -1], list(range(9))):
            with self.subTest(acting=acting):
                capture = Capture()
                placement = dict(PLACEMENT, acting=acting)
                capture.before, capture.after = json.dumps(placement).encode(), placement
                capture.record.update(before_sha256=hashlib.sha256(capture.before).hexdigest(), acting=acting)
                self.refused(capture, "the acting vector is not between 2 and 8 distinct OSDs")
        capture = Capture()
        with patch.object(Capture, "dumps", side_effect=AssertionError("no dump may be read")):
            receipt = shard_history.snapshot(capture.before, capture.after, b'{"schema_version":1', capture.dumps, capture.targets)
        self.assertFalse(receipt["complete"])

    def test_unattributed_or_refused_shard_copies_are_not_read(self):
        cases = {
            "another OSD's store": ({"whoami": "2"}, "the copy's whoami is '2', not osd.3"),
            "no whoami": ({"whoami": ""}, "the copy's whoami is '', not osd.3"),
            "whoami with a newline": ({"whoami": "3\n"}, "the copy's whoami is '3\\n', not osd.3"),
            "wrong shard collection": ({"spg": "4.2s0"}, "capture is for index 1, osd 3, '4.2s0'"),
            "another PG's collection": ({"spg": "4.3s1"}, "capture is for index 1, osd 3, '4.3s1'"),
            "wrong index": ({"index": 2}, "capture is for index 2, osd 3"),
            "wrong OSD": ({"osd": 2}, "capture is for index 1, osd 2"),
            "reader had a network": ({"reader_isolated": False}, "the reader was not isolated from every network"),
            "never paused": ({"paused": False}, "the OSD was not paused for the copy"),
            "not resumed": ({"resumed": False}, "the OSD was not resumed"),
            "copy not removed": ({"reader_removed": False}, "the reader and its copy were not removed"),
            "truthy flag": ({"resumed": 1}, "the OSD was not resumed"),
            "separate DB device": ({"unsupported_layout": "block.db"}, "unsupported block layout: block.db"),
            "info refused": ({"info_exit_code": 1}, "offline info exited 1"),
            "log refused": ({"log_exit_code": 1}, "offline log exited 1"),
            "log never ran": ({"log_exit_code": None}, "offline log exited None"),
            "exit code true": ({"log_exit_code": False}, "offline log exited False"),
            "decode warning": ({"log_stderr": "bluestore fsck warning: legacy statfs"}, "offline log wrote diagnostics: bluestore fsck warning"),
            "info warning": ({"info_stderr": "did not load config file"}, "offline info wrote diagnostics: did not load config file"),
            "recorded failure": ({"failure": "copy the paused OSD's data directory: context deadline exceeded"}, "capture failed: copy the paused"),
        }
        for name, (change, text) in cases.items():
            with self.subTest(name=name):
                capture = Capture()
                capture.record["shards"][1].update(change)
                with patch.object(Capture, "dumps", side_effect=lambda name, limit, c=capture: self.only_other_shards(c, name, limit)):
                    receipt = self.refused(capture, "shard 1 (osd.3): " + text, shard=1)
                self.assertNotIn("raw", receipt["shards"][1])
                self.assertNotIn("targets", receipt["shards"][1])
                # The other shards are still judged on their own.
                self.assertEqual([s["complete"] for s in receipt["shards"]], [True, False, True, True])
        for name, change in {"missing field": lambda r: r.pop("resumed"), "unknown field": lambda r: r.update(surprise=1), "not an object": None}.items():
            with self.subTest(name=name):
                capture = Capture()
                if change is None:
                    capture.record["shards"][1] = "4.2s1"
                else:
                    change(capture.record["shards"][1])
                self.refused(capture, "shard 1 (osd.3): capture record has an unexpected shape", shard=1)

    def test_a_dump_file_of_another_shard_or_operation_is_not_read(self):
        # Shard 1's own log holds the copy without the source identity. A
        # record that names shard 0's log file must not attribute shard 0's
        # nested identity to shard 1: the log dump carries no shard of its own.
        other = "client.4722.5:9"
        assert len(other) == len(SOURCE)
        capture = Capture(logs={1: copied().replace(SOURCE.encode(), other.encode())})
        own = capture.snapshot()["shards"][1]
        self.assertTrue(own["complete"], own["failures"])
        self.assertEqual([len(t["locations"]) for t in own["targets"]], [1, 0, 0])
        cases = {
            "another shard's log": ("log_file", "osd-shard-history-s0-log.json"),
            "another shard's info": ("info_file", "osd-shard-history-s0-info.json"),
            "this shard's info as its log": ("log_file", "osd-shard-history-s1-info.json"),
            "this shard's log as its info": ("info_file", "osd-shard-history-s1-log.json"),
            "a shard beyond the acting vector": ("log_file", "osd-shard-history-s7-log.json"),
            "no name": ("log_file", ""),
            "not a name": ("info_file", None),
        }
        for name, (field, value) in cases.items():
            with self.subTest(name=name):
                capture = Capture(logs={1: copied().replace(SOURCE.encode(), other.encode())})
                record = capture.record["shards"][1]
                record[field] = value
                if isinstance(value, str) and value in capture.files:
                    # Same recorded length, so only the name tells them apart.
                    record[field[:-5] + "_bytes"] = len(capture.files[value])
                with patch.object(Capture, "dumps", side_effect=lambda name, limit, c=capture: self.only_other_shards(c, name, limit)):
                    receipt = self.refused(capture, "shard 1 (osd.3): the capture names", shard=1)
                self.assertNotIn("raw", receipt["shards"][1])
                self.assertNotIn("targets", receipt["shards"][1])
                self.assertEqual([s["complete"] for s in receipt["shards"]], [True, False, True, True])

    @staticmethod
    def only_other_shards(capture, name, limit):
        if "-s1-" in name:
            raise AssertionError("a refused shard's dump was read")
        return capture.files[name]

    def test_truncated_reshaped_or_inconsistent_dumps_fail_closed(self):
        raw = copied()
        cases = {
            "truncated log": ("log", raw[:-9], "log: native output is not one complete JSON document"),
            "log cut in the missing set": ("log", raw[:-1], "log: native output is not one complete JSON document"),
            "empty log": ("log", b"", "log: native output is empty"),
            "live primary shape": ("log", raw[:raw.index(b',"pg_missing_t"')] + b"}", "log: offline output is not a pg_log_t and pg_missing_t dump"),
            "named outer section": ("log", b'{"op_log":' + raw + b"}", "log: offline output is not a pg_log_t and pg_missing_t dump"),
            "extra section": ("log", raw[:-1] + b',"more":1}', "log: offline output is not a pg_log_t and pg_missing_t dump"),
            "two documents": ("log", raw + raw, "log: native output is not one complete JSON document"),
            "duplicate section": ("log", raw[:-1] + b',"pg_log_t":{}}', "log: duplicate JSON key"),
            "missing set reshaped": ("log", raw.replace(b'"may_include_deletes":true', b'"may_include_deletes":1'), "log: pg_missing_t must have exactly"),
            "missing set is an object": ("log", raw.replace(b'"missing":[]', b'"missing":{}'), "log: pg_missing_t must have exactly"),
            "missing set without its array": ("log", raw.replace(b'"missing":[],', b""), "log: pg_missing_t must have exactly"),
            "log stops before head": ("log", log_dump([ENTRY % (OBJECT, "28'2", COPY, "")]), "log: the last log entry is not head"),
            "interior entry out of order": ("log", log_dump([ENTRY % (OBJECT, "28'3", COPY, ""), ENTRY % (OBJECT, "28'3", SOURCE, "")]),
                                            "log: log entry 1 version does not increase"),
            "unparsable nested identity": ("log", log_dump([ENTRY % (OBJECT, "28'3", COPY, EXTRA % ("client4722.7:1", 1))]), "log: unparsable request identity"),
            "nested identity reshaped": ("log", log_dump([ENTRY % (OBJECT, "28'3", COPY, '"client.4722.7:1"')]), "log: log entry 0 extra_reqid 0 has an unexpected shape"),
            "entry without extra_reqids": ("log", copied().replace(b'"extra_reqids":[],', b"", 1), "log: log entry 0 has an unexpected shape"),
            "dups reshaped": ("log", raw.replace(b'"dups":[]', b'"dups":{}'), "log: log and dups must be arrays"),
            "missing set not empty": ("log", log_dump([ENTRY % (OBJECT, "28'3", COPY, "")], missing='{"object":"x"}'), "the shard's missing set has 1 entries"),
            "oversized log": ("log", raw[:-1] + b" " * shard_history.MAX_SHARD_BYTES + b"}", "offline log dump is unreadable"),
            "truncated info": ("info", info_dump("4.2s1")[:-5], "info: native output is not one complete JSON document"),
            "info of another shard": ("info", info_dump("4.2s0"), "info: offline info is for '4.2s0', not 4.2s1"),
            "info of the bare PG": ("info", info_dump("4.2"), "info: offline info is for '4.2', not 4.2s1"),
            "info of another PG": ("info", info_dump("4.3s1"), "info: offline info is for '4.3s1', not 4.2s1"),
            "info without bounds": ("info", b'{"pgid":"4.2s1"}', "info: offline output is not a pg_info_t dump"),
            "info is the log": ("info", raw, "info: offline output is not a pg_info_t dump"),
            "info bounds unparsable": ("info", info_dump("4.2s1", last_update="28.3"), "info: unparsable info last_update"),
            "log behind the info": ("info", info_dump("4.2s1", last_update="28'4", last_complete="28'4"), "the log does not run from the info's log_tail to its last_update"),
            "log tail differs": ("info", info_dump("4.2s1", log_tail="28'1"), "the log does not run from the info's log_tail to its last_update"),
            "last_complete behind": ("info", info_dump("4.2s1", last_complete="28'2"), "the shard's last_complete is behind its last_update"),
        }
        for name, (op, data, text) in cases.items():
            with self.subTest(name=name):
                capture = Capture()
                capture.set_file(1, op, data)
                receipt = self.refused(capture, "shard 1 (osd.3): " + text, shard=1)
                self.assertEqual([s["complete"] for s in receipt["shards"]], [True, False, True, True])
        # A dump that changed after the capture recorded its length, and a
        # dump that is not there at all.
        capture = Capture()
        capture.set_file(1, "log", raw + b" ", recorded=False)
        self.refused(capture, "offline log dump has %d bytes; the capture recorded %d" % (len(raw) + 1, len(raw)), shard=1)
        capture = Capture()
        del capture.files["osd-shard-history-s1-log.json"]
        with patch.object(Capture, "dumps", side_effect=lambda name, limit: (_ for _ in ()).throw(FileNotFoundError(name))
                          if "-s1-log" in name else capture.files[name]):
            self.refused(capture, "offline log dump is unreadable", shard=1)

    def test_every_wrong_identity_field_misses_on_every_shard(self):
        for wrong in ("osd.4722.9:2", "client.4723.9:2", "client.4722.8:2", "client.4722.9:3", "client.4722.7:2",
                      "client.4722.9:1", "osd.4722.7:1", "client.4721.7:1", "client.4722.6:1", "client.4722.7:1048577"):
            with self.subTest(wrong=wrong):
                capture = Capture()
                capture.targets = [target(wrong)]
                receipt = capture.snapshot()
                self.assertTrue(receipt["complete"])
                self.assertEqual([s["targets"][0]["locations"] for s in receipt["shards"]], [[], [], [], []])

    def test_invalid_targets_are_refused(self):
        capture = Capture()
        for targets in ([], [target(COPY)] * (history.MAX_TARGETS + 1), [{"type": "client"}]):
            capture.targets = targets
            with self.subTest(targets=len(targets)), self.assertRaises(ValueError):
                capture.snapshot()


class FakeRados:
    lib = None
    nonprimary = None

    def dump(self, cluster):
        pool = {"pool_name": "ceph-msgr-owned-pool-2", "pool": 4}
        if self.nonprimary is not None:
            pool["nonprimary_shards"] = self.nonprimary
        return {"pools": [pool]}


class ModeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.out = pathlib.Path(self.directory.name)
        self.capture = Capture()
        self.placements = []

    def place(self, command, pool, name, pool_id, namespace=""):
        self.placements.append((pool, name, pool_id, namespace))
        return copy.deepcopy(self.capture.after)

    def run_mode(self, nonprimary=None, **request):
        request = dict({"slot": 2, "object": "ec-copy-history-destination"}, **request)
        rados = FakeRados()
        rados.nonprimary = nonprimary
        with patch.object(oracle.mutation, "collect_placement", side_effect=self.place):
            return oracle.mode_shardhistory(rados, None, request, self.out)

    def publish_capture(self):
        (self.out / oracle.SHARD_CAPTURE).write_bytes(json.dumps(self.capture.record).encode())
        for name, data in self.capture.files.items():
            (self.out / name).write_bytes(data)

    def test_before_stores_the_bracket_and_after_judges_one_capture(self):
        (self.out / oracle.SHARD_CAPTURE).write_text("stale")
        first = self.run_mode(phase="before")
        self.assertEqual(first, {"placement": PLACEMENT})
        self.assertFalse((self.out / oracle.SHARD_CAPTURE).exists())
        stored = (self.out / oracle.SHARD_BEFORE).read_bytes()
        self.assertEqual(json.loads(stored), PLACEMENT)
        self.capture.before = stored
        self.capture.record["before_sha256"] = hashlib.sha256(stored).hexdigest()
        self.publish_capture()
        receipt = self.run_mode(phase="after", targets=self.capture.targets)["shard_history"]
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual(self.placements, [("ceph-msgr-owned-pool-2", "ec-copy-history-destination", 4, "")] * 2)
        # One bracket admits one judgement.
        self.assertFalse((self.out / oracle.SHARD_BEFORE).exists())
        with self.assertRaises(FileNotFoundError):
            self.run_mode(phase="after", targets=self.capture.targets)

    def test_a_placement_change_during_the_capture_is_reported_incomplete(self):
        self.run_mode(phase="before")
        self.capture.before = (self.out / oracle.SHARD_BEFORE).read_bytes()
        self.capture.record["before_sha256"] = hashlib.sha256(self.capture.before).hexdigest()
        self.publish_capture()
        self.capture.after = dict(PLACEMENT, epoch=29, primary_id=3, osd_id=3)
        receipt = self.run_mode(phase="after", targets=self.capture.targets)["shard_history"]
        self.assertFalse(receipt["complete"])
        self.assertIn("placement or epoch changed between the before and after brackets", receipt["failures"])

    def test_after_without_a_capture_and_invalid_requests_are_refused(self):
        self.run_mode(phase="before")
        with self.assertRaises(FileNotFoundError):
            self.run_mode(phase="after", targets=self.capture.targets)
        placed = len(self.placements)
        for request in ({"phase": "during"}, {"phase": "during", "targets": self.capture.targets}, {}, {"phase": "before", "targets": self.capture.targets}, {"phase": "after"},
                        {"phase": "after", "targets": []}, {"phase": "after", "targets": [{"type": "client"}]},
                        {"phase": "after", "targets": self.capture.targets, "before": PLACEMENT},
                        {"phase": "before", "slot": 3}):
            with self.subTest(request=request), self.assertRaises(ValueError):
                self.run_mode(**request)
        self.assertEqual(len(self.placements), placed)

    def test_only_the_pools_own_nonprimary_shards_admit_a_head_past_the_last_entry(self):
        # The pool's nonprimary_shards dump in its pinned set print form
        # admits the shape for exactly those shards; any other value admits
        # none, and the shard is then refused as before.
        for nonprimary, complete in (("{1}", True), ("{1,2}", True), ("{}", False), ("{2}", False), (None, False),
                                     ([1], False), ("1", False), ("{1", False), (1, False)):
            with self.subTest(nonprimary=nonprimary):
                self.capture = SnapshotTest().advanced()
                self.run_mode(phase="before")
                self.capture.before = (self.out / oracle.SHARD_BEFORE).read_bytes()
                self.capture.record["before_sha256"] = hashlib.sha256(self.capture.before).hexdigest()
                self.publish_capture()
                receipt = self.run_mode(nonprimary=nonprimary, phase="after", targets=self.capture.targets)["shard_history"]
                self.assertEqual(receipt["complete"], complete, receipt["failures"])
                if not complete:
                    self.assertEqual(receipt["failures"], ["shard 1 (osd.3): log: the last log entry is not head; the log array is incomplete"])

    def test_only_capture_dump_names_are_read(self):
        self.run_mode(phase="before")
        self.capture.before = (self.out / oracle.SHARD_BEFORE).read_bytes()
        self.capture.record["before_sha256"] = hashlib.sha256(self.capture.before).hexdigest()
        self.capture.record["shards"][0]["log_file"] = "../key"
        self.publish_capture()
        receipt = self.run_mode(phase="after", targets=self.capture.targets)["shard_history"]
        self.assertFalse(receipt["complete"])
        # The judgement refuses the name before the mode's own name filter
        # would; nothing outside the control directory's dumps is opened.
        self.assertEqual(receipt["failures"], ["shard 0 (osd.1): the capture names '../key' as this shard's log dump"])
        self.assertNotIn("raw", receipt["shards"][0])


if __name__ == "__main__":
    unittest.main()
