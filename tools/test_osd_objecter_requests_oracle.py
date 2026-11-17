import base64
import hashlib
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_objecter_requests_oracle as requests

# Synthetic, compact (non-pretty) JSON in the field order of the pinned
# Objecter::dump_requests, _dump_ops and op_target_t::dump
# (src/osdc/Objecter.cc:4763-4774, 4808-4844) with the top-level section name
# dropped (src/common/Formatter.cc:207-226). Pools, objects, times and TIDs
# are invented; no daemon produced these bytes.
SOURCE = {"pool": 3, "name": "copy-primary-source"}
PLACEMENT = {"epoch": 31, "pgid": "4.0", "primary_id": 1, "osd_id": 1, "acting": [1, 0, 2], "up": [1, 0, 2],
             "pg_shard": -1, "address": "v2:192.0.2.11:6800/1"}


def op(name="copy-primary-source", locator="@3", paused=1, osd_ops=("copy-get max 8388608 in=35b",), tid=7, attempts=0):
    return {"tid": tid, "pg": "3.0", "osd": 2, "object_id": name, "object_locator": locator, "target_object_id": name,
            "target_object_locator": locator, "paused": paused, "used_replica": 0, "precalc_pgid": 0,
            "last_sent": "1234.5s", "age": 3.25, "attempts": attempts, "snapid": "head", "snap_context": "0=[]",
            "mtime": "1970-01-01T00:00:00.000000+0000", "osd_ops": list(osd_ops)}


def dump(items, **sections):
    document = {"ops": items, "linger_ops": [], "pool_ops": [], "pool_stat_ops": [], "statfs_ops": [], "command_ops": []}
    document.update(sections)
    return json.dumps(document, separators=(",", ":")).encode()


class ParseTest(unittest.TestCase):
    def test_reports_the_ops_of_the_exact_source(self):
        parsed, sections = requests.parse_requests(dump([op(name="other"), op(), op(locator="@3;ns"), op(locator="@30")]))
        self.assertEqual(sections, {"ops": 4, "linger_ops": 0, "pool_ops": 0, "pool_stat_ops": 0, "statfs_ops": 0, "command_ops": 0})
        found = requests.source_ops(parsed, 3, "copy-primary-source")
        self.assertEqual(found, [{"index": 1, "tid": 7, "pg": "3.0", "osd": 2, "paused": True, "attempts": 0,
                                  "target_object_id": "copy-primary-source", "target_object_locator": "@3",
                                  "osd_ops": ["copy-get max 8388608 in=35b"], "copy_get_max": [8388608]}])
        # Near misses: another pool, a namespace and a longer pool number.
        self.assertEqual(requests.source_ops(parsed, 30, "copy-primary-source")[0]["index"], 3)
        self.assertEqual(requests.source_ops(parsed, 4, "copy-primary-source"), [])

    def test_versioned_copy_and_empty_dump(self):
        parsed, _ = requests.parse_requests(dump([op(osd_ops=("assert-version v3", "copy-get max 32768 in=35b out=4096b"), paused=0, attempts=1)]))
        found = requests.source_ops(parsed, 3, "copy-primary-source")
        self.assertEqual((found[0]["paused"], found[0]["attempts"], found[0]["copy_get_max"]), (False, 1, [32768]))
        self.assertEqual(requests.parse_requests(dump([])), ([], dict.fromkeys(requests.SECTIONS, 0)))
        # A copy-get sent once and paused again by a later map keeps its
        # count; the caller tells it from one that was never sent.
        parsed, _ = requests.parse_requests(dump([op(paused=1, attempts=1)]))
        found = requests.source_ops(parsed, 3, "copy-primary-source")
        self.assertEqual((found[0]["paused"], found[0]["attempts"]), (True, 1))

    def test_refuses_untrusted_shapes(self):
        bad = {
            "not JSON": b"{",
            "truncated": dump([op()])[:-3],
            "empty": b"",
            "missing section": json.dumps({"ops": []}).encode(),
            "extra section": dump([], extra=[]),
            "section not an array": dump([], linger_ops={}),
            "duplicate key": b'{"ops":[],"ops":[],"linger_ops":[],"pool_ops":[],"pool_stat_ops":[],"statfs_ops":[],"command_ops":[]}',
            "op without osd_ops": dump([{k: v for k, v in op().items() if k != "osd_ops"}]),
            "op with an extra field": dump([dict(op(), extra=1)]),
            "tid as text": dump([dict(op(), tid="7")]),
            "paused as a boolean": dump([dict(op(), paused=True)]),
            "paused out of range": dump([dict(op(), paused=2)]),
            "negative attempts": dump([dict(op(), attempts=-1)]),
            "object as a number": dump([dict(op(), object_id=1)]),
            "osd_ops not text": dump([dict(op(), osd_ops=[1])]),
            "age as text": dump([dict(op(), age="3")]),
        }
        for name, raw in bad.items():
            with self.subTest(name):
                with self.assertRaises(requests.RequestsError):
                    requests.parse_requests(raw)
        with self.assertRaises(requests.RequestsError):
            requests.parse_requests(b" " * (requests.MAX_REQUESTS_BYTES + 1))
        with self.assertRaises(requests.RequestsError):
            requests.parse_requests(dump([op(tid=i) for i in range(requests.MAX_OPS + 1)]))

    def test_refuses_an_unparsable_copy_get_of_the_source(self):
        for printed in ("copy-get max", "copy-get max -1", "copy-get max 08", "copy-get", "copy-get max 1 in=b", "copy-get max 1 in=3", "copy-get max 1 out=1b in=1b"):
            with self.subTest(printed):
                parsed, _ = requests.parse_requests(dump([op(osd_ops=(printed,))]))
                with self.assertRaises(requests.RequestsError):
                    requests.source_ops(parsed, 3, "copy-primary-source")
        # Another object's operations are not judged.
        parsed, _ = requests.parse_requests(dump([op(name="other", osd_ops=("copy-get",))]))
        self.assertEqual(requests.source_ops(parsed, 3, "copy-primary-source"), [])


class SnapshotTest(unittest.TestCase):
    def test_complete_receipt(self):
        raw = dump([op()])
        receipt = requests.snapshot(PLACEMENT, dict(PLACEMENT), 0, raw, "", SOURCE)
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["num_ops"], receipt["source"], len(receipt["ops"]), receipt["absence_established"]), (1, SOURCE, 1, False))
        self.assertEqual((receipt["bytes"], receipt["sha256"], base64.b64decode(receipt["raw"])), (len(raw), hashlib.sha256(raw).hexdigest(), raw))

    def test_fails_closed(self):
        moved = dict(PLACEMENT, epoch=32)
        cases = {
            "refused": requests.snapshot(PLACEMENT, PLACEMENT, -22, b"", "unknown command", SOURCE),
            "oversized": requests.snapshot(PLACEMENT, PLACEMENT, 0, b"", "", SOURCE, oversized_length=requests.MAX_REQUESTS_BYTES + 1),
            "placement moved": requests.snapshot(PLACEMENT, moved, 0, dump([op()]), "", SOURCE),
            "unparsable": requests.snapshot(PLACEMENT, PLACEMENT, 0, b"{", "", SOURCE),
            "unparsable source copy-get": requests.snapshot(PLACEMENT, PLACEMENT, 0, dump([op(osd_ops=("copy-get max x",))]), "", SOURCE),
        }
        for name, receipt in cases.items():
            with self.subTest(name):
                self.assertFalse(receipt["complete"])
                self.assertTrue(receipt["failures"])
        # A moved placement still reports what the Objecter held.
        self.assertEqual(len(cases["placement moved"]["ops"]), 1)

    def test_refuses_invalid_sources(self):
        for source in ({"pool": -1, "name": "a"}, {"pool": True, "name": "a"}, {"pool": 1, "name": ""}, {"pool": 1, "name": "a\0b"},
                       {"pool": 1, "name": "a" * (requests.MAX_NAME + 1)}, {"pool": 1}, {"pool": 1, "name": "a", "namespace": ""}, ["a"]):
            with self.subTest(source):
                with self.assertRaises(ValueError):
                    requests.source_identity(source)


if __name__ == "__main__":
    unittest.main()
