import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_copyinterval_oracle as copyinterval


def reply(pgid="4.3", since=31, epoch=33, up=(1, 0, 2), acting=(1, 0, 2), state="active+clean"):
    return json.dumps({"state": state, "epoch": epoch, "up": list(up), "acting": list(acting),
                       "info": {"pgid": pgid, "shard": "255", "history": {"same_interval_since": since}}, "peer_info": []}).encode()


class ParseQueryTest(unittest.TestCase):
    def test_replicated_and_erasure_coded(self):
        query, failure = copyinterval.parse_query(0, reply(), "", None)
        self.assertIsNone(failure)
        self.assertEqual(query, {"state": "active+clean", "epoch": 33, "up": [1, 0, 2], "acting": [1, 0, 2], "pgid": "4.3", "same_interval_since": 31})
        query, failure = copyinterval.parse_query(0, reply(pgid="4.3s0", up=(1, 0, 2, 3), acting=(1, 0, 2, 3)), "", None)
        self.assertIsNone(failure)
        self.assertEqual(query["pgid"], "4.3s0")

    def test_refusals(self):
        for name, args in {
            "oversized": (0, b"", "", 300000),
            "refused": (-2, b"", "no such pg", None),
            "empty": (0, b"", "", None),
            "truncated": (0, reply()[:-3], "", None),
            "not an object": (0, b"[]", "", None),
            "no interval": (0, json.dumps({"state": "active", "epoch": 3, "up": [1], "acting": [1], "info": {"pgid": "4.3", "history": {}}}).encode(), "", None),
            "no history": (0, json.dumps({"state": "active", "epoch": 3, "up": [1], "acting": [1], "info": {"pgid": "4.3"}}).encode(), "", None),
            "no pgid": (0, json.dumps({"state": "active", "epoch": 3, "up": [1], "acting": [1], "info": {"history": {"same_interval_since": 3}}}).encode(), "", None),
            "zero interval": (0, reply(since=0), "", None),
            "bad set": (0, reply(up=(1, -1)), "", None),
            "no state": (0, reply(state=""), "", None),
            "duplicate key": (0, b'{"state":"a","state":"b"}', "", None),
        }.items():
            with self.subTest(name):
                query, failure = copyinterval.parse_query(*args)
                self.assertIsNone(query)
                self.assertTrue(failure)


class ModeTest(unittest.TestCase):
    def test_request_fields(self):
        with self.assertRaises(ValueError):
            copyinterval.mode_copyinterval(None, None, {"slot": 2, "object": "dst", "action": "repeer"}, None)


if __name__ == "__main__":
    unittest.main()
