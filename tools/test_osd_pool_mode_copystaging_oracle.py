import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_copystaging_oracle as staging

TEMP = "#-6:a7e1c86e:::temp_4.3_0_4123_1:head#"
LISTING = ("#4:00000000::::head#\n" + TEMP + "\n#4:a7e1c86e:::dst:head#\n#4:c0000000:::other:head#\n").encode()
# One shard's collection of an erasure-coded PG: every entry names shard 2,
# the temporary object the primary (shard 0) named, and a generation.
SHARD_TEMP = "2#-6:a7e1c86e:::temp_4.3s0_0_4123_1:head#"
SHARD_LISTING = ("2#4:00000000::::head#\n" + SHARD_TEMP + "\n2#4:a7e1c86e:::dst:head#\n2#4:a7e1c86e:::dst:head#1a\n").encode()


class FakeRados:
    """Scripted replies to OSD commands; no daemon is involved."""

    def __init__(self, *replies):
        self.replies, self.commands = list(replies), []

    def osd(self, cluster, osd, command, limit):
        self.commands.append((osd, command, limit))
        return self.replies.pop(0)


def ok(text):
    return 0, text, "", None


class CopyStaging(unittest.TestCase):
    def run_mode(self, *replies, pgid="4.3", shard=None):
        rados = FakeRados(*replies)
        request = {"osd": 2, "pgid": pgid}
        if shard is not None:
            request["shard"] = shard
        return staging.mode_copystaging(rados, None, request, None)["copy_staging"], rados

    def test_lists_one_collection_completely(self):
        receipt, rados = self.run_mode(ok(b"meta\n4.3_head\n5.0_head\n"), ok(LISTING))
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["objects"], receipt["temp"], receipt["collection"]), (4, [TEMP], "4.3_head"))
        self.assertEqual(rados.commands, [
            (2, {"prefix": "bluestore collections"}, staging.MAX_COLLECTIONS_BYTES),
            (2, {"prefix": "bluestore list", "collection": "4.3_head", "start": "GHMIN", "max": staging.MAX_LIST}, staging.MAX_LIST_BYTES),
        ])
        self.assertEqual(receipt["shard"], -1)
        receipt, _ = self.run_mode(ok(b"4.3_head\n"), ok(b"#4:00000000::::head#\n"))
        self.assertTrue(receipt["complete"])
        self.assertEqual(receipt["temp"], [])

    def test_lists_one_shard_collection(self):
        receipt, rados = self.run_mode(ok(b"4.3s0_head\n4.3s2_head\n"), ok(SHARD_LISTING), shard=2)
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["objects"], receipt["temp"], receipt["collection"], receipt["shard"]), (4, [SHARD_TEMP], "4.3s2_head", 2))
        self.assertEqual(rados.commands[1], (2, {"prefix": "bluestore list", "collection": "4.3s2_head", "start": "GHMIN", "max": staging.MAX_LIST},
                                             staging.MAX_LIST_BYTES))
        cases = {
            "unsharded collection only": (ok(b"4.3_head\n"),),
            "another shard's collection": (ok(b"4.3s1_head\n"),),
            "another shard's entry": (ok(b"4.3s2_head\n"), ok(b"2#4:00000000::::head#\n1#4:a7e1c86e:::dst:head#\n")),
            "unsharded entry": (ok(b"4.3s2_head\n"), ok(b"#4:00000000::::head#\n")),
            "decimal shard": (ok(b"4.3s2_head\n"), ok(b"02#4:00000000::::head#\n")),
            "generation not hexadecimal": (ok(b"4.3s2_head\n"), ok(b"2#4:00000000::::head#zz\n")),
        }
        for name, replies in cases.items():
            receipt, _ = self.run_mode(*replies, shard=2)
            self.assertFalse(receipt["complete"], name)
            self.assertTrue(receipt["failures"], name)
        # Shard 10 prints as "a".
        receipt, _ = self.run_mode(ok(b"4.3s10_head\n"), ok(b"a#4:00000000::::head#\n"), shard=10)
        self.assertTrue(receipt["complete"], receipt["failures"])

    def test_reports_incomplete_observations_without_raising(self):
        cases = {
            "collections refused": (( -2, b"", "no", None),),
            "collections oversized": ((0, b"", "", staging.MAX_COLLECTIONS_BYTES + 1),),
            "no collection": (ok(b"meta\n4.30_head\n"),),
            "list refused": (ok(b"4.3_head\n"), (-22, b"", "bad", None)),
            "list oversized": (ok(b"4.3_head\n"), (0, b"", "", staging.MAX_LIST_BYTES + 1)),
            "empty listing": (ok(b"4.3_head\n"), ok(b"")),
            "unparsable entry": (ok(b"4.3_head\n"), ok(b"#4:00000000::::head#\nnot an object\n")),
            "unterminated entry": (ok(b"4.3_head\n"), ok(b"#4:00000000::::head\n")),
            "pool not a number": (ok(b"4.3_head\n"), ok(b"#x:00000000::::head#\n")),
            "not UTF-8": (ok(b"4.3_head\n"), ok(b"#4:\xff::::head#\n")),
            "bound reached": (ok(b"4.3_head\n"), ok(b"#4:00000000::::head#\n" * staging.MAX_LIST)),
            "too many temporaries": (ok(b"4.3_head\n"), ok((TEMP + "\n").encode() * (staging.MAX_TEMP + 1))),
        }
        for name, replies in cases.items():
            receipt, _ = self.run_mode(*replies)
            self.assertFalse(receipt["complete"], name)
            self.assertTrue(receipt["failures"], name)

    def test_refuses_malformed_requests(self):
        for request in ({}, {"osd": 2}, {"pgid": "4.3"}, {"osd": -1, "pgid": "4.3"}, {"osd": "2", "pgid": "4.3"},
                        {"osd": 2, "pgid": "4.3", "slot": 1}, {"osd": 2, "pgid": "4.03"}, {"osd": 2, "pgid": "4.3A"},
                        {"osd": 2, "pgid": "4.3_head"}, {"osd": 2, "pgid": 4}, {"osd": True, "pgid": "4.3"},
                        {"osd": 2, "pgid": "4.3", "shard": -1}, {"osd": 2, "pgid": "4.3", "shard": 256}, {"osd": 2, "pgid": "4.3", "shard": "1"},
                        {"osd": 2, "pgid": "4.3", "shard": True}, {"osd": 2, "pgid": "4.3s1"}):
            with self.assertRaises(ValueError, msg=request):
                staging.mode_copystaging(FakeRados(), None, request, None)


if __name__ == "__main__":
    unittest.main()
