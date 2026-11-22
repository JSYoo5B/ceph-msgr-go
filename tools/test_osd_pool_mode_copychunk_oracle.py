import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_copychunk_oracle as chunk


class FakeOSD:
    """One OSD's runtime option behind config get and config set."""

    def __init__(self, value=8 << 20, refuse_set=False, ignore_set=False, get_reply=None):
        self.value, self.refuse_set, self.ignore_set, self.get_reply = value, refuse_set, ignore_set, get_reply
        self.commands = []

    def osd(self, cluster, osd, command, limit):
        self.commands.append((osd, command))
        if command["prefix"] == "config get":
            if self.get_reply is not None:
                return self.get_reply
            return 0, json.dumps({chunk.OPTION: str(self.value)}).encode(), "", None
        if self.refuse_set:
            return -22, b"", "refused", None
        if not self.ignore_set:
            self.value = int(command["val"][0])
        return 0, b'{"success":""}', "", None


class CopyChunk(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.TemporaryDirectory()
        self.addCleanup(self.out.cleanup)

    def act(self, rados, action, osd=1):
        return chunk.mode_copychunk(rados, None, {"osd": osd, "action": action}, self.out.name)["copy_chunk"]

    def sets(self, rados):
        return [c for _, c in rados.commands if c["prefix"] == "config set"]

    def test_lowers_once_and_restores_once(self):
        rados = FakeOSD()
        lowered = self.act(rados, "lower")
        self.assertTrue(lowered["lowered"], lowered)
        self.assertEqual((lowered["prior"], lowered["value"], rados.value), (8 << 20, chunk.LIMIT, chunk.LIMIT))
        self.assertEqual(self.sets(rados), [{"prefix": "config set", "var": chunk.OPTION, "val": [str(chunk.LIMIT)], "format": "json"}])
        again = self.act(rados, "lower")
        self.assertTrue(again["failures"] and not again.get("lowered"), again)
        self.assertEqual(len(self.sets(rados)), 1)
        self.assertEqual(self.act(rados, "show")["value"], chunk.LIMIT)
        restored = self.act(rados, "restore")
        self.assertTrue(restored["restored"] and restored["set"] and not restored["claimed"], restored)
        self.assertEqual(rados.value, 8 << 20)
        self.assertEqual(len(self.sets(rados)), 2)
        again = self.act(rados, "restore")
        self.assertFalse(again["restored"] or again["claimed"])
        self.assertEqual(len(self.sets(rados)), 2)

    def test_restore_of_a_restarted_osd_sets_nothing(self):
        rados = FakeOSD()
        self.act(rados, "lower")
        rados.value = 8 << 20
        restored = self.act(rados, "restore")
        self.assertTrue(restored["restored"] and not restored["set"], restored)
        self.assertEqual(len(self.sets(rados)), 1)

    def test_claims_are_per_osd(self):
        rados = FakeOSD()
        self.act(rados, "lower", osd=1)
        other = FakeOSD(value=4 << 20)
        self.assertTrue(self.act(other, "lower", osd=2)["lowered"])
        self.assertTrue(self.act(other, "restore", osd=2)["restored"])
        self.assertEqual(other.value, 4 << 20)
        self.assertTrue(self.act(rados, "restore", osd=1)["claimed"] is False)

    def test_reports_failures_and_keeps_the_claim(self):
        refused = FakeOSD(refuse_set=True)
        lowered = self.act(refused, "lower")
        self.assertFalse(lowered["lowered"])
        self.assertTrue(lowered["claimed"], "an uncertain set still owns its restoration")
        ignored = FakeOSD(ignore_set=True)
        lowered = chunk.mode_copychunk(ignored, None, {"osd": 3, "action": "lower"}, self.out.name)["copy_chunk"]
        self.assertFalse(lowered["lowered"])
        self.assertTrue(chunk.read_claim(self.out.name, 3))
        restore_refused = FakeOSD(value=chunk.LIMIT, refuse_set=True)
        restored = self.act(restore_refused, "restore")
        self.assertFalse(restored["restored"])
        self.assertTrue(restored["claimed"])
        low = FakeOSD(value=chunk.LIMIT)
        lowered = self.act(low, "lower", osd=4)
        self.assertFalse(lowered.get("lowered"))
        self.assertEqual(self.sets(low), [])
        self.assertIsNone(chunk.read_claim(self.out.name, 4))
        for reply in ((-2, b"", "down", None), (0, b"", "", chunk.MAX_REPLY + 1), (0, b"{}", "", None),
                      (0, b'{"osd_copyfrom_max_chunk":"x"}', "", None), (0, b'{"osd_copyfrom_max_chunk":"0"}', "", None),
                      (0, b"not json", "", None), (0, b'["8388608"]', "", None)):
            broken = FakeOSD(get_reply=reply)
            receipt = self.act(broken, "lower", osd=5)
            self.assertTrue(receipt["failures"], reply)
            self.assertEqual(self.sets(broken), [])

    def test_refuses_malformed_requests_and_claims(self):
        for request in ({}, {"osd": 1}, {"action": "lower"}, {"osd": -1, "action": "lower"}, {"osd": 1, "action": "raise"},
                        {"osd": 1, "action": "lower", "value": 1}, {"osd": "1", "action": "show"}):
            with self.assertRaises(ValueError, msg=request):
                chunk.mode_copychunk(FakeOSD(), None, request, self.out.name)
        chunk.claim_path(self.out.name, 6).write_text(json.dumps({"osd": 7, "prior": 1}))
        with self.assertRaises(ValueError):
            self.act(FakeOSD(), "restore", osd=6)


if __name__ == "__main__":
    unittest.main()
