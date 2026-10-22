import json
from pathlib import Path
import tempfile
import unittest

import osd_cache_op_go_oracle as oracle
from test_osd_cache_tier_ops_oracle import ScriptedTier
from test_osd_ignore_cache_oracle import plan


class ObserverTier(ScriptedTier):
    """The scripted tier with the Go actions applied directly by the test."""
    def close(self):
        pass


class GoCacheOpOracleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.out = Path(directory.name)
        (self.out / "osd-ignore-cache-plan.json").write_text(json.dumps(plan()))
        self.native = ObserverTier()

    def run_phase(self, *args):
        oracle.main(["oracle", "/root", str(self.out)] + list(args), native_factory=lambda *_: self.native, sleep=lambda _: None)
        return json.loads((self.out / oracle.RECEIPT).read_text())

    def test_complete_sequence_observes_without_cache_operations(self):
        receipt = self.run_phase("prepare")
        self.assertEqual(receipt["phase"], "prepared")
        self.assertEqual(len(receipt["placements"]), 6)
        for name in (oracle.FLUSH, oracle.TRY):
            self.assertIs(receipt["seeded"][name]["dirty"]["dirty"], True)
        tier = plan()["tier"]
        self.run_phase("observe", "after-evict-dirty")
        # Simulate the Go flush and clean eviction of FLUSH, then observe.
        self.native.dirty.discard(oracle.FLUSH)
        data, version, mtime = self.native.heads[(tier, oracle.FLUSH)]
        self.native.heads[(plan()["base"], oracle.FLUSH)] = (data, version + 1, mtime)
        flushed = self.run_phase("observe", "flushed")["labels"]["flushed"]
        self.assertIs(flushed["dirty"]["dirty"], False)
        self.native.dirty.discard(oracle.TRY)
        tried = self.run_phase("observe", "try-flushed")["labels"]["try-flushed"]
        self.assertEqual(tried["dirty_polls"], [False])
        del self.native.heads[(tier, oracle.FLUSH)]
        evicted = self.run_phase("observe", "evicted")["labels"]["evicted"]["snapshot"]
        self.assertEqual(evicted["tier"]["code"], -2)
        absent = self.run_phase("observe", "absent-final")["labels"]["absent-final"]
        self.assertNotIn("dirty", absent)
        final = self.run_phase("finish")
        self.assertEqual(final["phase"], "finished")
        self.assertTrue(final["cleanup"]["complete"])
        self.assertTrue(all(step["state"] == "completed" for step in final["steps"]))
        self.assertFalse(any(call[3].startswith("cache-") for call in self.native.calls))
        self.assertEqual({call[0] for call in self.native.calls}, {"observer"})

    def test_labels_phases_and_repeats_are_closed(self):
        with self.assertRaises(ValueError):
            self.run_phase("observe", "flushed")
        self.run_phase("prepare")
        for args in (("prepare",), ("observe", "foreign"), ("observe",), ("finish", "extra")):
            with self.assertRaises((ValueError, oracle.ops.Stop)):
                self.run_phase(*args)
        self.run_phase("observe", "pinned")
        with self.assertRaises(oracle.ops.Stop):
            self.run_phase("observe", "pinned")

    def test_observer_refuses_cache_operations(self):
        native = oracle.Native.__new__(oracle.Native)
        native.plan = plan()
        with self.assertRaises(ValueError):
            native.execute("observer", plan()["tier"], oracle.FLUSH, "cache-flush", 8)
        with self.assertRaises(ValueError):
            native.execute("rw", plan()["tier"], oracle.FLUSH, "snapshot", 8)

    def test_uncertain_call_blocks_every_later_phase(self):
        self.run_phase("prepare")
        self.native.overrides[("observer", oracle.FLUSH, "is-dirty", 8)] = {"code": -110, "version": 0}
        with self.assertRaises(oracle.ops.Stop):
            self.run_phase("observe", "pinned")
        receipt = json.loads((self.out / oracle.RECEIPT).read_text())
        self.assertEqual(receipt["steps"][-1]["state"], "unknown")
        count = len(self.native.calls)
        for args in (("observe", "flushed"), ("finish",)):
            with self.assertRaises(ValueError):
                self.run_phase(*args)
        self.assertEqual(len(self.native.calls), count)


if __name__ == "__main__":
    unittest.main()
