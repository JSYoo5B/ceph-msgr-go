import json
from pathlib import Path
import tempfile
import unittest

import osd_cache_copy_flags_oracle as oracle
from test_osd_cache_tier_ops_oracle import ScriptedTier
from test_osd_ignore_cache_oracle import plan


class ObserverTier(ScriptedTier):
    def close(self):
        pass


class CopyCacheFlagsOracleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.out = Path(directory.name)
        (self.out / "osd-ignore-cache-plan.json").write_text(json.dumps(plan()))
        self.native = ObserverTier()

    def run_phase(self, *args):
        oracle.main(["oracle", "/root", str(self.out)] + list(args), native_factory=lambda *_: self.native, sleep=lambda _: None)
        return json.loads((self.out / oracle.RECEIPT).read_text())

    def test_sequence_seeds_observes_and_removes_only(self):
        receipt = self.run_phase("prepare")
        self.assertEqual(receipt["phase"], "prepared")
        self.assertEqual(len(receipt["placements"]), 2 * len(oracle.OBJECTS))
        seeded = receipt["seeded"]
        self.assertEqual(seeded[oracle.DIRTY]["tier"]["code"], 0)
        for name in oracle.MISSES:
            self.assertEqual((seeded[name]["base"]["code"], seeded[name]["tier"]["code"]), (0, -2))
        for name in oracle.DESTINATIONS:
            self.assertEqual((seeded[name]["base"]["code"], seeded[name]["tier"]["code"]), (-2, -2))
        self.assertIs(receipt["seeded_dirty"]["dirty"], True)
        for name in oracle.OBJECTS:
            self.run_phase("observe", name)
        receipt = self.run_phase("observe", oracle.DESTINATIONS[0])
        self.assertEqual([entry["object"] for entry in receipt["labels"]], list(oracle.OBJECTS) + [oracle.DESTINATIONS[0]])
        self.assertEqual(len({entry["label"] for entry in receipt["labels"]}), len(receipt["labels"]))
        final = self.run_phase("finish")
        self.assertTrue(final["cleanup"]["complete"])
        self.assertTrue(all(step["state"] == "completed" for step in final["steps"]))
        # Every read names its pool and promotes nothing; no cache operation.
        reads = [call for call in self.native.calls if call[3] == "snapshot"]
        self.assertTrue(reads and all(call[4] == oracle.READ_FLAGS for call in reads))
        self.assertFalse(any(call[3].startswith("cache-") for call in self.native.calls))
        self.assertEqual({call[0] for call in self.native.calls}, {"observer"})

    def test_closed_inputs(self):
        with self.assertRaises(ValueError):
            self.run_phase("observe", oracle.DIRTY)  # no receipt yet
        self.run_phase("prepare")
        for args in (("observe", "foreign"), ("prepare",)):
            with self.assertRaises(Exception):
                self.run_phase(*args)
        with self.assertRaises(ValueError):
            oracle.main(["oracle", "/root", str(self.out), "observe"], native_factory=lambda *_: self.native)

    def test_native_refuses_cache_operations(self):
        native = oracle.Native.__new__(oracle.Native)
        with self.assertRaises(ValueError):
            native.execute("observer", plan()["tier"], oracle.DIRTY, "cache-evict", 0)
        with self.assertRaises(ValueError):
            native.execute("rw", plan()["tier"], oracle.DIRTY, "snapshot", 0)


if __name__ == "__main__":
    unittest.main()
