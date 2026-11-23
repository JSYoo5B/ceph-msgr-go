"""Admission checks for the held-lock recovery cell rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the rows of the
held-lock cells of issue #10 that came after the held-lock rows of
test_ci_rows_mds.py: each cell runs one exact test alone on a fresh fixture of
its own kind, with its capabilities, in the two usual profiles.
"""
import re
import unittest

import ci_row_catalog

# group: (test, fixture parameter, capabilities)
CELLS = {
    "mds-cephfs-waitflock-recovery": ("TestCephMDSCephFSWaitingFlockRecoveryIntegration", "mds_cephfs", "mds-held-flock-wait"),
    "mds-cephfs-intrflock-recovery": ("TestCephMDSCephFSInterruptedFlockRecoveryIntegration", "mds_cephfs", "mds-held-flock-wait"),
    "mds-cephfs-fcntlranges-recovery": ("TestCephMDSCephFSFcntlMergedRangeRecoveryIntegration", "mds_cephfs", "mds-held-fcntl-ranges"),
    "mds-cephfs-twoclient-recovery": ("TestCephMDSCephFSTwoClientFlockRecoveryIntegration", "mds_cephfs", "exclusive-mds-cephfs"),
    "mds-cephfs-dirtyflock-recovery": ("TestCephMDSCephFSDirtyFlockRecoveryIntegration", "mds_cephfs", "exclusive-mds-cephfs"),
    "mds-cephfs-noblob-recovery": ("TestCephMDSCephFSFlockWithoutBlobRecoveryIntegration", "mds_cephfs", "exclusive-mds-cephfs"),
}

FIXTURES = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data")


class MDSHeldLockCellRowsTest(ci_row_catalog.RowsTestCase):
    def test_each_cell_selects_only_its_test_alone_on_its_fixture(self):
        for group, (test, fixture, capabilities) in CELLS.items():
            with self.subTest(group=group):
                selector_text = "^" + test + "$"
                entries = [entry for entry in self.entries if entry["group"] == group]
                self.assertEqual(len(entries), 2)
                self.assertTrue(all(entry["params"].get("test_run") == selector_text for entry in entries))
                self.assertIn(test, self.declared)
                selector = re.compile(selector_text)
                self.assertEqual({name for name in self.declared if selector.search(name)}, {test})
                observed = set()
                for entry in entries:
                    row = entry["params"]
                    self.assertEqual(row.get("capabilities"), capabilities)
                    self.assertEqual((row["mds_fresh"], row[fixture], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]), ("1", "1", "legacy-v6", "1", "1", ""))
                    for other in FIXTURES:
                        if other != fixture:
                            self.assertNotIn(other, row)
                    for field in ("mds_cephfs_ranks_replay", "rbd", "race", "image"):
                        self.assertNotIn(field, row)
                    observed.add(tuple(row.get(field, fallback) for field, fallback in (
                        ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
                self.assertEqual(observed, {("aes", "aes", "crc", "4", "host", "bluestore"), ("aes256k", "aes256k", "secure", "6", "container", "memstore")})

    def test_cell_selectors_reach_no_other_lock_test(self):
        tests = [test for test, _, _ in CELLS.values()] + [
            "TestCephMDSCephFSRecoveryIntegration", "TestCephMDSCephFSSharedFlockRecoveryIntegration", "TestCephMDSCephFSFcntlRangeRecoveryIntegration",
            "TestCephMDSCephFSHeldFlockSplitReconnectIntegration", "TestCephMDSCephFSHeldFlockSplitFragmentsIntegration", "TestCephMDSCephFSMultiReconnectIntegration"]
        for test in tests:
            self.assertIn(test, self.declared)
            selector = re.compile("^" + test + "$")
            self.assertEqual([name for name in tests if selector.search(name)], [test])
        # The broad default selectors of the three fixtures reach no cell.
        for default in ("^TestCephMDSCephFS(Push|Recovery)?Integration$", "^TestCephMDSCephFSCompletionIntegration$"):
            selector = re.compile(default)
            for test, _, _ in CELLS.values():
                self.assertIsNone(selector.search(test))


if __name__ == "__main__":
    unittest.main()
