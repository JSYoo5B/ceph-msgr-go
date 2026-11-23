"""Admission checks for the rows of the held-request journal cells.

The held-request cells (replica hold, held primary change, dead primary) run
on three replicated BlueStore OSDs like the primary-change cell. Each row
selects its test alone under its own capability, and no other row matches it.
"""
import re
import unittest

import ci_row_catalog
from test_ci_rows_rbd import RBD_THREE_OSD_CAPABILITIES


HELD = {
    "^TestCephRBDJournalReplicaHoldIntegration$": "rbd-journal-replica-hold",
    "^TestCephRBDJournalHeldPrimaryChangeIntegration$": "rbd-journal-held-primary-change",
    "^TestCephRBDJournalDeadPrimaryIntegration$": "rbd-journal-dead-primary",
}


def exclusive_rows(case, selector, capability):
    """The two rows of selector: they declare capability alone, select
    exactly its test and no profile field of another acceptance, and no other
    row matches the test."""
    rows = [row for row in case.rows if row.get("test_run") == selector]
    case.assertEqual(len(rows), 2)
    expected = selector.strip("^$")
    pattern = re.compile(selector)
    case.assertEqual({name for name in case.declared if pattern.search(name)}, {expected})
    for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDJournalIntegration", "TestCephRBDJournalPrimaryChangeIntegration"):
        case.assertIsNone(pattern.search(unrelated))
    for row in case.rows:
        if row.get("test_run") != selector and "test_run" in row:
            case.assertIsNone(re.compile(row["test_run"]).search(expected))
    case.assertEqual({row["capabilities"] for row in rows}, {capability})
    for row in rows:
        for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "image", "race"):
            case.assertNotIn(field, row)
    return rows


class RBDJournalHeldRowsTest(ci_row_catalog.RowsTestCase):
    def test_held_rows_run_alone_on_three_bluestore_osds(self):
        self.assertTrue(set(HELD.values()) <= RBD_THREE_OSD_CAPABILITIES)
        for selector, capability in HELD.items():
            observed = set()
            for row in exclusive_rows(self, selector, capability):
                self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_store"], row["osd_profile"], row["stress"]),
                                 ("1", "1", "3", "bluestore", "legacy-v6", ""))
                observed.add(tuple(row.get(field, fallback) for field, fallback in (
                    ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
            self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})


if __name__ == "__main__":
    unittest.main()
