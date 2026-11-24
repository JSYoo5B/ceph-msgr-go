"""Admission checks for the rows of the pending-append journal cells.

The pending-hold and primary-restart cells run on three replicated BlueStore
OSDs like the held-request cells. Each row selects its test alone under its
own capability, and no other row matches it.
"""
import unittest

import ci_row_catalog
from test_ci_rows_rbd import RBD_THREE_OSD_CAPABILITIES
from test_ci_rows_rbd_journal_held import exclusive_rows


PENDING = {
    "^TestCephRBDJournalPendingHoldIntegration$": "rbd-journal-pending-hold",
    "^TestCephRBDJournalPrimaryRestartIntegration$": "rbd-journal-primary-restart",
}


class RBDJournalPendingRowsTest(ci_row_catalog.RowsTestCase):
    def test_pending_rows_run_alone_on_three_bluestore_osds(self):
        self.assertTrue(set(PENDING.values()) <= RBD_THREE_OSD_CAPABILITIES)
        for selector, capability in PENDING.items():
            observed = set()
            for row in exclusive_rows(self, selector, capability):
                self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_store"], row["osd_profile"], row["stress"]),
                                 ("1", "1", "3", "bluestore", "legacy-v6", ""))
                observed.add(tuple(row.get(field, fallback) for field, fallback in (
                    ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
            self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})


if __name__ == "__main__":
    unittest.main()
