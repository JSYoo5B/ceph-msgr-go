"""Admission checks for the rows of the fenced-hold, fenced pending and
pre-down restart journal cells.

They run on three replicated BlueStore OSDs like the other held-request
cells. Each row selects its test alone under its own capability, and no
other row matches it.
"""
import unittest

import ci_row_catalog
from test_ci_rows_rbd import RBD_THREE_OSD_CAPABILITIES
from test_ci_rows_rbd_journal_held import exclusive_rows


FENCED = {
    "^TestCephRBDJournalFencedHoldIntegration$": "rbd-journal-fenced-hold",
    "^TestCephRBDJournalErasureFencedHoldIntegration$": "rbd-journal-erasure-fenced-hold",
    "^TestCephRBDJournalPrimaryRestartBeforeDownIntegration$": "rbd-journal-primary-restart-before-down",
    "^TestCephRBDJournalFencedPendingHoldIntegration$": "rbd-journal-fenced-pending-hold",
    "^TestCephRBDJournalErasureFencedPendingHoldIntegration$": "rbd-journal-erasure-fenced-pending-hold",
    "^TestCephRBDJournalErasurePrimaryRestartBeforeDownIntegration$": "rbd-journal-erasure-primary-restart-before-down",
}


class RBDJournalFencedRowsTest(ci_row_catalog.RowsTestCase):
    def test_fenced_rows_run_alone_on_three_bluestore_osds(self):
        self.assertTrue(set(FENCED.values()) <= RBD_THREE_OSD_CAPABILITIES)
        for selector, capability in FENCED.items():
            observed = set()
            for row in exclusive_rows(self, selector, capability):
                self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_store"], row["osd_profile"], row["stress"]),
                                 ("1", "1", "3", "bluestore", "legacy-v6", ""))
                observed.add(tuple(row.get(field, fallback) for field, fallback in (
                    ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
            self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})


if __name__ == "__main__":
    unittest.main()
