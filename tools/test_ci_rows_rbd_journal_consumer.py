"""Admission checks for the rows of the journal consumer cell.

The consumer cell runs on the two single-OSD RBD profiles, its test alone
under its own capability, and no other row matches it.
"""
import unittest

import ci_row_catalog
from test_ci_rows_rbd import RBD_THREE_OSD_CAPABILITIES
from test_ci_rows_rbd_journal_held import exclusive_rows


SELECTOR, CAPABILITY = "^TestCephRBDJournalConsumerIntegration$", "rbd-journal-consumer"


class RBDJournalConsumerRowsTest(ci_row_catalog.RowsTestCase):
    def test_consumer_rows_run_alone_on_the_single_osd_rbd_profiles(self):
        self.assertNotIn(CAPABILITY, RBD_THREE_OSD_CAPABILITIES)
        observed = set()
        for row in exclusive_rows(self, SELECTOR, CAPABILITY):
            self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_profile"], row["stress"]), ("1", "1", "1", "legacy-v6", ""))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"), ("osd_store", ""))))
        self.assertEqual(observed, {("aes", "crc", "4", "host", "bluestore"), ("aes256k", "secure", "6", "container", "memstore")})


if __name__ == "__main__":
    unittest.main()
