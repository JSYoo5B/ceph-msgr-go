"""Admission checks for the rows of the further CephFS writeback cells.

The writeback-outcomes rows reuse the owned data fixture
opt-in with their own exact selectors and capabilities, in the same two MDS
profiles as the other writeback rows (test_ci_rows_mds).
"""
import unittest

import ci_row_catalog
import test_ci_rows_mds


class MDSWritebackOutcomesRowsTest(ci_row_catalog.RowsTestCase):
    def test_writeback_outcomes_rows_reuse_the_data_fixture_with_their_own_test(self):
        test_ci_rows_mds.MDSRowsTest.check_data_writeback_rows(
            self, "TestCephMDSCephFSWritebackOutcomesIntegration", "mds-cephfs-writeback-outcomes",
            "mds-writeback-fence,mds-writeback-mixed")



if __name__ == "__main__":
    unittest.main()
