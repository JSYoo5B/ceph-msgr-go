"""Admission checks for the MGR service rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the rows of
the mgr_service opt-in.
"""
import re
import unittest

import ci_row_catalog


class MgrServiceRowsTest(ci_row_catalog.RowsTestCase):
    def test_mgr_service_rows_require_explicit_opt_in_and_only_the_declared_test(self):
        self.assert_forwarded("mgr_service", "CEPH_MSGR_TEST_MGR_SERVICE", "0")
        self.assertIn("matrix.mgr_service == '1' && 'MGR-service ' || ''", self.workflow)
        rows = [row for row in self.rows if row.get("mgr_service") == "1"]
        common = {
            "stress": "", "osd": "1", "osd_store": "memstore", "osd_replicas": "1",
            "osd_profile": "legacy-v6", "mgr_service": "1",
            "test_run": "^TestCephMgrServiceRegistrationIntegration$",
        }
        self.assertCountEqual(rows, [
            dict(common, key="aes", service="aes", mode="crc", family="4", runtime="host"),
            dict(common, key="aes256k", service="aes256k", family="6", runtime="container"),
            dict(key="aes256k", service="aes256k", family="4", runtime="container", managers="0",
                 mgr_service="1", test_run="^TestCephMgrServiceColdBootstrapIntegration$"),
        ])
        for row in rows:
            expected = ("TestCephMgrServiceColdBootstrapIntegration" if row.get("managers") == "0"
                        else "TestCephMgrServiceRegistrationIntegration")
            self.assertIn(expected, self.declared)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
            for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephMonServiceMapIntegration"):
                self.assertIsNone(selector.search(unrelated))


if __name__ == "__main__":
    unittest.main()
