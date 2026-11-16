"""Admission checks for the four-OSD erasure-code rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the k2m2
erasure rows of the four-OSD topology.
"""
import re
import unittest

import ci_row_catalog
from ci_row_catalog import ROOT


class OSDErasureRowsTest(ci_row_catalog.RowsTestCase):
    def test_four_osd_topology_selects_only_the_k2m2_erasure_scenarios(self):
        # The topology travels in the already-forwarded osd_replicas input, so
        # the trusted default-branch workflow needs no new parameter.
        self.assertIn("CEPH_MSGR_TEST_OSD_REPLICAS: ${{ matrix.osd_replicas || '1' }}", self.workflow)
        for row in self.rows:
            self.assertIn(row.get("osd_replicas", "1"), {"1", "3", "4"})
        rows = [row for row in self.rows if row.get("osd_replicas") == "4"]
        base = {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container",
                "osd": "1", "osd_store": "bluestore", "osd_replicas": "4", "osd_profile": "replicated-pg-v8"}
        self.assertEqual(rows, [
            dict(base, test_run="^TestCephOSDErasureK2M2Integration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2(Failover)?Integration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2(InFlightMutation|SnapshotAndDegradedWrite)Integration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration$"),
            dict(base, test_run="^TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration$"),
        ])
        # Each row's selector must match the fixture's closed four-OSD list.
        fixture = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        for row, expected in zip(rows, ({"TestCephOSDErasureK2M2Integration"},
                                        {"TestCephOSDErasureOptimizedK2M2Integration", "TestCephOSDErasureOptimizedK2M2FailoverIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration",
                                         "TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration"})):
            with self.subTest(test_run=row["test_run"]):
                self.assertIn('"%s",' % row["test_run"], fixture)
                selector = re.compile(row["test_run"])
                self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
                for name in expected:
                    self.assertIsNone(selector.search(name + "Extended"))
                    self.assertIsNone(selector.search("Prefix" + name))
                for other in ("TestCephOSDErasureIntegration", "TestCephOSDErasureOptimizedIntegration",
                              "TestCephOSDErasureOptimizedFailoverIntegration", "TestCephOSDErasureFailoverIntegration",
                              "TestCephOSDErasureSnapshotAndDegradedWriteIntegration",
                              "TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration"):
                    self.assertIsNone(selector.search(other))


if __name__ == "__main__":
    unittest.main()
