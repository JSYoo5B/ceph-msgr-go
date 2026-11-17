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
        # Every row is the four-OSD BlueStore profile and selects one exact
        # test. The default k=2,m=2 baseline is the only row without a
        # capability; every other row declares osd-erasure-width, which the
        # topology requires of any other test, with the capabilities of its
        # own mode. No list of the rows is kept, so a new row edits none.
        default = "^TestCephOSDErasureK2M2Integration$"
        self.assertEqual([row for row in rows if "capabilities" not in row], [dict(base, test_run=default)])
        for row in rows:
            self.assertEqual({key: value for key, value in row.items() if key not in ("test_run", "capabilities")}, base)
            self.assertRegex(row["test_run"], r"^\^TestCephOSDErasure[A-Za-z0-9]+Integration\$$")
            if row["test_run"] != default:
                self.assertIn("osd-erasure-width", row["capabilities"].split(","))
        # The rows that existed when the list was removed keep the
        # capabilities of their modes. The mapping is frozen: a later row
        # checks its own capabilities in a module of its own.
        width, capture, interval = "osd-erasure-width", "osd-erasure-width,osd-shard-history", "osd-erasure-width,osd-pg-interval,osd-shard-history"
        frozen = [("OptimizedK2M2", width), ("OptimizedK2M2Failover", width),
                  ("OptimizedK2M2InFlightMutation", width), ("OptimizedK2M2SnapshotAndDegradedWrite", width),
                  ("OptimizedK2M2AppliedResubmit", width), ("OptimizedK2M2PrimaryHistoryCalibration", width),
                  ("OptimizedK2M2CopyFromHistory", capture), ("OptimizedK2M2PartialWriteShardHistory", capture),
                  ("OptimizedK2M2PartialWriteLaterWriteShardHistory", interval), ("OptimizedK2M2PartialWriteNewIntervalShardHistory", interval),
                  ("OptimizedK2M2PartialWriteShardReturnShardHistory", interval), ("OptimizedK2M2PartialWriteDegradedWriteShardHistory", interval),
                  ("OptimizedK2M2PartialWritePrimaryReplacementShardHistory", interval), ("OptimizedK2M2RequestHeldAtDyingPrimary", width)]
        for cell, capabilities in frozen:
            selector = "^TestCephOSDErasure%sIntegration$" % cell
            self.assertEqual([row.get("capabilities") for row in rows if row["test_run"] == selector], [capabilities], selector)
        fixture = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        self.assertIn('selected = "^TestCephOSDErasureK2M2Integration$"', fixture)
        for row in rows:
            name = row["test_run"][1:-1]
            with self.subTest(test_run=row["test_run"]):
                selector = re.compile(row["test_run"])
                self.assertEqual({test for test in self.declared if selector.search(test)}, {name})
                self.assertIsNone(selector.search(name + "Extended"))
                self.assertIsNone(selector.search("Prefix" + name))
                for other in ("TestCephOSDErasureIntegration", "TestCephOSDErasureOptimizedIntegration",
                              "TestCephOSDErasureOptimizedFailoverIntegration", "TestCephOSDErasureFailoverIntegration",
                              "TestCephOSDErasureSnapshotAndDegradedWriteIntegration",
                              "TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration"):
                    self.assertIsNone(selector.search(other))


if __name__ == "__main__":
    unittest.main()
