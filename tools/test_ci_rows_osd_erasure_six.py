"""Admission checks for the six-OSD erasure-code rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the k4m2
erasure rows of the six-OSD topology.
"""
import re
import unittest

import ci_row_catalog
from ci_row_catalog import ROOT


class OSDErasureSixRowsTest(ci_row_catalog.RowsTestCase):
    def test_six_osd_topology_selects_only_k4m2_erasure_scenarios(self):
        rows = [row for row in self.rows if row.get("osd_replicas") == "6"]
        base = {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container",
                "osd": "1", "osd_store": "bluestore", "osd_replicas": "6", "osd_profile": "replicated-pg-v8"}
        # The default optimized k=4,m=2 baseline is the only row without a
        # capability; every other row declares the six-OSD topology's own
        # exclusive capability and selects one exact k=4,m=2 test. A
        # shard-history row also declares the capture, and an interval row the
        # pginterval mode that extends it; the fixture admits both there.
        default = "^TestCephOSDErasureOptimizedK4M2Integration$"
        self.assertEqual([row for row in rows if "capabilities" not in row], [dict(base, test_run=default)])
        self.assertGreater(len(rows), 1)
        for row in rows:
            self.assertEqual({key: value for key, value in row.items() if key not in ("test_run", "capabilities")}, base)
            self.assertRegex(row["test_run"], r"^\^TestCephOSDErasureOptimizedK4M2[A-Za-z0-9]*Integration\$$")
            if row["test_run"] != default:
                self.assertIn(row["capabilities"], {"osd-erasure-width-six", "osd-erasure-width-six,osd-shard-history",
                                                    "osd-erasure-width-six,osd-pg-interval,osd-shard-history"})
            name = row["test_run"][1:-1]
            selector = re.compile(row["test_run"])
            self.assertEqual({test for test in self.declared if selector.search(test)}, {name})
        fixture = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        self.assertIn('erasureDefault: "^TestCephOSDErasureOptimizedK4M2Integration$"', fixture)
        self.assertIn('"OSD=1 OSD_REPLICAS=6"', fixture)


if __name__ == "__main__":
    unittest.main()
