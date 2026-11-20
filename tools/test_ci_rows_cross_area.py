"""Cross-area admission checks of the native CI matrix.

Shared loading lives in ci_row_catalog. These tests check what spans the
areas: the closed profile and opt-in fields of every row with their workflow
bindings, the broad legacy-v6 rows, the bounded and retried infrastructure
steps, and the separate soak opt-in. The dedicated rows of each area are
checked in that area's test_ci_rows_* module; this module names none of them.
"""
import re
import unittest

import ci_row_catalog


class CrossAreaRowsTest(ci_row_catalog.RowsTestCase):
    def test_closed_profile_and_selector_reach_the_fixture(self):
        self.assert_forwarded("osd_profile", "CEPH_MSGR_TEST_OSD_PROFILE", "legacy-v6")
        self.assert_forwarded("test_run", "CEPH_MSGR_TEST_RUN", "")
        self.assert_forwarded("capabilities", "CEPH_MSGR_TEST_CAPABILITIES", "")
        self.assert_forwarded("mds_fresh", "CEPH_MSGR_TEST_MDS_FRESH", "0")
        self.assert_forwarded("mds_commands", "CEPH_MSGR_TEST_MDS_COMMANDS", "0")
        self.assert_forwarded("mds_sessions", "CEPH_MSGR_TEST_MDS_SESSIONS", "0")
        self.assert_forwarded("mds_cephfs", "CEPH_MSGR_TEST_MDS_CEPHFS", "0")
        self.assert_forwarded("mds_cephfs_ranks", "CEPH_MSGR_TEST_MDS_CEPHFS_RANKS", "0")
        self.assert_forwarded("mds_cephfs_ranks_replay", "CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY", "0")
        self.assert_forwarded("mds_cephfs_completion", "CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION", "0")
        self.assert_forwarded("mds_cephfs_data", "CEPH_MSGR_TEST_MDS_CEPHFS_DATA", "0")
        self.assert_forwarded("rbd", "CEPH_MSGR_TEST_RBD", "0")
        opt_ins = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "mon_ping", "mgr_service", "osd_cache_pools")
        # A row selects tests explicitly exactly when its identifier says so
        # (a final ".t-" component), so a broad row cannot gain a selector
        # without a visible identifier change. The broad legacy rows keep
        # their suite selection; no list of the dedicated rows or count of
        # the broad ones is kept here.
        for entry in self.entries:
            self.assertEqual("test_run" in entry["params"], re.search(r"\.t-[a-z0-9-]+$", entry["id"]) is not None, entry["id"])
        for row in self.rows:
            profile = row.get("osd_profile", "legacy-v6")
            self.assertIn(profile, {"legacy-v6", "replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"})
            self.assertIn(row.get("mds_fresh", "0"), {"0", "1"})
            for field in opt_ins:
                self.assertIn(row.get(field, "0"), {"0", "1"})
            self.assertLessEqual(sum(row.get(field, "0") == "1" for field in opt_ins), 1)
            # The standby-replay extension is a sub-opt-in of the multi-rank profile.
            self.assertIn(row.get("mds_cephfs_ranks_replay", "0"), {"0", "1"})
            if row.get("mds_cephfs_ranks_replay", "0") == "1":
                self.assertEqual(row.get("mds_cephfs_ranks"), "1")
            if profile == "legacy-v6" and all(row.get(field, "0") == "0" for field in opt_ins) and "test_run" not in row:
                self.assertNotIn("mds_fresh", row)
                for field in opt_ins:
                    self.assertNotIn(field, row)

    def test_infrastructure_steps_are_bounded_and_retried(self):
        # The reaper pre-pull precedes both jobs that start Testcontainers clusters.
        for job in ("\n  race:\n", "\n  ceph:\n"):
            body = self.workflow.split(job, 1)[1]
            pull = body.index("name: Pre-pull the Testcontainers reaper image")
            run = body.index("sh integration/run.sh")
            self.assertLess(pull, run)
            self.assertIn("docker pull testcontainers/ryuk:", body[pull:run])
            self.assertIn("for attempt in", body[pull:run])
        windows = self.workflow.split("\n  windows-ceph:\n", 1)[1].split("\n  ceph:\n", 1)[0]
        step = windows.split("name: Import dedicated WSL2 development fixture", 1)[1].split("\n      - name:", 1)[0]
        self.assertIn("timeout-minutes: 12", step)
        self.assertIn("-TimeoutSec", step)
        self.assertIn("WaitForExit(240000)", step)
        self.assertIn("$attempt", step)
        self.assertIn("$expected", step)

    def test_soak_row_keeps_its_separate_opt_in_and_original_profiles(self):
        self.assert_forwarded("osd_soak", "CEPH_MSGR_TEST_OSD_SOAK", "")
        rows = [row for row in self.rows if row.get("osd_soak")]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "4", "stress": "",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "3", "osd_soak": "4m",
        }])
        self.assertTrue({"TestCephOSDSoakIntegration", "TestCephOSDSoakFailoverIntegration"} <= self.declared)


if __name__ == "__main__":
    unittest.main()
