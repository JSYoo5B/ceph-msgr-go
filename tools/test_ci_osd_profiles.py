"""Static admission checks for explicitly selected native OSD and MDS CI profiles.

The matrix rows come from .github/scenarios/catalog.json (standard library only,
for all three runner OSes); checked workflow text includes basic CI and the
reusable platform/native workflows. Workflow syntax is checked separately with the
available workflow/YAML tooling.
"""
from pathlib import Path
import json
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
SET_CHUNK_SELECTOR = "^TestCephOSDSetChunkNativeIntegration$"
SET_CHUNK_GO_SELECTOR = "^TestCephOSDSetChunkGoIntegration$"
COPY_FROM_LATE_ERROR_SELECTOR = "^TestCephOSDCopyFromLateErrorIntegration$"
COPY_FROM_MULTI_CHUNK_SELECTOR = "^TestCephOSDCopyFromMultiChunkLateErrorIntegration$"
COPY_FROM_HISTORY_SELECTOR = "^TestCephOSDCopyFromDestinationHistoryIntegration$"
COMPRESSION_SELECTOR = "^TestCephMsgrCompressionOfferIntegration$"
SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR = "^TestCephMonServiceMapLifecycleFailedMonHoldIntegration$"
SERVICE_MAP_LARGE_SELECTOR = "^TestCephMonServiceMapLargeMapIntegration$"
MON_MAP_VERSION_KILL_SELECTOR = "^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$"
APPLIED_NAMED_MGR_TELL_SELECTOR = "^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$"
APPLIED_CURRENT_MGR_TELL_SELECTOR = "^TestCephAppliedCurrentManagerTellIsNotReplayedIntegration$"
APPLIED_MGR_TELL_SECURE_SELECTOR = "^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$"
APPLIED_OSD_PG_COMMAND_SELECTOR = "^TestCephApplied(OSD|PG)CommandIsNotReplayedIntegration$"
APPLIED_MDS_COMMAND_SELECTOR = "^TestCephAppliedMDSCommandIsNotReplayedIntegration$"
SENT_TELL_CANCELLATION_SELECTOR = "^TestCephSentTellCancellationIntegration$"
OSD_COMMAND_OUTCOME_SELECTOR = "^TestCephOSDCommand(s|Crash)Integration$"
UNAVAILABLE_TARGET_SELECTOR = "^TestCeph(CompleteMonitorOutage|NamedMonitorTell|NamedManagerTell)Integration$"
RBD_MIRROR_SELECTOR = "^TestCephRBDMirrorPathRecoveryIntegration$"
RBD_JOURNAL_LOST_APPEND_SELECTOR = "^TestCephRBDJournalLostAppendReplyIntegration$"
RBD_LOCK_OWNER_LOSS_SELECTOR = "^TestCephRBDLockOwnerLossIntegration$"
RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR = "^TestCephRBDLockOwnerLossPendingIntegration$"
RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR = "^TestCephRBDJournalPrimaryChangeIntegration$"
# RBD rows that select one test alone on a fresh fixture.
RBD_EXCLUSIVE_SELECTORS = (RBD_JOURNAL_LOST_APPEND_SELECTOR, RBD_LOCK_OWNER_LOSS_SELECTOR, RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR,
                           RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR)


def ceph_rows():
    """The scenario matrix rows, from the catalog that drives the workflow."""
    document = json.loads((ROOT / ".github/scenarios/catalog.json").read_text(encoding="utf-8"))
    return [dict(entry["params"]) for entry in document["scenarios"]]


class NativeOSDCIProfilesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The matrix lives in the catalog; its job runs in the reusable workflow.
        cls.workflow = "\n".join((ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
                                 for name in ("ci.yml", "platform-checks.yml", "native-scenarios.yml"))
        cls.rows = ceph_rows()
        cls.declared = set()
        for source in (ROOT / "integration").glob("*_test.go"):
            cls.declared.update(re.findall(r"^func (Test\w+)\(", source.read_text(encoding="utf-8"), re.MULTILINE))

    def test_closed_profile_and_selector_reach_the_fixture(self):
        self.assertIn("CEPH_MSGR_TEST_OSD_PROFILE: ${{ matrix.osd_profile || 'legacy-v6' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_RUN: ${{ matrix.test_run || '' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_FRESH: ${{ matrix.mds_fresh || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_COMMANDS: ${{ matrix.mds_commands || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_SESSIONS: ${{ matrix.mds_sessions || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_CEPHFS: ${{ matrix.mds_cephfs || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS: ${{ matrix.mds_cephfs_ranks || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY: ${{ matrix.mds_cephfs_ranks_replay || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION: ${{ matrix.mds_cephfs_completion || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_MDS_CEPHFS_DATA: ${{ matrix.mds_cephfs_data || '0' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_RBD: ${{ matrix.rbd || '0' }}", self.workflow)
        # 63 original rows, 2 mgr-service, 3 erasure k2m2, 1 cache-tier-ops, 1 cache-op-go,
        # 2 mds-cephfs-capsend, 1 native SET_CHUNK checkpoint, 1 Go SET_CHUNK acceptance,
        # 2 mds-cephfs-writeback, 2 mds-cephfs-multi-reconnect,
        # 1 exclusive COPY_FROM2 late-error cell, 1 applied EC resubmission,
        # 1 exclusive multi-chunk COPY_FROM2 late-error cell, 1 CRC compression-offer row,
        # 1 zero-initial-MGR service bootstrap row, 1 primary PG-history calibration row,
        # 1 held stopped-MON dial ServiceMap lifecycle row, 1 RBD mirror path-recovery row,
        # 1 COPY_FROM2 destination PG-history control row,
        # 1 applied exact-name MGR Tell row with a withheld reply,
        # 1 applied current-manager MGR Tell row, 1 secure row for both MGR Tell cells,
        # 1 applied OSD and PG command row and 1 applied MDS command row,
        # 2 MapVersion connected-MON SIGKILL rows, 2 large ServiceMap rows,
        # 2 mds-cephfs-heldflock-split-reconnect rows,
        # 2 mds-cephfs-sharedflock-recovery rows,
        # 2 mds-cephfs-heldflock-split-fragments rows,
        # 2 RBD journal lost-append-reply rows,
        # 2 RBD lock-owner-loss rows, 2 RBD pending lock-owner-loss rows,
        # 2 RBD journal primary-change rows,
        # 1 secure applied OSD and PG command row, 1 secure applied MDS command row,
        # 2 sent Tell cancellation rows,
        # 2 OSD and PG command outcome rows, 2 unavailable MON and MGR target rows
        # and 6 optimized EC shard-history rows (COPY_FROM2, a partial overwrite,
        # a later write after it, an interval change after it, and an outage of
        # the left-out shard's OSD after it without and with a degraded write).
        self.assertEqual(len(self.rows), 124)
        opt_ins = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "mon_ping", "mgr_service", "osd_cache_pools")
        # Dedicated legacy-v6 rows select one test explicitly; the
        # original broad legacy rows keep their suite selection.
        dedicated = {SET_CHUNK_SELECTOR, SET_CHUNK_GO_SELECTOR, COPY_FROM_LATE_ERROR_SELECTOR, COPY_FROM_MULTI_CHUNK_SELECTOR,
                     COPY_FROM_HISTORY_SELECTOR, COMPRESSION_SELECTOR, SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR, RBD_MIRROR_SELECTOR,
                     APPLIED_NAMED_MGR_TELL_SELECTOR, APPLIED_CURRENT_MGR_TELL_SELECTOR, APPLIED_MGR_TELL_SECURE_SELECTOR,
                     APPLIED_OSD_PG_COMMAND_SELECTOR, SERVICE_MAP_LARGE_SELECTOR, MON_MAP_VERSION_KILL_SELECTOR,
                     SENT_TELL_CANCELLATION_SELECTOR, OSD_COMMAND_OUTCOME_SELECTOR, UNAVAILABLE_TARGET_SELECTOR}
        legacy = [row for row in self.rows if row.get("osd_profile", "legacy-v6") == "legacy-v6"
                  and not row.get("osd_soak") and row.get("test_run") not in dedicated
                  and all(row.get(field, "0") == "0" for field in opt_ins)]
        self.assertEqual(len(legacy), 30)
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
            if profile == "legacy-v6" and all(row.get(field, "0") == "0" for field in opt_ins) and row.get("test_run") not in dedicated:
                self.assertNotIn("test_run", row, "existing rows must retain their original suite selection")
                self.assertNotIn("mds_fresh", row)
                for field in opt_ins:
                    self.assertNotIn(field, row)

    def client_tests(self):
        client = set()
        for source in (ROOT / "cephmsgr").glob("*_test.go"):
            client.update(re.findall(r"^func (Test\w+)\(", source.read_text(encoding="utf-8"), re.MULTILINE))
        return client

    def assert_client_row(self, selector, params, tests, *further):
        # One row with exactly these parameters, and one for each further
        # profile, whose selector matches exactly these client-suite tests and
        # no API-suite test, so the result gate requires each of them to PASS.
        rows = [row for row in self.rows if row.get("test_run") == selector]
        self.assertEqual(rows, [dict(profile, test_run=selector) for profile in (params,) + further])
        compiled = re.compile(selector)
        self.assertEqual({name for name in self.client_tests() | self.declared if compiled.search(name)}, tests)
        self.assertFalse(tests & self.declared)

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
        self.assertIn("CEPH_MSGR_TEST_OSD_SOAK: ${{ matrix.osd_soak || '' }}", self.workflow)
        rows = [row for row in self.rows if row.get("osd_soak")]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "4", "stress": "",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "3", "osd_soak": "4m",
        }])
        self.assertTrue({"TestCephOSDSoakIntegration", "TestCephOSDSoakFailoverIntegration"} <= self.declared)


if __name__ == "__main__":
    unittest.main()
