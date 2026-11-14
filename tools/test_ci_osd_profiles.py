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
        # 1 secure applied OSD and PG command row, 1 secure applied MDS command row and
        # 2 sent Tell cancellation rows.
        self.assertEqual(len(self.rows), 114)
        opt_ins = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "mon_ping", "mgr_service", "osd_cache_pools")
        # Dedicated legacy-v6 rows select one test explicitly; the
        # original broad legacy rows keep their suite selection.
        dedicated = {SET_CHUNK_SELECTOR, SET_CHUNK_GO_SELECTOR, COPY_FROM_LATE_ERROR_SELECTOR, COPY_FROM_MULTI_CHUNK_SELECTOR,
                     COPY_FROM_HISTORY_SELECTOR, COMPRESSION_SELECTOR, SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR, RBD_MIRROR_SELECTOR,
                     APPLIED_NAMED_MGR_TELL_SELECTOR, APPLIED_CURRENT_MGR_TELL_SELECTOR, APPLIED_MGR_TELL_SECURE_SELECTOR,
                     APPLIED_OSD_PG_COMMAND_SELECTOR, SERVICE_MAP_LARGE_SELECTOR, MON_MAP_VERSION_KILL_SELECTOR,
                     SENT_TELL_CANCELLATION_SELECTOR}
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

    def test_mon_kill_and_large_service_map_rows_are_exclusive(self):
        # The MON SIGKILL cells run on CRC only (the relay decodes CRC frames)
        # with one BlueStore OSD; the large ServiceMap cell needs no OSD.
        rows = [row for row in self.rows if row.get("test_run") == MON_MAP_VERSION_KILL_SELECTOR]
        self.assertEqual(rows, [
            {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host", "osd": "1",
             "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6", "test_run": MON_MAP_VERSION_KILL_SELECTOR},
            {"key": "aes256k", "service": "aes256k", "mode": "crc", "family": "6", "stress": "", "runtime": "container", "osd": "1",
             "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6", "test_run": MON_MAP_VERSION_KILL_SELECTOR}])
        rows = [row for row in self.rows if row.get("test_run") == SERVICE_MAP_LARGE_SELECTOR]
        self.assertEqual(rows, [
            {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "test_run": SERVICE_MAP_LARGE_SELECTOR},
            {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
             "test_run": SERVICE_MAP_LARGE_SELECTOR}])

    def test_service_map_lifecycle_hold_row_is_exclusive(self):
        rows = [row for row in self.rows if row.get("test_run") == SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR]
        self.assertEqual(rows, [{"key": "aes256k", "service": "aes256k", "family": "4", "stress": "",
                                 "test_run": SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR}])
        client = set()
        for source in (ROOT / "cephmsgr").glob("*_test.go"):
            client.update(re.findall(r"^func (Test\w+)\(", source.read_text(encoding="utf-8"), re.MULTILINE))
        selector = re.compile(SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR)
        self.assertEqual({name for name in client | self.declared if selector.search(name)},
                         {"TestCephMonServiceMapLifecycleFailedMonHoldIntegration"})

    def test_applied_named_mgr_tell_row_requires_its_client_test(self):
        # The row is the broad AES/CRC IPv4 host mon-mgr profile narrowed to one
        # client-suite test, so the result gate requires that parent to PASS. The
        # test needs no opt-in parameter and also runs in broad CRC rows.
        rows = [row for row in self.rows if row.get("test_run") == APPLIED_NAMED_MGR_TELL_SELECTOR]
        self.assertEqual(rows, [{"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "",
                                 "runtime": "host", "test_run": APPLIED_NAMED_MGR_TELL_SELECTOR}])
        client = set()
        for source in (ROOT / "cephmsgr").glob("*_test.go"):
            client.update(re.findall(r"^func (Test\w+)\(", source.read_text(encoding="utf-8"), re.MULTILINE))
        selector = re.compile(APPLIED_NAMED_MGR_TELL_SELECTOR)
        self.assertEqual({name for name in client | self.declared if selector.search(name)},
                         {"TestCephAppliedNamedManagerTellIsNotReplayedIntegration"})
        self.assertNotIn("TestCephAppliedNamedManagerTellIsNotReplayedIntegration", self.declared)

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

    def test_applied_current_mgr_tell_row_requires_its_client_test(self):
        # The same AES/CRC IPv4 host mon-mgr profile as the exact-name row,
        # narrowed to the current-manager cell.
        self.assert_client_row(APPLIED_CURRENT_MGR_TELL_SELECTOR,
                               {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host"},
                               {"TestCephAppliedCurrentManagerTellIsNotReplayedIntegration"})

    def test_applied_mgr_tell_secure_row_requires_both_client_tests(self):
        # One secure AES256K IPv4 host mon-mgr fixture runs both MGR Tell cells
        # through their mode without decoded frames.
        self.assert_client_row(APPLIED_MGR_TELL_SECURE_SELECTOR,
                               {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "host"},
                               {"TestCephAppliedNamedManagerTellIsNotReplayedIntegration",
                                "TestCephAppliedCurrentManagerTellIsNotReplayedIntegration"})

    def test_applied_osd_and_pg_command_rows_require_both_client_tests(self):
        # The broad legacy OSD selection never matches these names, so one
        # CRC and one secure single-BlueStore legacy-v6 row select both cells
        # explicitly. The secure row has no mode parameter: frames are not
        # decoded there.
        self.assert_client_row(APPLIED_OSD_PG_COMMAND_SELECTOR,
                               {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
                                "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6"},
                               {"TestCephAppliedOSDCommandIsNotReplayedIntegration",
                                "TestCephAppliedPGCommandIsNotReplayedIntegration"},
                               {"key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
                                "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6"})
        broad = re.compile("^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$")
        self.assertIsNone(broad.search("TestCephAppliedOSDCommandIsNotReplayedIntegration"))
        self.assertIsNone(broad.search("TestCephAppliedPGCommandIsNotReplayedIntegration"))

    def test_applied_mds_command_rows_reuse_the_command_fixture_with_their_own_test(self):
        # The owned MDS command fixture admits this one further exact selector,
        # in the CRC and the secure profile of the public-command rows.
        self.assert_client_row(APPLIED_MDS_COMMAND_SELECTOR,
                               {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
                                "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
                                "mds_fresh": "1", "mds_commands": "1"},
                               {"TestCephAppliedMDSCommandIsNotReplayedIntegration"},
                               {"key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
                                "osd": "1", "osd_store": "memstore", "osd_replicas": "1", "osd_profile": "legacy-v6",
                                "mds_fresh": "1", "mds_commands": "1"})
        public = [{name: value for name, value in row.items() if name != "test_run"}
                  for row in self.rows if row.get("test_run") == "^TestCephMDSCommandsIntegration$"]
        applied = [{name: value for name, value in row.items() if name != "test_run"}
                   for row in self.rows if row.get("test_run") == APPLIED_MDS_COMMAND_SELECTOR]
        self.assertEqual(applied, public)
        profile = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        outcome = (ROOT / "integration" / "fixture" / "daemon_command_outcome.go").read_text(encoding="utf-8")
        self.assertIn("if p.testRun == daemonCommandMDSSelector {", profile)
        self.assertIn('daemonCommandMDSSelector = "%s"' % APPLIED_MDS_COMMAND_SELECTOR, " ".join(outcome.split()))

    def test_sent_tell_cancellation_rows_require_their_exclusive_client_test(self):
        # The cell ends MON and MGR sessions on purpose and skips without its
        # exact selector, so one CRC and one secure mon-mgr row select it.
        self.assert_client_row(SENT_TELL_CANCELLATION_SELECTOR,
                               {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host"},
                               {"TestCephSentTellCancellationIntegration"},
                               {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "host"})
        source = (ROOT / "cephmsgr" / "integration_sent_tell_cancellation_test.go").read_text(encoding="utf-8")
        self.assertIn('sentTellCancellationSelector = "%s"' % SENT_TELL_CANCELLATION_SELECTOR, source)
        self.assertIn("selected.Value.String() != sentTellCancellationSelector", source)

    def test_mon_ping_rows_are_exclusive_and_reach_the_native_fixture(self):
        self.assertIn("CEPH_MSGR_TEST_MON_PING: ${{ matrix.mon_ping || '0' }}", self.workflow)
        rows = [row for row in self.rows if row.get("mon_ping") == "1"]
        self.assertEqual(len(rows), 2)
        observed = set()
        for row in rows:
            self.assertEqual(row["stress"], "")
            for field in ("osd", "mds_fresh", "rbd", "image", "race", "expiry", "idle", "auth_epoch", "short_tickets", "mode_rejection", "mapped", "native_mgr_preparation"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            expected = "TestCephMonPingIntegration"
            self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
            self.assertIsNone(selector.search(expected + "Extended"))
            self.assertIsNone(selector.search("Prefix" + expected))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
        self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})

    def test_copyfrom_late_error_row_is_exclusive_single_bluestore(self):
        rows = [row for row in self.rows if row.get("test_run") == COPY_FROM_LATE_ERROR_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": COPY_FROM_LATE_ERROR_SELECTOR,
        }])
        expected = "TestCephOSDCopyFromLateErrorIntegration"
        selector = re.compile(COPY_FROM_LATE_ERROR_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephOSDCopyFromCompoundIntegration", "TestCephOSDCopyFromIntegration"):
            self.assertIsNone(selector.search(unrelated))

    def test_rbd_mirror_row_is_exclusive_single_bluestore_ipv4(self):
        # The fixture derives its destination cluster and mirror daemon from
        # this exact selector on the single-BlueStore IPv4 fixture; it needs no
        # workflow opt-in and does not use the RBD base opt-in.
        rows = [row for row in self.rows if row.get("test_run") == RBD_MIRROR_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": RBD_MIRROR_SELECTOR,
        }])
        expected = "TestCephRBDMirrorPathRecoveryIntegration"
        selector = re.compile(RBD_MIRROR_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestRBDMirrorRequiresExclusiveMarker", "TestCephRBDBaseIntegration"):
            self.assertIsNone(selector.search(unrelated))

    def test_copyfrom_multi_chunk_row_is_separate_and_exclusive(self):
        # The multi-chunk cell adds a row; it does not replace the
        # single-chunk late-error row checked above.
        rows = [row for row in self.rows if row.get("test_run") == COPY_FROM_MULTI_CHUNK_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": COPY_FROM_MULTI_CHUNK_SELECTOR,
        }])
        expected = "TestCephOSDCopyFromMultiChunkLateErrorIntegration"
        selector = re.compile(COPY_FROM_MULTI_CHUNK_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephOSDCopyFromLateErrorIntegration",
                          "TestCopyFromMultiChunkLateErrorRequiresExclusiveMarker"):
            self.assertIsNone(selector.search(unrelated))

    def test_copyfrom_destination_history_row_selects_only_its_control(self):
        # The successful-copy PG-history control uses the ordinary OSD fixture
        # and the owned-pool oracle. It needs no opt-in parameter, injects no
        # fault and does not replace either late-error row.
        rows = [row for row in self.rows if row.get("test_run") == COPY_FROM_HISTORY_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": COPY_FROM_HISTORY_SELECTOR,
        }])
        expected = "TestCephOSDCopyFromDestinationHistoryIntegration"
        selector = re.compile(COPY_FROM_HISTORY_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephOSDCopyFromDestinationCrashIntegration",
                          "TestCopyFromDestinationHistoryBinding", "TestCephOSDCopyFromMultiChunkLateErrorIntegration"):
            self.assertIsNone(selector.search(unrelated))
        for selector in (COPY_FROM_LATE_ERROR_SELECTOR, COPY_FROM_MULTI_CHUNK_SELECTOR):
            self.assertEqual(len([row for row in self.rows if row.get("test_run") == selector]), 1)

    def test_cache_pool_rows_require_explicit_opt_in_and_only_the_declared_test(self):
        self.assertIn("CEPH_MSGR_TEST_OSD_CACHE_POOLS: ${{ matrix.osd_cache_pools || '0' }}", self.workflow)
        self.assertIn("${{ matrix.osd_cache_pools == '1' && 'OSD-cache-pools ' || '' }}", self.workflow)
        rows = [row for row in self.rows if row.get("osd_cache_pools") == "1"]
        common = {
            "stress": "", "osd": "1", "osd_store": "bluestore", "osd_replicas": "1",
            "osd_profile": "legacy-v6", "osd_cache_pools": "1",
            "test_run": "^TestCephOSDIgnoreCacheIntegration$",
        }
        # The native-only tier-ops checkpoint and the Go cache-op test reuse
        # the same fixture opt-in with their own exclusive selectors; no new
        # workflow parameter exists.
        self.assertEqual(rows, [
            dict(common, key="aes", service="aes", mode="crc", family="4", runtime="host"),
            dict(common, key="aes256k", service="aes256k", family="6", runtime="container"),
            dict(common, key="aes256k", service="aes256k", family="6", runtime="container",
                 test_run="^TestCephOSDCacheTierOpsIntegration$"),
            dict(common, key="aes256k", service="aes256k", family="6", runtime="container",
                 test_run="^TestCephOSDCacheOpGoIntegration$"),
        ])
        for row in rows:
            expected = re.fullmatch(r"\^(\w+)\$", row["test_run"]).group(1)
            self.assertIn(expected, {"TestCephOSDIgnoreCacheIntegration", "TestCephOSDCacheTierOpsIntegration", "TestCephOSDCacheOpGoIntegration"})
            self.assertIn(expected, self.declared)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
            for unrelated in (expected + "Extended", "Prefix" + expected,
                              "TestCephOSDIOIntegration", "TestCephOSDReplicatedPGIntegration",
                              "TestCephMonPingIntegration", "TestCephMDSFreshIntegration"):
                self.assertIsNone(selector.search(unrelated))

    def test_mgr_service_rows_require_explicit_opt_in_and_only_the_declared_test(self):
        self.assertEqual(self.workflow.count("CEPH_MSGR_TEST_MGR_SERVICE: ${{ matrix.mgr_service || '0' }}"), 1)
        self.assertIn("${{ matrix.mgr_service == '1' && 'MGR-service ' || '' }}", self.workflow)
        rows = [row for row in self.rows if row.get("mgr_service") == "1"]
        common = {
            "stress": "", "osd": "1", "osd_store": "memstore", "osd_replicas": "1",
            "osd_profile": "legacy-v6", "mgr_service": "1",
            "test_run": "^TestCephMgrServiceRegistrationIntegration$",
        }
        self.assertEqual(rows, [
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

    def test_v8_rows_cover_both_security_store_and_address_profiles(self):
        for profile in ("replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"):
            with self.subTest(profile=profile):
                rows = [row for row in self.rows if row.get("osd_profile") == profile and row.get("mds_fresh", "0") == "0"
                        and row.get("osd_replicas") == "1"]
                self.assertEqual(len(rows), 2)
                observed = set()
                for row in rows:
                    self.assertEqual((row["osd"], row["osd_replicas"], row["stress"]), ("1", "1", ""))
                    self.assertNotIn("race", row)
                    self.assertNotIn("image", row, "v8 acceptance must retain the pinned Tentacle image")
                    observed.add(tuple(row.get(field, fallback) for field, fallback in (
                        ("key", ""), ("service", ""), ("mode", "secure"),
                        ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
                self.assertEqual(observed, {
                    ("aes", "aes", "crc", "4", "host", "bluestore"),
                    ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
                })

    def test_v8_selector_names_existing_acceptance_tests_and_excludes_unready_paths(self):
        common = {
            "TestCephOSDReplicatedPGIntegration", "TestCephOSDPGListV8Integration",
            "TestCephOSDBackoffIntegration", "TestCephOSDBackoffLateAckIntegration",
            "TestCephOSDWatchReconnectIntegration", "TestCephOSDSplitIntegration",
            "TestCephOSDSplitResubmitIntegration", "TestCephMDSMapsIntegration",
        }
        reply_v8 = "TestCephOSDReplyV8Integration"
        added = {"TestCephOSDEnforceSnapContextIntegration", "TestCephOSDChecksumCompoundIntegration"}
        self.assertTrue(common | {reply_v8} | added <= self.declared)
        checked_rows = 0
        for row in self.rows:
            profile = row.get("osd_profile")
            if (profile not in {"replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"} or row.get("mds_fresh", "0") != "0"
                    or row.get("osd_replicas") != "1"):
                continue
            expected = common | ({reply_v8} if profile == "replicated-pg-reply-v8" else set())
            if profile == "replicated-pg-request-v9-reply-v8":
                expected = (common - {"TestCephMDSMapsIntegration"}) | {
                    reply_v8, "TestCephOSDRequestV9Integration",
                    "TestCephOSDPGListFilterIntegration", "TestCephOSDRedirectIntegration",
                    "TestCephOSDFadviseIntegration", "TestCephOSDWatchDisconnectIntegration",
                    "TestCephOSDWatchBlocklistIntegration", "TestCephOSDWatchExpiredReconnectIntegration",
                    "TestCephOSDWatchNotifyRedeliveryIntegration", "TestCephOSDWatchPermissionsIntegration",
                }
            previous = expected
            expected = previous | added
            selector = re.compile(row["test_run"])
            selected = {name for name in self.declared if selector.search(name)}
            self.assertEqual(selected, expected)
            self.assertEqual(selected - previous, added)
            self.assertFalse(previous - selected, "modern selector dropped an existing acceptance test")
            checked_rows += 1
            for unready in ("TestCephOSDSparseReadIntegration", "TestCephOSDPGListIntegration",
                            "TestCephOSDMapExtIntegration", "TestCephOSDChecksumIntegration"):
                self.assertIsNone(selector.search(unready))
            # Go also applies -run to subtest path components. Keep unrelated
            # names and similarly prefixed future tests outside this opt-in.
            for name in expected:
                self.assertIsNone(selector.search(name + "Extended"))
                self.assertIsNone(selector.search("Prefix" + name))
        self.assertEqual(checked_rows, 6)

    def test_three_replica_v8_rows_select_only_their_closed_acceptance(self):
        rows = [row for row in self.rows if row.get("osd_profile") in {"replicated-pg-v8", "replicated-pg-reply-v8"}
                and row.get("osd_replicas") == "3"]
        self.assertEqual(len(rows), 4)
        expected = {"TestCephOSDObjectBackoffFailoverIntegration", "TestCephOSDUpmapIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = {name: set() for name in expected}
        counts = {name: 0 for name in expected}
        for row in rows:
            self.assertEqual((row["osd_profile"], row["osd"], row["osd_store"], row["stress"]),
                             ("replicated-pg-v8", "1", "bluestore", ""))
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            self.assertNotIn("mds_fresh", row)
            selector = re.compile(row["test_run"])
            selected = {name for name in self.declared if selector.search(name)}
            self.assertEqual(len(selected), 1)
            self.assertTrue(selected <= expected)
            name = next(iter(selected))
            self.assertIsNone(selector.search(name + "Extended"))
            self.assertIsNone(selector.search("Prefix" + name))
            counts[name] += 1
            observed[name].add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
        for name in expected:
            with self.subTest(test=name):
                self.assertEqual(counts[name], 2)
                self.assertEqual(observed[name], {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})

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
        ])
        # Each row's selector must match the fixture's closed four-OSD list.
        fixture = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        for row, expected in zip(rows, ({"TestCephOSDErasureK2M2Integration"},
                                        {"TestCephOSDErasureOptimizedK2M2Integration", "TestCephOSDErasureOptimizedK2M2FailoverIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration",
                                         "TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration"})):
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

    def test_compression_row_selects_only_the_crc_single_osd_test(self):
        rows = [row for row in self.rows if row.get("test_run") == COMPRESSION_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": COMPRESSION_SELECTOR,
        }])
        selector = re.compile(COMPRESSION_SELECTOR)
        expected = "TestCephMsgrCompressionOfferIntegration"
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for other in (expected + "Extended", "Prefix" + expected, "TestCephOSDIOIntegration",
                      "TestCephIntegration", "TestCephMgrServiceRegistrationIntegration"):
            self.assertIsNone(selector.search(other))

    def test_set_chunk_row_selects_only_the_native_checkpoint(self):
        # The native-only SET_CHUNK checkpoint needs only the ordinary OSD
        # fixture and the owned-pool oracle; it has no opt-in parameter.
        rows = [row for row in self.rows if row.get("test_run") == SET_CHUNK_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": SET_CHUNK_SELECTOR,
        }])
        selector = re.compile(SET_CHUNK_SELECTOR)
        expected = "TestCephOSDSetChunkNativeIntegration"
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for other in (expected + "Extended", "Prefix" + expected, "TestCephOSDRedirectIntegration", "TestCephOSDCacheTierOpsIntegration"):
            self.assertIsNone(selector.search(other))

    def test_set_chunk_go_row_selects_only_the_go_acceptance(self):
        # The Go-originated SET_CHUNK acceptance also uses only the ordinary
        # OSD fixture and the owned-pool oracle; it has no opt-in parameter.
        rows = [row for row in self.rows if row.get("test_run") == SET_CHUNK_GO_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": SET_CHUNK_GO_SELECTOR,
        }])
        selector = re.compile(SET_CHUNK_GO_SELECTOR)
        expected = "TestCephOSDSetChunkGoIntegration"
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for other in (expected + "Extended", "Prefix" + expected, "TestCephOSDSetChunkNativeIntegration", "TestCephOSDCacheOpGoIntegration"):
            self.assertIsNone(selector.search(other))

    def test_fresh_mds_rows_require_explicit_opt_in_and_only_their_declared_test(self):
        rows = [row for row in self.rows if row.get("mds_fresh") == "1"
                and row.get("mds_commands", "0") == "0" and row.get("mds_sessions", "0") == "0"
                and row.get("mds_cephfs", "0") == "0" and row.get("mds_cephfs_ranks", "0") == "0"
                and row.get("mds_cephfs_completion", "0") == "0" and row.get("mds_cephfs_data", "0") == "0"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSFreshIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("replicated-pg-reply-v8", "1", "1", ""))
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSFreshIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSFreshIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_command_rows_select_only_the_declared_public_command_test(self):
        # The applied-command rows share this fixture with their own selector
        # and are pinned by their own test.
        opted = [row for row in self.rows if row.get("mds_commands") == "1"]
        rows = [row for row in opted if row.get("test_run") != APPLIED_MDS_COMMAND_SELECTOR]
        self.assertEqual((len(opted), len(rows)), (4, 2))
        expected = {"TestCephMDSCommandsIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCommandsIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCommandsIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_session_rows_select_only_the_declared_session_test(self):
        rows = [row for row in self.rows if row.get("mds_sessions") == "1"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSSessionsIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            self.assertNotIn("mds_commands", row)
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSSessionsIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSSessionsIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_rows_select_only_the_declared_cephfs_tests(self):
        cephfs = [row for row in self.rows if row.get("mds_cephfs") == "1"]
        self.assertEqual(len(cephfs), 4)
        # The shared held-lock recovery rows reuse the metadata fixture opt-in
        # with their own selector; they are checked separately below.
        rows = [row for row in cephfs if row["test_run"] != "^TestCephMDSCephFSSharedFlockRecoveryIntegration$"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSIntegration", "TestCephMDSCephFSPushIntegration", "TestCephMDSCephFSRecoveryIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            self.assertNotIn("mds_commands", row)
            self.assertNotIn("mds_sessions", row)
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRecoveryIntegrationExtended"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRecoveryRecoveryIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSPushIntegrationExtended"))
            self.assertIsNone(selector.search("TestCephMDSCephFSPushRecoveryIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRanksIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSCompletionIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSDataIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_ranks_rows_select_only_the_declared_multi_rank_test(self):
        rows = [row for row in self.rows if row.get("mds_cephfs_ranks") == "1" and row.get("mds_cephfs_ranks_replay", "0") == "0"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSRanksIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            self.assertNotIn("mds_commands", row)
            self.assertNotIn("mds_sessions", row)
            self.assertNotIn("mds_cephfs", row)
            self.assertNotIn("image", row)
            self.assertNotIn("race", row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSRanksIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSRanksIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRanksReplayIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_ranks_replay_rows_select_only_the_declared_standby_replay_test(self):
        rows = [row for row in self.rows if row.get("mds_cephfs_ranks_replay") == "1"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSRanksReplayIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["mds_cephfs_ranks"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "1", "legacy-v6", "1", "1", ""))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSRanksReplayIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSRanksReplayIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRanksIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_completion_rows_select_only_the_declared_completion_test(self):
        rows = [row for row in self.rows if row.get("mds_cephfs_completion") == "1" and row["test_run"] == "^TestCephMDSCephFSCompletionIntegration$"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSCompletionIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_data", "rbd", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSCompletionIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSCompletionIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSRecoveryIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_sharedflock_recovery_rows_select_only_the_dedicated_acceptance(self):
        test = "TestCephMDSCephFSSharedFlockRecoveryIntegration"
        selector_text = "^" + test + "$"
        rows = [row for row in self.rows if row.get("test_run") == selector_text]
        self.assertEqual(len(rows), 2)
        catalog = json.loads((ROOT / ".github/scenarios/catalog.json").read_text(encoding="utf-8"))["scenarios"]
        self.assertEqual({entry["group"] for entry in catalog if entry["params"].get("test_run") == selector_text}, {"mds-cephfs-sharedflock-recovery"})
        self.assertIn(test, self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_cephfs"], row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]), ("1", "1", "legacy-v6", "1", "1", ""))
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, {test})
            self.assertIsNone(selector.search(test + "Extended"))
            self.assertIsNone(selector.search("Prefix" + test))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "race", "image"):
                self.assertNotIn(field, row)
            observed.add(tuple(row.get(field, fallback) for field, fallback in (("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {("aes", "aes", "crc", "4", "host", "bluestore"), ("aes256k", "aes256k", "secure", "6", "container", "memstore")})
        # The broad metadata selector does not reach the shared variant.
        self.assertIsNone(re.compile("^TestCephMDSCephFS(Push|Recovery)?Integration$").search(test))

    def test_mds_multi_reconnect_rows_select_only_the_dedicated_acceptance(self):
        self.check_exclusive_completion_rows("TestCephMDSCephFSMultiReconnectIntegration", "mds-cephfs-multi-reconnect")

    def test_mds_heldflock_split_reconnect_rows_select_only_the_dedicated_acceptance(self):
        self.check_exclusive_completion_rows("TestCephMDSCephFSHeldFlockSplitReconnectIntegration", "mds-cephfs-heldflock-split-reconnect")
        # Neither split selector reaches the other acceptance.
        self.assertIsNone(re.compile("^TestCephMDSCephFSMultiReconnectIntegration$").search("TestCephMDSCephFSHeldFlockSplitReconnectIntegration"))

    def test_mds_heldflock_split_fragments_rows_select_only_the_dedicated_acceptance(self):
        test = "TestCephMDSCephFSHeldFlockSplitFragmentsIntegration"
        self.check_exclusive_completion_rows(test, "mds-cephfs-heldflock-split-fragments")
        # None of the three split selectors reaches another acceptance.
        tests = ("TestCephMDSCephFSMultiReconnectIntegration", "TestCephMDSCephFSHeldFlockSplitReconnectIntegration", test)
        for selected in tests:
            selector = re.compile("^" + selected + "$")
            self.assertEqual([name for name in tests if selector.search(name)], [selected])

    def check_exclusive_completion_rows(self, test, group):
        selector_text = "^" + test + "$"
        rows = [row for row in self.rows if row.get("test_run") == selector_text]
        self.assertEqual(len(rows), 2)
        catalog = json.loads((ROOT / ".github/scenarios/catalog.json").read_text(encoding="utf-8"))["scenarios"]
        self.assertEqual({entry["group"] for entry in catalog if entry["params"].get("test_run") == selector_text}, {group})
        expected = {test}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_cephfs_completion"], row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]), ("1", "1", "legacy-v6", "1", "1", ""))
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            for name in expected:
                self.assertIsNone(selector.search(name + "Extended"))
                self.assertIsNone(selector.search("Prefix" + name))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_data", "rbd", "race", "image"):
                self.assertNotIn(field, row)
            observed.add(tuple(row.get(field, fallback) for field, fallback in (("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {("aes", "aes", "crc", "4", "host", "bluestore"), ("aes256k", "aes256k", "secure", "6", "container", "memstore")})

    def test_mds_cephfs_data_rows_select_only_the_declared_data_test(self):
        data = [row for row in self.rows if row.get("mds_cephfs_data") == "1"]
        self.assertEqual(len(data), 6)
        # The cap-send recovery and writeback-fence rows reuse the owned data
        # fixture opt-in with their own selectors; they are checked separately below.
        rows = [row for row in data if row["test_run"] not in (
            "^TestCephMDSCephFSCapSendRecoveryIntegration$", "^TestCephMDSCephFSWritebackFenceIntegration$")]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSDataIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["mds_fresh"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "legacy-v6", "1", "1", ""))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "rbd", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSDataIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSDataIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSCompletionIntegration"))
            self.assertIsNone(selector.search("TestCephFSStriperRestatement"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_capsend_rows_reuse_the_data_fixture_with_their_own_test(self):
        rows = [row for row in self.rows if row.get("test_run") == "^TestCephMDSCephFSCapSendRecoveryIntegration$"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSCapSendRecoveryIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            # Only already-forwarded workflow parameters: the data opt-in.
            self.assertEqual((row["mds_fresh"], row["mds_cephfs_data"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "1", "legacy-v6", "1", "1", ""))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "rbd", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSDataIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSCapSendRecoveryIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSCapSendRecoveryIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_writeback_rows_reuse_the_data_fixture_with_their_own_test(self):
        rows = [row for row in self.rows if row.get("test_run") == "^TestCephMDSCephFSWritebackFenceIntegration$"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSWritebackFenceIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            # Only already-forwarded workflow parameters: the data opt-in.
            self.assertEqual((row["mds_fresh"], row["mds_cephfs_data"], row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("1", "1", "legacy-v6", "1", "1", ""))
            for field in ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "rbd", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephMDSCephFSDataIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSCapSendRecoveryIntegration"))
            self.assertIsNone(selector.search("TestCephMDSCephFSWritebackFenceIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephMDSCephFSWritebackFenceIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_rbd_journal_lost_append_rows_are_exclusive(self):
        # The lost-append-reply cell needs a journal no other test advanced:
        # its rows reuse the RBD opt-in and both RBD profiles, select only its
        # test, and the broad RBD rows never match it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_JOURNAL_LOST_APPEND_SELECTOR]
        broad = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") not in RBD_EXCLUSIVE_SELECTORS]
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(broad), 2)
        expected = "TestCephRBDJournalLostAppendReplyIntegration"
        selector = re.compile(RBD_JOURNAL_LOST_APPEND_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDJournalIntegration", "TestRBDJournalLostAppendClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in broad:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
        profile = lambda row: {key: value for key, value in row.items() if key != "test_run"}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        for row in rows:
            self.assertEqual(row["rbd"], "1")
        modes = {row.get("mode", "secure") for row in rows}
        self.assertEqual(modes, {"crc", "secure"})

    def test_rbd_lock_owner_loss_rows_are_exclusive(self):
        # The lock-owner-loss cell blocklists two client addresses and kills a
        # native lock owner: its rows reuse the RBD opt-in and both RBD
        # profiles, select only its test, and no other RBD row matches it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_LOCK_OWNER_LOSS_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_LOCK_OWNER_LOSS_SELECTOR]
        broad = [row for row in others if row.get("test_run") not in RBD_EXCLUSIVE_SELECTORS]
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(others), 8)
        self.assertEqual(len(broad), 2)
        expected = "TestCephRBDLockOwnerLossIntegration"
        selector = re.compile(RBD_LOCK_OWNER_LOSS_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDExclusiveLockIntegration", "TestRBDLockOwnerLossClaims",
                          "TestRBDLockOwnerLossTestOnlyEncodings", "TestCephRBDJournalLostAppendReplyIntegration",
                          "TestCephRBDLockOwnerLossPendingIntegration"):
            self.assertIsNone(selector.search(unrelated))
        for row in others:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
        profile = lambda row: {key: value for key, value in row.items() if key != "test_run"}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        for row in rows:
            self.assertEqual(row["rbd"], "1")
        self.assertEqual({row.get("mode", "secure") for row in rows}, {"crc", "secure"})
        self.assertEqual({row["osd_store"] for row in rows}, {"bluestore", "memstore"})

    def test_rbd_lock_owner_loss_pending_rows_are_exclusive(self):
        # The pending variant holds two requests of the first lock owner on
        # the OSD through both handoffs and raises the OSDMap epoch: its rows
        # reuse the RBD opt-in and both RBD profiles, select only its test,
        # and no other RBD row matches it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR]
        broad = [row for row in others if row.get("test_run") not in RBD_EXCLUSIVE_SELECTORS]
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(others), 8)
        expected = "TestCephRBDLockOwnerLossPendingIntegration"
        selector = re.compile(RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDLockOwnerLossIntegration", "TestRBDLockOwnerLossPendingClaims",
                          "TestRBDLockOwnerLossClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in others:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
        profile = lambda row: {key: value for key, value in row.items() if key != "test_run"}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        self.assertEqual({row.get("mode", "secure") for row in rows}, {"crc", "secure"})
        self.assertEqual({row["osd_store"] for row in rows}, {"bluestore", "memstore"})

    def test_rbd_journal_primary_change_rows_are_exclusive(self):
        # The primary-change cell is the only RBD selection on three OSDs:
        # replicated BlueStore in both connection modes, the RBD opt-in, its
        # test alone, and no other RBD row matches it or shares its topology.
        rows = [row for row in self.rows if row.get("test_run") == RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR]
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(others), 8)
        expected = "TestCephRBDJournalPrimaryChangeIntegration"
        selector = re.compile(RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDJournalIntegration", "TestCephRBDJournalLostAppendReplyIntegration",
                          "TestRBDJournalPrimaryChangeClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in others:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
            self.assertEqual(row["osd_replicas"], "1")
        observed = set()
        for row in rows:
            self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_store"], row["osd_profile"], row["stress"]),
                             ("1", "1", "3", "bluestore", "legacy-v6", ""))
            for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "image", "race"):
                self.assertNotIn(field, row)
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
        self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})

    def test_rbd_rows_select_only_the_declared_rbd_tests(self):
        rows = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") not in RBD_EXCLUSIVE_SELECTORS]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephRBDBaseIntegration", "TestCephRBDWriteIntegration", "TestCephRBDExclusiveLockIntegration", "TestCephRBDCloneIntegration",
                    "TestCephRBDJournalIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("legacy-v6", "1", "1", ""))
            for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephRBDBaseIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephRBDBaseIntegration"))
            self.assertIsNone(selector.search("TestCephRBDWriteIntegrationExtended"))
            self.assertIsNone(selector.search("TestCephRBDBaseWriteIntegration"))
            self.assertIsNone(selector.search("TestCephRBDIntegration"))
            self.assertIsNone(selector.search("TestCephOSDIOIntegration"))
            self.assertIsNone(selector.search("TestRBDWriteTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestRBDLockTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestRBDLockAckDispositionClassification"))
            self.assertIsNone(selector.search("TestCephRBDExclusiveLockIntegrationExtended"))
            self.assertIsNone(selector.search("TestCephRBDLockIntegration"))
            self.assertIsNone(selector.search("TestCephRBDCloneIntegrationExtended"))
            self.assertIsNone(selector.search("TestRBDCloneTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestCephRBDCloneWriteIntegration"))
            self.assertIsNone(selector.search("TestCephRBDJournalIntegrationExtended"))
            self.assertIsNone(selector.search("TestRBDJournalTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestCephRBDJournalReplayIntegration"))
            self.assertIsNone(selector.search("TestCephRBDJournalLostAppendReplyIntegration"))
            self.assertIsNone(selector.search("TestCephRBDLockOwnerLossIntegration"))
            self.assertIsNone(selector.search("TestCephRBDLockOwnerLossPendingIntegration"))
            self.assertIsNone(selector.search("TestCephRBDJournalPrimaryChangeIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })


if __name__ == "__main__":
    unittest.main()
