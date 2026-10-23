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
COMPRESSION_SELECTOR = "^TestCephMsgrCompressionOfferIntegration$"


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
        # 1 exclusive COPY_FROM2 late-error cell and 1 CRC compression-offer row.
        self.assertEqual(len(self.rows), 80)
        opt_ins = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "rbd", "mon_ping", "mgr_service", "osd_cache_pools")
        # Dedicated legacy-v6 rows select one test explicitly; the
        # original broad legacy rows keep their suite selection.
        dedicated = {SET_CHUNK_SELECTOR, SET_CHUNK_GO_SELECTOR, COPY_FROM_LATE_ERROR_SELECTOR, COMPRESSION_SELECTOR}
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
        ])
        expected = "TestCephMgrServiceRegistrationIntegration"
        self.assertIn(expected, self.declared)
        for row in rows:
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
        ])
        # Each row's selector must match the fixture's closed four-OSD list.
        fixture = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        for row, expected in zip(rows, ({"TestCephOSDErasureK2M2Integration"},
                                        {"TestCephOSDErasureOptimizedK2M2Integration", "TestCephOSDErasureOptimizedK2M2FailoverIntegration"},
                                        {"TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration",
                                         "TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration"})):
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
        rows = [row for row in self.rows if row.get("mds_commands") == "1"]
        self.assertEqual(len(rows), 2)
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
        rows = [row for row in self.rows if row.get("mds_cephfs") == "1"]
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

    def test_mds_multi_reconnect_rows_select_only_the_dedicated_acceptance(self):
        selector_text = "^TestCephMDSCephFSMultiReconnectIntegration$"
        rows = [row for row in self.rows if row.get("test_run") == selector_text]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephMDSCephFSMultiReconnectIntegration"}
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

    def test_rbd_rows_select_only_the_declared_rbd_tests(self):
        rows = [row for row in self.rows if row.get("rbd") == "1"]
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
            self.assertIsNone(selector.search("TestCephRBDExclusiveLockIntegrationExtended"))
            self.assertIsNone(selector.search("TestCephRBDLockIntegration"))
            self.assertIsNone(selector.search("TestCephRBDCloneIntegrationExtended"))
            self.assertIsNone(selector.search("TestRBDCloneTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestCephRBDCloneWriteIntegration"))
            self.assertIsNone(selector.search("TestCephRBDJournalIntegrationExtended"))
            self.assertIsNone(selector.search("TestRBDJournalTestOnlyEncodings"))
            self.assertIsNone(selector.search("TestCephRBDJournalReplayIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })


if __name__ == "__main__":
    unittest.main()
