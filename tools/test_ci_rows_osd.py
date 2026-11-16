"""Admission checks for the replicated OSD rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the OSD and
PG command rows, the dedicated single-OSD legacy-v6 rows (COPY_FROM2,
SET_CHUNK, compression offer), the cache-pool rows and the replicated v8
profile rows. The four-OSD erasure rows are checked in
test_ci_rows_osd_erasure.
"""
import re
import unittest

import ci_row_catalog
from ci_row_catalog import ROOT


SET_CHUNK_SELECTOR = "^TestCephOSDSetChunkNativeIntegration$"
SET_CHUNK_GO_SELECTOR = "^TestCephOSDSetChunkGoIntegration$"
COPY_FROM_LATE_ERROR_SELECTOR = "^TestCephOSDCopyFromLateErrorIntegration$"
COPY_FROM_MULTI_CHUNK_SELECTOR = "^TestCephOSDCopyFromMultiChunkLateErrorIntegration$"
COPY_FROM_HISTORY_SELECTOR = "^TestCephOSDCopyFromDestinationHistoryIntegration$"
COMPRESSION_SELECTOR = "^TestCephMsgrCompressionOfferIntegration$"
APPLIED_OSD_PG_COMMAND_SELECTOR = "^TestCephApplied(OSD|PG)CommandIsNotReplayedIntegration$"
OSD_COMMAND_OUTCOME_SELECTOR = "^TestCephOSDCommand(s|Crash)Integration$"
# Dedicated legacy-v6 rows of this area that select one test explicitly
# (collected by the cross-area legacy check).
DEDICATED_LEGACY_SELECTORS = (SET_CHUNK_SELECTOR, SET_CHUNK_GO_SELECTOR, COPY_FROM_LATE_ERROR_SELECTOR, COPY_FROM_MULTI_CHUNK_SELECTOR,
                              COPY_FROM_HISTORY_SELECTOR, COMPRESSION_SELECTOR, APPLIED_OSD_PG_COMMAND_SELECTOR,
                              OSD_COMMAND_OUTCOME_SELECTOR)


class OSDRowsTest(ci_row_catalog.RowsTestCase):
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

    def test_osd_command_outcome_rows_require_the_two_api_tests(self):
        # Both tests also belong to the broad legacy OSD selection, where a
        # result gate requires no single test. One CRC and one secure
        # single-BlueStore legacy-v6 row narrow that selection to them, so the
        # gate requires both parents to PASS: the wire-observed cancellation
        # and Close of an OSD and a PG command, and the asserted
        # classification of a command to a killed OSD. The in-flight crash
        # test is not selected.
        rows = [row for row in self.rows if row.get("test_run") == OSD_COMMAND_OUTCOME_SELECTOR]
        self.assertEqual(rows, [
            {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
             "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
             "test_run": OSD_COMMAND_OUTCOME_SELECTOR},
            {"key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
             "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
             "test_run": OSD_COMMAND_OUTCOME_SELECTOR}])
        selector = re.compile(OSD_COMMAND_OUTCOME_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)},
                         {"TestCephOSDCommandsIntegration", "TestCephOSDCommandCrashIntegration"})
        self.assertEqual({name for name in self.client_tests() if selector.search(name)}, set())
        broad = re.compile("^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$")
        for name in ("TestCephOSDCommandsIntegration", "TestCephOSDCommandCrashIntegration"):
            self.assertIsNotNone(broad.search(name))
        crash = (ROOT / "integration" / "osd_command_crash_test.go").read_text(encoding="utf-8")
        self.assertIn('os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore"', crash)

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


if __name__ == "__main__":
    unittest.main()
