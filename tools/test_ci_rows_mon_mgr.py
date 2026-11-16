"""Admission checks for the MON and MGR command rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only MON and MGR
command, Tell, ServiceMap and MON ping rows.
"""
import re
import unittest

import ci_row_catalog
from ci_row_catalog import ROOT


SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR = "^TestCephMonServiceMapLifecycleFailedMonHoldIntegration$"
SERVICE_MAP_LARGE_SELECTOR = "^TestCephMonServiceMapLargeMapIntegration$"
MON_CONNECTED_KILL_SELECTOR = "^TestCephMonMapVersionConnectedKillIntegration$"
MON_UNPROCESSED_KILL_SELECTOR = "^TestCephMonMapVersionUnprocessedKillIntegration$"
APPLIED_NAMED_MGR_TELL_SELECTOR = "^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$"
APPLIED_CURRENT_MGR_TELL_SELECTOR = "^TestCephAppliedCurrentManagerTellIsNotReplayedIntegration$"
APPLIED_MGR_TELL_SECURE_SELECTOR = "^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$"
SENT_TELL_CANCELLATION_SELECTOR = "^TestCephSentTellCancellationIntegration$"
UNAVAILABLE_TARGET_SELECTOR = "^TestCeph(CompleteMonitorOutage|NamedMonitorTell|NamedManagerTell)Integration$"
# Dedicated legacy-v6 rows of this area that select one test explicitly
# (collected by the cross-area legacy check).
DEDICATED_LEGACY_SELECTORS = (SERVICE_MAP_LIFECYCLE_HOLD_SELECTOR, SERVICE_MAP_LARGE_SELECTOR, MON_CONNECTED_KILL_SELECTOR, MON_UNPROCESSED_KILL_SELECTOR,
                              APPLIED_NAMED_MGR_TELL_SELECTOR, APPLIED_CURRENT_MGR_TELL_SELECTOR, APPLIED_MGR_TELL_SECURE_SELECTOR,
                              SENT_TELL_CANCELLATION_SELECTOR, UNAVAILABLE_TARGET_SELECTOR)


class MonMgrCommandRowsTest(ci_row_catalog.RowsTestCase):
    def test_mon_kill_and_large_service_map_rows_are_exclusive(self):
        # The MON SIGKILL cells run on CRC only (the relay decodes CRC frames)
        # with one BlueStore OSD; the large ServiceMap cell needs no OSD.
        # Each cell has its own row, because the capability that admits the
        # MON kill admits it for one exact test.
        for selector in (MON_CONNECTED_KILL_SELECTOR, MON_UNPROCESSED_KILL_SELECTOR):
            rows = [row for row in self.rows if row.get("test_run") == selector]
            self.assertEqual(rows, [
                {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host", "osd": "1",
                 "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6", "test_run": selector, "capabilities": "mon-forced-kill"},
                {"key": "aes256k", "service": "aes256k", "mode": "crc", "family": "6", "stress": "", "runtime": "container", "osd": "1",
                 "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6", "test_run": selector, "capabilities": "mon-forced-kill"}])
        self.assertEqual([row for row in self.rows if row.get("capabilities") == "mon-forced-kill"],
                         [row for row in self.rows if row.get("test_run") in (MON_CONNECTED_KILL_SELECTOR, MON_UNPROCESSED_KILL_SELECTOR)])
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

    def test_unavailable_target_rows_require_one_client_and_two_api_tests(self):
        # The three tests need no opt-in parameter and also run in the broad
        # ordinary mon-mgr rows. One CRC and one secure host row narrow that
        # selection, so the gate requires each parent to PASS: every command
        # channel during a complete MON outage, the dial cause of an exact MON
        # Tell, and the exact MGR Tell across a manager failover.
        rows = [row for row in self.rows if row.get("test_run") == UNAVAILABLE_TARGET_SELECTOR]
        self.assertEqual(rows, [
            {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
             "test_run": UNAVAILABLE_TARGET_SELECTOR},
            {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "host",
             "test_run": UNAVAILABLE_TARGET_SELECTOR}])
        selector = re.compile(UNAVAILABLE_TARGET_SELECTOR)
        self.assertEqual({name for name in self.client_tests() if selector.search(name)},
                         {"TestCephCompleteMonitorOutageIntegration"})
        self.assertEqual({name for name in self.declared if selector.search(name)},
                         {"TestCephNamedMonitorTellIntegration", "TestCephNamedManagerTellIntegration"})

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


if __name__ == "__main__":
    unittest.main()
