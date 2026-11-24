"""Admission checks for the RBD rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the RBD rows:
the rows of the rbd opt-in and the RBD mirror row.
"""
import re
import unittest

import ci_row_catalog


RBD_MIRROR_SELECTOR = "^TestCephRBDMirrorPathRecoveryIntegration$"
RBD_JOURNAL_LOST_APPEND_SELECTOR = "^TestCephRBDJournalLostAppendReplyIntegration$"
RBD_LOCK_OWNER_LOSS_SELECTOR = "^TestCephRBDLockOwnerLossIntegration$"
RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR = "^TestCephRBDLockOwnerLossPendingIntegration$"
RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR = "^TestCephRBDJournalPrimaryChangeIntegration$"
# The capabilities the fixture admits on three replicated BlueStore OSDs, and
# only there (integration/fixture/profile.go rbdReplicas): the primary-change
# cell and the held-request cells (tools/test_ci_rows_rbd_journal_held.py).
RBD_THREE_OSD_CAPABILITIES = {"rbd-journal-primary-change", "rbd-journal-replica-hold", "rbd-journal-held-primary-change", "rbd-journal-dead-primary",
                              "rbd-journal-pending-hold", "rbd-journal-primary-restart", "rbd-journal-erasure-hold"}


def broad_rbd_rows(rows):
    """The rows of the RBD opt-in that run the fixture's own selection.

    Every other row of the opt-in selects one test alone on a fresh fixture,
    which the fixture admits only by a declared capability, so the broad rows
    are the ones without one; no list of the other rows is kept."""
    return [row for row in rows if row.get("rbd") == "1" and "capabilities" not in row]


class RBDRowsTest(ci_row_catalog.RowsTestCase):
    def test_rbd_mirror_row_is_exclusive_single_bluestore_ipv4(self):
        # The fixture derives its destination cluster and mirror daemon from
        # this exact selector on the single-BlueStore IPv4 fixture; it needs no
        # workflow opt-in and does not use the RBD base opt-in.
        rows = [row for row in self.rows if row.get("test_run") == RBD_MIRROR_SELECTOR]
        self.assertEqual(rows, [{
            "key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container",
            "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
            "test_run": RBD_MIRROR_SELECTOR, "capabilities": "rbd-mirror",
        }])
        expected = "TestCephRBDMirrorPathRecoveryIntegration"
        selector = re.compile(RBD_MIRROR_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestRBDMirrorRequiresExclusiveMarker", "TestCephRBDBaseIntegration"):
            self.assertIsNone(selector.search(unrelated))

    def test_rbd_journal_lost_append_rows_are_exclusive(self):
        # The lost-append-reply cell needs a journal no other test advanced:
        # its rows reuse the RBD opt-in and both RBD profiles, select only its
        # test, and the broad RBD rows never match it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_JOURNAL_LOST_APPEND_SELECTOR]
        broad = broad_rbd_rows(self.rows)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(broad), 2)
        expected = "TestCephRBDJournalLostAppendReplyIntegration"
        selector = re.compile(RBD_JOURNAL_LOST_APPEND_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDJournalIntegration", "TestRBDJournalLostAppendClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in broad:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
        profile = lambda row: {key: value for key, value in row.items() if key not in ("test_run", "capabilities")}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        for row in rows:
            self.assertEqual(row["rbd"], "1")
        modes = {row.get("mode", "secure") for row in rows}
        self.assertEqual(modes, {"crc", "secure"})
        self.assertEqual({row["capabilities"] for row in rows}, {"rbd-journal-lost-append"})

    def test_rbd_lock_owner_loss_rows_are_exclusive(self):
        # The lock-owner-loss cell blocklists two client addresses and kills a
        # native lock owner: its rows reuse the RBD opt-in and both RBD
        # profiles, select only its test, and no other RBD row matches it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_LOCK_OWNER_LOSS_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_LOCK_OWNER_LOSS_SELECTOR]
        broad = broad_rbd_rows(others)
        self.assertEqual(len(rows), 2)
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
        profile = lambda row: {key: value for key, value in row.items() if key not in ("test_run", "capabilities")}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        for row in rows:
            self.assertEqual(row["rbd"], "1")
        self.assertEqual({row.get("mode", "secure") for row in rows}, {"crc", "secure"})
        self.assertEqual({row["osd_store"] for row in rows}, {"bluestore", "memstore"})
        self.assertEqual({row["capabilities"] for row in rows}, {"rbd-lock-owner-loss"})

    def test_rbd_lock_owner_loss_pending_rows_are_exclusive(self):
        # The pending variant holds two requests of the first lock owner on
        # the OSD through both handoffs and raises the OSDMap epoch: its rows
        # reuse the RBD opt-in and both RBD profiles, select only its test,
        # and no other RBD row matches it.
        rows = [row for row in self.rows if row.get("test_run") == RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR]
        broad = broad_rbd_rows(others)
        self.assertEqual(len(rows), 2)
        expected = "TestCephRBDLockOwnerLossPendingIntegration"
        selector = re.compile(RBD_LOCK_OWNER_LOSS_PENDING_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDLockOwnerLossIntegration", "TestRBDLockOwnerLossPendingClaims",
                          "TestRBDLockOwnerLossClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in others:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
        profile = lambda row: {key: value for key, value in row.items() if key not in ("test_run", "capabilities")}
        self.assertEqual(sorted(map(repr, map(profile, rows))), sorted(map(repr, map(profile, broad))))
        self.assertEqual({row.get("mode", "secure") for row in rows}, {"crc", "secure"})
        self.assertEqual({row["osd_store"] for row in rows}, {"bluestore", "memstore"})
        self.assertEqual({row["capabilities"] for row in rows}, {"rbd-lock-owner-loss,rbd-lock-pending-requests"})

    def test_rbd_journal_primary_change_rows_are_exclusive(self):
        # The primary-change cell runs on three OSDs: replicated BlueStore in
        # both connection modes, the RBD opt-in, its test alone, and no other
        # RBD row matches it. Only the held-request cells share its topology.
        rows = [row for row in self.rows if row.get("test_run") == RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR]
        others = [row for row in self.rows if row.get("rbd") == "1" and row.get("test_run") != RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR]
        self.assertEqual(len(rows), 2)
        expected = "TestCephRBDJournalPrimaryChangeIntegration"
        selector = re.compile(RBD_JOURNAL_PRIMARY_CHANGE_SELECTOR)
        self.assertEqual({name for name in self.declared if selector.search(name)}, {expected})
        for unrelated in (expected + "Extended", "Prefix" + expected, "TestCephRBDJournalIntegration", "TestCephRBDJournalLostAppendReplyIntegration",
                          "TestRBDJournalPrimaryChangeClaims"):
            self.assertIsNone(selector.search(unrelated))
        for row in others:
            self.assertIsNone(re.compile(row["test_run"]).search(expected))
            self.assertEqual(row["osd_replicas"], "3" if row.get("capabilities") in RBD_THREE_OSD_CAPABILITIES else "1")
        observed = set()
        for row in rows:
            self.assertEqual((row["rbd"], row["osd"], row["osd_replicas"], row["osd_store"], row["osd_profile"], row["stress"]),
                             ("1", "1", "3", "bluestore", "legacy-v6", ""))
            for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "image", "race"):
                self.assertNotIn(field, row)
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"), ("family", ""), ("runtime", "container"))))
        self.assertEqual(observed, {("aes", "aes", "crc", "4", "host"), ("aes256k", "aes256k", "secure", "6", "container")})
        self.assertEqual({row["capabilities"] for row in rows}, {"rbd-journal-primary-change"})

    def test_rbd_rows_select_only_the_declared_rbd_tests(self):
        rows = broad_rbd_rows(self.rows)
        self.assertEqual(len(rows), 2)
        expected = {"TestCephRBDBaseIntegration", "TestCephRBDWriteIntegration", "TestCephRBDExclusiveLockIntegration", "TestCephRBDCloneIntegration",
                    "TestCephRBDJournalIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("legacy-v6", "1", "1", ""))
            for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "mds_cephfs_completion", "mds_cephfs_data", "image", "race",
                          "capabilities"):
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
