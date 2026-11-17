"""Admission checks for the MDS rows of the native CI matrix.

Shared loading lives in ci_row_catalog; these tests check only the rows of
the MDS opt-ins (fresh, commands, sessions and the CephFS profiles).
"""
import re
import unittest

import ci_row_catalog
from ci_row_catalog import ROOT


APPLIED_MDS_COMMAND_SELECTOR = "^TestCephAppliedMDSCommandIsNotReplayedIntegration$"


class MDSRowsTest(ci_row_catalog.RowsTestCase):
    def test_applied_mds_command_rows_reuse_the_command_fixture_with_their_own_test(self):
        # The owned MDS command fixture admits this one further exact selector,
        # in the CRC and the secure profile of the public-command rows.
        self.assert_client_row(APPLIED_MDS_COMMAND_SELECTOR,
                               {"key": "aes", "service": "aes", "mode": "crc", "family": "4", "stress": "", "runtime": "host",
                                "osd": "1", "osd_store": "bluestore", "osd_replicas": "1", "osd_profile": "legacy-v6",
                                "mds_fresh": "1", "mds_commands": "1", "capabilities": "mds-applied-command"},
                               {"TestCephAppliedMDSCommandIsNotReplayedIntegration"},
                               {"key": "aes256k", "service": "aes256k", "family": "6", "stress": "", "runtime": "container",
                                "osd": "1", "osd_store": "memstore", "osd_replicas": "1", "osd_profile": "legacy-v6",
                                "mds_fresh": "1", "mds_commands": "1", "capabilities": "mds-applied-command"})
        public = [{name: value for name, value in row.items() if name != "test_run"}
                  for row in self.rows if row.get("test_run") == "^TestCephMDSCommandsIntegration$"]
        applied = [{name: value for name, value in row.items() if name not in ("test_run", "capabilities")}
                   for row in self.rows if row.get("test_run") == APPLIED_MDS_COMMAND_SELECTOR]
        self.assertEqual(applied, public)
        # The fixture admits the cell by the capability its rows declare.
        profile = (ROOT / "integration" / "fixture" / "profile.go").read_text(encoding="utf-8")
        outcome = (ROOT / "integration" / "fixture" / "daemon_command_outcome.go").read_text(encoding="utf-8")
        self.assertIn("admitted = p.admitsExclusive(fixtureMDSCommands)", profile)
        self.assertIn('var capMDSAppliedCommand = defineExclusiveCapability(fixtureMDSCommands, "mds-applied-command",', outcome)

    def test_mds_stop_control_is_admitted_only_for_the_public_command_acceptance(self):
        # The stopped-daemon step of the public MDS command test is the only
        # user of an MDS daemon control. The fixture admits stop alone, and
        # not for the applied-command cell on the same fixture.
        control = " ".join((ROOT / "integration" / "fixture" / "control_daemon.go").read_text(encoding="utf-8").split())
        self.assertIn('return action == "stop" && p.mdsCommands == "1" && p.mdsFresh == "1" && !p.admits(capMDSAppliedCommand)', control)
        self.assertIn("if !c.profile.daemonControlAdmitted(kind, action) {", control)
        users = [source.name for source in sorted((ROOT / "integration").glob("*_test.go")) + sorted((ROOT / "cephmsgr").glob("*_test.go"))
                 if re.search(r'ControlDaemon\([^)]*"mds"', source.read_text(encoding="utf-8"))]
        self.assertEqual(users, ["mds_commands_test.go"])

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
        rows = [row for row in self.rows if row.get("mds_commands") == "1" and "capabilities" not in row]
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
        # The rows that reuse the metadata fixture opt-in with their own
        # selector declare the capability that admits it and are checked
        # by their own tests.
        rows = [row for row in self.rows if row.get("mds_cephfs") == "1" and "capabilities" not in row]
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
        self.check_dedicated_metadata_rows("TestCephMDSCephFSSharedFlockRecoveryIntegration", "mds-cephfs-sharedflock-recovery", "exclusive-mds-cephfs")

    def test_mds_fcntl_recovery_rows_select_only_the_dedicated_acceptance(self):
        self.check_dedicated_metadata_rows("TestCephMDSCephFSFcntlRangeRecoveryIntegration", "mds-cephfs-fcntl-recovery", "mds-held-fcntl")

    def check_dedicated_metadata_rows(self, test, group, capability):
        selector_text = "^" + test + "$"
        rows = [row for row in self.rows if row.get("test_run") == selector_text]
        self.assertEqual(len(rows), 2)
        self.assertEqual({entry["group"] for entry in self.entries if entry["params"].get("test_run") == selector_text}, {group})
        self.assertEqual({row.get("capabilities") for row in rows}, {capability})
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
        # The broad metadata selector does not reach the dedicated variant.
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
        self.assertEqual({entry["group"] for entry in self.entries if entry["params"].get("test_run") == selector_text}, {group})
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
        # The rows that reuse the owned data fixture opt-in with their own
        # selector declare the capability that admits it and are checked by
        # their own tests.
        rows = [row for row in self.rows if row.get("mds_cephfs_data") == "1" and "capabilities" not in row]
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
        self.check_data_writeback_rows("TestCephMDSCephFSWritebackFenceIntegration", "mds-cephfs-writeback", "mds-writeback-fence")

    def test_mds_cephfs_mixed_writeback_rows_reuse_the_data_fixture_with_their_own_test(self):
        self.check_data_writeback_rows("TestCephMDSCephFSMixedWritebackRecoveryIntegration", "mds-cephfs-mixed-writeback",
                                       "mds-writeback-fence,mds-writeback-mixed")

    def check_data_writeback_rows(self, test, group, capabilities):
        selector_text = "^" + test + "$"
        rows = [row for row in self.rows if row.get("test_run") == selector_text]
        self.assertEqual(len(rows), 2)
        self.assertEqual({entry["group"] for entry in self.entries if entry["params"].get("test_run") == selector_text}, {group})
        self.assertEqual({row.get("capabilities") for row in rows}, {capabilities})
        expected = {test}
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
            for other in ("TestCephMDSCephFSDataIntegration", "TestCephMDSCephFSCapSendRecoveryIntegration",
                          "TestCephMDSCephFSWritebackFenceIntegration", "TestCephMDSCephFSMixedWritebackRecoveryIntegration"):
                if other != test:
                    self.assertIsNone(selector.search(other))
            self.assertIsNone(selector.search(test + "Extended"))
            self.assertIsNone(selector.search("Prefix" + test))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })


if __name__ == "__main__":
    unittest.main()
