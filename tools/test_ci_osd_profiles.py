"""Static admission checks for explicitly selected native OSD and MDS CI profiles.

The matrix uses flat YAML flow rows; parse only that bounded scalar subset here
to keep the diagnostic suite standard-library-only on all three runner OSes.
Workflow syntax is checked separately with the available workflow/YAML tooling.
"""
from pathlib import Path
import re
import shlex
import unittest


ROOT = Path(__file__).resolve().parents[1]


def ceph_rows(workflow):
    job = workflow.split("\n  ceph:\n", 1)[1]
    matrix = job.split("        include:\n", 1)[1].split("\n    steps:", 1)[0]
    rows = []
    for line in matrix.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"\s*- \{ (.*) \}", line)
        if not match:
            raise ValueError("Ceph matrix row is outside the checked scalar flow subset")
        fields = shlex.shlex(match[1], posix=True)
        fields.whitespace = ","
        fields.whitespace_split = True
        fields.commenters = ""
        row = {}
        for field in fields:
            key, separator, value = field.partition(":")
            key, value = key.strip(), value.strip()
            if not separator or not re.fullmatch(r"[a-z_]+", key) or key in row:
                raise ValueError("invalid or duplicate Ceph matrix field")
            row[key] = value
        rows.append(row)
    return rows


class NativeOSDCIProfilesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        cls.rows = ceph_rows(cls.workflow)
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
        self.assertIn("CEPH_MSGR_TEST_RBD: ${{ matrix.rbd || '0' }}", self.workflow)
        self.assertEqual(len(self.rows), 46)
        opt_ins = ("mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "rbd")
        legacy = [row for row in self.rows if row.get("osd_profile", "legacy-v6") == "legacy-v6"
                  and all(row.get(field, "0") == "0" for field in opt_ins)]
        self.assertEqual(len(legacy), 30)
        for row in self.rows:
            profile = row.get("osd_profile", "legacy-v6")
            self.assertIn(profile, {"legacy-v6", "replicated-pg-v8", "replicated-pg-reply-v8"})
            self.assertIn(row.get("mds_fresh", "0"), {"0", "1"})
            for field in opt_ins:
                self.assertIn(row.get(field, "0"), {"0", "1"})
            self.assertLessEqual(sum(row.get(field, "0") == "1" for field in opt_ins), 1)
            if profile == "legacy-v6" and all(row.get(field, "0") == "0" for field in opt_ins):
                self.assertNotIn("test_run", row, "existing rows must retain their original suite selection")
                self.assertNotIn("mds_fresh", row)
                for field in opt_ins:
                    self.assertNotIn(field, row)

    def test_v8_rows_cover_both_security_store_and_address_profiles(self):
        for profile in ("replicated-pg-v8", "replicated-pg-reply-v8"):
            with self.subTest(profile=profile):
                rows = [row for row in self.rows if row.get("osd_profile") == profile and row.get("mds_fresh", "0") == "0"]
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
            "TestCephMDSMapsIntegration",
        }
        reply_v8 = "TestCephOSDReplyV8Integration"
        self.assertTrue(common | {reply_v8} <= self.declared)
        for row in self.rows:
            profile = row.get("osd_profile")
            if profile not in {"replicated-pg-v8", "replicated-pg-reply-v8"} or row.get("mds_fresh", "0") != "0":
                continue
            expected = common | ({reply_v8} if profile == "replicated-pg-reply-v8" else set())
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            for unready in ("TestCephOSDSparseReadIntegration", "TestCephOSDPGListIntegration",
                            "TestCephOSDMapExtIntegration", "TestCephOSDChecksumIntegration"):
                self.assertIsNone(selector.search(unready))
            # Go also applies -run to subtest path components. Keep unrelated
            # names and similarly prefixed future tests outside this opt-in.
            for name in expected:
                self.assertIsNone(selector.search(name + "Extended"))
                self.assertIsNone(selector.search("Prefix" + name))

    def test_fresh_mds_rows_require_explicit_opt_in_and_only_their_declared_test(self):
        rows = [row for row in self.rows if row.get("mds_fresh") == "1"
                and row.get("mds_commands", "0") == "0" and row.get("mds_sessions", "0") == "0"
                and row.get("mds_cephfs", "0") == "0" and row.get("mds_cephfs_ranks", "0") == "0"]
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
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_mds_cephfs_ranks_rows_select_only_the_declared_multi_rank_test(self):
        rows = [row for row in self.rows if row.get("mds_cephfs_ranks") == "1"]
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
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })

    def test_rbd_base_rows_select_only_the_declared_rbd_test(self):
        rows = [row for row in self.rows if row.get("rbd") == "1"]
        self.assertEqual(len(rows), 2)
        expected = {"TestCephRBDBaseIntegration"}
        self.assertTrue(expected <= self.declared)
        observed = set()
        for row in rows:
            self.assertEqual((row["osd_profile"], row["osd"], row["osd_replicas"], row["stress"]),
                             ("legacy-v6", "1", "1", ""))
            for field in ("mds_fresh", "mds_commands", "mds_sessions", "mds_cephfs", "mds_cephfs_ranks", "image", "race"):
                self.assertNotIn(field, row)
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in self.declared if selector.search(name)}, expected)
            self.assertIsNone(selector.search("TestCephRBDBaseIntegrationExtended"))
            self.assertIsNone(selector.search("PrefixTestCephRBDBaseIntegration"))
            self.assertIsNone(selector.search("TestCephOSDIOIntegration"))
            observed.add(tuple(row.get(field, fallback) for field, fallback in (
                ("key", ""), ("service", ""), ("mode", "secure"),
                ("family", ""), ("runtime", "container"), ("osd_store", "memstore"))))
        self.assertEqual(observed, {
            ("aes", "aes", "crc", "4", "host", "bluestore"),
            ("aes256k", "aes256k", "secure", "6", "container", "memstore"),
        })


if __name__ == "__main__":
    unittest.main()
