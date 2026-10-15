"""Static admission checks for explicitly selected native OSD CI profiles.

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

    def test_closed_profile_and_selector_reach_the_fixture(self):
        self.assertIn("CEPH_MSGR_TEST_OSD_PROFILE: ${{ matrix.osd_profile || 'legacy-v6' }}", self.workflow)
        self.assertIn("CEPH_MSGR_TEST_RUN: ${{ matrix.test_run || '' }}", self.workflow)
        self.assertEqual(len(self.rows), 32)
        self.assertEqual(sum(row.get("osd_profile", "legacy-v6") == "legacy-v6" for row in self.rows), 30)
        for row in self.rows:
            profile = row.get("osd_profile", "legacy-v6")
            self.assertIn(profile, {"legacy-v6", "replicated-pg-v8"})
            if profile == "legacy-v6":
                self.assertNotIn("test_run", row, "existing rows must retain their original suite selection")

    def test_v8_rows_cover_both_security_store_and_address_profiles(self):
        rows = [row for row in self.rows if row.get("osd_profile") == "replicated-pg-v8"]
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
        declared = set()
        for source in (ROOT / "integration").glob("*_test.go"):
            declared.update(re.findall(r"^func (Test\w+)\(", source.read_text(encoding="utf-8"), re.MULTILINE))
        expected = {
            "TestCephOSDReplicatedPGIntegration", "TestCephOSDPGListV8Integration",
            "TestCephOSDBackoffIntegration", "TestCephOSDBackoffLateAckIntegration",
            "TestCephOSDWatchReconnectIntegration", "TestCephOSDSplitIntegration",
        }
        self.assertTrue(expected <= declared)
        for row in self.rows:
            if row.get("osd_profile") != "replicated-pg-v8":
                continue
            selector = re.compile(row["test_run"])
            self.assertEqual({name for name in declared if selector.search(name)}, expected)
            for unready in ("TestCephOSDSparseReadIntegration", "TestCephOSDPGListIntegration",
                            "TestCephOSDMapExtIntegration", "TestCephOSDChecksumIntegration",
                            "TestCephMDSMapIntegration"):
                self.assertIsNone(selector.search(unready))
            # Go also applies -run to subtest path components. Keep unrelated
            # names and similarly prefixed future tests outside this opt-in.
            for name in expected:
                self.assertIsNone(selector.search(name + "Extended"))
                self.assertIsNone(selector.search("Prefix" + name))


if __name__ == "__main__":
    unittest.main()
