import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import osd_soak_receipts as receipts


class SoakReceiptsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sha = "a" * 40
        self.image = "quay.io/ceph/ceph:v20.2.4@sha256:" + "b" * 64
        self.duration = 240_000_000_000
        self.put("fixture-manifest.json", {"image": self.image, "image_id": "sha256:" + "c" * 64, "architecture": "amd64"})
        manifest = hashlib.sha256((self.root / "fixture-manifest.json").read_bytes()).hexdigest()
        self.put("osd-soak-v1-fixture-termination.json", {"schema_version": 1, "kind": "fixture_termination", "status": "passed", "configured_source_sha": self.sha, "configured_image": self.image, "observed_image_id": "sha256:" + "c" * 64, "architecture": "amd64", "fixture_manifest_sha256": manifest, "configured_duration_ns": self.duration, "suite_exit_success": True, "termination_succeeded": True, "termination_elapsed_ns": 1000})
        for label, test in receipts.TESTS.items():
            self.put_stage(label, "started", {"configured_source_sha": self.sha, "configured_duration_ns": self.duration, "fixture_manifest_sha256": manifest})
            owned = ["owned-seed"] + [f"owned-{w}" for w in range(4)]
            self.put_stage(label, "setup", {"elapsed_ns": 1000, "owned_objects": owned})
            self.put_stage(label, "workload", {"configured_duration_ns": self.duration, "actual_workload_ns": self.duration + 1000, "test_elapsed_ns": self.duration + 5000, "status": "passed", "first_error": "", "unresolved_outcomes": [], "counters": {"appends": 1000, "reads": 100, "churns": 100, "unknowns": 10, "resubmissions": 10, "faults": 1, "xattrs": 100, "truncations": 10, "unsent": 0, "replacements": 10, "resent": 0}})
            self.put_stage(label, "cleanup", {"removed": True, "elapsed_ns": 1000, "objects": [{"object": name, "code": 0} for name in owned]})
            for worker in range(4):
                data = bytes([worker, 255, 0])
                encoded = base64.b64encode(data).decode()
                digest = hashlib.sha256(data).hexdigest()
                self.put_stage(label, f"worker-{worker}", {"worker": worker, "native_observation_elapsed_ns": 1000, "object": f"owned-{worker}", "expected_data": encoded, "expected_xattr": "123", "expected_length": len(data), "expected_sha256": digest, "native_length": len(data), "native_sha256": digest, "matched": True, "native_source_commit": receipts.SOURCE, "native_daemon_version": "20.2.4 " + receipts.SOURCE, "independent_native_c_api": True, "native": {"code": 0, "version": 12, "data": encoded, "attributes": [{"name": "c29haw==", "value": "MTIz"}]}})
            if label == "failover":
                self.put_stage(label, "recovery-clean", {"passed": True})
                for when, phase in enumerate(["kill", "restart", "clean"], 1):
                    self.put_stage(label, "fault-001-" + phase, {"passed": True, "elapsed_ns": 1000, "after_workload_start_ns": when, "victim": "1"})

    def put(self, name, value):
        (self.root / name).write_text(json.dumps(value, sort_keys=True))

    def put_stage(self, label, kind, value):
        self.put(f"osd-soak-v1-{label}-{kind}.json", dict(value, schema_version=1, test=receipts.TESTS[label], kind=kind))

    def change(self, name, change):
        path = self.root / name
        value = json.loads(path.read_text())
        change(value)
        self.put(name, value)

    def verify(self):
        return receipts.verify(self.root, self.sha, self.image, self.duration)

    def test_complete_eight_observations(self):
        result = self.verify()
        self.assertEqual(result["observations"], 8)
        self.assertTrue(all(len(v) == 64 for v in result["receipt_sha256"].values()))

    def test_missing_worker_is_not_a_pass(self):
        (self.root / "osd-soak-v1-steady-worker-3.json").unlink()
        with self.assertRaisesRegex(ValueError, "missing receipt"):
            self.verify()

    def test_duplicate_worker_identity_is_not_a_pass(self):
        self.change("osd-soak-v1-failover-worker-3.json", lambda x: x.update(worker=2))
        with self.assertRaisesRegex(ValueError, "worker identity"):
            self.verify()

    def test_truncated_native_bytes_is_not_a_pass(self):
        self.change("osd-soak-v1-steady-worker-2.json", lambda x: x["native"].update(data="AA=="))
        with self.assertRaisesRegex(ValueError, "truncated or corrupt"):
            self.verify()

    def test_wrong_revision(self):
        with self.assertRaisesRegex(ValueError, "wrong tested revision"):
            receipts.verify(self.root, "d" * 40, self.image, self.duration)

    def test_wrong_image(self):
        with self.assertRaisesRegex(ValueError, "wrong image"):
            receipts.verify(self.root, self.sha, self.image + "wrong", self.duration)

    def test_shortened_workload(self):
        self.change("osd-soak-v1-steady-workload.json", lambda x: x.update(actual_workload_ns=self.duration - 1))
        with self.assertRaisesRegex(ValueError, "shortened or missing"):
            self.verify()

    def test_unresolved_original_submission(self):
        self.change("osd-soak-v1-failover-workload.json", lambda x: x.update(unresolved_outcomes=[{"Transaction": 10}]))
        with self.assertRaisesRegex(ValueError, "unresolved mutation"):
            self.verify()

    def test_cleanup_cannot_be_implied_by_matching_bytes(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x.update(removed=False))
        with self.assertRaisesRegex(ValueError, "owned-object cleanup"):
            self.verify()

    def test_fixture_termination_cannot_be_implied_by_matching_bytes(self):
        self.change("osd-soak-v1-fixture-termination.json", lambda x: x.update(termination_succeeded=False))
        with self.assertRaisesRegex(ValueError, "fixture termination"):
            self.verify()

    def test_missing_final_cleanup_verdict(self):
        (self.root / "osd-soak-v1-fixture-termination.json").unlink()
        with self.assertRaisesRegex(ValueError, "missing receipt"):
            self.verify()

    def test_missing_fault_phase(self):
        (self.root / "osd-soak-v1-failover-fault-001-restart.json").unlink()
        with self.assertRaisesRegex(ValueError, "missing receipt"):
            self.verify()

    def test_missing_recovery(self):
        self.change("osd-soak-v1-failover-recovery-clean.json", lambda x: x.update(passed=False))
        with self.assertRaisesRegex(ValueError, "clean-PG recovery"):
            self.verify()

    def test_native_mismatch_cannot_be_hidden_by_match_flag(self):
        self.change("osd-soak-v1-steady-worker-2.json", lambda x: x["native"]["attributes"][0].update(value="eA=="))
        with self.assertRaisesRegex(ValueError, "xattr mismatch"):
            self.verify()

    def test_manifest_change_is_not_accepted(self):
        self.put("fixture-manifest.json", {"image": self.image, "image_id": "changed", "architecture": "amd64"})
        with self.assertRaisesRegex(ValueError, "manifest hash mismatch"):
            self.verify()

    def test_symlink_receipt_is_not_read(self):
        path = self.root / "osd-soak-v1-steady-worker-3.json"
        saved = self.root / "unrelated.json"
        path.rename(saved)
        path.symlink_to(saved)
        with self.assertRaisesRegex(ValueError, "invalid workload receipt"):
            self.verify()

    def test_extra_worker_receipt_is_not_ignored(self):
        self.put("osd-soak-v1-steady-worker-4.json", {})
        with self.assertRaisesRegex(ValueError, "invalid workload receipt"):
            self.verify()

    def test_seed_cleanup_failure_is_not_hidden(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x["objects"][0].update(code=-5))
        with self.assertRaisesRegex(ValueError, "native cleanup failed"):
            self.verify()

    def test_duplicate_failed_entry_is_not_overwritten_by_success(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x["objects"].insert(0, {"object": "owned-0", "code": -5}))
        with self.assertRaisesRegex(ValueError, "native cleanup failed"):
            self.verify()

    def test_duplicate_successful_cleanup_entry_is_rejected(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x["objects"].append({"object": "owned-0", "code": 0}))
        with self.assertRaisesRegex(ValueError, "duplicate cleanup object"):
            self.verify()

    def test_missing_seed_cleanup_is_rejected(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x["objects"].pop(0))
        with self.assertRaisesRegex(ValueError, "cleanup coverage"):
            self.verify()

    def test_unknown_cleanup_object_is_rejected(self):
        self.change("osd-soak-v1-steady-cleanup.json", lambda x: x["objects"].append({"object": "unowned", "code": 0}))
        with self.assertRaisesRegex(ValueError, "unknown cleanup object"):
            self.verify()

    def test_seed_cannot_replace_worker_observation(self):
        self.change("osd-soak-v1-steady-worker-0.json", lambda x: x.update(object="owned-seed"))
        with self.assertRaisesRegex(ValueError, "worker object binding"):
            self.verify()

    def test_non_json_prefixed_receipt_is_not_ignored(self):
        self.put("osd-soak-v1-steady-worker-4.txt", {})
        with self.assertRaisesRegex(ValueError, "invalid workload receipt"):
            self.verify()

    def test_oversized_receipt_is_rejected_before_decoding(self):
        (self.root / "osd-soak-v1-steady-worker-0.json").write_bytes(b" " * ((512 << 10) + 1000))
        with self.assertRaisesRegex(ValueError, "oversized receipt"):
            self.verify()

    def test_validation_never_uses_unbounded_read_bytes(self):
        with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("unbounded read")):
            self.assertEqual(self.verify()["observations"], 8)


if __name__ == "__main__":
    unittest.main()
