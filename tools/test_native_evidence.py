"""Synthetic bundle rejection tests; these never establish native acceptance."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import native_evidence as evidence
import scenario_plan


class NativeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receipts = self.root / "private-input"
        self.receipts.mkdir()
        profile = evidence.PROFILES[0]
        self.request = {"execution_source": "linux-runner", "tested_sha": "a" * 40, "controller_sha": "b" * 40, "run_id": 101, "attempt": 1,
                        "matrix": dict(profile, scenario=evidence.scenario_id(profile))}
        self.tests = self.root / "tests.json"
        self.write(self.tests, {"selector": evidence.SELECTOR, "expected": [["api", evidence.TEST]],
                               "observed": [{"suite": "api", "test": evidence.TEST, "result": "PASS"}],
                               "problems": [], "passed": True})
        self.raw = self.fixture()
        for name, value in self.raw.items():
            self.write(self.receipts / name, value)
        self.output = self.root / "bundle"

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def fixture(self):
        def attempt(generation, admitted, kind):
            return {"Kind": kind, "Generation": generation, "ReconnectedFrom": generation - 1,
                    "Admitted": admitted, "CapID": 55, "Seq": 2, "IssueSeq": 3, "MigrateSeq": 0, "Barrier": 9,
                    "Error": "omitted private error", "Started": "omitted timestamp"}
        flushes = [{"Ino": 100, "Transaction": index + 7, "Caps": {"Size": 200 + index},
                    "Attempts": [attempt(1, index == 0, "first-send"), attempt(2, True, "resubmit" if index == 0 else "first-send")]}
                   for index in range(2)]
        facts = {"flushes": flushes, "acks": [{"Transaction": index + 7, "Arrival": index + 10, "Caps": {"Ino": 100, "CapID": 55}}
                                              for index in range(2)],
                 "adopted": {"state": "adopted", "armed": {"gid": 99}, "adopted": {"gid": 100}},
                 "new_branches": {"uncertain": "applied", "refused": "applied"},
                 "object_requests_before_restart": 5, "object_requests_after_recovery": 5,
                 "native_stat": {"exists": True, "ino": 100, "size": 201, "mtime": 1700000000, "version": 22}}
        runtime = {"key_type": "aes", "service_cipher": "aes", "connection_mode": "crc", "ip_family": "4", "runtime": "host",
                   "osd": "1", "osd_profile": "legacy-v6", "osd_store": "bluestore", "osd_replicas": "1",
                   "mds_fresh": True, "mds_cephfs_data": True, "image": evidence.IMAGE, "image_id": "sha256:" + "c" * 64,
                   "architecture": "amd64", "daemon_version": "ceph version 20.2.4 (%s) tentacle (stable)" % evidence.SOURCE,
                   "provider": "github.com/JSYoo5B/ceph-testcontainers-go", "provider_version": "v0.3.0"}
        return {"fixture-manifest.json": runtime,
                "mds-cephfs-data-results.json": {"source_commit": evidence.SOURCE, "global_id": 10, "cephfs_nonce": 17,
                                                "facts": {"restart_recipe": "cap-send", "cap_send_recovery": facts},
                                                "oracle": {"endpoint": "v2:192.0.2.3:3300/77", "baseline_config": {"private": "omitted"}}},
                "mds-cephfs-data-cleanup.json": dict.fromkeys(evidence.CLEAN_FLAGS, True) | {"source_commit": evidence.SOURCE,
                    "residual_owned_sessions": [], "kept_new_blocklist": [], "config": {"unreviewed": "omitted"}},
                "mds-fresh-cleanup.json": dict.fromkeys(evidence.FRESH_FLAGS, True) | {"source_commit": evidence.SOURCE}}

    def collect(self):
        return evidence.collect(self.request, self.receipts, self.tests, self.output)

    def test_successful_bundle_is_closed_hashed_and_exactly_attributed(self):
        manifest = self.collect()
        self.assertEqual({p.name for p in self.output.iterdir()}, evidence.FILES | {"manifest.json"})
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["tested_sha"], "a" * 40)
        self.assertEqual(manifest["controller_sha"], "b" * 40)
        self.assertEqual((manifest["run_id"], manifest["attempt"]), (101, 1))
        # Configured registry reference is not relabeled as the observed image ID.
        self.assertEqual(manifest["configured_image"], evidence.IMAGE)
        self.assertEqual(manifest["observed"]["image_id"], "sha256:" + "c" * 64)
        self.assertEqual(evidence.verify(self.output, evidence.context(self.request)), manifest)
        text = "".join(p.read_text() for p in self.output.iterdir())
        for omitted in ("baseline_config", "192.0.2.3", "private error", "omitted timestamp", "unreviewed", "oracle"):
            self.assertNotIn(omitted, text)

    def test_both_actual_catalog_profiles_are_supported_and_others_refused(self):
        rows = [entry for entry in scenario_plan.load_catalog() if entry["group"] == "mds-cephfs-capsend"]
        self.assertEqual(len(rows), 2)
        for row in rows:
            request = dict(self.request, matrix=dict(row["params"], scenario=row["id"]))
            self.assertIn(evidence.context(request)["profile"], evidence.PROFILES)
        for field, value in (("scenario", "arbitrary"), ("test_run", "^TestOther$"), ("mode", "secure"),
                             ("image", "quay.io/ceph/ceph:latest"), ("unknown", "1")):
            request = copy.deepcopy(self.request)
            request["matrix"][field] = value
            with self.subTest(field=field), self.assertRaises(evidence.EvidenceError):
                evidence.context(request)

    def test_missing_and_oversized_inputs_cannot_create_a_bundle(self):
        for name in self.raw:
            path = self.receipts / name
            saved = path.read_bytes()
            path.unlink()
            with self.subTest(name=name), self.assertRaises(evidence.EvidenceError):
                self.collect()
            self.assertFalse(self.output.exists())
            path.write_bytes(saved)
        (self.receipts / "mds-cephfs-data-results.json").write_bytes(b" " * (evidence.INPUT_LIMIT + 1))
        with self.assertRaisesRegex(evidence.EvidenceError, "file-limit"):
            self.collect()

    def test_skipped_missing_or_failed_required_parent_is_incomplete(self):
        for change in ({"passed": False}, {"passed": 1}, {"expected": []}, {"observed": []},
                       {"observed": [{"suite": "api", "test": evidence.TEST, "result": "SKIP"}]},
                       {"observed": [{"suite": "client", "test": evidence.TEST, "result": "PASS"}]}):
            with self.subTest(change=change):
                data = json.loads(self.tests.read_text())
                data.update(change)
                self.write(self.tests, data)
                with self.assertRaises(evidence.EvidenceError):
                    self.collect()
        self.assertFalse(self.output.exists())

    def test_incomplete_cleanup_profile_mismatch_or_wrong_receipt_is_rejected(self):
        cases = [("mds-cephfs-data-cleanup.json", "completed", False),
                 ("mds-cephfs-data-cleanup.json", "errors", ["not restored"]),
                 ("mds-fresh-cleanup.json", "filesystem_removed", False),
                 ("fixture-manifest.json", "connection_mode", "secure"),
                 ("fixture-manifest.json", "image", "other"),
                 ("fixture-manifest.json", "daemon_version", "ceph version 19.2.5"),
                 ("mds-cephfs-data-results.json", "source_commit", "d" * 40)]
        for name, field, value in cases:
            data = copy.deepcopy(self.raw[name])
            data[field] = value
            self.write(self.receipts / name, data)
            with self.subTest(name=name, field=field), self.assertRaises(evidence.EvidenceError):
                self.collect()
            self.write(self.receipts / name, self.raw[name])

    def test_file_hash_unknown_missing_and_duplicate_entries_are_rejected(self):
        original = self.collect()
        path = self.output / "cap-send.json"
        saved = path.read_bytes()
        path.write_bytes(saved + b" ")
        with self.assertRaisesRegex(evidence.EvidenceError, "file-hash-or-size"):
            evidence.verify(self.output, evidence.context(self.request))
        path.write_bytes(saved)
        unexpected = self.output / "unreviewed.log"
        unexpected.write_text("never publish this")
        with self.assertRaisesRegex(evidence.EvidenceError, "bundle-inventory"):
            evidence.verify(self.output, evidence.context(self.request))
        unexpected.unlink()
        path.unlink()
        with self.assertRaisesRegex(evidence.EvidenceError, "bundle-inventory"):
            evidence.verify(self.output, evidence.context(self.request))
        path.write_bytes(saved)
        manifest = copy.deepcopy(original)
        manifest["files"][1] = manifest["files"][0]
        self.write(self.output / "manifest.json", manifest)
        with self.assertRaisesRegex(evidence.EvidenceError, "bundle-inventory"):
            evidence.verify(self.output, evidence.context(self.request))

    def test_changed_manifest_context_attempt_or_observation_cannot_validate(self):
        original = self.collect()
        for field, value in (("schema_version", 2), ("attempt", 2), ("run_id", 102), ("tested_sha", "d" * 40),
                             ("controller_sha", "e" * 40), ("selector", "^TestOther$"),
                             ("observed", dict(original["observed"], architecture="arm64"))):
            manifest = dict(original, **{field: value})
            self.write(self.output / "manifest.json", manifest)
            with self.subTest(field=field), self.assertRaises(evidence.EvidenceError):
                evidence.verify(self.output, evidence.context(self.request))
        self.write(self.output / "manifest.json", original)

    def test_symlinks_nonregular_files_duplicate_json_and_limits_are_rejected(self):
        for name in ("mds-cephfs-data-results.json",):
            path = self.receipts / name
            path.unlink()
            path.symlink_to(self.tests)
            with self.assertRaisesRegex(evidence.EvidenceError, "unsafe-input-path"):
                self.collect()
        with self.assertRaisesRegex(evidence.EvidenceError, "duplicate-json-field"):
            evidence.decode('{"a":1,"a":2}')
        with mock.patch.object(evidence, "FILE_LIMIT", 8), self.assertRaises(evidence.EvidenceError):
            evidence.read(self.tests, evidence.FILE_LIMIT)

    def test_output_limits_and_attempt_collision_preserve_existing_bundle(self):
        self.collect()
        saved = (self.output / "manifest.json").read_bytes()
        with self.assertRaisesRegex(evidence.EvidenceError, "attempt-collision"):
            self.collect()
        self.assertEqual((self.output / "manifest.json").read_bytes(), saved)
        with mock.patch.object(evidence, "TOTAL_LIMIT", 8), self.assertRaisesRegex(evidence.EvidenceError, "total-limit"):
            evidence.verify(self.output, evidence.context(self.request))

    def test_public_fields_are_private_free_and_unknown_fields_rejected_even_after_rehash(self):
        self.collect()
        path = self.output / "cap-send.json"
        value = json.loads(path.read_text())
        value["private_hostname"] = "example.invalid"
        self.write(path, value)
        manifest = json.loads((self.output / "manifest.json").read_text())
        for record in manifest["files"]:
            if record["name"] == path.name:
                record.update(size=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        manifest["total_bytes"] = sum(record["size"] for record in manifest["files"])
        self.write(self.output / "manifest.json", manifest)
        with self.assertRaisesRegex(evidence.EvidenceError, "schema-fields"):
            evidence.verify(self.output, evidence.context(self.request))
        for text in ("v2:192.0.2.1:3300/77", "/Users/example/private", "keyring secret"):
            with self.subTest(text=text), self.assertRaisesRegex(evidence.EvidenceError, "private-or-unbounded-field"):
                evidence.privacy({"field": text})

    def test_invalid_test_result_never_publishes_a_passed_test(self):
        self.write(self.tests, {"selector": evidence.SELECTOR, "expected": [], "observed": [], "problems": [], "passed": True})
        request, status = self.root / "request.json", self.root / "status.json"
        self.write(request, self.request)
        self.assertEqual(evidence.main(["collect", "--request", str(request), "--receipts", str(self.receipts),
                                        "--tests", str(self.tests), "--output", str(self.output), "--status", str(status)]), 1)
        self.assertEqual(json.loads(status.read_text())["test_result"], "unknown")

    def test_retained_native_bundle_matches_the_independently_recorded_run(self):
        request = {"execution_source": "linux-runner", "tested_sha": "b537120efe464d9f21f76fe3fd9f5152cd91e4fc",
                   "controller_sha": "b537120efe464d9f21f76fe3fd9f5152cd91e4fc", "run_id": 2026102301, "attempt": 1,
                   "matrix": dict(evidence.PROFILES[0], scenario=evidence.scenario_id(evidence.PROFILES[0]))}
        path = scenario_plan.ROOT / ".github/scenarios/testdata/cap-send-native-v1"
        manifest = evidence.verify(path, evidence.context(request))
        self.assertEqual(manifest["total_bytes"], 3348)
        caps = json.loads((path / "cap-send.json").read_text())
        self.assertEqual([flush["tid"] for flush in caps["flushes"]], [6, 7])
        self.assertEqual((caps["object_requests_before"], caps["object_requests_after"]), (177, 177))

    def test_workflow_keeps_the_collector_trusted_and_the_output_scope_closed(self):
        text = scenario_plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        capture = text.index('cp .trusted-native/tools/native_evidence.py')
        self.assertLess(capture, text.index('ref: ${{ inputs.ref }}'))
        self.assertIn('test "$(git rev-parse HEAD)" = "$TESTED_SHA"', text)
        self.assertLess(text.index('sh integration/run.sh'), text.index('native_evidence.py" collect'))
        self.assertIn("if: steps.ceph.outcome == 'success' && (matrix.test_run == '^" + evidence.TEST + "$' || (matrix.osd_soak || '') != '')", text)
        artifact = text[text.index('      - name: Preserve reviewed successful-native evidence'):text.index('      - name: Require complete evidence')]
        self.assertIn('path: ${{ runner.temp }}/success-evidence/', artifact)
        self.assertIn('retention-days: 7', artifact)
        self.assertIn('if-no-files-found: error', artifact)
        self.assertIn('github.run_attempt', artifact)
        self.assertNotIn('ceph-diagnostics', artifact)
        self.assertNotIn('/out', artifact)
        self.assertIn('"test_result":"%s","evidence_result":"%s"', text)
        self.assertIn('if test "$COLLECTION:$UPLOAD" != success:success;', text)

    def test_collection_failure_preserves_passed_test_and_incomplete_evidence_separately(self):
        request = self.root / "request.json"
        self.write(request, self.request)
        status = self.root / "status.json"
        (self.receipts / "mds-fresh-cleanup.json").unlink()
        self.assertEqual(evidence.main(["collect", "--request", str(request), "--receipts", str(self.receipts),
                                        "--tests", str(self.tests), "--output", str(self.output), "--status", str(status)]), 1)
        result = json.loads(status.read_text())
        self.assertEqual((result["test_result"], result["evidence_result"]), ("passed", "incomplete"))
        self.assertFalse(self.output.exists())
        self.assertNotIn(str(self.root), status.read_text())


if __name__ == "__main__":
    unittest.main()
