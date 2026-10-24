"""Daemon-free tests for the soak successful-evidence family.

Synthetic receipts exercise the collector, the trusted-validator boundary and
the closed projection. They never establish a native run or a hosted upload.
"""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import native_evidence as evidence
import osd_soak_receipts as receipts
import scenario_plan

DURATION = evidence.SOAK_DURATION_NS
TESTED = "a" * 40
CONTROLLER = "b" * 40
PRIVATE_MARKER = "private-host.example.invalid"


class NativeSoakEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receipts = self.root / "private-input"
        self.receipts.mkdir()
        self.output = self.root / "bundle"
        self.request = {"execution_source": "github-actions", "tested_sha": TESTED, "controller_sha": CONTROLLER, "run_id": 202, "attempt": 3,
                        "matrix": dict(evidence.SOAK_PROFILE, scenario=evidence.SOAK_SCENARIO)}
        self.tests = self.root / "tests.json"
        self.put(self.tests, self.gate())
        self.populate()

    # Fixture construction ------------------------------------------------

    @staticmethod
    def put(path, value):
        Path(path).write_text(json.dumps(value, sort_keys=True), encoding="utf-8")

    @staticmethod
    def gate():
        return {"selector": "", "expected": [],
                "observed": [{"suite": "api", "test": evidence.SOAK_TESTS[label], "result": "PASS"} for label in ("failover", "steady")],
                "problems": [], "passed": True}

    def stage(self, label, kind, value):
        self.put(self.receipts / ("osd-soak-v1-%s-%s.json" % (label, kind)),
                 dict(value, schema_version=1, test=receipts.TESTS[label], kind=kind))

    def populate(self, faults=1):
        manifest = {"key_type": "aes256k", "service_cipher": "aes256k", "connection_mode": "secure", "ip_family": "4", "runtime": "container",
                    "osd": "1", "osd_profile": "legacy-v6", "osd_store": "bluestore", "osd_replicas": "3",
                    "mds_fresh": False, "mds_cephfs_data": False, "rbd_base": False, "image": evidence.IMAGE,
                    "image_id": "sha256:" + "c" * 64, "architecture": "amd64",
                    "daemon_version": "ceph version 20.2.4 (%s) tentacle (stable)" % evidence.SOURCE,
                    "provider": "github.com/JSYoo5B/ceph-testcontainers-go", "provider_version": "v0.3.0"}
        self.put(self.receipts / "fixture-manifest.json", manifest)
        digest = hashlib.sha256((self.receipts / "fixture-manifest.json").read_bytes()).hexdigest()
        self.put(self.receipts / "osd-soak-v1-fixture-termination.json", {
            "schema_version": 1, "kind": "fixture_termination", "status": "passed", "configured_source_sha": TESTED,
            "configured_image": evidence.IMAGE, "observed_image_id": manifest["image_id"], "architecture": "amd64",
            "fixture_manifest_sha256": digest, "configured_duration_ns": DURATION, "suite_exit_success": True,
            "termination_succeeded": True, "termination_elapsed_ns": 7000, "expected_tests": list(evidence.SOAK_LABELS)})
        for label in evidence.SOAK_LABELS:
            self.stage(label, "started", {"configured_source_sha": TESTED, "configured_duration_ns": DURATION, "fixture_manifest_sha256": digest})
            owned = ["%s-seed" % label] + ["%s-%d" % (label, worker) for worker in range(4)]
            self.stage(label, "setup", {"elapsed_ns": 1000, "owned_objects": owned})
            counters = {"appends": 1000, "reads": 100, "unknowns": 10, "resubmissions": 10, "xattrs": 100, "truncations": 10}
            counters |= {"churns": 100, "unsent": 0} if label == "steady" else {"faults": faults, "replacements": 10, "resent": 0}
            self.stage(label, "workload", {"configured_duration_ns": DURATION, "actual_workload_ns": DURATION + 1000,
                                           "after_workload_start_ns": DURATION + 2000, "test_elapsed_ns": DURATION + 5000, "status": "passed",
                                           "first_error": "", "unresolved_outcomes": [], "counters": counters})
            self.stage(label, "cleanup", {"removed": True, "elapsed_ns": 1000,
                                          "objects": [{"object": name, "code": -2 if index == 2 else 0} for index, name in enumerate(owned)]})
            for worker in range(4):
                data = bytes([worker, 255, 0]) * 7
                encoded = base64.b64encode(data).decode()
                digest_data = hashlib.sha256(data).hexdigest()
                self.stage(label, "worker-%d" % worker, {
                    "worker": worker, "native_observation_elapsed_ns": 1000 + worker, "object": owned[worker + 1],
                    "expected_data": encoded, "expected_xattr": "12%d" % worker, "expected_length": len(data), "expected_sha256": digest_data,
                    "native_length": len(data), "native_sha256": digest_data, "matched": True, "native_source_commit": receipts.SOURCE,
                    "native_daemon_version": "20.2.4 " + receipts.SOURCE, "independent_native_c_api": True,
                    "native": {"code": 0, "version": 12 + worker, "seconds": 1750000000 + worker, "nanos": 123 + worker, "data": encoded,
                               "attributes": [{"name": base64.b64encode(b"soak").decode(), "value": base64.b64encode(("12%d" % worker).encode()).decode()}]}})
            if label == "failover":
                self.stage(label, "recovery-clean", {"passed": True})
                when = 0
                for cycle in range(1, faults + 1):
                    for phase in ("kill", "restart", "clean"):
                        when += 1000
                        self.stage(label, "fault-%03d-%s" % (cycle, phase),
                                   {"passed": True, "elapsed_ns": 500, "after_workload_start_ns": when, "victim": str(cycle % 3)})

    def change(self, name, change):
        path = self.receipts / name
        value = json.loads(path.read_text())
        change(value)
        self.put(path, value)

    def collect(self, request=None):
        return evidence.collect(request or self.request, self.receipts, self.tests, self.output)

    def context(self, request=None):
        return evidence.context(request or self.request)

    def reset_output(self):
        for child in self.output.iterdir():
            child.unlink()
        self.output.rmdir()

    def rejected(self, code=None, request=None):
        """Collection must fail closed and leave no output behind."""
        with self.assertRaises(evidence.EvidenceError) as raised:
            self.collect(request)
        if code:
            self.assertEqual(str(raised.exception), code)
        self.assertFalse(self.output.exists())

    def rewrite_valid(self, name, change):
        """Change a published file and keep the manifest's size/hash consistent."""
        path = self.output / name
        value = json.loads(path.read_text())
        change(value)
        data = evidence.encoded(value)
        path.write_bytes(data)
        manifest = json.loads((self.output / "manifest.json").read_text())
        for record in manifest["files"]:
            if record["name"] == name:
                record.update(size=len(data), sha256=hashlib.sha256(data).hexdigest())
        manifest["total_bytes"] = sum(record["size"] for record in manifest["files"])
        self.put(self.output / "manifest.json", manifest)

    # Acceptance ----------------------------------------------------------

    def test_complete_bundle_is_attributed_hashed_and_closed(self):
        manifest = self.collect()
        self.assertEqual({p.name for p in self.output.iterdir()}, evidence.SOAK_FILES | {"manifest.json"})
        self.assertEqual((manifest["kind"], manifest["schema_version"]), ("native-soak-success", 1))
        self.assertEqual((manifest["tested_sha"], manifest["controller_sha"], manifest["run_id"], manifest["attempt"]), (TESTED, CONTROLLER, 202, 3))
        self.assertEqual(manifest["scenario"], evidence.SOAK_SCENARIO)
        self.assertEqual(manifest["selector"], evidence.SOAK_SELECTOR)
        self.assertEqual(manifest["configured_image"], evidence.IMAGE)
        self.assertEqual(manifest["observed"]["image_id"], "sha256:" + "c" * 64)
        self.assertEqual(evidence.verify(self.output, self.context()), manifest)
        soak = json.loads((self.output / "soak.json").read_text())
        self.assertEqual(soak["observations"], 8)
        self.assertEqual(soak["workloads"]["steady"]["workers"][1]["mtime_seconds"], 1750000001)
        self.assertEqual(len(soak["workloads"]["failover"]["faults"]), 3)
        inventory = json.loads((self.output / "receipts.json").read_text())
        self.assertEqual(len(inventory["files"]), 22)
        self.assertEqual(inventory["files"]["osd-soak-v1-steady-worker-0.json"]["sha256"],
                         hashlib.sha256((self.receipts / "osd-soak-v1-steady-worker-0.json").read_bytes()).hexdigest())
        cleanup = json.loads((self.output / "cleanup.json").read_text())
        self.assertEqual(cleanup["workloads"]["steady"]["codes"], [0, 0, -2, 0, 0])

    def test_raw_bytes_xattrs_names_and_private_strings_are_not_retained(self):
        self.change("osd-soak-v1-steady-setup.json", lambda x: x.update(unreviewed_note=PRIVATE_MARKER))
        self.collect()
        text = "".join(p.read_text() for p in self.output.iterdir())
        for omitted in (PRIVATE_MARKER, "steady-seed", "steady-0", "expected_data", "attributes", "AP8A", "native_daemon_version", "first_error"):
            self.assertNotIn(omitted, text)

    def test_many_fault_cycles_stay_within_projection_limits(self):
        self.populate(faults=64)
        self.collect()
        self.assertEqual(len(json.loads((self.output / "soak.json").read_text())["workloads"]["failover"]["faults"]), 192)
        sizes = [p.stat().st_size for p in self.output.iterdir()]
        self.assertLessEqual(max(sizes), evidence.FILE_LIMIT)
        self.assertLessEqual(sum(sizes), evidence.TOTAL_LIMIT)

    def test_projection_budget_is_the_reason_raw_retention_is_not_used(self):
        # Eight maximum-sized raw receipts alone exceed the bundle budget.
        self.assertGreater(8 * evidence.SOAK_RECEIPT_LIMIT, evidence.TOTAL_LIMIT)

    def test_catalog_row_is_the_only_admitted_profile(self):
        rows = [row for row in scenario_plan.load_catalog() if row.get("params", {}).get("osd_soak")]
        self.assertEqual([row["id"] for row in rows], [evidence.SOAK_SCENARIO])
        request = dict(self.request, matrix=dict(rows[0]["params"], scenario=rows[0]["id"]))
        self.assertEqual(evidence.context(request)["profile"], evidence.SOAK_PROFILE)

    # Trusted controller boundary -----------------------------------------

    def test_validator_must_be_the_one_beside_the_trusted_collector(self):
        fake = mock.Mock(__file__=str(self.root / "osd_soak_receipts.py"))
        with mock.patch.dict("sys.modules", {"osd_soak_receipts": fake}):
            self.rejected("trusted-soak-validator-location")

    def test_missing_trusted_validator_is_incomplete_not_a_pass(self):
        with mock.patch.dict("sys.modules", {"osd_soak_receipts": None}):
            self.rejected("trusted-soak-validator-missing")

    def test_tampered_native_bytes_fail_in_the_validator(self):
        self.change("osd-soak-v1-steady-worker-0.json", lambda x: x["native"].update(data="AA=="))
        self.rejected("soak-receipts-invalid")

    # Externally expected workload selection -----------------------------

    def test_receipts_cannot_shrink_the_externally_expected_pair(self):
        for path in self.receipts.glob("osd-soak-v1-failover-*"):
            path.unlink()
        self.change("osd-soak-v1-fixture-termination.json", lambda x: x.update(expected_tests=["steady"]))
        self.rejected("soak-receipts-invalid")

    def test_mismatched_workload_labels_are_rejected(self):
        for expected in (["failover"], ["failover", "steady"], ["steady", "steady"], ["steady", "other"], []):
            with self.subTest(expected=expected):
                self.change("osd-soak-v1-fixture-termination.json", lambda x: x.update(expected_tests=expected))
                self.rejected("soak-receipts-invalid")
        self.change("osd-soak-v1-fixture-termination.json", lambda x: x.update(expected_tests=["steady", "failover"]))
        self.collect()

    def test_wrong_test_identity_is_rejected(self):
        self.change("osd-soak-v1-failover-worker-1.json", lambda x: x.update(test=receipts.TESTS["steady"]))
        self.rejected("soak-receipts-invalid")

    def test_unselected_extra_family_member_is_refused(self):
        self.put(self.receipts / "osd-soak-v1-steady-worker-4.json", {"schema_version": 1})
        self.rejected("soak-receipts-invalid")

    # Workers, objects and native mtime -----------------------------------

    def test_missing_worker_is_rejected(self):
        (self.receipts / "osd-soak-v1-failover-worker-3.json").unlink()
        self.rejected("soak-receipts-invalid")

    def test_duplicate_worker_identity_or_object_is_rejected(self):
        self.change("osd-soak-v1-steady-worker-2.json", lambda x: x.update(worker=1))
        self.rejected("soak-receipts-invalid")
        self.change("osd-soak-v1-steady-worker-2.json", lambda x: x.update(worker=2, object="steady-1"))
        self.rejected("soak-receipts-invalid")

    def test_absent_or_invalid_native_mtime_is_rejected(self):
        name = "osd-soak-v1-failover-worker-0.json"
        for field, values in (("seconds", [None, True, "1", 1.5, -1, 1 << 32]), ("nanos", [None, True, "1", 1.5, -1, 10**9])):
            for value in values:
                with self.subTest(field=field, value=value):
                    self.change(name, lambda x: x["native"].update({field: value}))
                    self.rejected("soak-receipts-invalid")
            self.change(name, lambda x: x["native"].pop(field))
            self.rejected("soak-receipts-invalid")
            self.change(name, lambda x: x["native"].update(seconds=1750000000, nanos=5))

    def test_mtime_wire_bounds_are_admitted_and_projected(self):
        self.change("osd-soak-v1-steady-worker-0.json", lambda x: x["native"].update(seconds=0xFFFFFFFF, nanos=999_999_999))
        self.collect()
        worker = json.loads((self.output / "soak.json").read_text())["workloads"]["steady"]["workers"][0]
        self.assertEqual((worker["mtime_seconds"], worker["mtime_nanos"]), (0xFFFFFFFF, 999_999_999))

    # Fault intervals ------------------------------------------------------

    def test_out_of_interval_faults_are_rejected(self):
        original = {"kill": 1000, "restart": 2000, "clean": 3000}
        for phase, when in (("kill", DURATION + 1500), ("restart", DURATION + 6000), ("clean", -1), ("clean", None), ("clean", True)):
            name = "osd-soak-v1-failover-fault-001-%s.json" % phase
            with self.subTest(phase=phase, when=when):
                self.change(name, lambda x: x.update(after_workload_start_ns=when))
                self.rejected("soak-receipts-invalid")
            self.change(name, lambda x: x.update(after_workload_start_ns=original[phase]))
        self.collect()

    def test_fault_order_victim_and_missing_phase_are_rejected(self):
        name = "osd-soak-v1-failover-fault-001-restart.json"
        self.change(name, lambda x: x.update(after_workload_start_ns=500))
        self.rejected("soak-receipts-invalid")
        self.change(name, lambda x: x.update(after_workload_start_ns=2000, victim="5"))
        self.rejected("soak-receipts-invalid")
        (self.receipts / name).unlink()
        self.rejected("soak-receipts-invalid")

    def test_projection_rejects_out_of_interval_fault_even_with_a_consistent_rehash(self):
        self.collect()
        self.rewrite_valid("soak.json", lambda x: x["workloads"]["failover"]["faults"][2].update(after_workload_start_ns=10**18))
        with self.assertRaisesRegex(evidence.EvidenceError, "fault-outside-interval"):
            evidence.verify(self.output, self.context())

    # Owned cleanup --------------------------------------------------------

    def test_incomplete_owned_cleanup_is_rejected(self):
        name = "osd-soak-v1-steady-cleanup.json"
        good = json.loads((self.receipts / name).read_text())
        for mutate in (lambda x: x["objects"].pop(),
                       lambda x: x["objects"][0].update(code=-5),
                       lambda x: x.update(removed=False),
                       lambda x: x["objects"].append({"object": "unknown", "code": 0}),
                       lambda x: x["objects"].append(dict(x["objects"][0]))):
            self.put(self.receipts / name, good)
            self.change(name, mutate)
            self.rejected("soak-receipts-invalid")
        self.put(self.receipts / name, good)
        self.collect()

    def test_fixture_termination_must_have_succeeded(self):
        name = "osd-soak-v1-fixture-termination.json"
        good = json.loads((self.receipts / name).read_text())
        for field, value in (("termination_succeeded", False), ("suite_exit_success", False), ("status", "failed"), ("termination_elapsed_ns", 0)):
            with self.subTest(field=field):
                self.put(self.receipts / name, dict(good, **{field: value}))
                self.rejected("soak-receipts-invalid")
        (self.receipts / name).unlink()
        self.rejected("soak-receipts-invalid")

    def test_projection_rejects_cleanup_downgrade_even_with_a_consistent_rehash(self):
        self.collect()
        self.rewrite_valid("cleanup.json", lambda x: x["workloads"]["failover"].update(codes=[0, 0, 0, 0, -5]))
        with self.assertRaisesRegex(evidence.EvidenceError, "cleanup-incomplete"):
            evidence.verify(self.output, self.context())

    # Attribution -----------------------------------------------------------

    def test_wrong_tested_sha_is_rejected_at_collection(self):
        self.rejected("soak-receipts-invalid", dict(self.request, tested_sha="d" * 40))

    def test_wrong_sha_controller_run_or_attempt_cannot_validate(self):
        self.collect()
        for field, value in (("controller_sha", "e" * 40), ("tested_sha", "d" * 40), ("run_id", 203), ("attempt", 4), ("execution_source", "linux-runner")):
            with self.subTest(field=field), self.assertRaisesRegex(evidence.EvidenceError, "manifest-context-mismatch"):
                evidence.verify(self.output, self.context(dict(self.request, **{field: value})))

    def test_wrong_image_and_fixture_profile_are_rejected(self):
        self.change("fixture-manifest.json", lambda x: x.update(image="quay.io/ceph/ceph:latest"))
        self.rejected()
        self.change("fixture-manifest.json", lambda x: x.update(image=evidence.IMAGE, osd_replicas="1"))
        self.rejected()

    def test_fixture_manifest_changed_after_termination_is_rejected(self):
        self.change("fixture-manifest.json", lambda x: x.update(image_id="sha256:" + "d" * 64))
        self.rejected("soak-receipts-invalid")

    def test_attempt_collision_preserves_the_first_bundle(self):
        self.collect()
        saved = (self.output / "manifest.json").read_bytes()
        with self.assertRaisesRegex(evidence.EvidenceError, "attempt-collision"):
            self.collect()
        self.assertEqual((self.output / "manifest.json").read_bytes(), saved)

    # Unsupported profile ---------------------------------------------------

    def test_unsupported_profiles_are_refused(self):
        for field, value in (("osd_soak", "8h"), ("osd_soak", "30s"), ("osd_replicas", "1"), ("osd_store", "memstore"), ("key", "aes"),
                             ("family", "6"), ("mode", "crc"), ("runtime", "host"), ("test_run", "^TestCephOSDSoakIntegration$"),
                             ("mds_fresh", "1"), ("scenario", "other"), ("image", "quay.io/ceph/ceph:latest"), ("unknown", "1")):
            request = copy.deepcopy(self.request)
            request["matrix"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(evidence.EvidenceError):
                evidence.context(request)

    def test_soak_bundle_is_not_a_cap_send_bundle(self):
        self.collect()
        capsend = evidence.PROFILES[0]
        request = dict(self.request, matrix=dict(capsend, scenario=evidence.scenario_id(capsend)))
        with self.assertRaises(evidence.EvidenceError):
            evidence.verify(self.output, evidence.context(request))
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.put(self.output / "manifest.json", dict(manifest, kind="native-success"))
        with self.assertRaisesRegex(evidence.EvidenceError, "schema-version"):
            evidence.verify(self.output, self.context())

    # Required tests --------------------------------------------------------

    def test_gate_must_show_both_actual_parent_passes(self):
        both = self.gate()["observed"]
        cases = [{"passed": False}, {"observed": both[:1]}, {"observed": []}, {"expected": [["api", "TestCephOSDSoakIntegration"]]},
                 {"selector": "^x$"}, {"problems": ["a problem"]},
                 {"observed": [dict(both[0], result="SKIP"), both[1]]}, {"observed": [dict(both[0], suite="client"), both[1]]},
                 {"observed": [both[0], both[0]]}, {"observed": both + [{"suite": "api", "test": "TestExtra", "result": "PASS"}]},
                 {"observed": [dict(both[0], test="TestCephOSDSoakOtherIntegration"), both[1]]}]
        for change in cases:
            with self.subTest(change=change):
                self.put(self.tests, dict(self.gate(), **change))
                self.rejected("required-test-not-passed")

    # Unsafe names, inputs and outputs --------------------------------------

    def test_unsafe_receipt_names_are_refused(self):
        for name in ("osd-soak-v1-steady-worker-0.json.bak", "osd-soak-v1-steady-worker-0.json ", "osd-soak-v1-fixture-termination.JSON",
                     "osd-soak-v1-steady-worker-00.json", "osd-soak-v1-extra", "osd-soak-v1-.._x.json", "osd-soak-v1-steady-started.json\n"):
            path = self.receipts / name
            path.write_text("{}")
            with self.subTest(name=name):
                self.rejected("soak-receipts-invalid")
            path.unlink()

    def test_symlink_and_nonregular_receipts_are_refused(self):
        target = self.root / "outside.json"
        target.write_text("{}")
        path = self.receipts / "osd-soak-v1-steady-started.json"
        path.unlink()
        path.symlink_to(target)
        self.rejected("soak-receipts-invalid")
        path.unlink()
        path.mkdir()
        self.rejected("soak-receipts-invalid")

    def test_symlink_receipt_directory_is_refused(self):
        link = self.root / "linked"
        link.symlink_to(self.receipts)
        with self.assertRaisesRegex(evidence.EvidenceError, "unsafe-receipt-directory"):
            evidence.collect(self.request, link, self.tests, self.output)

    def test_duplicate_json_fields_and_invalid_json_are_refused(self):
        path = self.receipts / "osd-soak-v1-steady-workload.json"
        path.write_text('{"schema_version": 1, "schema_version": 1}')
        self.rejected("soak-receipts-invalid")
        path.write_text("{not json")
        self.rejected("soak-receipts-invalid")

    def test_receipt_size_boundary(self):
        path = self.receipts / "osd-soak-v1-steady-started.json"
        base = path.read_bytes().rstrip()
        limit = evidence.SOAK_RECEIPT_LIMIT
        path.write_bytes(base + b" " * (limit - len(base)))
        self.assertEqual(path.stat().st_size, limit)
        self.collect()
        self.assertEqual(json.loads((self.output / "receipts.json").read_text())["files"][path.name]["size"], limit)
        self.reset_output()
        path.write_bytes(base + b" " * (limit + 1 - len(base)))
        self.rejected("soak-receipts-invalid")

    def test_gate_input_boundary(self):
        self.collect()
        limit = max(p.stat().st_size for p in self.output.iterdir())
        self.reset_output()
        base = self.tests.read_bytes()
        self.assertLess(len(base), limit)
        with mock.patch.object(evidence, "FILE_LIMIT", limit):
            self.tests.write_bytes(base + b" " * (limit - len(base)))
            self.collect()
            self.reset_output()
            self.tests.write_bytes(base + b" " * (limit + 1 - len(base)))
            self.rejected("file-limit")

    def test_output_file_and_total_boundaries(self):
        self.collect()
        sizes = [p.stat().st_size for p in self.output.iterdir()]
        self.reset_output()
        with mock.patch.object(evidence, "FILE_LIMIT", max(sizes)):
            self.collect()
        self.reset_output()
        with mock.patch.object(evidence, "FILE_LIMIT", max(sizes) - 1):
            self.rejected()
        with mock.patch.object(evidence, "TOTAL_LIMIT", sum(sizes)):
            self.collect()
        self.reset_output()
        with mock.patch.object(evidence, "TOTAL_LIMIT", sum(sizes) - 1):
            self.rejected("total-limit")

    def test_bundle_inventory_and_hash_tampering_are_rejected(self):
        self.collect()
        path = self.output / "soak.json"
        saved = path.read_bytes()
        path.write_bytes(saved + b" ")
        with self.assertRaisesRegex(evidence.EvidenceError, "file-hash-or-size"):
            evidence.verify(self.output, self.context())
        path.write_bytes(saved)
        extra = self.output / "raw-worker.json"
        extra.write_text("{}")
        with self.assertRaisesRegex(evidence.EvidenceError, "bundle-inventory"):
            evidence.verify(self.output, self.context())
        extra.unlink()
        (self.output / "receipts.json").unlink()
        with self.assertRaisesRegex(evidence.EvidenceError, "bundle-inventory"):
            evidence.verify(self.output, self.context())

    def test_receipt_inventory_must_match_fault_count_and_manifest_hash(self):
        self.collect()
        self.rewrite_valid("receipts.json", lambda x: x["files"].pop("osd-soak-v1-failover-fault-001-clean.json"))
        with self.assertRaisesRegex(evidence.EvidenceError, "receipt-inventory"):
            evidence.verify(self.output, self.context())

    # Workflow boundary ------------------------------------------------------

    def test_workflow_copies_the_validator_before_tested_code_and_gates_every_soak_row(self):
        text = scenario_plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        tested_checkout = text.index("ref: ${{ inputs.ref }}")
        for name in ("native_evidence.py", "osd_soak_receipts.py"):
            self.assertLess(text.index("cp .trusted-native/tools/%s" % name), tested_checkout)
        self.assertNotIn("tools/osd_soak_receipts.py", text.replace("cp .trusted-native/tools/osd_soak_receipts.py", ""))
        # Evidence is required for any soak row, so an unadmitted profile fails
        # closed in the collector instead of silently skipping retention.
        condition = "(matrix.test_run == '^" + evidence.TEST + "$' || (matrix.osd_soak || '') != '')"
        self.assertEqual(text.count(condition), 3)
        self.assertIn('--receipts "$RUNNER_TEMP/ceph-diagnostics"', text)

    # Public verdict ---------------------------------------------------------

    def run_main(self, *argv):
        with mock.patch("builtins.print"):
            return evidence.main(list(argv))

    def collect_cli(self, request, status):
        return self.run_main("collect", "--request", str(request), "--receipts", str(self.receipts), "--tests", str(self.tests),
                             "--output", str(self.output), "--status", str(status))

    def test_native_pass_with_failed_collection_is_incomplete_not_failed_or_complete(self):
        request, status = self.root / "request.json", self.root / "status.json"
        self.put(request, self.request)
        (self.receipts / "osd-soak-v1-steady-worker-0.json").unlink()
        self.assertEqual(self.collect_cli(request, status), 1)
        self.assertEqual(json.loads(status.read_text()), {"evidence_result": "incomplete", "reason": "soak-receipts-invalid", "test_result": "passed"})
        self.assertFalse(self.output.exists())

    def test_unreadable_request_classifies_the_test_with_the_cap_send_gate(self):
        status = self.root / "status.json"
        self.assertEqual(self.collect_cli(self.root / "missing.json", status), 1)
        value = json.loads(status.read_text())
        self.assertEqual((value["test_result"], value["evidence_result"], value["reason"]), ("unknown", "incomplete", "required-test-not-passed"))

    def test_failed_test_gate_is_unknown_never_passed(self):
        request, status = self.root / "request.json", self.root / "status.json"
        self.put(request, self.request)
        self.put(self.tests, dict(self.gate(), passed=False))
        self.assertEqual(self.collect_cli(request, status), 1)
        self.assertEqual(json.loads(status.read_text())["test_result"], "unknown")

    def test_complete_collection_and_independent_verify_through_the_cli(self):
        request, status = self.root / "request.json", self.root / "status.json"
        self.put(request, self.request)
        self.assertEqual(self.collect_cli(request, status), 0)
        self.assertEqual(json.loads(status.read_text()), {"evidence_result": "complete", "test_result": "passed"})
        self.assertEqual(self.run_main("verify", "--request", str(request), "--bundle", str(self.output)), 0)
        self.put(request, dict(self.request, attempt=4))
        self.assertEqual(self.run_main("verify", "--request", str(request), "--bundle", str(self.output)), 1)


if __name__ == "__main__":
    unittest.main()
