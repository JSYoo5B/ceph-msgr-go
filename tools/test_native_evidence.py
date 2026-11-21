"""Synthetic bundle rejection tests; these never establish native acceptance."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import native_evidence as evidence
import native_evidence_capsend as capsend

REPO = Path(__file__).resolve().parents[1]
# The release this repository's fixture pins; receipts default to it.
PIN = evidence.pinned_provider_text((REPO / "integration" / "fixture" / "go.mod").read_bytes())
import scenario_plan


class NativeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receipts = self.root / "private-input"
        self.receipts.mkdir()
        profile = capsend.PROFILES[0]
        self.request = {"execution_source": "linux-runner", "tested_sha": "a" * 40, "controller_sha": "b" * 40, "run_id": 101, "attempt": 1,
                        "matrix": dict(profile, scenario=capsend.scenario_id(profile))}
        self.tests = self.root / "tests.json"
        self.write(self.tests, {"selector": capsend.SELECTOR, "expected": [["api", capsend.TEST]],
                               "observed": [{"suite": "api", "test": capsend.TEST, "result": "PASS"}],
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
                   "provider": evidence.PROVIDER, "provider_version": PIN}
        return {"fixture-manifest.json": runtime,
                "mds-cephfs-data-results.json": {"source_commit": evidence.SOURCE, "global_id": 10, "cephfs_nonce": 17,
                                                "facts": {"restart_recipe": "cap-send", "cap_send_recovery": facts},
                                                "oracle": {"endpoint": "v2:192.0.2.3:3300/77", "baseline_config": {"private": "omitted"}}},
                "mds-cephfs-data-cleanup.json": dict.fromkeys(capsend.CLEAN_FLAGS, True) | {"source_commit": evidence.SOURCE,
                    "residual_owned_sessions": [], "kept_new_blocklist": [], "config": {"unreviewed": "omitted"}},
                "mds-fresh-cleanup.json": dict.fromkeys(capsend.FRESH_FLAGS, True) | {"source_commit": evidence.SOURCE}}

    def collect(self, pin=PIN):
        return evidence.collect(self.request, self.receipts, self.tests, self.output, pin)

    def test_successful_bundle_is_closed_hashed_and_exactly_attributed(self):
        manifest = self.collect()
        self.assertEqual({p.name for p in self.output.iterdir()}, capsend.FILES | {"manifest.json"})
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
            self.assertIn(evidence.context(request)["profile"], capsend.PROFILES)
        # The rows declare their fixture capability; evidence recorded
        # before the declaration (no capabilities field) keeps its context.
        self.assertEqual({row["params"].get("capabilities") for row in rows}, {capsend.CAPABILITIES})
        legacy = dict(self.request, matrix={key: value for key, value in rows[0]["params"].items() if key != "capabilities"} | {"scenario": rows[0]["id"]})
        self.assertIn(evidence.context(legacy)["profile"], capsend.PROFILES)
        for field, value in (("scenario", "arbitrary"), ("test_run", "^TestOther$"), ("mode", "secure"),
                             ("image", "quay.io/ceph/ceph:latest"), ("unknown", "1"), ("capabilities", "exclusive-mds-cephfs"),
                             ("capabilities", "")):
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
                       {"observed": [{"suite": "api", "test": capsend.TEST, "result": "SKIP"}]},
                       {"observed": [{"suite": "client", "test": capsend.TEST, "result": "PASS"}]}):
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

    def test_collection_admits_the_release_the_tested_revision_pins(self):
        # The trusted controller and the tested revision can pin different
        # collectable releases during a provider move. The receipt must
        # report the tested revision's pin, never the controller's.
        self.assertEqual(evidence.COLLECTABLE_PROVIDER_VERSIONS, {"v0.5.0", "v1.0.1"})
        for pin in sorted(evidence.COLLECTABLE_PROVIDER_VERSIONS):
            others = sorted(evidence.COLLECTABLE_PROVIDER_VERSIONS - {pin})
            for field, value in [("provider_version", other) for other in others] + [
                    ("provider_version", "v0.3.0"), ("provider_version", "v1.0.0"), ("provider_version", ""),
                    ("provider", "example.org/other-provider")]:
                data = dict(self.raw["fixture-manifest.json"], provider_version=pin) | {field: value}
                self.write(self.receipts / "fixture-manifest.json", data)
                with self.subTest(pin=pin, field=field, value=value), self.assertRaisesRegex(evidence.EvidenceError, "provider-mismatch"):
                    self.collect(pin)
            self.write(self.receipts / "fixture-manifest.json", dict(self.raw["fixture-manifest.json"], provider_version=pin))
            self.assertEqual(self.collect(pin)["observed"]["provider_version"], pin)
            for name in list(self.output.iterdir()):
                name.unlink()
            self.output.rmdir()
        self.write(self.receipts / "fixture-manifest.json", dict(self.raw["fixture-manifest.json"], provider_version="v1.0.0"))
        for pin in ("v1.0.0", "v0.4.0", "", None):
            with self.subTest(pin=pin), self.assertRaisesRegex(evidence.EvidenceError, "provider-pin"):
                self.collect(pin)
        self.write(self.receipts / "fixture-manifest.json", self.raw["fixture-manifest.json"])

    def test_the_tested_pin_uses_the_go_module_parser(self):
        module = evidence.PROVIDER
        self.assertIn(PIN, evidence.COLLECTABLE_PROVIDER_VERSIONS)
        good = {
            "block": "module m\n\ngo 1.26.0\n\nrequire (\n\t%s v1.0.1\n\tother.example/x v1.2.3\n)\n" % module,
            "single line": "module m\nrequire %s v0.5.0\n" % module,
            "indirect comment": "module m\nrequire (\n\t%s v1.0.1 // indirect\n)\n" % module,
            "quoted path and release": 'module m\nrequire "%s" "v1.0.1"\n' % module,
            "replace of another module": "module m\nrequire %s v1.0.1\nreplace other.example/x => ../x\n" % module,
        }
        for name, text in good.items():
            with self.subTest(name):
                self.assertIn(evidence.pinned_provider_text(text.encode()), evidence.COLLECTABLE_PROVIDER_VERSIONS)
        bad = {
            "missing": "module m\nrequire other.example/x v1.2.3\n",
            "unknown release": "module m\nrequire %s v1.0.0\n" % module,
            "two requirements": "module m\nrequire %s v0.5.0\nrequire %s v1.0.1\n" % (module, module),
            "no release": "module m\nrequire (\n\t%s\n)\n" % module,
            "replace line": "module m\nrequire %s v1.0.1\nreplace %s => ../ctg\n" % (module, module),
            "replace block": "module m\nrequire %s v1.0.1\nreplace (\n\t%s v1.0.1 => ../ctg\n)\n" % (module, module),
            "quoted replace": 'module m\nrequire %s v1.0.1\nreplace "%s" => "../ctg"\n' % (module, module),
            "pseudo-version": "module m\nrequire %s v1.0.2-0.20261120000000-abcdefabcdef\n" % module,
            "unparsable": "module m\nrequire (\n\t%s v1.0.1\n" % module,
            "binary": "require \xff\n",
        }
        for name, text in bad.items():
            with self.subTest(name), self.assertRaisesRegex(evidence.EvidenceError, "provider-pin"):
                evidence.pinned_provider_text(text.encode("latin-1"))
        with self.assertRaisesRegex(evidence.EvidenceError, "provider-pin"):
            evidence.pinned_provider_text(b"// x\n" * (evidence.PIN_LIMIT // 4))

    def test_the_expected_release_comes_from_the_tested_commit(self):
        # The tested run may edit its working tree; the expected release is
        # read from the commit object before it runs and must not follow an edit.
        repo = self.root / "tested"
        mod = repo / "integration" / "fixture" / "go.mod"
        mod.parent.mkdir(parents=True)
        mod.write_text("module m\nrequire %s v0.5.0\n" % evidence.PROVIDER)

        def git(*arguments):
            return subprocess.run(["git", "-C", str(repo), *arguments], check=True, capture_output=True, text=True,
                                  env={"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
                                       "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.org",
                                       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.org"}).stdout.strip()

        git("init", "-q")
        git("add", ".")
        git("commit", "-q", "-m", "pin")
        commit = git("rev-parse", "HEAD")
        mod.write_text("module m\nrequire %s v1.0.1\n" % evidence.PROVIDER)
        self.assertEqual(evidence.pinned_provider(repo, commit), "v0.5.0")
        for value in ("HEAD", commit[:12], "0" * 40, ""):
            with self.subTest(commit=value), self.assertRaisesRegex(evidence.EvidenceError, "provider-pin"):
                evidence.pinned_provider(repo, value)
        (repo / "go.work").write_text("go 1.26.0\nuse ./integration/fixture\n")
        git("add", "go.work")
        git("commit", "-q", "-m", "workspace")
        with self.assertRaisesRegex(evidence.EvidenceError, "provider-pin"):
            evidence.pinned_provider(repo, git("rev-parse", "HEAD"))
        with mock.patch("builtins.print") as printed:
            self.assertEqual(evidence.main(["pin", "--repo", str(repo), "--commit", commit]), 0)
        self.assertEqual(printed.call_args.args, ("provider_pin=v0.5.0",))

    def test_verification_admits_only_the_named_retained_provider_releases(self):
        ctx = evidence.context(self.request)
        observed = self.collect()["observed"]
        self.assertEqual(evidence.RETAINED_PROVIDER_VERSIONS, {"v0.3.0", "v0.4.0", "v0.5.0", "v1.0.1"})
        for version in ("v0.3.0", "v0.4.0", "v0.5.0", "v1.0.1"):
            evidence.validate_runtime(dict(observed, provider_version=version), ctx)
        for version in ("v0.2.0", "v0.6.0", "v1.0.0", "", "main"):
            with self.subTest(version=version), self.assertRaisesRegex(evidence.EvidenceError, "provider-mismatch"):
                evidence.validate_runtime(dict(observed, provider_version=version), ctx)

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
        self.write(self.tests, {"selector": capsend.SELECTOR, "expected": [], "observed": [], "problems": [], "passed": True})
        request, status = self.root / "request.json", self.root / "status.json"
        self.write(request, self.request)
        self.assertEqual(evidence.main(["collect", "--request", str(request), "--receipts", str(self.receipts),
                                        "--tests", str(self.tests), "--output", str(self.output), "--status", str(status),
                                        "--provider-pin", PIN]), 1)
        self.assertEqual(json.loads(status.read_text())["test_result"], "unknown")

    def test_retained_native_bundle_matches_the_independently_recorded_run(self):
        request = {"execution_source": "linux-runner", "tested_sha": "b537120efe464d9f21f76fe3fd9f5152cd91e4fc",
                   "controller_sha": "b537120efe464d9f21f76fe3fd9f5152cd91e4fc", "run_id": 2026102301, "attempt": 1,
                   "matrix": dict(capsend.PROFILES[0], scenario=capsend.scenario_id(capsend.PROFILES[0]))}
        path = scenario_plan.ROOT / ".github/scenarios/testdata/cap-send-native-v1"
        try:
            manifest = evidence.verify(path, evidence.context(request))
        except evidence.EvidenceError as error:
            # Preserve checkout-byte diagnostics without normalizing evidence or
            # changing the independently recorded manifest's hashes.
            records = json.loads((path / "manifest.json").read_bytes())["files"]
            diagnostic = []
            for record in records:
                data = (path / record["name"]).read_bytes()
                diagnostic.append({"name": record["name"], "recorded_size": record["size"], "size": len(data),
                                   "recorded_sha256": record["sha256"], "sha256": hashlib.sha256(data).hexdigest(),
                                   "crlf": data.count(b"\r\n"), "lf": data.count(b"\n")})
            relative = [str((path / record["name"]).relative_to(scenario_plan.ROOT)).replace("\\", "/") for record in records]
            attributes = subprocess.run(["git", "check-attr", "text", "eol", "--", *relative], cwd=scenario_plan.ROOT,
                                        capture_output=True, text=True, check=False)
            self.fail("retained bundle failed %s; checkout bytes=%s; attributes=%s" %
                      (error, json.dumps(diagnostic, sort_keys=True), attributes.stdout.strip()))
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
        # The family decision is the trusted collector's step output
        # (test_native_evidence_families.py).
        self.assertEqual(text.count("steps.evidence_family.outputs.family != ''"), 4)
        # The expected provider release is read from the tested commit before
        # any tested code runs, and collect takes only that step's output.
        pin = text.index('native_evidence.py" pin --repo "$GITHUB_WORKSPACE" --commit "$TESTED_SHA"')
        self.assertLess(text.index('test "$(git rev-parse HEAD)" = "$TESTED_SHA"'), pin)
        self.assertLess(pin, text.index('go -C integration/fixture mod download'))
        self.assertLess(pin, text.index('sh integration/run.sh'))
        self.assertIn('PROVIDER_PIN: ${{ steps.provider_pin.outputs.provider_pin }}', text)
        self.assertIn('--provider-pin "$PROVIDER_PIN"', text)
        self.assertNotIn('--provider-pin "$GITHUB_WORKSPACE', text)
        artifact = text[text.index('      - name: Preserve reviewed successful-native evidence'):text.index('      - name: Require complete evidence')]
        self.assertIn('path: ${{ runner.temp }}/success-evidence/', artifact)
        self.assertIn('retention-days: 7', artifact)
        self.assertIn('if-no-files-found: error', artifact)
        self.assertIn('github.run_attempt', artifact)
        self.assertNotIn('ceph-diagnostics', artifact)
        self.assertNotIn('/out', artifact)
        self.assertIn('native_terminal.py" --directory "$RUNNER_TEMP"', text)
        self.assertIn('if test "$COLLECTION:$UPLOAD" != success:success;', text)

    def test_collection_failure_preserves_passed_test_and_incomplete_evidence_separately(self):
        request = self.root / "request.json"
        self.write(request, self.request)
        status = self.root / "status.json"
        (self.receipts / "mds-fresh-cleanup.json").unlink()
        self.assertEqual(evidence.main(["collect", "--request", str(request), "--receipts", str(self.receipts),
                                        "--tests", str(self.tests), "--output", str(self.output), "--status", str(status),
                                        "--provider-pin", PIN]), 1)
        result = json.loads(status.read_text())
        self.assertEqual((result["test_result"], result["evidence_result"]), ("passed", "incomplete"))
        self.assertFalse(self.output.exists())
        self.assertNotIn(str(self.root), status.read_text())


if __name__ == "__main__":
    unittest.main()
