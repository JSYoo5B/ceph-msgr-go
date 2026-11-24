"""Controls for the ordered shared-FLOCK retention contract."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import native_evidence as evidence
import native_evidence_flock as flock
import scenario_plan
from test_native_evidence import PIN, recorded_gate


def raw_receipt():
    calls, ledger, after = [], [], []
    for index, tid in enumerate((13, 16, 42, 45)):
        op = "acquire" if index < 2 else "unlock"
        name = "flock-shared-%s-owner-%d" % (op, index % 2)
        calls.append({"Name": name, "Owner": flock.OWNERS[index % 2], "Rule": 2, "Type": 1 if index < 2 else 4,
                      "TID": tid, "Outcome": "reply result 0"})
        ledger.append({"Name": name, "Op": 0x01109, "Transaction": tid, "FirstArrival": index + 1, "FirstResult": 0,
                       "Session": "restart" if index < 2 else "reconnected"})
        success = index in (0, 3)
        probe = {"Action": "flock-sh-nb" if index == 0 else "flock-ex-nb", "RequestID": 101 + index,
                 "Probe": {"errno": 0 if success else 11, "acquired": success, "unlocked": success}}
        if index >= 2:
            probe["AfterUnlockTID"] = tid
        after.append(probe)
    return {"source_commit": evidence.SOURCE, "requests": ledger,
            "facts": {"held_flock": {"Recipe": "shared-two-owners", "SetFileLockSubmissions": 4, "Calls": calls,
                                     "ProbeAfter": after, "NativeAddress": "omitted"}}}


class SharedFlockEvidenceTests(unittest.TestCase):
    def test_catalog_profiles_and_projection(self):
        rows = [row for row in scenario_plan.load_catalog() if row["group"] == "mds-cephfs-sharedflock-recovery"]
        self.assertEqual(len(rows), 2)
        for row in rows:
            ctx = evidence.context({"execution_source": "linux-runner", "tested_sha": "a" * 40, "controller_sha": "b" * 40,
                                    "run_id": 5, "attempt": 2, "matrix": dict(row["params"], scenario=row["id"])})
            self.assertEqual(ctx["selector"], flock.SELECTOR)
        projection = flock.flock_projection(raw_receipt())
        self.assertEqual(projection["accepted_tids"], [13, 16, 42, 45])
        self.assertEqual([p["after_unlock_tid"] for p in projection["post_restart_probes"]], [None, None, 42, 45])
        self.assertNotIn("omitted", str(projection))

    def test_extra_reordered_unsettled_or_unobserved_calls_fail(self):
        for change in ("extra", "reordered ledger", "reordered calls", "wait error", "wrong result", "missing reply",
                       "wrong unlock tid", "missing unlock tid", "baseline substitution", "duplicate probe"):
            with self.subTest(change=change):
                raw = raw_receipt()
                locks, ledger = raw["facts"]["held_flock"], raw["requests"]
                calls, probes = locks["Calls"], locks["ProbeAfter"]
                if change == "extra":
                    ledger.append(dict(ledger[0], Transaction=111))
                elif change == "reordered ledger":
                    ledger[2], ledger[3] = ledger[3], ledger[2]
                elif change == "reordered calls":
                    calls[0], calls[1] = calls[1], calls[0]
                elif change == "wait error":
                    calls[0]["Outcome"] = "wait error: unavailable"
                elif change == "wrong result":
                    ledger[1]["FirstResult"] = -11
                elif change == "missing reply":
                    ledger[3]["FirstArrival"] = 0
                elif change == "wrong unlock tid":
                    probes[2]["AfterUnlockTID"] = 45
                elif change == "missing unlock tid":
                    del probes[2]["AfterUnlockTID"]
                elif change == "baseline substitution":
                    probes[2] = copy.deepcopy(probes[1])
                elif change == "duplicate probe":
                    probes[3]["RequestID"] = probes[2]["RequestID"]
                with self.assertRaises(evidence.EvidenceError):
                    flock.flock_projection(raw)

    def test_download_verifier_rechecks_order_and_sizes(self):
        for field in ("operations", "accepted_tids", "post_restart_probes"):
            with self.subTest(field=field):
                value = flock.flock_projection(raw_receipt())
                value[field][2], value[field][3] = value[field][3], value[field][2]
                with self.assertRaises(evidence.EvidenceError):
                    flock.validate_flock(value)
                value[field].append(value[field][0])
                with self.assertRaises(evidence.EvidenceError):
                    flock.validate_flock(value)


class SharedFlockCollectionTests(unittest.TestCase):
    """Collection of both catalog rows from the current result recorder's output."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.rows = [row for row in scenario_plan.load_catalog() if row["group"] == "mds-cephfs-sharedflock-recovery"]
        self.assertEqual(len(self.rows), 2)

    @staticmethod
    def write(path, value):
        Path(path).write_text(json.dumps(value), encoding="utf-8")

    def stage(self, row, tests):
        """(request, receipts, tests, output, status) of one row's attempt."""
        base = self.root / row["id"]
        receipts = base / "receipts"
        receipts.mkdir(parents=True)
        profile = evidence.context(self.request(row))["profile"]
        runtime = {"key_type": profile["key"], "service_cipher": profile["service"], "connection_mode": profile["mode"],
                   "ip_family": profile["family"], "runtime": profile["runtime"], "osd": "1", "osd_profile": "legacy-v6",
                   "osd_store": profile["osd_store"], "osd_replicas": "1", "mds_fresh": True, "mds_cephfs": True,
                   "mds_cephfs_data": False, "image": evidence.IMAGE, "image_id": "sha256:" + "c" * 64,
                   "architecture": "amd64", "daemon_version": "ceph version 20.2.4 (%s) tentacle (stable)" % evidence.SOURCE,
                   "provider": evidence.PROVIDER, "provider_version": PIN}
        self.write(receipts / "fixture-manifest.json", runtime)
        self.write(receipts / "mds-cephfs-sharedflock-recovery-results.json", raw_receipt())
        self.write(receipts / "mds-cephfs-cleanup.json", dict.fromkeys(flock.CLEAN_FLAGS, True) | {
            "source_commit": evidence.SOURCE, "residual_owned_sessions": [], "kept_new_blocklist": []})
        self.write(receipts / "mds-fresh-cleanup.json", dict.fromkeys(flock.FRESH_FLAGS, True) | {"source_commit": evidence.SOURCE})
        self.write(base / "tests.json", tests)
        self.write(base / "request.json", self.request(row))
        return base / "request.json", receipts, base / "tests.json", base / "bundle", base / "status.json"

    @staticmethod
    def request(row):
        return {"execution_source": "github-actions", "tested_sha": "a" * 40, "controller_sha": "b" * 40,
                "run_id": 45280741527, "attempt": 1, "matrix": dict(row["params"], scenario=row["id"])}

    def collect(self, row, tests):
        request, receipts, tests, output, status = self.stage(row, tests)
        code = evidence.main(["collect", "--request", str(request), "--receipts", str(receipts), "--tests", str(tests),
                              "--output", str(output), "--status", str(status), "--provider-pin", PIN])
        return code, json.loads(status.read_text()), output

    def test_both_rows_collect_the_current_recorder_output(self):
        for row in self.rows:
            with self.subTest(row=row["id"]):
                recorded = recorded_gate(flock.SELECTOR, [flock.TEST], [flock.TEST], [flock.TEST])
                self.assertTrue(recorded["passed"], recorded["problems"])
                self.assertEqual(recorded["selected"], [["api", flock.TEST]])
                code, status, output = self.collect(row, recorded)
                self.assertEqual((code, status), (0, {"test_result": "passed", "evidence_result": "complete"}))
                manifest = evidence.verify(output, evidence.context(self.request(row)))
                self.assertEqual((manifest["schema_version"], manifest["kind"]), (2, flock.KIND))
                self.assertEqual(json.loads((output / "tests.json").read_text()), recorded)

    def test_a_record_without_the_exact_selection_is_refused(self):
        legacy = recorded_gate(flock.SELECTOR, [flock.TEST], [flock.TEST], [flock.TEST])
        del legacy["selected"]
        cases = [("no announcement", recorded_gate(flock.SELECTOR, [flock.TEST], None, [flock.TEST]), "required-test-not-passed"),
                 ("legacy record", legacy, "schema-fields")]
        for name, value, code in cases:
            with self.subTest(name=name):
                self.assertTrue(value["passed"])
                self.root = Path(tempfile.mkdtemp(dir=self.root))
                result, status, output = self.collect(self.rows[0], value)
                self.assertEqual((result, status), (1, {"test_result": "unknown", "evidence_result": "incomplete", "reason": code}))
                self.assertFalse(output.exists())
        for selected in ([], [["api", flock.TEST], ["client", "TestCephOtherIntegration"]], [["client", flock.TEST]]):
            with self.subTest(selected=selected), self.assertRaisesRegex(evidence.EvidenceError, "required-test-not-passed"):
                flock.gate(dict(recorded_gate(flock.SELECTOR, [flock.TEST], [flock.TEST], [flock.TEST]), selected=selected))
