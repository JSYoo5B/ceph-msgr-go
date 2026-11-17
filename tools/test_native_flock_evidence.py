"""Controls for the ordered shared-FLOCK retention contract."""
import copy
import unittest

import native_evidence as evidence
import scenario_plan


def raw_receipt():
    calls, ledger, after = [], [], []
    for index, tid in enumerate((13, 16, 42, 45)):
        op = "acquire" if index < 2 else "unlock"
        name = "flock-shared-%s-owner-%d" % (op, index % 2)
        calls.append({"Name": name, "Owner": evidence.FLOCK_OWNERS[index % 2], "Rule": 2, "Type": 1 if index < 2 else 4,
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
            self.assertEqual(ctx["selector"], evidence.FLOCK_SELECTOR)
        projection = evidence.flock_projection(raw_receipt())
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
                    evidence.flock_projection(raw)

    def test_download_verifier_rechecks_order_and_sizes(self):
        for field in ("operations", "accepted_tids", "post_restart_probes"):
            with self.subTest(field=field):
                value = evidence.flock_projection(raw_receipt())
                value[field][2], value[field][3] = value[field][3], value[field][2]
                with self.assertRaises(evidence.EvidenceError):
                    evidence.validate_flock(value)
                value[field].append(value[field][0])
                with self.assertRaises(evidence.EvidenceError):
                    evidence.validate_flock(value)
