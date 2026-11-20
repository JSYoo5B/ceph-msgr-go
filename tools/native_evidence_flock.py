"""Successful-evidence family of the two mds-cephfs-sharedflock-recovery rows.

The rows select TestCephMDSCephFSSharedFlockRecoveryIntegration on the CRC/AES
IPv4 host BlueStore and secure/aes256k IPv6 container MemStore profiles. The
bundle retains flock.json (the ordered shared acquires and unlocks checked
against the request ledger, and the post-restart native probes) and the
metadata and fresh-filesystem cleanup flags. The request ledger, native
addresses, paths, error text and reconnect blob stay outside the bundle.
"""
import native_evidence as evidence
from native_evidence import integer, keys, need

KIND = "native-shared-flock-success"
TEST = "TestCephMDSCephFSSharedFlockRecoveryIntegration"
SELECTOR = "^" + TEST + "$"
FILES = {"tests.json", "flock.json", "cleanup.json", "runtime.json"}
COMMON = dict(evidence.DEFAULTS, osd="1", mds_fresh="1", mds_cephfs="1", test_run=SELECTOR)
PROFILES = [dict(COMMON, key="aes", service="aes", mode="crc", family="4", runtime="host", osd_store="bluestore"),
            dict(COMMON, family="6")]
CAPABILITIES = "exclusive-mds-cephfs"
OWNERS = (0x80000000474F0001, 0x80000000474F0003)
CLEAN_FLAGS = ("attempted", "completed", "namespace_checked", "namespace_restored", "journal_pause_restored",
               "blocklist_restored", "client_identity_removed", "recovery_identity_removed", "native_identity_removed")
FRESH_FLAGS = evidence.FRESH_COMMON + ("cephfs_verified",)


def claims(matrix):
    return matrix.get("test_run") == SELECTOR


def scenario_id(profile):
    return evidence.row_scenario_id("mds-cephfs-sharedflock-recovery", "mdscephfssharedflockrecovery", profile)


def selector(profile):
    return profile["test_run"]


def runtime_facts(raw, ctx):
    need(raw["mds_fresh"] is True and raw["mds_cephfs"] is True and raw["mds_cephfs_data"] is False
         and raw["image"] == ctx["configured_image"], "fixture-profile-mismatch")


def gate(raw_tests):
    return evidence.tests_projection(raw_tests, TEST)


def project(receipts, tests, ctx):
    document = evidence.document
    return {"tests.json": gate(document(tests, evidence.FILE_LIMIT)),
            "flock.json": flock_projection(document(receipts / "mds-cephfs-sharedflock-recovery-results.json")),
            "cleanup.json": evidence.cleanup_projection(document(receipts / "mds-cephfs-cleanup.json"),
                                                        document(receipts / "mds-fresh-cleanup.json"), CLEAN_FLAGS, FRESH_FLAGS),
            "runtime.json": evidence.runtime_projection(document(receipts / "fixture-manifest.json"), ctx)}


def validate(payloads, ctx):
    gate(payloads["tests.json"])
    validate_flock(payloads["flock.json"])
    evidence.validate_cleanup(payloads["cleanup.json"], CLEAN_FLAGS, FRESH_FLAGS)


def flock_projection(raw):
    locks = raw["facts"]["held_flock"]
    need(raw["source_commit"] == evidence.SOURCE and locks["Recipe"] == "shared-two-owners", "receipt-source-or-recipe")
    calls = locks["Calls"]
    ledger = [item for item in raw["requests"] if item["Op"] == 0x01109 and item["Transaction"] != 0]
    need(len(calls) == len(ledger) == integer(locks["SetFileLockSubmissions"]) == 4, "flock-call-count")
    operations = []
    for index, (call, entry) in enumerate(zip(calls, ledger)):
        operation = "acquire" if index < 2 else "unlock"
        name = "flock-shared-%s-owner-%d" % (operation, index % 2)
        need(call["Name"] == entry["Name"] == name and call["Rule"] == 2 and
             call["Type"] == (1 if index < 2 else 4) and entry["Session"] == ("restart" if index < 2 else "reconnected"),
             "flock-operation")
        result = entry["FirstResult"]
        need(not entry.get("Error") and integer(entry["FirstArrival"], 1) and type(result) is int and
             call["Outcome"] == "reply result %d" % result, "flock-reply")
        operations.append({"operation": operation, "owner": call["Owner"], "tid": call["TID"], "result": result})
    after = locks["ProbeAfter"]
    need(len(after) == 4, "flock-probe-count")
    probes = [{"after_unlock_tid": item.get("AfterUnlockTID"), "request_id": item["RequestID"],
               "action": item["Action"], "errno": item["Probe"]["errno"],
               "acquired": item["Probe"]["acquired"], "unlocked": item["Probe"]["unlocked"]} for item in after]
    value = {"operations": operations, "accepted_tids": [item["Transaction"] for item in ledger], "post_restart_probes": probes}
    validate_flock(value)
    return value


def validate_flock(value):
    keys(value, {"operations", "accepted_tids", "post_restart_probes"})
    ops, tids, probes = value["operations"], value["accepted_tids"], value["post_restart_probes"]
    need(all(isinstance(seq, list) and len(seq) == 4 for seq in (ops, tids, probes)), "flock-count")
    for index, item in enumerate(ops):
        keys(item, {"operation", "owner", "tid", "result"})
        need(item["operation"] == ("acquire" if index < 2 else "unlock") and integer(item["owner"]) == OWNERS[index % 2]
             and integer(item["tid"], 1) == integer(tids[index], 1) and type(item["result"]) is int and item["result"] == 0,
             "flock-operation-ledger")
    need(len(set(tids)) == 4, "flock-repeated-tid")
    previous = 0
    for index, item in enumerate(probes):
        keys(item, {"after_unlock_tid", "request_id", "action", "errno", "acquired", "unlocked"})
        tid = item["after_unlock_tid"]
        expected_errno = (0, 11, 11, 0)[index]
        need((tid is None if index < 2 else integer(tid, 1) == tids[index])
             and integer(item["request_id"], 1) > previous
             and item["action"] == ("flock-sh-nb" if index == 0 else "flock-ex-nb")
             and type(item["errno"]) is int and item["errno"] == expected_errno
             and item["acquired"] is (expected_errno == 0) and item["unlocked"] is item["acquired"], "flock-probe-order")
        previous = item["request_id"]
