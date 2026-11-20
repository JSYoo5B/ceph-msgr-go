"""Successful-evidence family of the two explicit mds-cephfs-capsend rows.

The rows select TestCephMDSCephFSCapSendRecoveryIntegration on the CRC/AES
IPv4 host BlueStore and secure/aes256k IPv6 container MemStore profiles. The
bundle retains cap-send.json (client and MDS identities, flush and ACK
identities and order, native apply branches, object-request counts and
selected native stat fields) and the metadata and fresh-filesystem cleanup
flags. See .github/scenarios/NATIVE_EVIDENCE.md#cap-send-recovery.
"""
import native_evidence as evidence
from native_evidence import integer, keys, need

KIND = "native-success"
TEST = "TestCephMDSCephFSCapSendRecoveryIntegration"
SELECTOR = "^" + TEST + "$"
FILES = {"tests.json", "cap-send.json", "cleanup.json", "runtime.json"}
COMMON = dict(evidence.DEFAULTS, osd="1", mds_fresh="1", mds_cephfs_data="1", test_run=SELECTOR)
PROFILES = [dict(COMMON, key="aes", service="aes", mode="crc", family="4", runtime="host", osd_store="bluestore"),
            dict(COMMON, family="6")]
# The fixture capability the cap-send rows declare. It travels in the matrix
# but is not part of the recorded profile, so evidence recorded before rows
# declared capabilities keeps its context.
CAPABILITIES = "exclusive-mds-cephfs-data"
CLEAN_FLAGS = ("attempted", "completed", "config_restored", "osd_config_restored", "namespace_checked",
               "namespace_restored", "layout_restored", "blocklist_restored", "client_identity_removed", "native_identity_removed")
FRESH_FLAGS = evidence.FRESH_COMMON + ("cephfs_data_verified",)


def claims(matrix):
    return matrix.get("test_run") == SELECTOR


def scenario_id(profile):
    return evidence.row_scenario_id("mds-cephfs-capsend", "mdscephfscapsendrecovery", profile)


def selector(profile):
    return profile["test_run"]


def runtime_facts(raw, ctx):
    need(raw["mds_fresh"] is True and raw["mds_cephfs_data"] is True and raw["image"] == ctx["configured_image"], "fixture-profile-mismatch")


def gate(raw_tests):
    return evidence.tests_projection(raw_tests, TEST)


def project(receipts, tests, ctx):
    document = evidence.document
    return {"tests.json": gate(document(tests, evidence.FILE_LIMIT)),
            "cap-send.json": cap_send_projection(document(receipts / "mds-cephfs-data-results.json")),
            "cleanup.json": evidence.cleanup_projection(document(receipts / "mds-cephfs-data-cleanup.json"),
                                                        document(receipts / "mds-fresh-cleanup.json"), CLEAN_FLAGS, FRESH_FLAGS),
            "runtime.json": evidence.runtime_projection(document(receipts / "fixture-manifest.json"), ctx)}


def validate(payloads, ctx):
    gate(payloads["tests.json"])
    validate_caps(payloads["cap-send.json"])
    evidence.validate_cleanup(payloads["cleanup.json"], CLEAN_FLAGS, FRESH_FLAGS)


def cap_send_projection(raw):
    need(raw["source_commit"] == evidence.SOURCE and raw["facts"]["restart_recipe"] == "cap-send", "receipt-source-or-recipe")
    facts = raw["facts"]["cap_send_recovery"]
    need(isinstance(facts["flushes"], list) and len(facts["flushes"]) == 2 and
         isinstance(facts["acks"], list) and len(facts["acks"]) == 2, "flush-count")
    flushes = []
    for flush in facts["flushes"]:
        need(isinstance(flush["Attempts"], list) and len(flush["Attempts"]) == 2, "attempt-count")
        attempts = []
        for item in flush["Attempts"]:
            attempts.append({key: item[field] for key, field in (
                ("kind", "Kind"), ("generation", "Generation"), ("reconnected_from", "ReconnectedFrom"),
                ("admitted", "Admitted"), ("cap_id", "CapID"), ("seq", "Seq"), ("issue_seq", "IssueSeq"),
                ("migrate_seq", "MigrateSeq"), ("barrier", "Barrier"))})
        flushes.append({"inode": flush["Ino"], "tid": flush["Transaction"], "size": flush["Caps"]["Size"], "attempts": attempts})
    acks = [{"inode": item["Caps"]["Ino"], "tid": item["Transaction"], "arrival": item["Arrival"],
             "cap_id": item["Caps"]["CapID"]} for item in facts["acks"]]
    adopted = facts["adopted"]
    need(adopted["state"] == "adopted", "incarnation-not-adopted")
    value = {"source_commit": evidence.SOURCE, "global_id": raw["global_id"], "cephfs_nonce": raw["cephfs_nonce"],
             "old_mds_gid": adopted["armed"]["gid"], "new_mds_gid": adopted["adopted"]["gid"],
             "flushes": flushes, "acks": acks, "native_branches": facts["new_branches"],
             "object_requests_before": facts["object_requests_before_restart"], "object_requests_after": facts["object_requests_after_recovery"],
             "native_stat": {key: facts["native_stat"][key] for key in ("exists", "ino", "size", "mtime", "version")}}
    validate_caps(value)
    return value


def validate_caps(value):
    keys(value, {"source_commit", "global_id", "cephfs_nonce", "old_mds_gid", "new_mds_gid", "flushes", "acks",
                 "native_branches", "object_requests_before", "object_requests_after", "native_stat"})
    need(value["source_commit"] == evidence.SOURCE, "receipt-source")
    for key in ("global_id", "cephfs_nonce", "old_mds_gid", "new_mds_gid", "object_requests_before", "object_requests_after"):
        integer(value[key], 1)
    need(value["old_mds_gid"] != value["new_mds_gid"] and value["object_requests_before"] == value["object_requests_after"], "recovery-identity-or-io")
    need(value["native_branches"] == {"uncertain": "applied", "refused": "applied"}, "native-branch")
    need(isinstance(value["flushes"], list) and len(value["flushes"]) == 2 and isinstance(value["acks"], list) and len(value["acks"]) == 2, "flush-count")
    for index, (flush, ack) in enumerate(zip(value["flushes"], value["acks"])):
        keys(flush, {"inode", "tid", "size", "attempts"})
        for key in ("inode", "tid", "size"):
            integer(flush[key], 1)
        need(isinstance(flush["attempts"], list) and len(flush["attempts"]) == 2, "attempt-count")
        for attempt in flush["attempts"]:
            keys(attempt, {"kind", "generation", "reconnected_from", "admitted", "cap_id", "seq", "issue_seq", "migrate_seq", "barrier"})
            need(attempt["kind"] in {"first-send", "resubmit"} and type(attempt["admitted"]) is bool, "attempt-class")
            for key in ("generation", "reconnected_from", "cap_id", "seq", "issue_seq", "migrate_seq", "barrier"):
                integer(attempt[key])
        first, last = flush["attempts"]
        need(first["admitted"] is (index == 0) and last["admitted"] is True and last["kind"] == ("resubmit" if index == 0 else "first-send") and last["generation"] > first["generation"], "attempt-outcome")
        keys(ack, {"inode", "tid", "arrival", "cap_id"})
        for key in ack:
            integer(ack[key], 1)
        need(ack["inode"] == flush["inode"] and ack["tid"] == flush["tid"] and ack["cap_id"] == last["cap_id"], "ack-identity")
    first, second = value["flushes"]
    need(first["inode"] == second["inode"] and first["tid"] < second["tid"] and value["acks"][0]["arrival"] < value["acks"][1]["arrival"], "flush-order")
    st = value["native_stat"]
    keys(st, {"exists", "ino", "size", "mtime", "version"})
    need(st["exists"] is True and st["ino"] == second["inode"] and st["size"] == second["size"], "native-stat")
    for key in ("ino", "size", "mtime", "version"):
        integer(st[key], 1)
