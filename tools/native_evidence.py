#!/usr/bin/env python3
"""Collect/verify bounded successful-native receipts; never archive fixture IPC.

Version 1 admits two closed families: the explicit cap-send rows and the single
four-minute OSD soak pair. Each projects numeric identity/time/outcome fields,
cleanup results and hashes, not raw logs, endpoints, commands, configuration,
credentials, full capability objects or raw workload bytes. Hashes bind the
projected files and name the omitted raw receipts; they do not let a reader
recompute omitted native bytes or xattrs, nor establish the truth of a native
assertion. The soak family validates its raw receipts with the validator copied
from the trusted controller, never with the tested checkout's copy.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys

SCHEMA_VERSION = 1
SOURCE = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
TEST = "TestCephMDSCephFSCapSendRecoveryIntegration"
SELECTOR = "^" + TEST + "$"
INPUT_LIMIT = 16 << 20
FILE_LIMIT = 256 << 10
TOTAL_LIMIT = 1 << 20
FILES = {"tests.json", "cap-send.json", "cleanup.json", "runtime.json"}
DEFAULTS = {
    "release": "20.2.4", "key": "aes256k", "service": "aes256k", "family": "4", "mode": "secure",
    "stress": "", "runtime": "container", "managers": "2", "race": "0", "expiry": "0", "idle": "0",
    "auth_epoch": "0", "short_tickets": "0", "mode_rejection": "", "mapped": "0", "osd": "0",
    "osd_profile": "legacy-v6", "osd_store": "memstore", "osd_replicas": "1", "osd_soak": "",
    "native_mgr_preparation": "0", "mon_ping": "0", "mgr_service": "0", "osd_cache_pools": "0",
    "mds_fresh": "0", "mds_commands": "0", "mds_sessions": "0", "mds_cephfs": "0",
    "mds_cephfs_ranks": "0", "mds_cephfs_ranks_replay": "0", "mds_cephfs_completion": "0",
    "mds_cephfs_data": "0", "rbd": "0", "test_run": "",
}
SOAK_SCENARIO = "osd.aes256k.ip4.osd-store-bluestore.osd-replicas-3.osd-soak-4m"
SOAK_SELECTOR = "^TestCephOSDSoak(Failover)?Integration$"
SOAK_DURATION_NS = 240_000_000_000
SOAK_LABELS = ("steady", "failover")
SOAK_TESTS = {"steady": "TestCephOSDSoakIntegration", "failover": "TestCephOSDSoakFailoverIntegration"}
SOAK_FILES = {"tests.json", "soak.json", "cleanup.json", "receipts.json", "runtime.json"}
SOAK_RECEIPT_LIMIT = 512 << 10
SOAK_COUNTERS = {"steady": {"appends", "reads", "unknowns", "resubmissions", "xattrs", "truncations", "churns", "unsent"},
                 "failover": {"appends", "reads", "unknowns", "resubmissions", "xattrs", "truncations", "faults", "replacements", "resent"}}
SOAK_PROFILE = dict(DEFAULTS, key="aes256k", service="aes256k", family="4", osd="1", osd_store="bluestore",
                    osd_replicas="3", osd_soak="4m")
COMMON = dict(DEFAULTS, osd="1", mds_fresh="1", mds_cephfs_data="1", test_run=SELECTOR)
PROFILES = [dict(COMMON, key="aes", service="aes", mode="crc", family="4", runtime="host", osd_store="bluestore"),
            dict(COMMON, family="6")]
CONTEXT_KEYS = {"execution_source", "tested_sha", "controller_sha", "run_id", "attempt", "scenario", "selector", "profile", "configured_image"}
CLEAN_FLAGS = ("attempted", "completed", "config_restored", "osd_config_restored", "namespace_checked",
               "namespace_restored", "layout_restored", "blocklist_restored", "client_identity_removed", "native_identity_removed")
FRESH_FLAGS = ("attempted", "completed", "filesystem_removed", "pools_removed", "identity_removed", "cephfs_data_verified")
PRIVATE = re.compile(r"(?:[\\/]Users[\\/]|[\\/]home[\\/]|\b(?:\d{1,3}\.){3}\d{1,3}\b|\b(?:ssh|keyring)\b)", re.I)


KIND_CAPSEND = "native-success"
KIND_SOAK = "native-soak-success"


class EvidenceError(ValueError):
    """Only fixed public codes escape the collector; never echo private input."""


def need(condition, code):
    if not condition:
        raise EvidenceError(code)


def keys(value, wanted):
    need(isinstance(value, dict) and set(value) == set(wanted), "schema-fields")


def integer(value, minimum=0):
    need(type(value) is int and minimum <= value <= (1 << 64) - 1, "numeric-field")
    return value


def digest(value):
    need(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "digest-field")
    return value


def sha(value):
    need(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value), "sha-field")
    return value


def privacy(value):
    if isinstance(value, dict):
        for key, item in value.items():
            privacy(key)
            privacy(item)
    elif isinstance(value, list):
        for item in value:
            privacy(item)
    elif isinstance(value, str):
        need(len(value) <= 512 and not PRIVATE.search(value), "private-or-unbounded-field")


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        need(key not in value, "duplicate-json-field")
        value[key] = item
    return value


def decode(data):
    try:
        return json.loads(data, object_pairs_hook=unique_object)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise EvidenceError("invalid-json") from error


def read(path, limit):
    path = Path(path)
    need(not path.is_symlink() and path.parent.resolve() == path.resolve().parent, "unsafe-input-path")
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as stream:
            need(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "nonregular-input")
            data = stream.read(limit + 1)
        need(len(data) <= limit, "file-limit")
        return data
    except OSError as error:
        raise EvidenceError("missing-or-unreadable-input") from error


def document(path, limit=INPUT_LIMIT):
    return decode(read(path, limit))


def encoded(value):
    privacy(value)
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def is_soak(profile):
    return profile == SOAK_PROFILE


def scenario_id(profile):
    if is_soak(profile):
        return SOAK_SCENARIO
    mode = ".crc" if profile["mode"] == "crc" else ""
    return ("mds-cephfs-capsend.%s.ip%s%s.%s.osd-store-%s.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfscapsendrecovery"
            % (profile["key"], profile["family"], mode, profile["runtime"], profile["osd_store"]))


def context(request):
    keys(request, {"execution_source", "tested_sha", "controller_sha", "run_id", "attempt", "matrix"})
    need(request["execution_source"] in {"github-actions", "linux-runner"}, "execution-source")
    matrix = request["matrix"]
    need(isinstance(matrix, dict) and set(matrix) <= set(DEFAULTS) | {"scenario", "image"}, "unsupported-matrix-field")
    need(all(isinstance(value, str) for value in matrix.values()), "matrix-value")
    profile = dict(DEFAULTS, **{key: value for key, value in matrix.items() if key in DEFAULTS})
    need((profile in PROFILES or is_soak(profile)) and matrix.get("scenario") == scenario_id(profile), "unsupported-profile")
    image = matrix.get("image", IMAGE)
    need(image == IMAGE, "unsupported-image")
    value = {"execution_source": request["execution_source"], "tested_sha": sha(request["tested_sha"]), "controller_sha": sha(request["controller_sha"]),
             "run_id": integer(request["run_id"], 1), "attempt": integer(request["attempt"], 1),
             "scenario": matrix["scenario"], "selector": SOAK_SELECTOR if is_soak(profile) else SELECTOR, "profile": profile, "configured_image": image}
    privacy(value)
    return value


def validate_context(value):
    keys(value, CONTEXT_KEYS)
    matrix = dict(value["profile"], scenario=value["scenario"], image=value["configured_image"])
    expected = context({key: value[key] for key in ("execution_source", "tested_sha", "controller_sha", "run_id", "attempt")} | {"matrix": matrix})
    need(value == expected, "context-mismatch")


def tests_projection(value):
    keys(value, {"selector", "expected", "observed", "problems", "passed"})
    need(value["passed"] is True and value == {"selector": SELECTOR, "expected": [["api", TEST]],
                   "observed": [{"suite": "api", "test": TEST, "result": "PASS"}],
                   "problems": [], "passed": True}, "required-test-not-passed")
    return value


def runtime_projection(raw, ctx):
    mapping = {"key": "key_type", "service": "service_cipher", "mode": "connection_mode", "family": "ip_family",
               "runtime": "runtime", "osd": "osd", "osd_profile": "osd_profile", "osd_store": "osd_store",
               "osd_replicas": "osd_replicas"}
    reported = {key: raw[field] for key, field in mapping.items()}
    need(reported == {key: ctx["profile"][key] for key in mapping}, "fixture-profile-mismatch")
    if is_soak(ctx["profile"]):
        need(raw.get("mds_fresh") is False and raw.get("mds_cephfs_data") is False and raw.get("rbd_base") is False
             and raw.get("image") == ctx["configured_image"], "fixture-profile-mismatch")
    else:
        need(raw["mds_fresh"] is True and raw["mds_cephfs_data"] is True and raw["image"] == ctx["configured_image"], "fixture-profile-mismatch")
    need(raw["provider"] == "github.com/JSYoo5B/ceph-testcontainers-go" and raw["provider_version"] == "v0.3.0", "provider-mismatch")
    value = {"image_id": raw["image_id"], "architecture": raw["architecture"], "daemon_version": raw["daemon_version"],
             "fixture_reported_profile": reported, "provider_version": raw["provider_version"]}
    validate_runtime(value, ctx)
    return value


def validate_runtime(value, ctx):
    keys(value, {"image_id", "architecture", "daemon_version", "fixture_reported_profile", "provider_version"})
    need(isinstance(value["image_id"], str) and value["image_id"].startswith("sha256:"), "observed-image-id")
    digest(value["image_id"][7:])
    need(value["architecture"] in {"amd64", "arm64"}, "observed-architecture")
    need(value["daemon_version"] == "ceph version 20.2.4 (%s) tentacle (stable)" % SOURCE, "observed-version")
    need(value["provider_version"] == "v0.3.0", "provider-mismatch")
    names = {"key", "service", "mode", "family", "runtime", "osd", "osd_profile", "osd_store", "osd_replicas"}
    keys(value["fixture_reported_profile"], names)
    need(value["fixture_reported_profile"] == {key: ctx["profile"][key] for key in names}, "fixture-profile-mismatch")


def cap_send_projection(raw):
    need(raw["source_commit"] == SOURCE and raw["facts"]["restart_recipe"] == "cap-send", "receipt-source-or-recipe")
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
    value = {"source_commit": SOURCE, "global_id": raw["global_id"], "cephfs_nonce": raw["cephfs_nonce"],
             "old_mds_gid": adopted["armed"]["gid"], "new_mds_gid": adopted["adopted"]["gid"],
             "flushes": flushes, "acks": acks, "native_branches": facts["new_branches"],
             "object_requests_before": facts["object_requests_before_restart"], "object_requests_after": facts["object_requests_after_recovery"],
             "native_stat": {key: facts["native_stat"][key] for key in ("exists", "ino", "size", "mtime", "version")}}
    validate_caps(value)
    return value


def validate_caps(value):
    keys(value, {"source_commit", "global_id", "cephfs_nonce", "old_mds_gid", "new_mds_gid", "flushes", "acks",
                 "native_branches", "object_requests_before", "object_requests_after", "native_stat"})
    need(value["source_commit"] == SOURCE, "receipt-source")
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


def soak_tests_projection(value):
    """The broad gate lists no expected tests for this row, so require both parents here."""
    keys(value, {"selector", "expected", "observed", "problems", "passed"})
    need(value["passed"] is True and value["selector"] == "" and value["expected"] == [] and value["problems"] == [], "required-test-not-passed")
    observed = value["observed"]
    need(isinstance(observed, list) and len(observed) == 2, "required-test-not-passed")
    for item in observed:
        keys(item, {"suite", "test", "result"})
    need(sorted((i["suite"], i["test"], i["result"]) for i in observed) ==
         sorted(("api", test, "PASS") for test in SOAK_TESTS.values()), "required-test-not-passed")
    return {"selector": "", "fixture_selector": SOAK_SELECTOR,
            "required": [{"suite": "api", "test": SOAK_TESTS[label], "result": "PASS"} for label in SOAK_LABELS], "passed": True}


def validate_soak_tests(value):
    keys(value, {"selector", "fixture_selector", "required", "passed"})
    need(value == {"selector": "", "fixture_selector": SOAK_SELECTOR, "passed": True,
                   "required": [{"suite": "api", "test": SOAK_TESTS[label], "result": "PASS"} for label in SOAK_LABELS]},
         "required-test-not-passed")


def receipt_names(faults):
    names = {"osd-soak-v1-fixture-termination.json"}
    for label in SOAK_LABELS:
        prefix = "osd-soak-v1-%s-" % label
        names |= {prefix + stage + ".json" for stage in ("started", "setup", "workload", "cleanup")}
        names |= {prefix + "worker-%d.json" % worker for worker in range(4)}
    prefix = "osd-soak-v1-failover-"
    names.add(prefix + "recovery-clean.json")
    names |= {prefix + "fault-%03d-%s.json" % (cycle, phase) for cycle in range(1, faults + 1) for phase in ("kill", "restart", "clean")}
    return names


def trusted_soak_validator():
    try:
        import osd_soak_receipts as module
    except ImportError as error:
        raise EvidenceError("trusted-soak-validator-missing") from error
    # The workflow copies this module beside the collector before the tested
    # checkout exists; any other location would not be the trusted controller.
    need(Path(module.__file__).resolve().parent == Path(__file__).resolve().parent, "trusted-soak-validator-location")
    return module


def soak_projection(directory, ctx):
    directory = Path(directory)
    need(not directory.is_symlink() and directory.is_dir(), "unsafe-receipt-directory")
    validator = trusted_soak_validator()
    try:
        result = validator.verify(directory, ctx["tested_sha"], ctx["configured_image"], SOAK_DURATION_NS, SOAK_LABELS)
    except (ValueError, TypeError, KeyError, OSError, RecursionError):
        raise EvidenceError("soak-receipts-invalid") from None
    hashes = result["receipt_sha256"]
    raw, files = {}, {}
    for name, expected in sorted(hashes.items()):
        data = read(directory / name, SOAK_RECEIPT_LIMIT)
        need(hashlib.sha256(data).hexdigest() == expected, "receipt-hash-mismatch")
        files[name] = {"size": len(data), "sha256": expected}
        raw[name] = decode(data)
    workloads, cleanups = {}, {}
    for label in SOAK_LABELS:
        prefix = "osd-soak-v1-%s-" % label
        setup, run, clean = raw[prefix + "setup.json"], raw[prefix + "workload.json"], raw[prefix + "cleanup.json"]
        owned = setup["owned_objects"]
        codes = {entry["object"]: entry["code"] for entry in clean["objects"]}
        workers = []
        for worker in range(4):
            item = raw[prefix + "worker-%d.json" % worker]
            native = item["native"]
            xattr = item["expected_xattr"].encode()
            workers.append({"worker": worker, "native_observation_elapsed_ns": item["native_observation_elapsed_ns"],
                            "version": native["version"], "mtime_seconds": native["seconds"], "mtime_nanos": native["nanos"],
                            "expected_length": item["expected_length"], "native_length": item["native_length"],
                            "data_sha256": item["native_sha256"], "xattr_length": len(xattr), "xattr_sha256": hashlib.sha256(xattr).hexdigest()})
        value = {"setup_elapsed_ns": setup["elapsed_ns"], "owned_object_count": len(owned),
                 "workload": {key: run[key] for key in ("actual_workload_ns", "after_workload_start_ns", "test_elapsed_ns")}
                 | {"counters": {key: run["counters"][key] for key in sorted(SOAK_COUNTERS[label])}},
                 "workers": workers}
        if label == "failover":
            faults = []
            for cycle in range(1, run["counters"]["faults"] + 1):
                for phase in ("kill", "restart", "clean"):
                    item = raw[prefix + "fault-%03d-%s.json" % (cycle, phase)]
                    faults.append({"cycle": cycle, "phase": phase, "after_workload_start_ns": item["after_workload_start_ns"],
                                   "elapsed_ns": item["elapsed_ns"], "victim": int(item["victim"])})
            value["faults"] = faults
            value["recovery_clean"] = True
        workloads[label] = value
        cleanups[label] = {"removed": True, "elapsed_ns": clean["elapsed_ns"], "codes": [codes[name] for name in owned]}
    end = raw["osd-soak-v1-fixture-termination.json"]
    soak = {"tested_revision": ctx["tested_sha"], "configured_duration_ns": SOAK_DURATION_NS, "observations": result["observations"], "workloads": workloads}
    cleanup = {"fixture_termination": {"succeeded": True, "suite_exit_success": True, "elapsed_ns": end["termination_elapsed_ns"]}, "workloads": cleanups}
    receipts = {"fixture_manifest_sha256": hashes["fixture-manifest.json"], "files": files}
    validate_soak(soak, ctx)
    validate_soak_cleanup(cleanup)
    validate_receipts(receipts, soak)
    return soak, cleanup, receipts


def validate_soak(value, ctx):
    keys(value, {"tested_revision", "configured_duration_ns", "observations", "workloads"})
    need(value["tested_revision"] == ctx["tested_sha"] and value["configured_duration_ns"] == SOAK_DURATION_NS and value["observations"] == 8, "soak-context")
    keys(value["workloads"], set(SOAK_LABELS))
    for label in SOAK_LABELS:
        item = value["workloads"][label]
        keys(item, {"setup_elapsed_ns", "owned_object_count", "workload", "workers"} | ({"faults", "recovery_clean"} if label == "failover" else set()))
        integer(item["setup_elapsed_ns"], 1)
        need(item["owned_object_count"] == 5, "owned-object-count")
        run = item["workload"]
        keys(run, {"actual_workload_ns", "after_workload_start_ns", "test_elapsed_ns", "counters"})
        for key in ("actual_workload_ns", "after_workload_start_ns", "test_elapsed_ns"):
            integer(run[key], 1)
        need(SOAK_DURATION_NS <= run["actual_workload_ns"] <= run["after_workload_start_ns"] <= run["test_elapsed_ns"], "workload-interval")
        keys(run["counters"], SOAK_COUNTERS[label])
        for counter in run["counters"].values():
            integer(counter)
        need(run["counters"]["appends"] >= 100 and run["counters"]["reads"] >= 10 and (label != "steady" or run["counters"]["churns"] >= 10), "workload-counters")
        need(isinstance(item["workers"], list) and len(item["workers"]) == 4, "worker-count")
        for index, worker in enumerate(item["workers"]):
            keys(worker, {"worker", "native_observation_elapsed_ns", "version", "mtime_seconds", "mtime_nanos", "expected_length", "native_length",
                          "data_sha256", "xattr_length", "xattr_sha256"})
            need(worker["worker"] == index, "worker-identity")
            integer(worker["native_observation_elapsed_ns"], 1)
            integer(worker["version"], 1)
            need(type(worker["mtime_seconds"]) is int and 0 <= worker["mtime_seconds"] <= 0xFFFFFFFF, "native-mtime")
            need(type(worker["mtime_nanos"]) is int and 0 <= worker["mtime_nanos"] < 1_000_000_000, "native-mtime")
            integer(worker["xattr_length"])
            integer(worker["expected_length"])
            need(worker["expected_length"] == worker["native_length"] and worker["native_length"] <= 128 << 10, "worker-length")
            digest(worker["data_sha256"])
            digest(worker["xattr_sha256"])
        if label == "failover":
            faults = run["counters"]["faults"]
            need(1 <= faults <= 64 and item["recovery_clean"] is True and isinstance(item["faults"], list) and len(item["faults"]) == 3 * faults, "fault-cycles")
            previous, victims = 0, {}
            for index, fault in enumerate(item["faults"]):
                keys(fault, {"cycle", "phase", "after_workload_start_ns", "elapsed_ns", "victim"})
                cycle, phase = divmod(index, 3)
                need(fault["cycle"] == cycle + 1 and fault["phase"] == ("kill", "restart", "clean")[phase], "fault-order")
                when = fault["after_workload_start_ns"]
                need(type(when) is int and 0 <= when <= run["after_workload_start_ns"], "fault-outside-interval")
                need(phase != 0 or when <= run["actual_workload_ns"], "fault-outside-workload")
                need(type(fault["elapsed_ns"]) is int and 0 < fault["elapsed_ns"] <= when and when >= previous, "fault-interval")
                previous = when
                need(type(fault["victim"]) is int and fault["victim"] in (0, 1, 2), "fault-victim")
                need(victims.setdefault(fault["cycle"], fault["victim"]) == fault["victim"], "fault-victim")


def validate_soak_cleanup(value):
    keys(value, {"fixture_termination", "workloads"})
    keys(value["fixture_termination"], {"succeeded", "suite_exit_success", "elapsed_ns"})
    need(value["fixture_termination"]["succeeded"] is True and value["fixture_termination"]["suite_exit_success"] is True, "cleanup-incomplete")
    integer(value["fixture_termination"]["elapsed_ns"], 1)
    keys(value["workloads"], set(SOAK_LABELS))
    for item in value["workloads"].values():
        keys(item, {"removed", "elapsed_ns", "codes"})
        integer(item["elapsed_ns"], 1)
        need(item["removed"] is True and isinstance(item["codes"], list) and len(item["codes"]) == 5 and
             all(type(code) is int and code in (0, -2) for code in item["codes"]), "cleanup-incomplete")


def validate_receipts(value, soak):
    keys(value, {"fixture_manifest_sha256", "files"})
    digest(value["fixture_manifest_sha256"])
    files = value["files"]
    need(isinstance(files, dict), "receipt-inventory")
    faults = soak["workloads"]["failover"]["workload"]["counters"]["faults"]
    need(1 <= faults <= 64 and set(files) == receipt_names(faults) | {"fixture-manifest.json"}, "receipt-inventory")
    for record in files.values():
        keys(record, {"size", "sha256"})
        need(type(record["size"]) is int and 0 < record["size"] <= SOAK_RECEIPT_LIMIT, "receipt-size")
        digest(record["sha256"])
    need(files["fixture-manifest.json"]["sha256"] == value["fixture_manifest_sha256"], "receipt-inventory")


def cleanup_projection(data, fresh):
    need(data["source_commit"] == fresh["source_commit"] == SOURCE, "cleanup-source")
    need(not data.get("errors") and not fresh.get("error") and not data["residual_owned_sessions"] and not data["kept_new_blocklist"], "cleanup-incomplete")
    value = {"source_commit": SOURCE, "data": {key: data[key] for key in CLEAN_FLAGS},
             "filesystem": {key: fresh[key] for key in FRESH_FLAGS}}
    validate_cleanup(value)
    return value


def validate_cleanup(value):
    keys(value, {"source_commit", "data", "filesystem"})
    need(value["source_commit"] == SOURCE, "cleanup-source")
    for group, flags in (("data", CLEAN_FLAGS), ("filesystem", FRESH_FLAGS)):
        keys(value[group], flags)
        need(all(value[group][key] is True for key in flags), "cleanup-incomplete")


def verify(bundle, expected):
    bundle = Path(bundle)
    need(not bundle.is_symlink() and bundle.is_dir(), "unsafe-bundle")
    soak = isinstance(expected, dict) and isinstance(expected.get("profile"), dict) and is_soak(expected["profile"])
    files, kind = (SOAK_FILES, KIND_SOAK) if soak else (FILES, KIND_CAPSEND)
    need({item.name for item in bundle.iterdir()} == files | {"manifest.json"}, "bundle-inventory")
    manifest = document(bundle / "manifest.json", FILE_LIMIT)
    keys(manifest, CONTEXT_KEYS | {"schema_version", "kind", "files", "total_bytes", "observed"})
    need(type(manifest["schema_version"]) is int and manifest["schema_version"] == SCHEMA_VERSION and manifest["kind"] == kind, "schema-version")
    ctx = {key: manifest[key] for key in CONTEXT_KEYS}
    validate_context(ctx)
    need(ctx == expected, "manifest-context-mismatch")
    need(isinstance(manifest["files"], list) and len(manifest["files"]) == len(files), "bundle-inventory")
    total, payloads = 0, {}
    for record in manifest["files"]:
        keys(record, {"name", "size", "sha256"})
        name = record["name"]
        need(name in files and name not in payloads, "bundle-inventory")
        data = read(bundle / name, FILE_LIMIT)
        need(integer(record["size"]) == len(data) and digest(record["sha256"]) == hashlib.sha256(data).hexdigest(), "file-hash-or-size")
        total += len(data)
        payloads[name] = decode(data)
    need(manifest["total_bytes"] == total and total + len(read(bundle / "manifest.json", FILE_LIMIT)) <= TOTAL_LIMIT, "total-limit")
    validate_runtime(payloads["runtime.json"], ctx)
    need(manifest["observed"] == payloads["runtime.json"], "observed-mismatch")
    if soak:
        validate_soak_tests(payloads["tests.json"])
        validate_soak(payloads["soak.json"], ctx)
        validate_soak_cleanup(payloads["cleanup.json"])
        validate_receipts(payloads["receipts.json"], payloads["soak.json"])
    else:
        tests_projection(payloads["tests.json"])
        validate_caps(payloads["cap-send.json"])
        validate_cleanup(payloads["cleanup.json"])
    privacy(manifest)
    for value in payloads.values():
        privacy(value)
    return manifest


def collect(request, receipts, tests, output):
    ctx = context(request)
    receipts, output = Path(receipts), Path(output)
    need(not output.exists() and not output.is_symlink(), "attempt-collision")
    need(not receipts.is_symlink() and receipts.is_dir(), "unsafe-receipt-directory")
    if is_soak(ctx["profile"]):
        soak, cleanup, inventory = soak_projection(receipts, ctx)
        payloads = {"tests.json": soak_tests_projection(document(tests, FILE_LIMIT)), "soak.json": soak, "cleanup.json": cleanup,
                    "receipts.json": inventory, "runtime.json": runtime_projection(document(receipts / "fixture-manifest.json"), ctx)}
        kind = KIND_SOAK
    else:
        payloads = {"tests.json": tests_projection(document(tests, FILE_LIMIT)),
                    "cap-send.json": cap_send_projection(document(receipts / "mds-cephfs-data-results.json")),
                    "cleanup.json": cleanup_projection(document(receipts / "mds-cephfs-data-cleanup.json"), document(receipts / "mds-fresh-cleanup.json")),
                    "runtime.json": runtime_projection(document(receipts / "fixture-manifest.json"), ctx)}
        kind = KIND_CAPSEND
    files, total, content = [], 0, {}
    for name in sorted(payloads):
        data = encoded(payloads[name])
        need(len(data) <= FILE_LIMIT, "file-limit")
        total += len(data)
        content[name] = data
        files.append({"name": name, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    manifest = dict(ctx, schema_version=SCHEMA_VERSION, kind=kind, files=files, total_bytes=total, observed=payloads["runtime.json"])
    data = encoded(manifest)
    need(len(data) <= FILE_LIMIT and total + len(data) <= TOTAL_LIMIT, "total-limit")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        for name, payload in content.items():
            (output / name).write_bytes(payload)
        (output / "manifest.json").write_bytes(data)
        return verify(output, ctx)
    except BaseException:
        shutil.rmtree(output)
        raise


def environment_request():
    try:
        return {"execution_source": os.environ["EXECUTION_SOURCE"], "tested_sha": os.environ["TESTED_SHA"], "controller_sha": os.environ["CONTROLLER_SHA"],
                "run_id": int(os.environ["RUN_ID"]), "attempt": int(os.environ["RUN_ATTEMPT"]),
                "matrix": decode(os.environ["MATRIX_JSON"])}
    except (KeyError, ValueError) as error:
        raise EvidenceError("invalid-run-environment") from error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("collect")
    capture.add_argument("--receipts", required=True)
    capture.add_argument("--tests", required=True)
    capture.add_argument("--output", required=True)
    capture.add_argument("--status", required=True)
    check = sub.add_parser("verify")
    check.add_argument("--bundle", required=True)
    for command in (capture, check):
        command.add_argument("--request", help="run request JSON; omitted reads trusted workflow environment")
    args = parser.parse_args(argv)
    status = {"test_result": "unknown", "evidence_result": "incomplete"}
    result = 1
    try:
        request, failure = None, None
        try:
            request = document(args.request, FILE_LIMIT) if args.request else environment_request()
        except EvidenceError as error:
            failure = error
        if args.command == "collect":
            # An unreadable request leaves the cap-send gate in force; the
            # request error is reported after the test result is classified.
            matrix = request.get("matrix") if isinstance(request, dict) else None
            requested_soak = isinstance(matrix, dict) and bool(matrix.get("osd_soak"))
            (soak_tests_projection if requested_soak else tests_projection)(document(args.tests, FILE_LIMIT))
            status["test_result"] = "passed"
        if failure:
            raise failure
        if args.command == "collect":
            collect(request, args.receipts, args.tests, args.output)
        else:
            verify(args.bundle, context(request))
        status["test_result"] = "passed"
        status["evidence_result"] = "complete"
        result = 0
    except EvidenceError as error:
        status["reason"] = str(error)
    except (KeyError, TypeError, ValueError, OSError, RecursionError):
        status["reason"] = "invalid-or-missing-evidence"
    if args.command == "collect":
        Path(args.status).write_bytes(encoded(status))
    print(json.dumps(status, sort_keys=True))
    return result


if __name__ == "__main__":
    sys.exit(main())
