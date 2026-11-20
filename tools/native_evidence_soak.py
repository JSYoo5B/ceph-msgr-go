"""Successful-evidence family of the four-minute OSD soak row.

Every matrix with an osd_soak value claims this family, so an unadmitted soak
row fails closed in the collector instead of skipping retention. It admits
exactly one profile. The bundle retains soak.json (per workload timing,
counters, worker observations and fault phases), cleanup.json, and
receipts.json, the size and hash of every raw receipt. Raw receipts are
validated with osd_soak_receipts.py imported from this module's own
directory, which the workflow fills from the trusted controller; the tested
checkout's copy is never used. See
.github/scenarios/NATIVE_EVIDENCE.md#four-minute-osd-soak-family.
"""
import hashlib
from pathlib import Path

import native_evidence as evidence
from native_evidence import digest, integer, keys, need

KIND = "native-soak-success"
SCENARIO = "osd.aes256k.ip4.osd-store-bluestore.osd-replicas-3.osd-soak-4m"
SELECTOR = "^TestCephOSDSoak(Failover)?Integration$"
DURATION_NS = 240_000_000_000
LABELS = ("steady", "failover")
TESTS = {"steady": "TestCephOSDSoakIntegration", "failover": "TestCephOSDSoakFailoverIntegration"}
FILES = {"tests.json", "soak.json", "cleanup.json", "receipts.json", "runtime.json"}
RECEIPT_LIMIT = 512 << 10
COUNTERS = {"steady": {"appends", "reads", "unknowns", "resubmissions", "xattrs", "truncations", "churns", "unsent"},
            "failover": {"appends", "reads", "unknowns", "resubmissions", "xattrs", "truncations", "faults", "replacements", "resent"}}
PROFILE = dict(evidence.DEFAULTS, key="aes256k", service="aes256k", family="4", osd="1", osd_store="bluestore",
               osd_replicas="3", osd_soak="4m")
PROFILES = [PROFILE]
CAPABILITIES = ""


def claims(matrix):
    return bool(matrix.get("osd_soak"))


def scenario_id(profile):
    return SCENARIO


def selector(profile):
    return SELECTOR


def runtime_facts(raw, ctx):
    need(raw.get("mds_fresh") is False and raw.get("mds_cephfs_data") is False and raw.get("rbd_base") is False
         and raw.get("image") == ctx["configured_image"], "fixture-profile-mismatch")


def project(receipts, tests, ctx):
    soak, cleanup, inventory = soak_projection(receipts, ctx)
    return {"tests.json": gate(evidence.document(tests, evidence.FILE_LIMIT)), "soak.json": soak, "cleanup.json": cleanup,
            "receipts.json": inventory, "runtime.json": evidence.runtime_projection(evidence.document(receipts / "fixture-manifest.json"), ctx)}


def validate(payloads, ctx):
    validate_tests(payloads["tests.json"])
    validate_soak(payloads["soak.json"], ctx)
    validate_cleanup(payloads["cleanup.json"])
    validate_receipts(payloads["receipts.json"], payloads["soak.json"])


def gate(value):
    """The broad gate lists no expected tests for this row, so require both parents here."""
    keys(value, {"selector", "expected", "observed", "problems", "passed"})
    need(value["passed"] is True and value["selector"] == "" and value["expected"] == [] and value["problems"] == [], "required-test-not-passed")
    observed = value["observed"]
    need(isinstance(observed, list) and len(observed) == 2, "required-test-not-passed")
    for item in observed:
        keys(item, {"suite", "test", "result"})
    need(sorted((i["suite"], i["test"], i["result"]) for i in observed) ==
         sorted(("api", test, "PASS") for test in TESTS.values()), "required-test-not-passed")
    return {"selector": "", "fixture_selector": SELECTOR,
            "required": [{"suite": "api", "test": TESTS[label], "result": "PASS"} for label in LABELS], "passed": True}


def validate_tests(value):
    keys(value, {"selector", "fixture_selector", "required", "passed"})
    need(value == {"selector": "", "fixture_selector": SELECTOR, "passed": True,
                   "required": [{"suite": "api", "test": TESTS[label], "result": "PASS"} for label in LABELS]},
         "required-test-not-passed")


def receipt_names(faults):
    names = {"osd-soak-v1-fixture-termination.json"}
    for label in LABELS:
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
        raise evidence.EvidenceError("trusted-soak-validator-missing") from error
    # The workflow copies this module beside the collector before the tested
    # checkout exists; any other location would not be the trusted controller.
    need(Path(module.__file__).resolve().parent == Path(__file__).resolve().parent, "trusted-soak-validator-location")
    return module


def soak_projection(directory, ctx):
    directory = Path(directory)
    need(not directory.is_symlink() and directory.is_dir(), "unsafe-receipt-directory")
    validator = trusted_soak_validator()
    try:
        result = validator.verify(directory, ctx["tested_sha"], ctx["configured_image"], DURATION_NS, LABELS)
    except (ValueError, TypeError, KeyError, OSError, RecursionError):
        raise evidence.EvidenceError("soak-receipts-invalid") from None
    hashes = result["receipt_sha256"]
    raw, files = {}, {}
    for name, expected in sorted(hashes.items()):
        data = evidence.read(directory / name, RECEIPT_LIMIT)
        need(hashlib.sha256(data).hexdigest() == expected, "receipt-hash-mismatch")
        files[name] = {"size": len(data), "sha256": expected}
        raw[name] = evidence.decode(data)
    workloads, cleanups = {}, {}
    for label in LABELS:
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
                 | {"counters": {key: run["counters"][key] for key in sorted(COUNTERS[label])}},
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
    soak = {"tested_revision": ctx["tested_sha"], "configured_duration_ns": DURATION_NS, "observations": result["observations"], "workloads": workloads}
    cleanup = {"fixture_termination": {"succeeded": True, "suite_exit_success": True, "elapsed_ns": end["termination_elapsed_ns"]}, "workloads": cleanups}
    receipts = {"fixture_manifest_sha256": hashes["fixture-manifest.json"], "files": files}
    validate_soak(soak, ctx)
    validate_cleanup(cleanup)
    validate_receipts(receipts, soak)
    return soak, cleanup, receipts


def validate_soak(value, ctx):
    keys(value, {"tested_revision", "configured_duration_ns", "observations", "workloads"})
    need(value["tested_revision"] == ctx["tested_sha"] and value["configured_duration_ns"] == DURATION_NS and value["observations"] == 8, "soak-context")
    keys(value["workloads"], set(LABELS))
    for label in LABELS:
        item = value["workloads"][label]
        keys(item, {"setup_elapsed_ns", "owned_object_count", "workload", "workers"} | ({"faults", "recovery_clean"} if label == "failover" else set()))
        integer(item["setup_elapsed_ns"], 1)
        need(item["owned_object_count"] == 5, "owned-object-count")
        run = item["workload"]
        keys(run, {"actual_workload_ns", "after_workload_start_ns", "test_elapsed_ns", "counters"})
        for key in ("actual_workload_ns", "after_workload_start_ns", "test_elapsed_ns"):
            integer(run[key], 1)
        need(DURATION_NS <= run["actual_workload_ns"] <= run["after_workload_start_ns"] <= run["test_elapsed_ns"], "workload-interval")
        keys(run["counters"], COUNTERS[label])
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


def validate_cleanup(value):
    keys(value, {"fixture_termination", "workloads"})
    keys(value["fixture_termination"], {"succeeded", "suite_exit_success", "elapsed_ns"})
    need(value["fixture_termination"]["succeeded"] is True and value["fixture_termination"]["suite_exit_success"] is True, "cleanup-incomplete")
    integer(value["fixture_termination"]["elapsed_ns"], 1)
    keys(value["workloads"], set(LABELS))
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
        need(type(record["size"]) is int and 0 < record["size"] <= RECEIPT_LIMIT, "receipt-size")
        digest(record["sha256"])
    need(files["fixture-manifest.json"]["sha256"] == value["fixture_manifest_sha256"], "receipt-inventory")
