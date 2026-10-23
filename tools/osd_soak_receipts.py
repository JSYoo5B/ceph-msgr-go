#!/usr/bin/env python3
"""Validate workload-specific immutable soak receipts, never a common bundle.

The requested revision/image/duration are caller expectations. Native image
identity and the fixture manifest remain independently attributed observations.
Only closed workload filenames and the fixed fixture manifest are read.
"""
import argparse
import base64
import hashlib
import json
import re
from pathlib import Path

SOURCE = "7f793731f1b39eb4f465e960113d2363c311b964"
TESTS = {"steady": "TestCephOSDSoakIntegration",
         "failover": "TestCephOSDSoakFailoverIntegration"}
NAME = re.compile(r"osd-soak-v1-(fixture-termination|(steady|failover)-(started|setup|workload|cleanup|recovery-clean|worker-[0-3]|fault-[0-9]{3}-(kill|restart|clean)))\.json")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    out = {}
    for key, value in pairs:
        require(key not in out, "duplicate JSON field: " + key)
        out[key] = value
    return out


def binary(value, label):
    # An empty buffer may marshal as null in an interrupted Go test. Its
    # declared length and digest still have to match, as does the native view.
    if value is None:
        return b""
    require(isinstance(value, str), label + ": expected base64 bytes")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError(label + ": malformed base64") from error
    require(len(decoded) <= 128 << 10, label + ": bytes exceed workload bound")
    return decoded


def verify(directory, revision, image, duration_ns):
    require(re.fullmatch(r"[0-9a-f]{40}", revision) is not None, "expected revision must be a full SHA")
    require(duration_ns >= 30_000_000_000, "expected duration below admitted workload minimum")
    files = {}
    for path in sorted(directory.glob("osd-soak-v1-*.json")):
        require(NAME.fullmatch(path.name) is not None and not path.is_symlink() and path.is_file(), "invalid workload receipt entry")
        require(len(files) < 256, "too many workload receipts")
        files[path.name] = path
    hashes = {}

    def read(name, label=None, kind=None):
        path = directory / name
        require(path.is_file() and not path.is_symlink(), "missing receipt: " + name)
        raw = path.read_bytes()
        require(len(raw) <= 512 << 10, "oversized receipt: " + name)
        value = json.loads(raw, object_pairs_hook=unique_object)
        hashes[name] = hashlib.sha256(raw).hexdigest()
        if name.startswith("osd-soak-v1-"):
            require(value.get("schema_version") == 1, "wrong receipt version: " + name)
        if label:
            require(value.get("test") == TESTS[label], "wrong test identity: " + name)
        if kind:
            require(value.get("kind") == kind, "wrong stage identity: " + name)
        return value

    manifest_path = directory / "fixture-manifest.json"
    manifest = read("fixture-manifest.json")
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    end = read("osd-soak-v1-fixture-termination.json", kind="fixture_termination")
    require(end.get("configured_source_sha") == revision, "wrong tested revision")
    require(end.get("configured_image") == image and manifest.get("image") == image, "wrong image reference")
    require(end.get("fixture_manifest_sha256") == manifest_hash, "fixture manifest hash mismatch")
    require(bool(end.get("observed_image_id")) and end.get("observed_image_id") == manifest.get("image_id"), "missing or wrong observed image identity")
    require(end.get("architecture") in {"amd64", "arm64"} and end.get("architecture") == manifest.get("architecture"), "missing or wrong image architecture")
    require(end.get("configured_duration_ns") == duration_ns, "wrong configured duration")
    require(end.get("status") == "passed" and end.get("suite_exit_success") is True, "native suite did not succeed")
    require(end.get("termination_succeeded") is True, "missing successful fixture termination")
    require(end.get("termination_elapsed_ns", 0) > 0, "missing fixture termination interval")
    seen = set()
    for label in TESTS:
        prefix = "osd-soak-v1-" + label + "-"
        started = read(prefix + "started.json", label, "started")
        require(started.get("configured_source_sha") == revision, "wrong started revision: " + label)
        require(started.get("configured_duration_ns") == duration_ns, "wrong started duration: " + label)
        require(started.get("fixture_manifest_sha256") == manifest_hash, "started manifest mismatch: " + label)
        require(read(prefix + "setup.json", label, "setup").get("elapsed_ns", 0) > 0, "missing setup interval: " + label)
        run = read(prefix + "workload.json", label, "workload")
        require(run.get("status") == "passed" and not run.get("first_error"), "workload failed: " + label)
        require(run.get("configured_duration_ns") == duration_ns, "workload duration configuration mismatch: " + label)
        require(type(run.get("actual_workload_ns")) is int and run["actual_workload_ns"] >= duration_ns, "shortened or missing monotonic workload: " + label)
        require(run.get("test_elapsed_ns", 0) >= run["actual_workload_ns"], "missing total test interval: " + label)
        require(run.get("unresolved_outcomes") == [], "unresolved mutation outcomes: " + label)
        counters = run.get("counters", {})
        # Every successful run has complete counters, even when some are zero.
        required = {"appends", "reads", "unknowns", "resubmissions", "xattrs", "truncations"}
        required |= {"churns", "unsent"} if label == "steady" else {"faults", "replacements", "resent"}
        require(required <= set(counters), "missing workload counters: " + label)
        require(counters.get("appends", 0) >= 100 and counters.get("reads", 0) >= 10, "insufficient workload counters: " + label)
        require(all(type(v) is int and v >= 0 for v in counters.values()), "invalid workload counters: " + label)
        if label == "steady":
            require(counters.get("churns", 0) >= 10, "missing connection churn")
        cleanup = read(prefix + "cleanup.json", label, "cleanup")
        require(cleanup.get("removed") is True and cleanup.get("elapsed_ns", 0) > 0, "missing owned-object cleanup: " + label)
        removed = {entry["object"]: entry["code"] for entry in cleanup.get("objects", [])}
        objects = set()
        for worker in range(4):
            name = prefix + f"worker-{worker}.json"
            observation = read(name, label, f"worker-{worker}")
            require(observation.get("worker") == worker, "duplicate or wrong worker identity: " + name)
            require(observation.get("native_observation_elapsed_ns", 0) > 0, "missing native observation interval: " + name)
            require(observation.get("object") and observation["object"] not in objects, "duplicate object within workload: " + name)
            objects.add(observation["object"])
            require(observation["object"] in removed and removed[observation["object"]] in {0, -2}, "worker object lacks native cleanup verdict: " + name)
            require(observation.get("matched") is True and observation.get("independent_native_c_api") is True, "missing native match: " + name)
            require(observation.get("native_source_commit") == SOURCE and SOURCE in observation.get("native_daemon_version", ""), "wrong native source: " + name)
            expected = binary(observation.get("expected_data"), name)
            native = observation.get("native", {})
            require("expected_data" in observation and "data" in native, "truncated observation fields: " + name)
            actual = binary(native.get("data"), name)
            for field, data in [("expected", expected), ("native", actual)]:
                require(observation.get(field + "_length") == len(data) and observation.get(field + "_sha256") == hashlib.sha256(data).hexdigest(), "truncated or corrupt observation: " + name)
            require(native.get("code") == 0 and native.get("version", 0) > 0 and expected == actual, "native bytes/version mismatch: " + name)
            attrs = {binary(a["name"], name): binary(a["value"], name) for a in native.get("attributes", [])}
            require(isinstance(observation.get("expected_xattr"), str) and attrs.get(b"soak") == observation["expected_xattr"].encode(), "native xattr mismatch: " + name)
            seen.add((label, worker))
        if label == "failover":
            faults = counters.get("faults", 0)
            require(1 <= faults <= 64, "missing or excessive fault cycles")
            require(read(prefix + "recovery-clean.json", label, "recovery-clean").get("passed") is True, "missing final clean-PG recovery")
            previous = 0
            for fault in range(1, faults + 1):
                victims = set()
                for phase in ["kill", "restart", "clean"]:
                    kind = f"fault-{fault:03d}-{phase}"
                    record = read(prefix + kind + ".json", label, kind)
                    require(record.get("passed") is True and record.get("elapsed_ns", 0) > 0, "failed or missing fault phase: " + kind)
                    when = record.get("after_workload_start_ns", 0)
                    require(when >= previous, "fault phase order differs: " + kind)
                    previous = when
                    victims.add(record.get("victim"))
                require(len(victims) == 1 and next(iter(victims)) in {"0", "1", "2"}, "fault victim changed within cycle")
    require(len(seen) == 8, "missing final observations")
    require(set(files) == {name for name in hashes if name.startswith("osd-soak-v1-")}, "unexpected or duplicate workload receipt")
    return {"schema_version": 1, "status": "passed", "observations": len(seen), "configured_duration_ns": duration_ns, "tested_revision": revision, "receipt_sha256": hashes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["verify"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--duration-ns", type=int, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.directory, args.revision, args.image, args.duration_ns), sort_keys=True))
    except OSError as error:
        print("soak evidence missing or unreadable: errno=" + str(error.errno))
        return 1
    except (ValueError, TypeError, KeyError) as error:
        print("soak evidence missing or invalid: " + str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
