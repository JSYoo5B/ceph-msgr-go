#!/usr/bin/env python3
"""Collect/verify bounded successful-native receipts; never archive fixture IPC.

This module holds the bundle mechanics every family shares: the run context,
input reading, privacy and size bounds, the observed runtime file, the
manifest and its hash inventory. A family is a module named
native_evidence_<name>.py beside this one; it declares KIND, FILES (always
with tests.json and runtime.json), PROFILES, CAPABILITIES and claims,
scenario_id, selector, runtime_facts, gate, project and validate (see
family_problems). Families are imported only from this module's own
directory, which the workflow fills from the trusted controller before it
checks out tested code; the tested checkout supplies data only. The
collector refuses every family when one declaration is incomplete or
imported from elsewhere, when two families share a KIND or admit one
profile, and a matrix two families claim (ambiguous-evidence-family). A
request no family claims has its test result classified with the cap-send
gate (UNCLAIMED_GATE) and is then refused. The workflow lists no family: the step
`native_evidence.py family` prints the claimed family's name, or nothing.

The fixture's metadata directory is input only: a family reads the files it
names and the trusted gate's native-tests.json, and projects numeric
identity/time/outcome fields, closed labels, cleanup results and hashes, not
raw logs, endpoints, commands, configuration, credentials, full capability
objects or raw workload bytes. Hashes bind the projected files and name the
omitted raw receipts; they do not let a reader recompute omitted native bytes
or xattrs, nor establish the truth of a native assertion, and they are not a
signature.

Every bundle has manifest.json (schema_version 1, the family's kind, the
execution context: execution_source, tested and controller SHA, run ID and
attempt, scenario, selector, normalized requested profile, configured image,
observed runtime, and the size and SHA-256 of each payload file), tests.json
and runtime.json (inspected image ID and architecture, native daemon version,
provider version, fixture-reported profile). Every field is required and
unknown fields are refused. Collection requires the pinned SOURCE, IMAGE and
PROVIDER_VERSION; verification also admits the provider releases of bundles
recorded earlier. verify requires an expected run request (--request, or the
workflow's EXECUTION_SOURCE, TESTED_SHA, CONTROLLER_SHA, RUN_ID, RUN_ATTEMPT
and MATRIX_JSON) supplied independently of the manifest; the manifest's own
identity given back establishes only self-consistency.

Bounds: INPUT_LIMIT per source document, FILE_LIMIT per gate document and
exported file, TOTAL_LIMIT per bundle including its manifest. Duplicate JSON
keys, invalid JSON, nonregular files, symlinks, missing or oversized inputs,
unexpected files, bad hashes, mismatched context, profile or runtime facts,
incomplete cleanup and unreviewed path or address strings (PRIVATE) are
refused, and an existing output directory is never overwritten. A native
PASS with failed collection or upload stays test_result=passed with
evidence_result=incomplete and a fixed public failure code; the final
evidence step then fails the job. Missing required-test evidence is unknown,
never PASS, and no retry replaces the first attempt. The collector uses only
Python's standard library, executes nothing it reads, and its daemon-free
tests are tools/test_native_*evidence*.py.

    native_evidence.py collect --request REQUEST --receipts METADATA \
        --tests native-tests.json --output BUNDLE --status STATUS
    native_evidence.py verify --request REQUEST --bundle BUNDLE
"""
import argparse
import hashlib
import importlib
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
# Collection admits only the fixture provider release that integration/fixture
# pins now. Verification of a retained bundle also admits the earlier releases
# named by bundles recorded before each move; a bundle never changes release.
PROVIDER = "github.com/JSYoo5B/ceph-testcontainers-go"
PROVIDER_VERSION = "v0.5.0"
RETAINED_PROVIDER_VERSIONS = frozenset({"v0.3.0", "v0.4.0", PROVIDER_VERSION})
INPUT_LIMIT = 16 << 20
FILE_LIMIT = 256 << 10
TOTAL_LIMIT = 1 << 20
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
CONTEXT_KEYS = {"execution_source", "tested_sha", "controller_sha", "run_id", "attempt", "scenario", "selector", "profile", "configured_image"}
# Fresh-filesystem cleanup flags every MDS fixture family reports.
FRESH_COMMON = ("attempted", "completed", "filesystem_removed", "pools_removed", "identity_removed")
PRIVATE = re.compile(r"(?:[\\/]Users[\\/]|[\\/]home[\\/]|\b(?:\d{1,3}\.){3}\d{1,3}\b|\b(?:ssh|keyring)\b)", re.I)

FAMILY_PREFIX = "native_evidence_"
# What a family module declares. Values are checked by family_problems.
FAMILY_VALUES = ("KIND", "FILES", "PROFILES", "CAPABILITIES")
FAMILY_FUNCTIONS = ("claims", "scenario_id", "selector", "runtime_facts", "gate", "project", "validate")
# A request that names no family has its test result classified with this
# family's gate, as before families were modules; the collector then refuses
# the request. A new family never changes it.
UNCLAIMED_GATE = "capsend"


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


# Families ------------------------------------------------------------------


def family_problems(name, module, path):
    """Why a discovered module is not a usable family declaration."""
    problems = []
    if not re.fullmatch(r"[a-z][a-z0-9]*", name):
        problems.append("%s: a family name is lowercase letters and digits" % path.name)
    if Path(getattr(module, "__file__", "") or "").resolve() != path.resolve():
        problems.append("%s: imported from another location" % path.name)
    missing = [attr for attr in FAMILY_VALUES + FAMILY_FUNCTIONS if not hasattr(module, attr)]
    if missing:
        return problems + ["%s: missing %s" % (path.name, ", ".join(missing))]
    problems += ["%s: %s is not callable" % (path.name, attr) for attr in FAMILY_FUNCTIONS if not callable(getattr(module, attr))]
    if not isinstance(module.KIND, str) or not module.KIND:
        problems.append("%s: KIND is not a name" % path.name)
    files = module.FILES
    if not (isinstance(files, (set, frozenset)) and all(isinstance(item, str) for item in files)
            and {"tests.json", "runtime.json"} <= files and "manifest.json" not in files):
        problems.append("%s: FILES needs tests.json and runtime.json and never manifest.json" % path.name)
    if not isinstance(module.CAPABILITIES, str):
        problems.append("%s: CAPABILITIES is not a string" % path.name)
    profiles = module.PROFILES
    if not (isinstance(profiles, list) and profiles and all(isinstance(item, dict) and set(item) == set(DEFAULTS) for item in profiles)):
        problems.append("%s: PROFILES needs complete normalized profiles" % path.name)
    return problems


def declarations(directory):
    """Import every family module in directory: (families, problems with them)."""
    directory = Path(directory).resolve()
    found, problems = {}, []
    for path in sorted(directory.glob(FAMILY_PREFIX + "*.py")):
        name = path.stem[len(FAMILY_PREFIX):]
        try:
            module = importlib.import_module(path.stem)
        except ImportError:
            problems.append("%s: not importable" % path.name)
            continue
        problems += family_problems(name, module, path)
        found[name] = module
    if not found:
        problems.append("no family module beside the collector")
    if problems:
        return found, problems
    kinds = [module.KIND for module in found.values()]
    if len(kinds) != len(set(kinds)):
        problems.append("two families declare one KIND")
    for name, module in found.items():
        for profile in module.PROFILES:
            # Each admitted profile belongs to one family, and the matrix of
            # its row is claimed by that family alone, so the workflow never
            # skips collection for an admitted row.
            matrix = dict(profile, scenario=module.scenario_id(profile))
            owners = sorted(other for other, item in found.items() if profile in item.PROFILES)
            claimants = sorted(other for other, item in found.items() if item.claims(matrix))
            if owners != [name] or claimants != [name]:
                problems.append("%s: an admitted profile is admitted by %s and claimed by %s" % (name, owners, claimants))
    return found, problems


def discover(directory):
    """The usable families in directory; any declaration problem refuses them all."""
    found, problems = declarations(directory)
    need(not problems, "evidence-family-declaration")
    return found


def families():
    return discover(Path(__file__).parent)


def claimed_family(matrix):
    """The family a job's matrix asks for, or None; two claims are refused."""
    names = [name for name, module in families().items() if module.claims(matrix)]
    need(len(names) <= 1, "ambiguous-evidence-family")
    return names[0] if names else None


def profile_family(profile):
    """The family admitting a normalized profile, or None (discover keeps them disjoint)."""
    for module in families().values():
        if profile in module.PROFILES:
            return module
    return None


# Shared projections ----------------------------------------------------------


def row_scenario_id(group, test, profile):
    """The catalog ID of an explicit single-test MDS row."""
    mode = ".crc" if profile["mode"] == "crc" else ""
    return ("%s.%s.ip%s%s.%s.osd-store-%s.osd-replicas-1.osd-profile-legacy-v6.t-%s"
            % (group, profile["key"], profile["family"], mode, profile["runtime"], profile["osd_store"], test))


def context(request):
    keys(request, {"execution_source", "tested_sha", "controller_sha", "run_id", "attempt", "matrix"})
    need(request["execution_source"] in {"github-actions", "linux-runner"}, "execution-source")
    matrix = request["matrix"]
    need(isinstance(matrix, dict) and set(matrix) <= set(DEFAULTS) | {"scenario", "image", "capabilities"}, "unsupported-matrix-field")
    need(all(isinstance(value, str) for value in matrix.values()), "matrix-value")
    profile = dict(DEFAULTS, **{key: value for key, value in matrix.items() if key in DEFAULTS})
    family = profile_family(profile)
    need(family is not None and matrix.get("scenario") == family.scenario_id(profile), "unsupported-profile")
    need(matrix.get("capabilities") in (None, family.CAPABILITIES), "unsupported-capabilities")
    image = matrix.get("image", IMAGE)
    need(image == IMAGE, "unsupported-image")
    value = {"execution_source": request["execution_source"], "tested_sha": sha(request["tested_sha"]), "controller_sha": sha(request["controller_sha"]),
             "run_id": integer(request["run_id"], 1), "attempt": integer(request["attempt"], 1),
             "scenario": matrix["scenario"], "selector": family.selector(profile), "profile": profile, "configured_image": image}
    privacy(value)
    return value


def validate_context(value):
    keys(value, CONTEXT_KEYS)
    matrix = dict(value["profile"], scenario=value["scenario"], image=value["configured_image"])
    expected = context({key: value[key] for key in ("execution_source", "tested_sha", "controller_sha", "run_id", "attempt")} | {"matrix": matrix})
    need(value == expected, "context-mismatch")


def tests_projection(value, test):
    """The gate of a family whose row selects exactly one API parent test."""
    keys(value, {"selector", "expected", "observed", "problems", "passed"})
    need(value["passed"] is True and value == {"selector": "^" + test + "$", "expected": [["api", test]],
                   "observed": [{"suite": "api", "test": test, "result": "PASS"}],
                   "problems": [], "passed": True}, "required-test-not-passed")
    return value


def runtime_projection(raw, ctx):
    mapping = {"key": "key_type", "service": "service_cipher", "mode": "connection_mode", "family": "ip_family",
               "runtime": "runtime", "osd": "osd", "osd_profile": "osd_profile", "osd_store": "osd_store",
               "osd_replicas": "osd_replicas"}
    reported = {key: raw[field] for key, field in mapping.items()}
    need(reported == {key: ctx["profile"][key] for key in mapping}, "fixture-profile-mismatch")
    profile_family(ctx["profile"]).runtime_facts(raw, ctx)
    need(raw["provider"] == PROVIDER and raw["provider_version"] == PROVIDER_VERSION, "provider-mismatch")
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
    need(value["provider_version"] in RETAINED_PROVIDER_VERSIONS, "provider-mismatch")
    names = {"key", "service", "mode", "family", "runtime", "osd", "osd_profile", "osd_store", "osd_replicas"}
    keys(value["fixture_reported_profile"], names)
    need(value["fixture_reported_profile"] == {key: ctx["profile"][key] for key in names}, "fixture-profile-mismatch")


def cleanup_projection(data, fresh, data_flags, fresh_flags):
    """An MDS fixture's metadata and fresh-filesystem cleanup verdicts."""
    need(data["source_commit"] == fresh["source_commit"] == SOURCE, "cleanup-source")
    need(not data.get("errors") and not fresh.get("error") and not data["residual_owned_sessions"] and not data["kept_new_blocklist"], "cleanup-incomplete")
    value = {"source_commit": SOURCE, "data": {key: data[key] for key in data_flags},
             "filesystem": {key: fresh[key] for key in fresh_flags}}
    validate_cleanup(value, data_flags, fresh_flags)
    return value


def validate_cleanup(value, data_flags, fresh_flags):
    keys(value, {"source_commit", "data", "filesystem"})
    need(value["source_commit"] == SOURCE, "cleanup-source")
    for group, flags in (("data", data_flags), ("filesystem", fresh_flags)):
        keys(value[group], flags)
        need(all(value[group][key] is True for key in flags), "cleanup-incomplete")


# Bundle ----------------------------------------------------------------------


def verify(bundle, expected):
    bundle = Path(bundle)
    need(not bundle.is_symlink() and bundle.is_dir(), "unsafe-bundle")
    family = profile_family(expected.get("profile")) if isinstance(expected, dict) else None
    need(family is not None, "unsupported-profile")
    files = family.FILES
    need({item.name for item in bundle.iterdir()} == files | {"manifest.json"}, "bundle-inventory")
    manifest = document(bundle / "manifest.json", FILE_LIMIT)
    keys(manifest, CONTEXT_KEYS | {"schema_version", "kind", "files", "total_bytes", "observed"})
    need(type(manifest["schema_version"]) is int and manifest["schema_version"] == SCHEMA_VERSION and manifest["kind"] == family.KIND, "schema-version")
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
    family.validate(payloads, ctx)
    privacy(manifest)
    for value in payloads.values():
        privacy(value)
    return manifest


def collect(request, receipts, tests, output):
    ctx = context(request)
    receipts, output = Path(receipts), Path(output)
    need(not output.exists() and not output.is_symlink(), "attempt-collision")
    need(not receipts.is_symlink() and receipts.is_dir(), "unsafe-receipt-directory")
    family = profile_family(ctx["profile"])
    payloads = family.project(receipts, tests, ctx)
    need(set(payloads) == family.FILES, "bundle-inventory")
    files, total, content = [], 0, {}
    for name in sorted(payloads):
        data = encoded(payloads[name])
        need(len(data) <= FILE_LIMIT, "file-limit")
        total += len(data)
        content[name] = data
        files.append({"name": name, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    manifest = dict(ctx, schema_version=SCHEMA_VERSION, kind=family.KIND, files=files, total_bytes=total, observed=payloads["runtime.json"])
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


def family_output():
    """The workflow's step output naming the job's family; empty when none claims it."""
    try:
        matrix = decode(os.environ["MATRIX_JSON"])
    except KeyError as error:
        raise EvidenceError("invalid-run-environment") from error
    need(isinstance(matrix, dict), "invalid-run-environment")
    return "family=%s" % (claimed_family(matrix) or "")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("family", help="print the GitHub step output naming the family MATRIX_JSON claims")
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
    if args.command == "family":
        try:
            print(family_output())
            return 0
        except EvidenceError as error:
            print(str(error), file=sys.stderr)
            return 1
    status = {"test_result": "unknown", "evidence_result": "incomplete"}
    result = 1
    try:
        request, failure = None, None
        try:
            request = document(args.request, FILE_LIMIT) if args.request else environment_request()
        except EvidenceError as error:
            failure = error
        if args.command == "collect":
            # An unreadable or unclaimed request is classified with the
            # UNCLAIMED_GATE family's gate; the request error is reported
            # after the test result is classified.
            matrix = request.get("matrix") if isinstance(request, dict) else None
            raw_tests = document(args.tests, FILE_LIMIT)
            name = claimed_family(matrix) if isinstance(matrix, dict) else None
            families()[name or UNCLAIMED_GATE].gate(raw_tests)
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
    # Family modules import this module by name; give them this instance so
    # they raise the EvidenceError that main() classifies.
    sys.modules.setdefault("native_evidence", sys.modules[__name__])
    sys.exit(main())
