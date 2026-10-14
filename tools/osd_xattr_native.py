#!/usr/bin/env python3
"""Independent, disposable Tentacle librados xattr oracle; never product code.

API/source provenance: Ceph 20.2.4, commit
7f793731f1b39eb4f465e960113d2363c311b964, librados.h/librados_c.cc and
PrimaryLogPG.cc (LGPL-2.1). This adapter copies no Ceph implementation.

The C API does not expose raw CMPXATTR/SET/RM per-op results, including CMP's
positive true result 1. It also has no compound single-GETXATTR builder. Direct
GET and compound GETXATTRS are identified separately. Aggregate codes are native
observations; source-derived comparison expectations are not observed per-op
codes. No native mutation is retried. --selfcheck constructs/releases native
operations without a daemon; it does not prove server or wire interoperability.

The fixed CLI matrix reads fixture pool/object identity and credentials, uses
a separate '-native-xattr' object, records byte-exact before/after observations
and removes that object. It never modifies the original fixture object. Binary
values/names in JSON evidence use base64 without text normalization.
Explicit --compare ROOT OUT instead reads bounded osd-xattr-input.json and
observes the caller object without modifying it, executes one independently
constructed native vector on a separate mirror, and writes osd-xattr-result.json.
"""
import argparse
import base64
import ctypes as C
from contextlib import contextmanager
import json
import os
import sys
from pathlib import Path

from osd_oracle import Timespec

MAX_VALUE = 1 << 20
MAX_NAME = 4096
MAX_ENTRIES = 1024
MAX_OPERATIONS = 64
MAX_INPUT = 1 << 20
MAX_COMPARE_OUTPUT = 512 << 10
COMPARE = {"eq": 1, "ne": 2, "gt": 3, "gte": 4, "lt": 5, "lte": 6}
SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
CLIENT_ROLES = {"test": b"client.test", "readonly": b"client.readonly"}


def encode(data):
    return base64.b64encode(data).decode("ascii")


def binary(text, limit=MAX_VALUE):
    if not isinstance(text, str) or len(text) > 4 * ((limit + 2) // 3):
        raise ValueError("invalid or oversized base64 input")
    value = base64.b64decode(text, validate=True)
    if len(value) > limit:
        raise ValueError("binary input exceeds oracle bound")
    return value


def name_bytes(text):
    return checked_name(binary(text, MAX_NAME))


def checked_name(name):
    if not isinstance(name, bytes) or not name or b"\0" in name:
        raise ValueError("xattr name must be nonempty and NUL-free")
    if len(name) > MAX_NAME:
        raise ValueError("xattr name exceeds oracle allocation bound")
    return name


def string_comparison(operator, caller, stored):
    """Source-derived caller-left expectation, NOT a native observed result."""
    if operator not in COMPARE:
        raise ValueError("unsupported STRING comparison")
    ordering = (caller > stored) - (caller < stored)
    return {"eq": ordering == 0, "ne": ordering != 0, "gt": ordering > 0,
            "gte": ordering >= 0, "lt": ordering < 0, "lte": ordering <= 0}[operator]


def validate_request(request):
    if not isinstance(request, dict):
        raise ValueError("xattr input must be an object")
    allowed_fields = {"object", "namespace", "observe_actual", "compare", "exists", "kind",
                      "operations", "attributes", "get_names", "baseline", "seconds", "nanos",
                      "client_role"}
    if set(request) - allowed_fields:
        raise ValueError("unknown xattr input fields")
    role = request.get("client_role", "test")
    if not isinstance(role, str) or role not in CLIENT_ROLES:
        raise ValueError("client_role must be test or readonly")
    for field in ("object", "namespace"):
        if field in request:
            identity(request[field], field, empty=field == "namespace")
    for field in ("observe_actual", "compare", "exists"):
        if field in request and type(request[field]) is not bool:
            raise ValueError("invalid boolean " + field)
    kind = request.get("kind", "read")
    if kind not in ("read", "write"):
        raise ValueError("invalid compound kind")
    seconds, nanos = request.get("seconds", 0), request.get("nanos", 0)
    if type(seconds) is not int or not 0 <= seconds < 1 << 32 or type(nanos) is not int or not 0 <= nanos < 10**9:
        raise ValueError("invalid comparison vector mtime")
    if kind == "read" and (seconds or nanos):
        raise ValueError("read-only comparison vector requires zero mtime")
    actions = request.get("operations", [])
    attrs = request.get("attributes", [])
    names = request.get("get_names", [])
    if (not isinstance(actions, list) or len(actions) > MAX_OPERATIONS
            or not isinstance(attrs, list) or len(attrs) >= MAX_OPERATIONS
            or not isinstance(names, list) or len(names) > MAX_ENTRIES):
        raise ValueError("oracle vector or inventory exceeds bound")
    decoded = []
    for action in actions:
        if not isinstance(action, dict):
            raise ValueError("invalid xattr operation")
        op = action.get("op")
        allowed = {"op", "flags"}
        result = {"op": op, "flags": action.get("flags", 0)}
        if type(result["flags"]) is not int or result["flags"] not in (0, 2):
            raise ValueError("only FAILOK operation flag is supported")
        if op in ("cmp", "set", "remove"):
            result["name"] = name_bytes(action["name"])
            allowed.add("name")
        if op in ("cmp", "set"):
            result["value"] = binary(action["value"])
            allowed.add("value")
        if op == "cmp":
            result["comparison"] = action["comparison"]
            if result["comparison"] not in COMPARE:
                raise ValueError("unsupported STRING comparison")
            allowed.add("comparison")
        if op not in ("cmp", "set", "remove", "list"):
            raise ValueError("unsupported xattr operation")
        if kind == "read" and op in ("set", "remove") or kind == "write" and op == "list":
            raise ValueError("operation cannot be expressed by this C compound API")
        if set(action) - allowed:
            raise ValueError("unknown xattr operation fields")
        decoded.append(result)
    attributes = []
    for attribute in attrs:
        if not isinstance(attribute, dict) or set(attribute) != {"name", "value"}:
            raise ValueError("invalid baseline xattr fields")
        attributes.append((name_bytes(attribute["name"]), binary(attribute["value"])))
    if len({name for name, _ in attributes}) != len(attributes):
        raise ValueError("duplicate baseline xattr name")
    if request.get("compare") and not decoded:
        raise ValueError("native comparison needs a nonempty vector")
    if not request.get("exists", True) and attributes:
        raise ValueError("absent baseline cannot carry attributes")
    baseline = binary(request.get("baseline", ""))
    if len(baseline) + sum(len(n) + len(v) for n, v in attributes) > MAX_VALUE:
        raise ValueError("baseline exceeds aggregate oracle bound")
    if (len(baseline) + sum(len(n) + len(v) for n, v in attributes)
            + sum(len(a.get("name", b"")) + len(a.get("value", b"")) for a in decoded)
            + sum(len(name_bytes(n)) for n in names) > MAX_INPUT):
        raise ValueError("vector exceeds aggregate oracle bound")
    return dict(request, kind=kind, operations=decoded, attributes=attributes,
                get_names=[name_bytes(n) for n in names], baseline=baseline, seconds=seconds, nanos=nanos)


def identity(value, label, empty=False):
    if not isinstance(value, str) or "\0" in value or not empty and not value:
        raise ValueError("invalid " + label + " identity")
    encoded = value.encode("utf-8")
    if len(encoded) > MAX_NAME:
        raise ValueError(label + " identity exceeds oracle bound")
    return encoded


def read_json_bounded(path):
    with path.open("rb") as file:
        data = file.read(MAX_INPUT + 1)
    if len(data) > MAX_INPUT:
        raise ValueError("xattr JSON input exceeds 1 MiB bound")
    return json.loads(data)


def publish_compare_result(out, result):
    data = bytearray()
    for chunk in json.JSONEncoder().iterencode(result):
        encoded = chunk.encode("utf-8")
        if len(data) + len(encoded) + 1 > MAX_COMPARE_OUTPUT:
            raise ValueError("xattr JSON result exceeds 512 KiB bound")
        data.extend(encoded)
    data.extend(b"\n")
    temporary = out / "osd-xattr-result.tmp"
    temporary.write_bytes(data)
    os.replace(temporary, out / "osd-xattr-result.json")


class Native:
    def __init__(self, library=None):
        self.lib = library if library is not None else C.CDLL("librados.so.2")
        p, s, i, u64, char = C.c_void_p, C.c_size_t, C.c_int, C.c_uint64, C.c_char_p
        definitions = {
            "version": ([C.POINTER(i), C.POINTER(i), C.POINTER(i)], None),
            "create2": ([C.POINTER(p), char, char, u64], i),
            "conf_read_file": ([p, char], i), "conf_set": ([p, char, char], i),
            "connect": ([p], i), "shutdown": ([p], None),
            "ioctx_create": ([p, char, C.POINTER(p)], i), "ioctx_destroy": ([p], None),
            "ioctx_set_namespace": ([p, char], None),
            "remove": ([p, char], i), "get_last_version": ([p], u64),
            "stat2": ([p, char, C.POINTER(u64), C.POINTER(Timespec)], i),
            "read": ([p, char, p, s, u64], i), "getxattr": ([p, char, char, p, s], i),
            "getxattrs_next": ([p, C.POINTER(p), C.POINTER(p), C.POINTER(s)], i),
            "getxattrs_end": ([p], None),
            "create_read_op": ([], p), "release_read_op": ([p], None),
            "create_write_op": ([], p), "release_write_op": ([p], None),
            "read_op_getxattrs": ([p, C.POINTER(p), C.POINTER(i)], None),
            "read_op_cmpxattr": ([p, char, C.c_uint8, p, s], None),
            "write_op_cmpxattr": ([p, char, C.c_uint8, p, s], None),
            "write_op_setxattr": ([p, char, p, s], None),
            "write_op_rmxattr": ([p, char], None),
            "write_op_write_full": ([p, p, s], None),
            "read_op_set_flags": ([p, i], None), "write_op_set_flags": ([p, i], None),
            "read_op_operate": ([p, p, char, i], i),
            "write_op_operate2": ([p, p, char, C.POINTER(Timespec), i], i),
        }
        for name, (arguments, result) in definitions.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, result

    @staticmethod
    def check(code):
        if code < 0:
            raise RuntimeError("native xattr oracle setup failed: " + str(code))

    def entries(self, iterator):
        entries, total = [], 0
        previous = None
        while True:
            name, value, length = C.c_void_p(), C.c_void_p(), C.c_size_t()
            self.check(self.lib.rados_getxattrs_next(iterator, C.byref(name), C.byref(value), C.byref(length)))
            # A NULL value with a non-NULL name represents an EMPTY attribute.
            if not name.value:
                if value.value or length.value:
                    raise ValueError("invalid native iterator terminator")
                return entries
            raw_name = C.string_at(name.value)
            if not raw_name or len(raw_name) > MAX_NAME or (previous is not None and raw_name <= previous):
                raise ValueError("invalid native name or inventory order")
            if length.value > MAX_VALUE or (length.value and not value.value):
                raise ValueError("invalid native value pointer or length")
            total += len(raw_name) + length.value
            if total > MAX_VALUE or len(entries) >= MAX_ENTRIES:
                raise ValueError("native inventory exceeds oracle bound")
            # Copy before next/end: librados releases the previous value buffer.
            raw_value = C.string_at(value.value, length.value) if length.value else b""
            entries.append({"name": encode(raw_name), "value": encode(raw_value)})
            previous = raw_name

    def append(self, operation, kind, actions):
        outputs, buffers = [], []
        if kind not in ("read", "write") or len(actions) > MAX_OPERATIONS:
            raise ValueError("invalid native vector kind or count")
        for action in actions:
            code = action["op"]
            if (code not in ("cmp", "set", "remove", "list")
                    or type(action["flags"]) is not int or action["flags"] not in (0, 2)):
                raise ValueError("unsupported native operation or flag")
            if kind == "read" and code in ("set", "remove") or kind == "write" and code == "list":
                raise ValueError("operation cannot be expressed by this C compound API")
            if code != "list":
                checked_name(action["name"])
            if code in ("cmp", "set") and (not isinstance(action["value"], bytes) or len(action["value"]) > MAX_VALUE):
                raise ValueError("invalid native binary input")
            if code == "cmp" and action["comparison"] not in COMPARE:
                raise ValueError("unsupported native STRING comparison")
        for index, action in enumerate(actions):
            code = action["op"]
            if code in ("cmp", "set"):
                value = action["value"]
                buffer = C.create_string_buffer(value, len(value) + 1)
                buffers.append(buffer)
            if code == "cmp":
                getattr(self.lib, "rados_" + kind + "_op_cmpxattr")(
                    operation, action["name"], COMPARE[action["comparison"]], buffer, len(value))
            elif code == "set":
                self.lib.rados_write_op_setxattr(operation, action["name"], buffer, len(value))
            elif code == "remove":
                self.lib.rados_write_op_rmxattr(operation, action["name"])
            elif code == "list":
                iterator, result = C.c_void_p(), C.c_int(999)
                self.lib.rados_read_op_getxattrs(operation, C.byref(iterator), C.byref(result))
                outputs.append((index, iterator, result))
            if action["flags"]:
                getattr(self.lib, "rados_" + kind + "_op_set_flags")(operation, action["flags"])
        return outputs, buffers

    def vector(self, io, object_name, kind, actions, mtime=None):
        operation = getattr(self.lib, "rados_create_" + kind + "_op")()
        if not operation:
            raise RuntimeError("native operation allocation failed")
        outputs = []
        try:
            outputs, buffers = self.append(operation, kind, actions)
            if kind == "read":
                code = self.lib.rados_read_op_operate(operation, io, object_name, 0)
            else:
                code = self.lib.rados_write_op_operate2(operation, io, object_name,
                                                      C.byref(mtime) if mtime is not None else None, 0)
            listed = []
            for index, iterator, result in outputs:
                listed.append({"index": index, "code": result.value,
                               "entries": self.entries(iterator) if code >= 0 and result.value >= 0 else None})
            return {"code": code, "lists": listed, "raw_cmp_per_op_result_available": False,
                    "raw_set_remove_per_op_result_available": False}
        finally:
            for _, iterator, _ in outputs:
                if iterator.value:
                    self.lib.rados_getxattrs_end(iterator)
            getattr(self.lib, "rados_release_" + kind + "_op")(operation)

    def snapshot(self, io, object_name, names):
        for name in names:
            checked_name(name)
        length, mtime = C.c_uint64(), Timespec()
        code = self.lib.rados_stat2(io, object_name, C.byref(length), C.byref(mtime))
        if code == -2:
            return {"code": code, "version": 0, "data": "", "attributes": None, "gets": []}
        self.check(code)
        if length.value > MAX_VALUE or not 0 <= mtime.tv_sec < 1 << 32 or not 0 <= mtime.tv_nsec < 10**9:
            raise ValueError("native object size/stat2 time exceeds oracle bound")
        version = self.lib.rados_get_last_version(io)
        buffer = C.create_string_buffer(length.value + 1)
        count = self.lib.rados_read(io, object_name, buffer, length.value, 0)
        self.check(count)
        if count != length.value:
            raise ValueError("native object read changed size")
        inventory = self.vector(io, object_name, "read", [{"op": "list", "flags": 0}])
        self.check(inventory["code"])
        self.check(inventory["lists"][0]["code"])
        gets = []
        for name in names:
            buffer_value = C.create_string_buffer(MAX_VALUE + 1)
            result = self.lib.rados_getxattr(io, object_name, name, buffer_value, MAX_VALUE)
            if result > MAX_VALUE:
                raise ValueError("native GET output exceeds buffer")
            gets.append({"name": encode(name), "code": result,
                         "value": encode(buffer_value.raw[:result]) if result >= 0 else None})
        # -ENODATA read replies carry no current user_version in pinned
        # PrimaryLogPG::complete_read_ctx. Objecter/IoCtxImpl nevertheless
        # replace last_objver from that reply. A failed GET's last_version
        # therefore cannot establish whether the object itself changed.
        # One fresh successful STAT rechecks real object state; no mutation
        # or failed GET is repeated, and concurrent changes still fail.
        final_length, final_mtime = C.c_uint64(), Timespec()
        self.check(self.lib.rados_stat2(io, object_name, C.byref(final_length), C.byref(final_mtime)))
        final_version = self.lib.rados_get_last_version(io)
        if (final_version != version or final_length.value != length.value
                or final_mtime.tv_sec != mtime.tv_sec or final_mtime.tv_nsec != mtime.tv_nsec):
            raise ValueError("native state changed during separate observation reads")
        return {"code": 0, "version": version, "seconds": mtime.tv_sec, "nanos": mtime.tv_nsec,
                "data": encode(buffer.raw[:count]), "attributes": inventory["lists"][0]["entries"],
                "gets": gets, "single_get_api": "rados_getxattr (separate request)"}


def selfcheck(native):
    """Real native operation construction only; deliberately never connects."""
    for kind in ("read", "write"):
        actions = [{"op": "cmp", "name": b"binary-check", "value": b"\0\xffa\0b",
                    "comparison": comparison, "flags": 0} for comparison in COMPARE]
        if kind == "write":
            actions += [{"op": "set", "name": b"_prefixed", "value": b"\0\xff\0", "flags": 0},
                        {"op": "set", "name": b"empty", "value": b"", "flags": 0},
                        {"op": "remove", "name": b"empty", "flags": 0}]
        operation = getattr(native.lib, "rados_create_" + kind + "_op")()
        if not operation:
            raise RuntimeError("native operation allocation failed")
        try:
            native.append(operation, kind, actions)
        finally:
            getattr(native.lib, "rados_release_" + kind + "_op")(operation)
    return {"native_c_api_construction": True, "binary_input_lengths": [5, 3, 0],
            "read_write_string_comparison_ids": list(COMPARE.values()),
            "daemon_interoperability_verified": False, "wire_payload_verified": False,
            "raw_cmp_per_op_result_available": False}


def compare_case(native, io, object_name, kind, comparison, caller, stored, marker):
    before = native.snapshot(io, object_name, [b"compare", b"guard"])
    expected = string_comparison(comparison, caller, stored)
    action = {"op": "cmp", "name": b"compare", "comparison": comparison,
              "value": caller, "flags": 0}
    actions = [action]
    if kind == "write":
        actions.append({"op": "set", "name": b"guard", "value": marker, "flags": 0})
    else:
        actions.append({"op": "list", "flags": 0})
    observed = native.vector(io, object_name, kind, actions)
    after = native.snapshot(io, object_name, [b"compare", b"guard"])
    context = kind + ":" + comparison + " caller=" + encode(caller) + " aggregate=" + str(observed["code"])
    if expected:
        if observed["code"] < 0:
            raise AssertionError("true caller-left native comparison rejected: " + context)
        if kind == "write":
            if after["gets"][1]["value"] != encode(marker) or after["version"] <= before["version"]:
                raise AssertionError("accepted native guard did not commit marker/version: " + context)
        elif after != before:
            raise AssertionError("read-only native comparison changed object state: " + context)
    elif observed["code"] != -125 or after != before:
        raise AssertionError("false native comparison did not cancel and preserve state: " + context)
    if before["data"] != after["data"] or after["gets"][0]["value"] != encode(stored):
        raise AssertionError("comparison altered object bytes or stored operand")
    return {"kind": kind, "comparison": comparison, "caller": encode(caller),
            "stored": encode(stored), "expected_boolean": expected,
            "aggregate": observed["code"], "before": before, "after": after,
            "marker": encode(marker), "guard_written": kind == "write" and expected,
            "raw_cmp_per_op_result_available": False}


def fixed_matrix(native, io, metadata, daemon_version):
    """Assert native effects/results only, with no Go-generated protocol bytes."""
    object_name = metadata["object"].encode() + b"-native-xattr"
    baseline_data = b"\0native-xattr-object\xff\n"
    attributes = {b"binary": b"\0A\xffB\0", b"empty": b"",
                  b"_prefixed": b"\xff\0prefix", b"compare": b"m\0z"}
    names = sorted(attributes) + [b"guard", b"missing"]
    removed = native.lib.rados_remove(io, object_name)
    if removed != -2:
        native.check(removed)
    major, minor, extra = C.c_int(), C.c_int(), C.c_int()
    native.lib.rados_version(C.byref(major), C.byref(minor), C.byref(extra))
    result = {"schema_version": 1, "source_commit": SOURCE_COMMIT,
              "fixture": {key: metadata[key] for key in ("pool", "pool_id", "object")},
              "oracle_object": object_name.decode(), "fixture_daemon_version": daemon_version,
              "librados_version": [major.value, minor.value, extra.value],
              "scope": {"raw_cmp_per_op_result_available": False,
                        "raw_set_remove_per_op_result_available": False,
                        "single_get_compound_available": False,
                        "positive_cmp_true_result": "1 (pinned server source, not exposed by C API)",
                        "comparison_order": "caller operand left, stored value right"},
              "comparisons": [], "assertions": []}
    try:
        seed = native.lib.rados_create_write_op()
        if not seed:
            raise RuntimeError("native operation allocation failed")
        try:
            buffer = C.create_string_buffer(baseline_data, len(baseline_data) + 1)
            native.lib.rados_write_op_write_full(seed, buffer, len(baseline_data))
            _, buffers = native.append(seed, "write", [
                {"op": "set", "name": name, "value": value, "flags": 0}
                for name, value in attributes.items()])
            native.check(native.lib.rados_write_op_operate2(seed, io, object_name, None, 0))
        finally:
            native.lib.rados_release_write_op(seed)
        before = native.snapshot(io, object_name, names)
        expected_inventory = [{"name": encode(n), "value": encode(v)} for n, v in sorted(attributes.items())]
        if before["data"] != encode(baseline_data) or before["attributes"] != expected_inventory:
            raise AssertionError("binary/empty/prefixed native inventory differs")
        for entry, (name, value) in zip(before["gets"], sorted(attributes.items())):
            if entry["name"] != encode(name) or entry["code"] != len(value) or entry["value"] != encode(value):
                raise AssertionError("direct native GET did not preserve value bytes/length")
        if before["gets"][-1]["code"] != -61:
            raise AssertionError("native missing attribute GET did not return ENODATA")
        result["binary_inventory"] = before
        result["assertions"].append("binary/empty/prefixed inventory and direct GET byte lengths")

        replacement = b"\xff\0replacement\0"
        vector = native.vector(io, object_name, "write", [
            {"op": "set", "name": b"binary", "value": replacement, "flags": 0},
            {"op": "remove", "name": b"empty", "flags": 0}])
        after = native.snapshot(io, object_name, names)
        updated = dict(attributes)
        updated[b"binary"] = replacement
        del updated[b"empty"]
        if (vector["code"] < 0 or after["attributes"] != [{"name": encode(n), "value": encode(v)} for n, v in sorted(updated.items())]
                or after["data"] != before["data"] or after["version"] <= before["version"]):
            raise AssertionError("native compound SET/RM effects/version/content differ")
        result["set_remove"] = {"aggregate": vector["code"], "before": before, "after": after}
        result["assertions"].append("compound SET/RM commits attributes and preserves object content")

        for kind in ("read", "write"):
            for comparison in COMPARE:
                for label, caller in (("less", b"m\0y"), ("equal", b"m\0z"), ("greater", b"m\0{")):
                    marker = (kind + ":" + comparison + ":" + label).encode()
                    result["comparisons"].append(compare_case(native, io, object_name, kind,
                                                            comparison, caller, b"m\0z", marker))
        result["assertions"].append("all six STRING comparisons in read/write vectors with caller operand on left")

        before = native.snapshot(io, object_name, names)
        false_guard = {"op": "cmp", "name": b"compare", "comparison": "eq", "value": b"wrong\0", "flags": 0}
        actions = [{"op": "set", "name": b"binary", "value": b"staged\0", "flags": 0}, false_guard]
        vector = native.vector(io, object_name, "write", actions)
        after = native.snapshot(io, object_name, names)
        if vector["code"] != -125 or after != before:
            raise AssertionError("failing guard did not roll back staged metadata/version/mtime")
        result["rollback"] = {"aggregate": vector["code"], "before": before, "after": after}
        result["assertions"].append("ordinary false guard rolls back earlier staged xattr and preserves stat2")
        actions[1] = dict(false_guard, flags=2)
        actions.append({"op": "set", "name": b"guard", "value": b"after-failok", "flags": 0})
        vector = native.vector(io, object_name, "write", actions)
        after = native.snapshot(io, object_name, [b"binary", b"guard"])
        if (vector["code"] < 0 or after["gets"][0]["value"] != encode(b"staged\0")
                or after["gets"][1]["value"] != encode(b"after-failok")
                or after["data"] != before["data"] or after["version"] <= before["version"]):
            raise AssertionError("FAILOK did not permit native later commit")
        result["failok"] = {"aggregate": vector["code"], "before": before, "after": after,
                            "expected_comparison_boolean": False, "raw_cmp_per_op_result_available": False}
        result["assertions"].append("FAILOK false guard allows later native mutation; raw per-op result remains unavailable")
        result["checked_counts"] = {"comparisons": 36, "comparison_operators": 6,
                                    "compound_kinds": 2, "baseline_attributes": 4,
                                    "rollback_vectors": 1, "failok_vectors": 1}
        return result
    finally:
        code = native.lib.rados_remove(io, object_name)
        result["cleanup"] = {"object_removed": code in (0, -2), "code": code}
        if code not in (0, -2) and sys.exc_info()[0] is None:
            native.check(code)


def collect(root, out):
    metadata = json.loads((out / "osd-oracle.json").read_bytes())
    for field in ("pool", "object"):
        if not isinstance(metadata[field], str) or not metadata[field] or "\0" in metadata[field]:
            raise ValueError("invalid fixture identity")
    daemon_version = (out / "daemon-version").read_text().strip()
    if len(daemon_version) > 512 or "20.2.4" not in daemon_version or SOURCE_COMMIT not in daemon_version:
        raise ValueError("native xattr fixture does not identify the pinned Tentacle source")
    native, cluster, io = Native(), C.c_void_p(), C.c_void_p()
    lib = native.lib
    native.check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
    try:
        native.check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        native.check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        native.check(lib.rados_connect(cluster))
        native.check(lib.rados_ioctx_create(cluster, metadata["pool"].encode(), C.byref(io)))
        try:
            result = fixed_matrix(native, io, metadata, daemon_version)
            temporary = out / "osd-xattr-native.tmp"
            temporary.write_text(json.dumps(result) + "\n")
            os.replace(temporary, out / "osd-xattr-native.json")
        finally:
            lib.rados_ioctx_destroy(io)
    finally:
        lib.rados_shutdown(cluster)


def comparison_vector(native, io, metadata, request, vector_io=None):
    """Only the disposable native mirror is mutated; the actual object is read."""
    original = identity(request.get("object", metadata["object"]), "object")
    namespace = request.get("namespace", "")
    native.lib.rados_ioctx_set_namespace(io, identity(namespace, "namespace", empty=True))
    mirror = original + b"-native-xattr-compare"
    result = {"schema_version": 1, "source_commit": SOURCE_COMMIT,
              "fixture": {key: metadata[key] for key in ("pool", "pool_id", "object")},
              "object": original.decode("utf-8"), "namespace": namespace,
              "mirror_object": mirror.decode("utf-8"),
              "scope": {"raw_cmp_per_op_result_available": False,
                        "raw_set_remove_per_op_result_available": False,
                        "single_get_compound_available": False,
                        "positive_cmp_true_result": "1 (pinned server source, not exposed by C API)"}}
    if request.get("observe_actual", True):
        result["actual"] = native.snapshot(io, original, request["get_names"])
    if not request.get("compare", False):
        result["cleanup"] = {"mirror_touched": False, "object_removed": False, "code": None}
        return result
    removed = native.lib.rados_remove(io, mirror)
    if removed != -2:
        native.check(removed)
    try:
        if request.get("exists", True):
            seed = native.lib.rados_create_write_op()
            if not seed:
                raise RuntimeError("native operation allocation failed")
            try:
                baseline = request["baseline"]
                buffer = C.create_string_buffer(baseline, len(baseline) + 1)
                native.lib.rados_write_op_write_full(seed, buffer, len(baseline))
                actions = [{"op": "set", "name": n, "value": v, "flags": 0}
                           for n, v in request["attributes"]]
                _, buffers = native.append(seed, "write", actions)
                seed_time = Timespec(1700000000, 123456789)
                native.check(native.lib.rados_write_op_operate2(seed, io, mirror, C.byref(seed_time), 0))
            finally:
                native.lib.rados_release_write_op(seed)
        result["before"] = native.snapshot(io, mirror, request["get_names"])
        mtime = Timespec(request["seconds"], request["nanos"]) if request["kind"] == "write" else None
        result["vector"] = native.vector(io if vector_io is None else vector_io, mirror,
                                          request["kind"], request["operations"], mtime=mtime)
        result["after"] = native.snapshot(io, mirror, request["get_names"])
        return result
    finally:
        code = native.lib.rados_remove(io, mirror)
        result["cleanup"] = {"mirror_touched": True, "object_removed": code in (0, -2), "code": code}
        if code not in (0, -2) and sys.exc_info()[0] is None:
            native.check(code)


@contextmanager
def client_connection(native, root, metadata, role):
    """Fixed fixture identities only; never caller-supplied auth/config paths."""
    if role not in CLIENT_ROLES:
        raise ValueError("client_role must be test or readonly")
    cluster, io = C.c_void_p(), C.c_void_p()
    lib = native.lib
    native.check(lib.rados_create2(C.byref(cluster), b"ceph", CLIENT_ROLES[role], 0))
    try:
        native.check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        # cluster.sh combines the public fixture's private identity keyrings
        # here. rados_create2 selects the entity; key material never leaves it.
        native.check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        native.check(lib.rados_connect(cluster))
        native.check(lib.rados_ioctx_create(cluster, identity(metadata["pool"], "pool"), C.byref(io)))
        try:
            yield io
        finally:
            lib.rados_ioctx_destroy(io)
    finally:
        lib.rados_shutdown(cluster)


def collect_comparison(root, out):
    # Parse every caller-controlled field, including metadata defaults, before
    # loading librados or admitting any native request/mirror reset.
    request = validate_request(read_json_bounded(out / "osd-xattr-input.json"))
    metadata = read_json_bounded(out / "osd-oracle.json")
    identity(metadata["pool"], "pool")
    identity(request.get("object", metadata["object"]), "object")
    if type(metadata["pool_id"]) is not int or not 0 <= metadata["pool_id"] < 1 << 63:
        raise ValueError("invalid fixture pool identity")
    with (out / "daemon-version").open("rb") as version_file:
        daemon_version = version_file.read(513).decode("utf-8").strip()
    if len(daemon_version) > 512 or "20.2.4" not in daemon_version or SOURCE_COMMIT not in daemon_version:
        raise ValueError("native xattr fixture does not identify the pinned Tentacle source")
    native = Native()
    with client_connection(native, root, metadata, "test") as io:
        if request.get("client_role", "test") == "readonly":
            with client_connection(native, root, metadata, "readonly") as restricted:
                native.lib.rados_ioctx_set_namespace(restricted, identity(request.get("namespace", ""),
                                                                          "namespace", empty=True))
                result = comparison_vector(native, io, metadata, request, vector_io=restricted)
                result["client_identity"] = "client.readonly"
        else:
            result = comparison_vector(native, io, metadata, request)
        result["fixture_daemon_version"] = daemon_version
        publish_compare_result(out, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, nargs="?")
    parser.add_argument("out", type=Path, nargs="?")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--selfcheck", action="store_true")
    mode.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    if args.selfcheck:
        print(json.dumps(selfcheck(Native())))
    elif args.root is not None and args.out is not None:
        if args.compare:
            collect_comparison(args.root, args.out)
        else:
            collect(args.root, args.out)
    else:
        parser.error("provide fixture root/out, or --selfcheck")
