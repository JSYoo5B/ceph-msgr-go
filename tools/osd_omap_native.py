#!/usr/bin/env python3
"""Independent test-only Tentacle C OMAP read oracle, never product code.

Ceph 20.2.4 source 7f793731f1b39eb4f465e960113d2363c311b964:
src/include/rados/librados.h:1794,3107,3527,3567,3603 and
src/librados/librados_c.cc (LGPL-2.1; header LGPL-2.1 or LGPL-3.0).
This adapter copies no Ceph implementation and emits no Go-generated wire data.
GETKEYS2/GETVALS2 expose native more and prval; BYKEYS2/SET2/next2 preserve
explicit key/value lengths. Pagination cursor/prefix are still C strings:
NUL rejection here is an oracle limitation, NOT an OMAP wire/product rule.
Empty and binary NUL keys are admitted for seed and BYKEYS2. A NULL key ends
iteration; a non-NULL empty key or NULL zero-length value is an existing entry.

ROOT contains fixture ceph.conf/keyring. OUT contains pinned osd-oracle.json,
daemon-version and bounded osd-omap-input.json. --prepare exclusively creates
a UUID-suffixed fixture object and publishes an ownership receipt for later Go
reads; --cleanup removes only that receipt's object. --compare creates and
removes a separate native mirror around ONE read vector. --observe executes
ONE read vector on a caller-explicit object without writes. Each records native
bytes/stat2/version/xattrs and OMAP before/after. Snapshot enumeration starts
after empty, so explicit BYKEYS2 covers empty keys separately; this is not an
automatic pagination oracle. Header/CMP/writes are not read-vector support.
There is no native request retry. Unit adaptation tests are not daemon proof.
"""
import argparse
import ctypes as C
import json
import os
from pathlib import Path
import sys
import uuid

import osd_xattr_native as xattr

SOURCE_COMMIT = xattr.SOURCE_COMMIT
MAX_INPUT = 1 << 20
MAX_OUTPUT = 512 << 10
MAX_BINARY = 128 << 10
MAX_KEY = 4096
MAX_ENTRIES = 128
MAX_OPERATIONS = 64
READ_CODES = {"keys": 0x1211, "vals": 0x1212, "bykeys": 0x1214}


def binary(text, limit=MAX_BINARY):
    return xattr.binary(text, limit)


def integer(value, maximum, label):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError("invalid " + label)
    return value


def cursor(text):
    value = binary(text, MAX_KEY)
    if b"\0" in value:
        raise ValueError("C oracle cursor/prefix cannot contain NUL; wire support remains separate")
    return value


def validate_request(raw, mode):
    if not isinstance(raw, dict) or set(raw) - {"object", "namespace", "baseline", "attributes", "entries", "operations", "inventory_keys"}:
        raise ValueError("unknown OMAP input fields")
    if mode not in ("prepare", "compare", "observe", "cleanup"):
        raise ValueError("invalid OMAP oracle mode")
    for field in ("object", "namespace"):
        if field in raw:
            xattr.identity(raw[field], field, empty=field == "namespace")
    if mode == "observe" and "object" not in raw:
        raise ValueError("observe needs an explicit object")
    if mode != "observe" and "object" in raw:
        raise ValueError("seed/cleanup object identity comes only from fixture ownership")
    baseline = binary(raw.get("baseline", ""))
    entries, attrs = [], []
    for field, target, names in (("entries", entries, ("key", "value")), ("attributes", attrs, ("name", "value"))):
        values = raw.get(field, [])
        limit = xattr.MAX_OPERATIONS if field == "attributes" else MAX_ENTRIES
        if not isinstance(values, list) or len(values) > limit:
            raise ValueError("invalid " + field + " inventory count")
        for item in values:
            if not isinstance(item, dict) or set(item) != set(names):
                raise ValueError("invalid " + field + " entry")
            key = binary(item[names[0]], MAX_KEY)
            if field == "attributes":
                xattr.checked_name(key)
            target.append((key, binary(item["value"])))
        if len({k for k, _ in target}) != len(target):
            raise ValueError("duplicate " + field + " key")
        target.sort()
    keys = raw.get("inventory_keys", [])
    if not isinstance(keys, list) or len(keys) > MAX_ENTRIES:
        raise ValueError("invalid inventory key count")
    keys = [binary(k, MAX_KEY) for k in keys]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate inventory key")
    if mode in ("observe", "cleanup") and (baseline or entries or attrs):
        raise ValueError("observation/cleanup cannot seed baseline state")
    actions = raw.get("operations", [])
    if not isinstance(actions, list) or len(actions) > MAX_OPERATIONS:
        raise ValueError("invalid read vector count")
    decoded = []
    for action in actions:
        if not isinstance(action, dict) or action.get("op") not in READ_CODES:
            raise ValueError("unsupported OMAP read operation")
        op = action["op"]
        admitted = {"op", "flags", "keys"} if op == "bykeys" else {"op", "flags", "start_after", "max_return"}
        if op == "vals":
            admitted.add("prefix")
        if set(action) - admitted:
            raise ValueError("unknown OMAP operation fields")
        flags = integer(action.get("flags", 0), 2, "operation flags")
        if flags not in (0, 2):
            raise ValueError("only FAILOK flag is admitted")
        result = {"op": op, "flags": flags}
        if op == "bykeys":
            requested = action.get("keys", [])
            if not isinstance(requested, list) or len(requested) > MAX_ENTRIES:
                raise ValueError("invalid requested key count")
            result["keys"] = [binary(k, MAX_KEY) for k in requested]
            if len(set(result["keys"])) != len(result["keys"]):
                raise ValueError("duplicate requested key")
        else:
            result.update(start_after=cursor(action.get("start_after", "")),
                          max_return=integer(action.get("max_return", MAX_ENTRIES), (1 << 64) - 1, "max_return"))
            if op == "vals":
                result["prefix"] = cursor(action.get("prefix", ""))
        decoded.append(result)
    if mode in ("observe", "compare") and not decoded:
        raise ValueError("read vector must not be empty")
    if mode in ("prepare", "cleanup") and decoded:
        raise ValueError("prepare/cleanup cannot execute a read vector")
    total = len(baseline) + sum(len(k) + len(v) for k, v in entries + attrs) + sum(map(len, keys))
    total += sum(len(a.get("start_after", b"")) + len(a.get("prefix", b"")) + sum(map(len, a.get("keys", []))) for a in decoded)
    if total > MAX_BINARY:
        raise ValueError("aggregate oracle input exceeds 128 KiB")
    return dict(raw, baseline=baseline, entries=entries, attributes=attrs,
                inventory_keys=keys, operations=decoded, namespace=raw.get("namespace", ""))


def read_json(path):
    with path.open("rb") as source:
        data = source.read(MAX_INPUT + 1)
    if len(data) > MAX_INPUT:
        raise ValueError("OMAP JSON input exceeds 1 MiB")
    return json.loads(data)


def publish(out, name, result):
    data = bytearray()
    for chunk in json.JSONEncoder().iterencode(result):
        encoded = chunk.encode()
        if len(data) + len(encoded) + 1 > MAX_OUTPUT:
            raise ValueError("OMAP JSON output exceeds 512 KiB")
        data.extend(encoded)
    temporary = out / (name + ".tmp")
    temporary.write_bytes(data + b"\n")
    os.replace(temporary, out / (name + ".json"))


def pointers(values):
    buffers = [C.create_string_buffer(value, len(value) + 1) for value in values]
    array = (C.c_void_p * len(buffers))(*(C.addressof(b) for b in buffers))
    lengths = (C.c_size_t * len(values))(*(len(v) for v in values))
    return array, lengths, buffers


class Native(xattr.Native):
    def __init__(self, library=None):
        super().__init__(library)
        p, s, i, char = C.c_void_p, C.c_size_t, C.c_int, C.c_char_p
        definitions = {
            "write_op_create": ([p, i, char], None),
            "write_op_omap_set2": ([p, C.POINTER(p), C.POINTER(p), C.POINTER(s), C.POINTER(s), s], None),
            "read_op_omap_get_keys2": ([p, char, C.c_uint64, C.POINTER(p), C.POINTER(C.c_ubyte), C.POINTER(i)], None),
            "read_op_omap_get_vals2": ([p, char, char, C.c_uint64, C.POINTER(p), C.POINTER(C.c_ubyte), C.POINTER(i)], None),
            "read_op_omap_get_vals_by_keys2": ([p, C.POINTER(p), s, C.POINTER(s), C.POINTER(p), C.POINTER(i)], None),
            "omap_get_next2": ([p, C.POINTER(p), C.POINTER(p), C.POINTER(s), C.POINTER(s)], i),
            "omap_get_end": ([p], None),
        }
        for name, (arguments, result) in definitions.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, result

    def omap_entries(self, iterator, keys_only=False):
        entries, previous, total = [], None, 0
        while True:
            key, value, kl, vl = C.c_void_p(), C.c_void_p(), C.c_size_t(), C.c_size_t()
            self.check(self.lib.rados_omap_get_next2(iterator, C.byref(key), C.byref(value), C.byref(kl), C.byref(vl)))
            if not key.value:
                if value.value or kl.value or vl.value:
                    raise ValueError("invalid OMAP iterator terminator")
                return entries
            if kl.value > MAX_KEY or vl.value > MAX_BINARY or (vl.value and not value.value):
                raise ValueError("invalid OMAP pointer or length")
            total += kl.value + vl.value
            if total > MAX_BINARY or len(entries) >= MAX_ENTRIES:
                raise ValueError("OMAP iterator exceeds bound")
            raw_key = C.string_at(key.value, kl.value)
            if previous is not None and raw_key <= previous:
                raise ValueError("OMAP native order/uniqueness violated")
            if keys_only and vl.value:
                raise ValueError("GETKEYS returned a nonempty value")
            raw_value = C.string_at(value.value, vl.value) if vl.value else b""
            entries.append({"key": xattr.encode(raw_key), "value": xattr.encode(raw_value)})
            previous = raw_key

    def append_reads(self, operation, actions, outputs, buffers):
        for index, action in enumerate(actions):
            iterator, result, more = C.c_void_p(), C.c_int(999), C.c_ubyte(2)
            # Register immediately: even later builder/iterator failure releases
            # every allocated native iterator, before operation destruction.
            outputs.append((index, action["op"], iterator, result, more))
            if action["op"] == "keys":
                self.lib.rados_read_op_omap_get_keys2(operation, action["start_after"], action["max_return"], C.byref(iterator), C.byref(more), C.byref(result))
            elif action["op"] == "vals":
                self.lib.rados_read_op_omap_get_vals2(operation, action["start_after"], action["prefix"], action["max_return"], C.byref(iterator), C.byref(more), C.byref(result))
            else:
                keys, lens, keep = pointers(action["keys"])
                buffers.extend(keep)
                buffers.extend((keys, lens))
                self.lib.rados_read_op_omap_get_vals_by_keys2(operation, keys, len(action["keys"]), lens, C.byref(iterator), C.byref(result))
            if action["flags"]:
                self.lib.rados_read_op_set_flags(operation, action["flags"])

    def read_vector(self, io, name, actions):
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native OMAP read allocation failed")
        outputs, buffers = [], []
        try:
            self.append_reads(operation, actions, outputs, buffers)
            aggregate = self.lib.rados_read_op_operate(operation, io, name, 0)
            observed = []
            for index, op, iterator, prval, more in outputs:
                success = aggregate >= 0 and prval.value >= 0
                if success and not iterator.value:
                    raise ValueError("successful native OMAP action has no iterator")
                if success and op != "bykeys" and more.value not in (0, 1):
                    raise ValueError("native OMAP more is not bool")
                observed.append({"index": index, "op": op, "opcode": READ_CODES[op], "prval": prval.value,
                                 "more": bool(more.value) if success and op != "bykeys" else None,
                                 "entries": self.omap_entries(iterator, op == "keys") if success else None})
            return {"aggregate": aggregate, "operations": observed, "raw_wire_payload_available": False}
        finally:
            for _, _, iterator, _, _ in outputs:
                if iterator.value:
                    self.lib.rados_omap_get_end(iterator)
            self.lib.rados_release_read_op(operation)

    def seed(self, io, name, request):
        operation = self.lib.rados_create_write_op()
        if not operation:
            raise RuntimeError("native OMAP seed allocation failed")
        try:
            self.lib.rados_write_op_create(operation, 1, None)  # exclusive, no delete/reset
            baseline = C.create_string_buffer(request["baseline"], len(request["baseline"]) + 1)
            self.lib.rados_write_op_write_full(operation, baseline, len(request["baseline"]))
            _, attrs = self.append(operation, "write", [{"op": "set", "name": k, "value": v, "flags": 0} for k, v in request["attributes"]])
            keys, kl, kb = pointers([k for k, _ in request["entries"]])
            vals, vl, vb = pointers([v for _, v in request["entries"]])
            self.lib.rados_write_op_omap_set2(operation, keys, vals, kl, vl, len(request["entries"]))
            mtime = xattr.Timespec(1700000000, 123456789)
            return self.lib.rados_write_op_operate2(operation, io, name, C.byref(mtime), 0)
        finally:
            self.lib.rados_release_write_op(operation)

    def state(self, io, name, keys):
        result = self.snapshot(io, name, [])  # native bytes/stat2/xattrs, not OMAP
        if result["code"] < 0:
            result["omap"] = None
            return result
        actions = [{"op": "vals", "flags": 0, "start_after": b"", "prefix": b"", "max_return": MAX_ENTRIES},
                   {"op": "bykeys", "flags": 0, "keys": sorted(set(keys + [b""]))}]
        result["omap"] = self.read_vector(io, name, actions)
        self.check(result["omap"]["aggregate"])
        for op in result["omap"]["operations"]:
            self.check(op["prval"])
        if result["omap"]["operations"][0]["more"]:
            raise ValueError("native OMAP inventory exceeds one bounded page; no C binary-cursor pagination claimed")
        length, mtime = C.c_uint64(), xattr.Timespec()
        self.check(self.lib.rados_stat2(io, name, C.byref(length), C.byref(mtime)))
        if (self.lib.rados_get_last_version(io) != result["version"] or length.value != len(binary(result["data"]))
                or mtime.tv_sec != result["seconds"] or mtime.tv_nsec != result["nanos"]):
            raise ValueError("native state changed during OMAP observation")
        return result


def owned_name(metadata):
    name = xattr.identity(metadata["object"], "fixture object") + b"-native-omap-" + uuid.uuid4().hex.encode()
    # Include suffix in the bound before any native creation. Otherwise a
    # published long ownership identity could not pass cleanup admission.
    xattr.identity(name.decode(), "owned object")
    return name


def ownership(metadata, request, name):
    return {"schema_version": 1, "source_commit": SOURCE_COMMIT, "pool": metadata["pool"],
            "pool_id": metadata["pool_id"], "fixture_object": metadata["object"],
            "object": name.decode(), "namespace": request["namespace"]}


def check_ownership(receipt, metadata, namespace):
    if not isinstance(receipt, dict) or receipt.get("source_commit") != SOURCE_COMMIT or receipt.get("schema_version") != 1:
        raise ValueError("invalid OMAP ownership provenance")
    if any(receipt.get(k) != metadata[k] for k in ("pool", "pool_id")) or receipt.get("fixture_object") != metadata["object"] or receipt.get("namespace") != namespace:
        raise ValueError("OMAP ownership scope differs from fixture/input")
    name = xattr.identity(receipt.get("object"), "owned object")
    prefix = xattr.identity(metadata["object"], "fixture object") + b"-native-omap-"
    suffix = name[len(prefix):]
    if not name.startswith(prefix) or len(suffix) != 32 or any(c not in b"0123456789abcdef" for c in suffix):
        raise ValueError("OMAP owned object is not a fixture UUID name")
    return name


def execute(native, io, metadata, request, mode, out):
    native.lib.rados_ioctx_set_namespace(io, xattr.identity(request["namespace"], "namespace", empty=True))
    result = {"schema_version": 1, "source_commit": SOURCE_COMMIT, "mode": mode,
              "fixture": {k: metadata[k] for k in ("pool", "pool_id", "object")}, "namespace": request["namespace"],
              "scope": {"independent_native_c_api": True, "raw_wire_payload_available": False,
                        "header_verified": False, "comparison_verified": False,
                        "product_writes_verified": False, "pagination_nul_cursors_supported": False,
                        "inventory_empty_key_via_bykeys2": True}}
    if mode == "cleanup":
        name = check_ownership(read_json(out / "osd-omap-owned.json"), metadata, request["namespace"])
        code = native.lib.rados_remove(io, name)
        result.update(object=name.decode(), cleanup={"code": code, "object_removed": code in (0, -2)})
        native.check(code if code != -2 else 0)
        return result
    owned = mode != "observe"
    name = owned_name(metadata) if owned else xattr.identity(request["object"], "object")
    keys = [k for k, _ in request["entries"]] if owned else request["inventory_keys"]
    result["object"] = name.decode()
    created = False
    try:
        if owned:
            # If submission throws or returns a transport failure, creation
            # may already have happened. Repair this unique owned candidate;
            # never replay the seed. An explicit EEXIST is not ours to remove.
            created = True
            seed_code = native.seed(io, name, request)
            result["seed_aggregate"] = seed_code
            if seed_code == -17:
                created = False
            native.check(seed_code)
        result["before"] = native.state(io, name, keys)
        if owned:
            state = result["before"]
            expected = [{"key": xattr.encode(k), "value": xattr.encode(v)} for k, v in request["entries"]]
            if (state["code"] != 0 or state["data"] != xattr.encode(request["baseline"]) or state["version"] <= 0
                    or state["seconds"] != 1700000000 or state["nanos"] != 123456789
                    or state["attributes"] != [{"name": xattr.encode(k), "value": xattr.encode(v)} for k, v in request["attributes"]]
                    or state["omap"]["operations"][1]["entries"] != expected):
                raise AssertionError("native OMAP seed bytes/attrs/keys/version/mtime differ")
        if mode == "prepare":
            publish(out, "osd-omap-owned", ownership(metadata, request, name))
            created = False  # ownership transferred only after atomic receipt
            result["cleanup"] = {"object_removed": False, "prepared_for_explicit_reads": True}
            return result
        result["vector"] = native.read_vector(io, name, request["operations"])
        result["after"] = native.state(io, name, keys)
        if result["after"] != result["before"]:
            raise AssertionError("native read vector changed bytes/attrs/OMAP/version/mtime")
        return result
    finally:
        if created:
            code = native.lib.rados_remove(io, name)
            result["cleanup"] = {"object_removed": code in (0, -2), "code": code}
            if code not in (0, -2) and sys.exc_info()[0] is None:
                native.check(code)
        elif mode == "observe":
            result["cleanup"] = {"object_removed": False, "object_touched": False}


def collect(root, out, mode):
    request = validate_request(read_json(out / "osd-omap-input.json"), mode)
    metadata = read_json(out / "osd-oracle.json")
    for field in ("pool", "object"):
        xattr.identity(metadata[field], field)
    integer(metadata["pool_id"], (1 << 63) - 1, "fixture pool_id")
    with (out / "daemon-version").open("rb") as source:
        version = source.read(513).decode().strip()
    if len(version) > 512 or "20.2.4" not in version or SOURCE_COMMIT not in version:
        raise ValueError("OMAP fixture is not pinned Tentacle")
    if mode == "cleanup":
        check_ownership(read_json(out / "osd-omap-owned.json"), metadata, request["namespace"])
    native = Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        result = execute(native, io, metadata, request, mode, out)
        result["fixture_daemon_version"] = version
        publish(out, "osd-omap-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    modes = parser.add_mutually_exclusive_group(required=True)
    for mode in ("prepare", "compare", "observe", "cleanup"):
        modes.add_argument("--" + mode, dest="mode", action="store_const", const=mode)
    args = parser.parse_args()
    collect(args.root, args.out, args.mode)
