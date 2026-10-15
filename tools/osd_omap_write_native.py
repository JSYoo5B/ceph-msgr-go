#!/usr/bin/env python3
"""Independent test-only Tentacle OMAP header/write oracle, never product code.

Ceph 20.2.4 source 7f793731f1b39eb4f465e960113d2363c311b964:
src/include/rados/librados.h:2991,3107,3133,3149 and src/librados/librados_c.cc
(LGPL-2.1; header LGPL-2.1 or LGPL-3.0); src/tools/rados/rados.cc:2879-2943
(LGPL-2.1). This adapter copies no Ceph implementation and emits no
Go-generated wire data.

The pinned C API has no OMAP header get/set entrypoint. Header observations
therefore use the pinned rados CLI `getomapheader OBJECT FILE`, which writes the
raw bufferlist to a private file. CLI header reads are separate requests;
every state observation rechecks native stat2/version around them. Native C
write vectors use SET2/RM_KEYS2/CLEAR, whose per-op results are not exposed:
only aggregate results are native observations. OMAP_CMP uses the
length-aware read/write cmp2 builders, which expose per-op prval; each C call
is one CMP operation with exactly one assertion. Native write vectors pass
LIBRADOS_OPERATION_RETURNVEC like the Go client.

ROOT contains fixture ceph.conf/keyring. OUT contains pinned osd-oracle.json,
daemon-version and bounded osd-omap-write-input.json.
--prepare exclusively creates one UUID-suffixed owned object (baseline bytes,
attrs, OMAP entries; no header) and reserves separate absent UUID names that a
Go mutation may create. Ownership is published atomically before any Go write.
--observe records bytes/stat2/version/attrs/OMAP/header for one owned name
without writes. --assert executes ONE read-only vector of single-assertion
CMP operations on one owned name and records per-op prvals and unchanged state. --compare seeds a separate native mirror (or leaves it absent),
executes ONE native C write vector with explicit mtime, observes before/after
and removes the mirror. --cleanup removes every owned name. No native mutation
is retried; unit adaptation tests are not daemon proof.
"""
import argparse
import ctypes as C
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

import osd_omap_native as omap
import osd_xattr_native as xattr

SOURCE_COMMIT = xattr.SOURCE_COMMIT
MAX_INPUT = 1 << 20
MAX_OUTPUT = 512 << 10
MAX_BINARY = 128 << 10
MAX_KEY = 4096
MAX_ENTRIES = 128
MAX_HEADER = 64 << 10
MAX_RESERVED = 8
MAX_OPERATIONS = 16
MAX_CLI_OUTPUT = 16 << 10
OWNED = "osd-omap-write-owned"
WRITE_OPS = {"set", "remove", "clear", "cmp", "rmrange", "call", "allochint"}
MAX_OBJECT_INFO = 64 << 10
MAX_CLASS_NAME = 255
MAX_CALL_OUTPUT = 1 << 20
RETURNVEC = 1024  # librados.h:137 LIBRADOS_OPERATION_RETURNVEC


def binary(text, limit=MAX_BINARY):
    return xattr.binary(text, limit)


def entries(values, label):
    if not isinstance(values, list) or len(values) > MAX_ENTRIES:
        raise ValueError("invalid " + label + " count")
    result = []
    for item in values:
        if not isinstance(item, dict) or set(item) != {"key", "value"}:
            raise ValueError("invalid " + label + " entry")
        result.append((binary(item["key"], MAX_KEY), binary(item["value"])))
    if len({k for k, _ in result}) != len(result):
        raise ValueError("duplicate " + label + " key")
    return sorted(result)


def keys(values, label):
    if not isinstance(values, list) or len(values) > MAX_ENTRIES:
        raise ValueError("invalid " + label + " count")
    result = [binary(k, MAX_KEY) for k in values]
    if len(set(result)) != len(result):
        raise ValueError("duplicate " + label)
    return sorted(result)


def class_name(text):
    if not isinstance(text, str) or not 0 < len(text.encode()) <= MAX_CLASS_NAME or "\0" in text:
        raise ValueError("invalid native class or method name")
    return text.encode()


def seed(raw):
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) - {"baseline", "attributes", "entries"}:
        raise ValueError("invalid seed fields")
    attrs = []
    for item in raw.get("attributes", []):
        if not isinstance(item, dict) or set(item) != {"name", "value"}:
            raise ValueError("invalid seed attribute")
        attrs.append((xattr.checked_name(binary(item["name"], MAX_KEY)), binary(item["value"])))
    if len(attrs) > xattr.MAX_OPERATIONS or len({k for k, _ in attrs}) != len(attrs):
        raise ValueError("invalid seed attribute inventory")
    return {"baseline": binary(raw.get("baseline", "")), "attributes": sorted(attrs),
            "entries": entries(raw.get("entries", []), "seed entry")}


def operations(raw, allowed=WRITE_OPS):
    if not isinstance(raw, list) or not 0 < len(raw) <= MAX_OPERATIONS:
        raise ValueError("invalid native write vector count")
    result = []
    for action in raw:
        if not isinstance(action, dict) or action.get("op") not in allowed:
            raise ValueError("unsupported native OMAP write operation")
        admitted = {"op", "flags", {"set": "entries", "remove": "keys", "clear": "flags", "cmp": "key", "rmrange": "begin",
                                    "call": "class", "allochint": "expected_object_size"}[action["op"]]}
        if action["op"] == "allochint":
            admitted |= {"expected_write_size", "hint_flags"}
        if action["op"] == "call":
            admitted |= {"method", "input"}
        if action["op"] == "rmrange":
            admitted.add("end")
        if action["op"] == "cmp":
            admitted |= {"value", "comparison"}
        if set(action) - admitted:
            raise ValueError("unknown native OMAP write fields")
        flags = action.get("flags", 0)
        if type(flags) is not int or flags not in (0, 2):
            raise ValueError("only FAILOK flag is admitted")
        decoded = {"op": action["op"], "flags": flags}
        if action["op"] == "set":
            decoded["entries"] = entries(action.get("entries", []), "set entry")
        elif action["op"] == "remove":
            decoded["keys"] = keys(action.get("keys", []), "removal key")
        elif action["op"] == "allochint":
            for field, limit in (("expected_object_size", (1 << 64) - 1), ("expected_write_size", (1 << 64) - 1), ("hint_flags", (1 << 32) - 1)):
                value = action.get(field, 0)
                if type(value) is not int or not 0 <= value <= limit:
                    raise ValueError("invalid native allocation hint " + field)
                decoded[field] = value
        elif action["op"] == "call":
            decoded.update(cls=class_name(action.get("class")), method=class_name(action.get("method")),
                           input=binary(action.get("input", "")))
        elif action["op"] == "allochint":
            for field, limit in (("expected_object_size", (1 << 64) - 1), ("expected_write_size", (1 << 64) - 1), ("hint_flags", (1 << 32) - 1)):
                value = action.get(field, 0)
                if type(value) is not int or not 0 <= value <= limit:
                    raise ValueError("invalid native allocation hint " + field)
                decoded[field] = value
        elif action["op"] == "call":
            decoded.update(cls=class_name(action.get("class")), method=class_name(action.get("method")),
                           input=binary(action.get("input", "")))
        elif action["op"] == "rmrange":
            # Reversed ranges stay expressible here to characterize the
            # server; the Go product refuses them before transmission.
            decoded.update(begin=binary(action.get("begin", ""), MAX_KEY), end=binary(action.get("end", ""), MAX_KEY))
        elif action["op"] == "cmp":
            # The C builder takes uint8; unsupported identifiers stay
            # expressible here to observe the server, never in the Go product.
            comparison = action.get("comparison")
            if type(comparison) is not int or not 0 <= comparison <= 255:
                raise ValueError("invalid native OMAP comparison identifier")
            decoded.update(key=binary(action.get("key", ""), MAX_KEY), value=binary(action.get("value", "")),
                           comparison=comparison)
        result.append(decoded)
    return result


def validate_request(raw, mode):
    allowed = {"prepare": {"seed", "reserve"}, "observe": {"object", "inventory_keys", "object_info"},
               "exec": {"seed", "object", "role", "operations", "inventory_keys"},
               "assert": {"object", "operations", "inventory_keys"},
               "compare": {"seed", "operations", "mtime", "inventory_keys", "object_info"}, "cleanup": set()}
    if mode not in allowed:
        raise ValueError("invalid OMAP write oracle mode")
    if not isinstance(raw, dict) or set(raw) - allowed[mode]:
        raise ValueError("unknown OMAP write oracle input fields")
    request = {"inventory_keys": keys(raw.get("inventory_keys", []), "inventory key")}
    if type(raw.get("object_info", False)) is not bool:
        raise ValueError("object_info must be a boolean")
    request["object_info"] = raw.get("object_info", False)
    if mode == "prepare":
        request["seed"] = seed(raw.get("seed", {}))
        reserve = raw.get("reserve", 0)
        if type(reserve) is not int or not 0 <= reserve <= MAX_RESERVED:
            raise ValueError("invalid absent-name reservation count")
        request["reserve"] = reserve
    elif mode == "observe":
        request["object"] = xattr.identity(raw.get("object"), "object")
    elif mode == "exec":
        # ONE read-op CALL, on an owned object or on a fresh seeded/absent mirror.
        request["operations"] = operations(raw.get("operations"), {"call"})
        if len(request["operations"]) != 1:
            raise ValueError("exec runs exactly one CALL")
        request["role"] = raw.get("role", "test")
        if request["role"] not in xattr.CLIENT_ROLES:
            raise ValueError("unsupported fixture identity")
        if "object" in raw:
            if "seed" in raw:
                raise ValueError("exec targets either an owned object or a mirror")
            request["object"] = xattr.identity(raw["object"], "object")
        else:
            if request["role"] != "test":
                raise ValueError("a restricted identity cannot create a mirror")
            request["seed"] = seed(raw.get("seed"))
    elif mode == "assert":
        request["object"] = xattr.identity(raw.get("object"), "object")
        request["operations"] = operations(raw.get("operations"), {"cmp"})
    elif mode == "compare":
        request["seed"] = seed(raw.get("seed"))
        request["operations"] = operations(raw.get("operations"))
        mtime = raw.get("mtime")
        if (not isinstance(mtime, dict) or set(mtime) != {"seconds", "nanos"} or type(mtime["seconds"]) is not int
                or type(mtime["nanos"]) is not int or not 0 <= mtime["seconds"] < 1 << 32 or not 0 <= mtime["nanos"] < 10**9):
            raise ValueError("compare needs an explicit valid mtime")
        request["mtime"] = mtime
    total = sum(len(k) for k in request["inventory_keys"])
    for source in (request.get("seed"),):
        if source:
            total += len(source["baseline"]) + sum(len(k) + len(v) for k, v in source["entries"] + source["attributes"])
    for action in request.get("operations", []):
        total += sum(len(k) + len(v) for k, v in action.get("entries", [])) + sum(map(len, action.get("keys", [])))
        total += len(action.get("key", b"")) + len(action.get("value", b""))
        total += len(action.get("begin", b"")) + len(action.get("end", b"")) + len(action.get("input", b""))
    if total > MAX_BINARY:
        raise ValueError("aggregate OMAP write oracle input exceeds 128 KiB")
    return request


def publish(out, name, result):
    data = bytearray()
    for chunk in json.JSONEncoder().iterencode(result):
        encoded = chunk.encode()
        if len(data) + len(encoded) + 1 > MAX_OUTPUT:
            raise ValueError("OMAP write JSON output exceeds 512 KiB")
        data.extend(encoded)
    temporary = out / (name + ".tmp")
    temporary.write_bytes(data + b"\n")
    os.replace(temporary, out / (name + ".json"))


class HeaderCLI:
    """Pinned rados CLI raw header reader with fixed fixture identity."""

    def __init__(self, root, metadata, namespace):
        self.base = ["rados", "--conf", str(root / "ceph.conf"), "--keyring", str(root / "keyring"),
                     "--name", "client.test", "--pool", metadata["pool"]]
        if namespace:
            self.base += ["--namespace", namespace]

    def get(self, name):
        with tempfile.TemporaryDirectory(prefix="omap-header-") as directory:
            path = Path(directory) / "header"
            result = subprocess.run(self.base + ["getomapheader", name.decode(), str(path)], stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            if result.returncode != 0 or not path.is_file():
                raise RuntimeError("native rados getomapheader failed: exit=%d %s" % (
                    result.returncode, result.stderr[-MAX_CLI_OUTPUT:].decode(errors="replace")))
            with path.open("rb") as source:
                header = source.read(MAX_HEADER + 1)
            if len(header) > MAX_HEADER:
                raise ValueError("native OMAP header exceeds oracle bound")
            return header


class Native(omap.Native):
    def __init__(self, library=None):
        super().__init__(library)
        p, s, char = C.c_void_p, C.c_size_t, C.c_char_p
        cmp2 = [p, char, C.c_uint8, char, s, s, C.POINTER(C.c_int)]
        for name, arguments in {"write_op_omap_rm_keys2": [p, C.POINTER(p), C.POINTER(s), s],
                                "write_op_omap_clear": [p], "write_op_omap_cmp2": cmp2,
                                "write_op_omap_rm_range2": [p, char, s, char, s],
                                "write_op_exec": [p, char, char, char, s, C.POINTER(C.c_int)],
                                "write_op_set_alloc_hint2": [p, C.c_uint64, C.c_uint64, C.c_uint32],
                                "read_op_exec": [p, char, char, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(C.c_int)],
                                "read_op_omap_cmp2": cmp2}.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, None
        self.lib.rados_buffer_free.argtypes, self.lib.rados_buffer_free.restype = [p], None

    def append_cmp(self, operation, kind, index, action, outputs, buffers):
        prval = C.c_int(999)
        # Register before construction so every prval is reported once.
        outputs.append((index, prval))
        buffers.append(prval)
        getattr(self.lib, "rados_" + kind + "_op_omap_cmp2")(
            operation, action["key"], action["comparison"], action["value"],
            len(action["key"]), len(action["value"]), C.byref(prval))
        if action["flags"]:
            getattr(self.lib, "rados_" + kind + "_op_set_flags")(operation, action["flags"])

    def assert_vector(self, io, name, actions):
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native OMAP comparison allocation failed")
        outputs, buffers = [], []
        try:
            for index, action in enumerate(actions):
                self.append_cmp(operation, "read", index, action, outputs, buffers)
            aggregate = self.lib.rados_read_op_operate(operation, io, name, 0)
            return {"aggregate": aggregate, "prvals": [{"index": i, "prval": v.value} for i, v in outputs]}
        finally:
            self.lib.rados_release_read_op(operation)

    def write_vector(self, io, name, actions, mtime):
        operation = self.lib.rados_create_write_op()
        if not operation:
            raise RuntimeError("native OMAP write allocation failed")
        buffers, outputs = [], []
        try:
            for index, action in enumerate(actions):
                if action["op"] == "cmp":
                    self.append_cmp(operation, "write", index, action, outputs, buffers)
                    continue
                if action["op"] == "call":
                    prval = C.c_int(999)
                    outputs.append((index, prval))
                    buffers.append(prval)
                    self.lib.rados_write_op_exec(operation, action["cls"], action["method"], action["input"],
                                                 len(action["input"]), C.byref(prval))
                    if action["flags"]:
                        self.lib.rados_write_op_set_flags(operation, action["flags"])
                    continue
                if action["op"] == "set":
                    k, kl, kb = omap.pointers([key for key, _ in action["entries"]])
                    v, vl, vb = omap.pointers([value for _, value in action["entries"]])
                    buffers.extend((k, kl, kb, v, vl, vb))
                    self.lib.rados_write_op_omap_set2(operation, k, v, kl, vl, len(action["entries"]))
                elif action["op"] == "remove":
                    k, kl, kb = omap.pointers(action["keys"])
                    buffers.extend((k, kl, kb))
                    self.lib.rados_write_op_omap_rm_keys2(operation, k, kl, len(action["keys"]))
                elif action["op"] == "allochint":
                    # The pinned builder adds per-op FAILOK (Objecter.h:1640).
                    self.lib.rados_write_op_set_alloc_hint2(operation, action["expected_object_size"],
                                                            action["expected_write_size"], action["hint_flags"])
                elif action["op"] == "rmrange":
                    self.lib.rados_write_op_omap_rm_range2(operation, action["begin"], len(action["begin"]),
                                                           action["end"], len(action["end"]))
                else:
                    self.lib.rados_write_op_omap_clear(operation)
                if action["flags"]:
                    self.lib.rados_write_op_set_flags(operation, action["flags"])
            stamp = xattr.Timespec(mtime["seconds"], mtime["nanos"])
            aggregate = self.lib.rados_write_op_operate2(operation, io, name, C.byref(stamp), RETURNVEC)
            return aggregate, [{"index": i, "prval": v.value} for i, v in outputs]
        finally:
            self.lib.rados_release_write_op(operation)

    def exec_vector(self, io, name, action):
        """One read-op CALL with RETURNVEC: the OSD classifies the method."""
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native CALL allocation failed")
        out, length, prval = C.c_void_p(), C.c_size_t(), C.c_int(999)
        try:
            self.lib.rados_read_op_exec(operation, action["cls"], action["method"], action["input"], len(action["input"]),
                                        C.byref(out), C.byref(length), C.byref(prval))
            if action["flags"]:
                self.lib.rados_read_op_set_flags(operation, action["flags"])
            aggregate = self.lib.rados_read_op_operate(operation, io, name, RETURNVEC)
            if length.value > MAX_CALL_OUTPUT:
                raise ValueError("native CALL output exceeds oracle bound")
            data = C.string_at(out.value, length.value) if out.value and length.value else b""
            return {"aggregate": aggregate, "prvals": [{"index": 0, "prval": prval.value}], "output": xattr.encode(data)}
        finally:
            if out.value:
                self.lib.rados_buffer_free(out)
            self.lib.rados_release_read_op(operation)

    def object_info(self, io, name):
        """Committed object_info via the empty xattr name, which the OSD maps
        to OI_ATTR "_" (PrimaryLogPG.cc:6429), decoded by pinned ceph-dencoder."""
        buffer = C.create_string_buffer(MAX_OBJECT_INFO + 1)
        length = self.lib.rados_getxattr(io, name, b"", buffer, MAX_OBJECT_INFO + 1)
        self.check(length)
        if length > MAX_OBJECT_INFO:
            raise ValueError("native object_info exceeds oracle bound")
        with tempfile.TemporaryDirectory(prefix="object-info-") as directory:
            path = Path(directory) / "oi"
            path.write_bytes(buffer.raw[:length])
            result = subprocess.run(["ceph-dencoder", "type", "object_info_t", "import", str(path), "decode", "dump_json"],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if result.returncode != 0 or len(result.stdout) > MAX_OBJECT_INFO:
            raise RuntimeError("ceph-dencoder object_info_t failed: " + result.stderr[-MAX_CLI_OUTPUT:].decode(errors="replace"))
        decoded = json.loads(result.stdout)
        return {field: decoded[field] for field in ("expected_object_size", "expected_write_size", "alloc_hint_flags", "size", "user_version")}

    def full_state(self, io, name, inventory_keys, cli):
        length, mtime = C.c_uint64(), xattr.Timespec()
        code = self.lib.rados_stat2(io, name, C.byref(length), C.byref(mtime))
        if code == -2:
            return {"code": -2, "version": 0, "data": "", "attributes": None, "gets": [], "omap": None, "header": None}
        self.check(code)
        result = self.state(io, name, inventory_keys)
        result["header"] = xattr.encode(cli.get(name))
        length, mtime = C.c_uint64(), xattr.Timespec()
        self.check(self.lib.rados_stat2(io, name, C.byref(length), C.byref(mtime)))
        if (self.lib.rados_get_last_version(io) != result["version"] or length.value != len(binary(result["data"], xattr.MAX_VALUE))
                or mtime.tv_sec != result["seconds"] or mtime.tv_nsec != result["nanos"]):
            raise ValueError("native state changed during separate CLI header observation")
        return result


def create_seeded(native, io, name, source):
    operation = native.lib.rados_create_write_op()
    if not operation:
        raise RuntimeError("native OMAP seed allocation failed")
    try:
        native.lib.rados_write_op_create(operation, 1, None)  # exclusive
        baseline = C.create_string_buffer(source["baseline"], len(source["baseline"]) + 1)
        native.lib.rados_write_op_write_full(operation, baseline, len(source["baseline"]))
        _, keep = native.append(operation, "write", [{"op": "set", "name": k, "value": v, "flags": 0} for k, v in source["attributes"]])
        k, kl, kb = omap.pointers([key for key, _ in source["entries"]])
        v, vl, vb = omap.pointers([value for _, value in source["entries"]])
        native.lib.rados_write_op_omap_set2(operation, k, v, kl, vl, len(source["entries"]))
        stamp = xattr.Timespec(1700000000, 123456789)
        return native.lib.rados_write_op_operate2(operation, io, name, C.byref(stamp), 0)
    finally:
        native.lib.rados_release_write_op(operation)


def owned_name(metadata, label):
    name = xattr.identity(metadata["object"], "fixture object") + b"-native-omapw-" + label + uuid.uuid4().hex.encode()
    xattr.identity(name.decode(), "owned object")
    return name


def check_ownership(receipt, metadata):
    if (not isinstance(receipt, dict) or receipt.get("schema_version") != 1 or receipt.get("source_commit") != SOURCE_COMMIT
            or any(receipt.get(k) != metadata[k] for k in ("pool", "pool_id")) or receipt.get("fixture_object") != metadata["object"]
            or receipt.get("namespace") != ""):
        raise ValueError("invalid OMAP write ownership provenance or scope")
    names = receipt.get("objects")
    if not isinstance(names, list) or not 1 <= len(names) <= MAX_RESERVED + 1:
        raise ValueError("invalid OMAP write ownership set")
    prefix = xattr.identity(metadata["object"], "fixture object") + b"-native-omapw-"
    result = []
    for index, text in enumerate(names):
        name = xattr.identity(text, "owned object")
        label = b"main-" if index == 0 else b"absent-"
        suffix = name[len(prefix + label):]
        if not name.startswith(prefix + label) or len(suffix) != 32 or any(c not in b"0123456789abcdef" for c in suffix):
            raise ValueError("OMAP write owned name is not a fixture UUID name")
        result.append(name)
    if len(set(result)) != len(result):
        raise ValueError("duplicate OMAP write owned name")
    return result


def execute(native, io, metadata, request, mode, out, cli):
    result = {"schema_version": 1, "source_commit": SOURCE_COMMIT, "mode": mode,
              "fixture": {k: metadata[k] for k in ("pool", "pool_id", "object")},
              "scope": {"independent_native_c_api": True, "header_via_pinned_rados_cli": True,
                        "raw_wire_payload_available": False, "native_write_per_op_results_available": False,
                        "native_cmp_per_op_prval_available": True}}
    if mode == "cleanup":
        codes = []
        for name in check_ownership(omap.read_json(out / (OWNED + ".json")), metadata):
            codes.append({"object": name.decode(), "code": native.lib.rados_remove(io, name)})
        result["cleanup"] = {"objects": codes, "objects_removed": all(c["code"] in (0, -2) for c in codes)}
        for entry in codes:
            native.check(entry["code"] if entry["code"] != -2 else 0)
        return result
    if mode in ("observe", "assert"):
        owned = check_ownership(omap.read_json(out / (OWNED + ".json")), metadata)
        if request["object"] not in owned:
            raise ValueError("observation is limited to owned OMAP write objects")
        result["object"] = request["object"].decode()
        result["state"] = native.full_state(io, request["object"], request["inventory_keys"], cli)
        if request["object_info"] and result["state"]["code"] == 0:
            result["object_info"] = native.object_info(io, request["object"])
        if mode == "assert":
            result["vector"] = native.assert_vector(io, request["object"], request["operations"])
            result["after"] = native.full_state(io, request["object"], request["inventory_keys"], cli)
            if result["after"] != result["state"]:
                raise AssertionError("native read-only OMAP comparison changed object state")
        return result
    if mode == "prepare":
        main = owned_name(metadata, b"main-")
        absent = [owned_name(metadata, b"absent-") for _ in range(request["reserve"])]
        for name in absent:
            length, mtime = C.c_uint64(), xattr.Timespec()
            if native.lib.rados_stat2(io, name, C.byref(length), C.byref(mtime)) != -2:
                raise RuntimeError("reserved UUID name unexpectedly exists")
        created = True
        try:
            code = create_seeded(native, io, main, request["seed"])
            result["seed_aggregate"] = code
            if code == -17:
                created = False
            native.check(code)
            result["state"] = native.full_state(io, main, [k for k, _ in request["seed"]["entries"]], cli)
            publish(out, OWNED, {"schema_version": 1, "source_commit": SOURCE_COMMIT, "pool": metadata["pool"],
                                 "pool_id": metadata["pool_id"], "fixture_object": metadata["object"], "namespace": "",
                                 "objects": [n.decode() for n in [main] + absent]})
            created = False  # ownership transferred after the atomic receipt
        finally:
            if created:
                native.lib.rados_remove(io, main)
        result["objects"] = [n.decode() for n in [main] + absent]
        return result
    if mode == "exec":
        action = request["operations"][0]
        if "object" in request:
            owned = check_ownership(omap.read_json(out / (OWNED + ".json")), metadata)
            if request["object"] not in owned:
                raise ValueError("exec is limited to owned objects")
            result["object"] = request["object"].decode()
            result["vector"] = native.exec_vector(io, request["object"], action)
            return result
        mirror = owned_name(metadata, b"mirror-")
        created = False
        try:
            if request["seed"] is not None:
                created = True
                code = create_seeded(native, io, mirror, request["seed"])
                result["seed_aggregate"] = code
                if code == -17:
                    created = False
                native.check(code)
            result["object"] = mirror.decode()
            result["before"] = native.full_state(io, mirror, request["inventory_keys"], cli)
            created = True  # a write-classified method may create the mirror
            result["vector"] = native.exec_vector(io, mirror, action)
            result["after"] = native.full_state(io, mirror, request["inventory_keys"], cli)
            return result
        finally:
            if created:
                code = native.lib.rados_remove(io, mirror)
                result["cleanup"] = {"mirror_removed": code in (0, -2), "code": code}
                if code not in (0, -2) and sys.exc_info()[0] is None:
                    native.check(code)
    # compare: one disposable native mirror around one C write vector.
    mirror = owned_name(metadata, b"mirror-")
    created = False
    try:
        if request["seed"] is not None:
            created = True
            code = create_seeded(native, io, mirror, request["seed"])
            result["seed_aggregate"] = code
            if code == -17:
                created = False
            native.check(code)
        result["object"] = mirror.decode()
        result["before"] = native.full_state(io, mirror, request["inventory_keys"], cli)
        created = True  # a write vector may create an absent mirror
        result["aggregate"], result["prvals"] = native.write_vector(io, mirror, request["operations"], request["mtime"])
        result["after"] = native.full_state(io, mirror, request["inventory_keys"], cli)
        if request["object_info"] and result["after"]["code"] == 0:
            result["object_info"] = native.object_info(io, mirror)
        return result
    finally:
        if created:
            code = native.lib.rados_remove(io, mirror)
            result["cleanup"] = {"mirror_removed": code in (0, -2), "code": code}
            if code not in (0, -2) and sys.exc_info()[0] is None:
                native.check(code)


def collect(root, out, mode):
    request = validate_request(omap.read_json(out / "osd-omap-write-input.json"), mode)
    metadata = omap.read_json(out / "osd-oracle.json")
    for field in ("pool", "object"):
        xattr.identity(metadata[field], field)
    omap.integer(metadata["pool_id"], (1 << 63) - 1, "fixture pool_id")
    with (out / "daemon-version").open("rb") as source:
        version = source.read(513).decode().strip()
    if len(version) > 512 or "20.2.4" not in version or SOURCE_COMMIT not in version:
        raise ValueError("OMAP write fixture is not pinned Tentacle")
    if mode != "prepare":
        check_ownership(omap.read_json(out / (OWNED + ".json")), metadata)
    native = Native()
    cli = HeaderCLI(root, metadata, "")
    with xattr.client_connection(native, root, metadata, request.get("role", "test")) as io:
        result = execute(native, io, metadata, request, mode, out, cli)
        result["fixture_daemon_version"] = version
        publish(out, "osd-omap-write-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    modes = parser.add_mutually_exclusive_group(required=True)
    for mode in ("prepare", "observe", "assert", "exec", "compare", "cleanup"):
        modes.add_argument("--" + mode, dest="mode", action="store_const", const=mode)
    args = parser.parse_args()
    collect(args.root, args.out, args.mode)
