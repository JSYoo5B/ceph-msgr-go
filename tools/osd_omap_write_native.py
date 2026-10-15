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

Self-managed snapshot modes (librados.h:1358-1426, librados_c.cc:1278-1306 and
1796-1857, IoCtxImpl.cc:253-272/388-481/646-654; rados.cc:3628-3755 for the
CLI listing; OSDMonitor.cc:291-345/14649-14668 for the MON cap gate):
--snapcreate allocates self-managed snap ids through the MON as client.test
and records each id in a separate atomic receipt before returning it; the
first id puts the shared fixture pool into SELFMANAGED_SNAPS mode for good.
--snapwrite executes ONE native C write vector on an owned name under an
explicit set_write_ctx SnapContext (caller order kept; librados validates it
locally and its code is reported) with optional ORDERSNAP. --snapread
executes ONE compound read (stat2, read, getxattrs, OMAP vals/bykeys) with
rados_ioctx_snap_set_read and always restores LIBRADOS_SNAP_HEAD. The C API
has no OMAP header read and the CLI cannot select a self-managed snap, so no
header is observed at a snap. --snaprollback runs the C self-managed rollback
under an explicit SnapContext; librados stamps mtime with its own clock.
--listsnaps runs the pinned `rados listsnaps --format json` CLI (no C clone
listing exists) and/or decodes caller-supplied raw obj_list_snap_response_t
bytes with ceph-dencoder. --cleanup removes owned heads first, then every
owned snap id with selfmanaged_snap_remove.
"""
import argparse
import ctypes as C
import json
import os
from pathlib import Path
import re
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
ORDERSNAP = 512  # librados.h:135 LIBRADOS_OPERATION_ORDERSNAP
SNAP_WRITE_OPS = WRITE_OPS | {"writefull", "write", "setxattr", "rmxattr", "delete"}
SNAP_HEAD = (1 << 64) - 2  # librados.h:171 LIBRADOS_SNAP_HEAD
MAX_SNAP_ID = (1 << 64) - 3  # CEPH_MAXSNAP
MAX_SNAPC = 64
MAX_OWNED_SNAPS = 32
MAX_SNAP_CREATE = 4
MAX_SNAP_JSON = 64 << 10
SNAPS = "osd-omap-write-snaps"
SNAP_MODES = ("snapcreate", "snapwrite", "snapread", "snaprollback", "listsnaps")
OBJECT_INFO_FIELDS = ("oid", "version", "prior_version", "user_version", "size", "mtime", "flags",
                      "expected_object_size", "expected_write_size", "alloc_hint_flags")


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
                                    "call": "class", "allochint": "expected_object_size", "writefull": "data",
                                    "write": "data", "setxattr": "name", "rmxattr": "name", "delete": "flags"}[action["op"]]}
        if action["op"] == "write":
            admitted.add("offset")
        if action["op"] == "setxattr":
            admitted.add("value")
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
        elif action["op"] in ("writefull", "write"):
            decoded["data"] = binary(action.get("data", ""))
            if action["op"] == "write":
                offset = action.get("offset", 0)
                if type(offset) is not int or not 0 <= offset < 1 << 64:
                    raise ValueError("invalid native write offset")
                decoded["offset"] = offset
        elif action["op"] in ("setxattr", "rmxattr"):
            # The C builders take NUL-terminated names; the value keeps its length.
            decoded["name"] = xattr.checked_name(binary(action.get("name"), MAX_KEY))
            if action["op"] == "setxattr":
                decoded["value"] = binary(action.get("value", ""))
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


def explicit_mtime(mtime, label):
    if (not isinstance(mtime, dict) or set(mtime) != {"seconds", "nanos"} or type(mtime["seconds"]) is not int
            or type(mtime["nanos"]) is not int or not 0 <= mtime["seconds"] < 1 << 32 or not 0 <= mtime["nanos"] < 10**9):
        raise ValueError(label + " needs an explicit valid mtime")
    return mtime


def snap_id(value, label, minimum=1):
    if type(value) is not int or not minimum <= value < 1 << 64:
        raise ValueError("invalid " + label + " snap id")
    return value


def snap_context(raw):
    if not isinstance(raw, dict) or set(raw) != {"seq", "snaps"}:
        raise ValueError("snapc needs exactly seq and snaps")
    snaps = raw["snaps"]
    if not isinstance(snaps, list) or len(snaps) > MAX_SNAPC:
        raise ValueError("invalid snapc snap count")
    # Caller order and seq bounds are NOT normalized: librados set_write_ctx
    # validates SnapContext locally and its exact code is the observation.
    return {"seq": snap_id(raw["seq"], "snapc seq", 0), "snaps": [snap_id(s, "snapc", 0) for s in snaps]}


def validate_request(raw, mode):
    allowed = {"prepare": {"seed", "reserve"}, "observe": {"object", "inventory_keys", "object_info"},
               "exec": {"seed", "object", "role", "operations", "inventory_keys"},
               "assert": {"object", "operations", "inventory_keys"},
               "compare": {"seed", "operations", "mtime", "inventory_keys", "object_info"}, "cleanup": set(),
               "snapcreate": {"count"},
               "snapwrite": {"object", "snapc", "ordersnap", "operations", "mtime", "inventory_keys", "object_info"},
               "snapread": {"object", "snap", "inventory_keys", "object_info"},
               "snaprollback": {"object", "snap", "snapc", "inventory_keys", "object_info"},
               "listsnaps": {"object", "raw"}}
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
        request["mtime"] = explicit_mtime(raw.get("mtime"), "compare")
    elif mode == "snapcreate":
        count = raw.get("count", 1)
        if type(count) is not int or not 1 <= count <= MAX_SNAP_CREATE:
            raise ValueError("invalid self-managed snap allocation count")
        request["count"] = count
    elif mode in ("snapwrite", "snapread", "snaprollback"):
        request["object"] = xattr.identity(raw.get("object"), "object")
        if mode != "snapread":
            request["snapc"] = snap_context(raw.get("snapc"))
        if mode == "snapwrite":
            request["operations"] = operations(raw.get("operations"), SNAP_WRITE_OPS)
            request["mtime"] = explicit_mtime(raw.get("mtime"), "snapwrite")
            if type(raw.get("ordersnap", False)) is not bool:
                raise ValueError("ordersnap must be a boolean")
            request["ordersnap"] = raw.get("ordersnap", False)
        elif mode == "snapread":
            # Zero is rejected: librados silently maps it to the head.
            request["snap"] = snap_id(raw.get("snap"), "read")
        else:
            # Zero stays expressible to characterize the server natively.
            request["snap"] = snap_id(raw.get("snap"), "rollback", 0)
    elif mode == "listsnaps":
        if "object" not in raw and "raw" not in raw:
            raise ValueError("listsnaps needs an owned object, raw response bytes, or both")
        if "object" in raw:
            request["object"] = xattr.identity(raw["object"], "object")
        if "raw" in raw:
            request["raw"] = binary(raw["raw"])
    total = sum(len(k) for k in request["inventory_keys"]) + len(request.get("raw", b""))
    for source in (request.get("seed"),):
        if source:
            total += len(source["baseline"]) + sum(len(k) + len(v) for k, v in source["entries"] + source["attributes"])
    for action in request.get("operations", []):
        total += sum(len(k) + len(v) for k, v in action.get("entries", [])) + sum(map(len, action.get("keys", [])))
        total += len(action.get("key", b"")) + len(action.get("value", b""))
        total += len(action.get("begin", b"")) + len(action.get("end", b"")) + len(action.get("input", b""))
        total += len(action.get("data", b"")) + len(action.get("name", b""))
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

    def listsnaps(self, name):
        """Pinned CLI clone listing (rados.cc:3628-3755). A refusal is kept as
        the exit status plus the strerror text's errno, never retried."""
        result = subprocess.run(self.base + ["listsnaps", name.decode(), "--format", "json"], stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        return tool_result(result, "native rados listsnaps")


def tool_result(result, label):
    if len(result.stdout) > MAX_SNAP_JSON:
        raise ValueError(label + " output exceeds oracle bound")
    stderr = result.stderr[-MAX_CLI_OUTPUT:].decode(errors="replace")
    if result.returncode != 0:
        # rados/ceph-dencoder report cpp_strerror text such as "(2) No such file".
        errno = re.search(r"\((\d+)\) ", stderr)
        return {"exit": result.returncode, "code": -int(errno.group(1)) if errno else None, "json": None, "stderr": stderr}
    return {"exit": 0, "code": 0, "json": json.loads(result.stdout), "stderr": ""}


def decode_snap_list(raw):
    """Pinned ceph-dencoder decode of caller-supplied LIST_SNAPS output bytes."""
    with tempfile.TemporaryDirectory(prefix="list-snaps-") as directory:
        path = Path(directory) / "response"
        path.write_bytes(raw)
        result = subprocess.run(["ceph-dencoder", "type", "obj_list_snap_response_t", "import", str(path), "decode", "dump_json"],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    return tool_result(result, "ceph-dencoder obj_list_snap_response_t")


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
        u64, i = C.c_uint64, C.c_int
        for name, (arguments, result) in {
                "ioctx_snap_set_read": ([p, u64], None),
                "ioctx_selfmanaged_snap_create": ([p, C.POINTER(u64)], i),
                "ioctx_selfmanaged_snap_remove": ([p, u64], i),
                "ioctx_selfmanaged_snap_rollback": ([p, char, u64], i),
                "ioctx_selfmanaged_snap_set_write_ctx": ([p, u64, C.POINTER(u64), i], i),
                "write_op_write": ([p, p, s, u64], None), "write_op_remove": ([p], None),
                "read_op_stat2": ([p, C.POINTER(u64), C.POINTER(xattr.Timespec), C.POINTER(i)], None),
                "read_op_read": ([p, u64, s, p, C.POINTER(s), C.POINTER(i)], None)}.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, result

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

    def write_vector(self, io, name, actions, mtime, flags=RETURNVEC):
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
                elif action["op"] in ("writefull", "write", "setxattr"):
                    payload = action["value"] if action["op"] == "setxattr" else action["data"]
                    buffer = C.create_string_buffer(payload, len(payload) + 1)
                    buffers.append(buffer)
                    if action["op"] == "writefull":
                        self.lib.rados_write_op_write_full(operation, buffer, len(payload))
                    elif action["op"] == "write":
                        self.lib.rados_write_op_write(operation, buffer, len(payload), action["offset"])
                    else:
                        self.lib.rados_write_op_setxattr(operation, action["name"], buffer, len(payload))
                elif action["op"] == "rmxattr":
                    self.lib.rados_write_op_rmxattr(operation, action["name"])
                elif action["op"] == "delete":
                    self.lib.rados_write_op_remove(operation)
                elif action["op"] == "clear":
                    self.lib.rados_write_op_omap_clear(operation)
                else:
                    raise ValueError("unsupported native write operation")
                if action["flags"]:
                    self.lib.rados_write_op_set_flags(operation, action["flags"])
            stamp = xattr.Timespec(mtime["seconds"], mtime["nanos"])
            aggregate = self.lib.rados_write_op_operate2(operation, io, name, C.byref(stamp), flags)
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
        code, info = self.object_info_code(io, name)
        self.check(code)
        return info

    def object_info_code(self, io, name):
        """Committed object_info via the empty xattr name, which the OSD maps
        to OI_ATTR "_" (PrimaryLogPG.cc:6429), decoded by pinned ceph-dencoder.
        With a read snap set this is the clone's object_info (oid.snapid)."""
        buffer = C.create_string_buffer(MAX_OBJECT_INFO + 1)
        length = self.lib.rados_getxattr(io, name, b"", buffer, MAX_OBJECT_INFO + 1)
        if length < 0:
            return length, None
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
        return 0, {field: decoded[field] for field in OBJECT_INFO_FIELDS}

    def set_write_context(self, io, snapc):
        """Exact librados SnapContext admission (IoCtxImpl.cc:261-272)."""
        snaps = (C.c_uint64 * len(snapc["snaps"]))(*snapc["snaps"])
        return self.lib.rados_ioctx_selfmanaged_snap_set_write_ctx(io, snapc["seq"], snaps, len(snapc["snaps"]))

    def reset_write_context(self, io):
        self.check(self.set_write_context(io, {"seq": 0, "snaps": []}))

    def snap_read(self, io, name, snap, inventory_keys, object_info):
        """ONE compound read at hobj.snap=snap with per-op prvals; the ioctx
        read snap is always restored to the head afterwards."""
        self.lib.rados_ioctx_snap_set_read(io, snap)
        try:
            operation = self.lib.rados_create_read_op()
            if not operation:
                raise RuntimeError("native snapshot read allocation failed")
            size, mtime, stat_rval = C.c_uint64(), xattr.Timespec(), C.c_int(999)
            data, count, read_rval = C.create_string_buffer(MAX_BINARY + 1), C.c_size_t(), C.c_int(999)
            iterator, xattr_rval = C.c_void_p(), C.c_int(999)
            outputs, keep = [], []
            try:
                self.lib.rados_read_op_stat2(operation, C.byref(size), C.byref(mtime), C.byref(stat_rval))
                # One byte beyond the bound proves a complete bounded read.
                self.lib.rados_read_op_read(operation, 0, MAX_BINARY + 1, data, C.byref(count), C.byref(read_rval))
                self.lib.rados_read_op_getxattrs(operation, C.byref(iterator), C.byref(xattr_rval))
                self.append_reads(operation, [
                    {"op": "vals", "flags": 0, "start_after": b"", "prefix": b"", "max_return": MAX_ENTRIES},
                    {"op": "bykeys", "flags": 0, "keys": sorted(set(inventory_keys + [b""]))}], outputs, keep)
                aggregate = self.lib.rados_read_op_operate(operation, io, name, 0)
                version = self.lib.rados_get_last_version(io)
                ops = [{"index": 0, "op": "stat", "prval": stat_rval.value}, {"index": 1, "op": "read", "prval": read_rval.value},
                       {"index": 2, "op": "getxattrs", "prval": xattr_rval.value}]
                if aggregate >= 0 and stat_rval.value >= 0:
                    if size.value > MAX_BINARY or not 0 <= mtime.tv_nsec < 10**9:
                        raise ValueError("native snapshot object size/stat2 time exceeds oracle bound")
                    ops[0].update(size=size.value, seconds=mtime.tv_sec, nanos=mtime.tv_nsec)
                if aggregate >= 0 and read_rval.value >= 0:
                    if count.value > MAX_BINARY or stat_rval.value >= 0 and count.value != size.value:
                        raise ValueError("native snapshot read is unbounded or disagrees with stat2")
                    ops[1]["data"] = xattr.encode(data.raw[:count.value])
                if aggregate >= 0 and xattr_rval.value >= 0:
                    ops[2]["entries"] = self.entries(iterator)
                for index, op, omap_iterator, prval, more in outputs:
                    success = aggregate >= 0 and prval.value >= 0
                    if success and not omap_iterator.value:
                        raise ValueError("successful native OMAP action has no iterator")
                    if success and op != "bykeys" and more.value not in (0, 1):
                        raise ValueError("native OMAP more is not bool")
                    ops.append({"index": 3 + index, "op": "omap_" + op, "opcode": omap.READ_CODES[op], "prval": prval.value,
                                "more": bool(more.value) if success and op != "bykeys" else None,
                                "entries": self.omap_entries(omap_iterator, op == "keys") if success else None})
                if aggregate >= 0 and ops[3]["more"]:
                    raise ValueError("native snapshot OMAP inventory exceeds one bounded page")
            finally:
                if iterator.value:
                    self.lib.rados_getxattrs_end(iterator)
                for _, _, omap_iterator, _, _ in outputs:
                    if omap_iterator.value:
                        self.lib.rados_omap_get_end(omap_iterator)
                self.lib.rados_release_read_op(operation)
            result = {"aggregate": aggregate, "version": version, "operations": ops}
            if object_info:
                result["object_info_code"], result["object_info"] = self.object_info_code(io, name)
            return result
        finally:
            self.lib.rados_ioctx_snap_set_read(io, SNAP_HEAD)

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


def snap_receipt(metadata, snaps):
    return {"schema_version": 1, "source_commit": SOURCE_COMMIT, "pool": metadata["pool"], "pool_id": metadata["pool_id"],
            "fixture_object": metadata["object"], "namespace": "", "snaps": snaps}


def owned_snaps(out, metadata):
    """Self-managed snap ids this oracle allocated in the fixture pool. They
    accumulate across prepares until a cleanup removes them."""
    path = out / (SNAPS + ".json")
    if not path.exists():
        return []
    receipt = omap.read_json(path)
    snaps = receipt.get("snaps") if isinstance(receipt, dict) else None
    if (not isinstance(snaps, list) or receipt != snap_receipt(metadata, snaps) or len(snaps) > MAX_OWNED_SNAPS
            or any(type(s) is not int or not 1 <= s <= MAX_SNAP_ID for s in snaps) or len(set(snaps)) != len(snaps)):
        raise ValueError("invalid self-managed snapshot ownership provenance or scope")
    return list(snaps)


def publish_snaps(out, metadata, snaps):
    if snaps:
        publish(out, SNAPS, snap_receipt(metadata, snaps))
    else:
        (out / (SNAPS + ".json")).unlink(missing_ok=True)


def owned_target(out, metadata, request, label):
    if request["object"] not in check_ownership(omap.read_json(out / (OWNED + ".json")), metadata):
        raise ValueError(label + " is limited to owned objects")
    return request["object"]


def execute_snapshot(native, io, metadata, request, mode, out, cli, result):
    if mode == "snapcreate":
        snaps = owned_snaps(out, metadata)
        if len(snaps) + request["count"] > MAX_OWNED_SNAPS:
            raise ValueError("self-managed snapshot ownership bound reached; clean up first")
        result["snaps"] = []
        for _ in range(request["count"]):
            snap = C.c_uint64(0)
            code = native.lib.rados_ioctx_selfmanaged_snap_create(io, C.byref(snap))
            result["snaps"].append({"code": code, "snap": snap.value if code == 0 else None})
            if code != 0:
                break  # the exact MON refusal is the observation; no retry
            if not 1 <= snap.value <= MAX_SNAP_ID or snap.value in snaps:
                raise ValueError("MON returned an invalid or already-owned snap id")
            try:
                snaps.append(snap.value)
                publish_snaps(out, metadata, snaps)
            except BaseException:
                # Unrecorded ids are never kept: repair once, never retry.
                native.lib.rados_ioctx_selfmanaged_snap_remove(io, snap.value)
                raise
        result["owned_snaps"] = snaps
        return result
    if mode == "listsnaps":
        if "object" in request:
            result["object"] = owned_target(out, metadata, request, "listsnaps").decode()
            result["listsnaps"] = cli.listsnaps(request["object"])
        if "raw" in request:
            result["raw_length"] = len(request["raw"])
            result["decoded"] = decode_snap_list(request["raw"])
        return result
    name = owned_target(out, metadata, request, mode)
    result["object"] = name.decode()
    if mode == "snapread":
        result["snap"] = request["snap"]
        result["read"] = native.snap_read(io, name, request["snap"], request["inventory_keys"], request["object_info"])
        return result
    result["before"] = native.full_state(io, name, request["inventory_keys"], cli)
    code = native.set_write_context(io, request["snapc"])
    result["snapc"] = dict(request["snapc"], code=code)
    try:
        if mode == "snapwrite":
            flags = RETURNVEC | (ORDERSNAP if request["ordersnap"] else 0)
            result.update(ordersnap=request["ordersnap"], operate_flags=flags, aggregate=None, prvals=[], last_version=None)
            if code == 0:
                result["aggregate"], result["prvals"] = native.write_vector(io, name, request["operations"], request["mtime"], flags)
                result["last_version"] = native.lib.rados_get_last_version(io)
        else:
            # librados sends ROLLBACK with the ioctx snapc and its own clock.
            result["rollback"] = {"snap": request["snap"], "code": None}
            if code == 0:
                result["rollback"]["code"] = native.lib.rados_ioctx_selfmanaged_snap_rollback(io, name, request["snap"])
    finally:
        native.reset_write_context(io)
    result["after"] = native.full_state(io, name, request["inventory_keys"], cli)
    if request["object_info"] and result["after"]["code"] == 0:
        result["object_info"] = native.object_info(io, name)
    return result


def execute(native, io, metadata, request, mode, out, cli):
    result = {"schema_version": 1, "source_commit": SOURCE_COMMIT, "mode": mode,
              "fixture": {k: metadata[k] for k in ("pool", "pool_id", "object")},
              "scope": {"independent_native_c_api": True, "header_via_pinned_rados_cli": True,
                        "raw_wire_payload_available": False, "native_write_per_op_results_available": False,
                        "native_cmp_per_op_prval_available": True, "listsnaps_via_pinned_rados_cli": True,
                        "snap_read_header_available": False, "rollback_mtime_explicit": False}}
    if mode == "cleanup":
        codes = []
        for name in check_ownership(omap.read_json(out / (OWNED + ".json")), metadata):
            codes.append({"object": name.decode(), "code": native.lib.rados_remove(io, name)})
        result["cleanup"] = {"objects": codes, "objects_removed": all(c["code"] in (0, -2) for c in codes)}
        for entry in codes:
            native.check(entry["code"] if entry["code"] != -2 else 0)
        # Snap ids go only after every owned head; the OSD then trims clones.
        removed = [{"snap": snap, "code": native.lib.rados_ioctx_selfmanaged_snap_remove(io, snap)}
                   for snap in owned_snaps(out, metadata)]
        result["cleanup"].update(snaps=removed, snaps_removed=all(e["code"] == 0 for e in removed))
        publish_snaps(out, metadata, [e["snap"] for e in removed if e["code"] != 0])
        for entry in removed:
            native.check(entry["code"])
        return result
    if mode in SNAP_MODES:
        return execute_snapshot(native, io, metadata, request, mode, out, cli, result)
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
    for mode in ("prepare", "observe", "assert", "exec", "compare", "cleanup") + SNAP_MODES:
        modes.add_argument("--" + mode, dest="mode", action="store_const", const=mode)
    args = parser.parse_args()
    collect(args.root, args.out, args.mode)
