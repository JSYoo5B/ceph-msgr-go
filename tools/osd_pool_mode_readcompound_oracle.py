"""Owned-pool oracle mode readcompound: one native read-op vector on one owned head.

Test-only; never product code. Action seed replaces one object of an owned
pool with the given bytes, xattrs and OMAP entries in one native write op
(remove, then write_full, setxattr each, omap_set2 when entries are given)
with a fixed mtime, and returns its native snapshot. On an erasure-coded pool
the caller seeds no OMAP: the pinned OSD refuses every OMAP write there.

Action execute builds ONE rados_read_op from the listed actions, in order,
and runs one rados_read_op_operate with no flags. Each action is one native C
builder of the pinned librados API (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964, src/include/rados/librados.h):
read (rados_read_op_read), stat (stat2), assert (assert_version), cmpext,
getxattrs, cmpxattr, keys (omap_get_keys2), vals (omap_get_vals2), bykeys
(omap_get_vals_by_keys2), omapcmp (omap_cmp2), exec and checksum. It records
the aggregate, the IoCtx's last version and, per action, the C callback
result where the API exposes one (assert and cmpxattr have none) and the
outputs it filled. These are convenience API outputs, not raw wire rvals: a
callback decodes an operation's output, so an operation after the first
failure, which the OSD leaves with rval 0 and no output, can report a decode
error, and its buffers are reported as they were left. A native C vector
carries no GETXATTR, SPARSE_READ, MAPEXT or OMAP header read; the C API has
no such read-op builder. Nothing is retried, and execute changes nothing.
"""
import ctypes as C
import struct

import osd_omap_write_native as omapwrite
import osd_xattr_native as xattr
from osd_pool_oracle import Timespec, object_request, oracle_mode, pool_name

ACTIONS = ("read", "stat", "assert", "cmpext", "getxattrs", "cmpxattr", "keys", "vals", "bykeys", "omapcmp", "exec", "checksum")
MAX_ACTIONS = 8
MAX_DATA = 64 << 10
MAX_ENTRIES = 32
MAX_CALL_OUTPUT = 4096
SENTINEL = -99999
UNSET = (1 << 63) - 1
CHECKSUM_WIDTHS = {0: 4, 1: 8, 2: 4}
SEED_MTIME = (1700000700, 234567891)


def b64(value, limit=MAX_DATA):
    return xattr.binary(value, limit)


def count(value, high, label):
    if type(value) is not int or not 0 <= value <= high:
        raise ValueError("invalid " + label)
    return value


def validate(action):
    """One action with exactly its own fields, decoded and bounded."""
    if not isinstance(action, dict) or action.get("op") not in ACTIONS:
        raise ValueError("unsupported native read-op action")
    op = action["op"]
    fields = {"read": {"offset", "length"}, "stat": set(), "assert": {"version"}, "cmpext": {"offset", "data"},
              "getxattrs": set(), "cmpxattr": {"name", "comparison", "value"}, "keys": {"start_after", "max_return"},
              "vals": {"start_after", "prefix", "max_return"}, "bykeys": {"keys"}, "omapcmp": {"key", "comparison", "value"},
              "exec": {"class", "method", "input"}, "checksum": {"type", "offset", "length", "chunk_size", "seed"}}[op]
    if set(action) != fields | {"op"}:
        raise ValueError("native %s action takes exactly %s" % (op, sorted(fields)))
    out = {"op": op}
    if op == "read":
        out.update(offset=count(action["offset"], MAX_DATA, "offset"), length=count(action["length"], MAX_DATA, "length"))
    elif op == "assert":
        out["version"] = count(action["version"], (1 << 64) - 1, "version")
    elif op == "cmpext":
        out.update(offset=count(action["offset"], MAX_DATA, "offset"), data=b64(action["data"], 4096))
    elif op == "cmpxattr":
        if action["comparison"] not in xattr.COMPARE:
            raise ValueError("unsupported native xattr comparison")
        out.update(name=xattr.checked_name(b64(action["name"], xattr.MAX_NAME)), comparison=xattr.COMPARE[action["comparison"]],
                   value=b64(action["value"], 4096))
    elif op == "keys":
        out.update(start_after=b64(action["start_after"], 4096), max_return=count(action["max_return"], MAX_ENTRIES, "max_return"))
    elif op == "vals":
        out.update(start_after=b64(action["start_after"], 4096), prefix=b64(action["prefix"], 4096),
                   max_return=count(action["max_return"], MAX_ENTRIES, "max_return"))
    elif op == "bykeys":
        if not isinstance(action["keys"], list) or not 0 < len(action["keys"]) <= MAX_ENTRIES:
            raise ValueError("invalid native OMAP key set")
        out["keys"] = [b64(k, 4096) for k in action["keys"]]
    elif op == "omapcmp":
        if action["comparison"] not in (1, 3, 5):
            raise ValueError("unsupported native OMAP comparison")
        out.update(key=b64(action["key"], 4096), comparison=action["comparison"], value=b64(action["value"], 4096))
    elif op == "exec":
        out.update(cls=omapwrite.class_name(action["class"]), method=omapwrite.class_name(action["method"]), input=b64(action["input"], 4096))
    elif op == "checksum":
        if action["type"] not in CHECKSUM_WIDTHS:
            raise ValueError("unsupported native checksum type")
        out.update(type=action["type"], offset=count(action["offset"], MAX_DATA, "offset"), length=count(action["length"], MAX_DATA, "length"),
                   chunk_size=count(action["chunk_size"], MAX_DATA, "chunk_size"), seed=count(action["seed"], (1 << 64) - 1, "seed"))
        if out["chunk_size"] and out["length"] // out["chunk_size"] > 64 or not out["chunk_size"] and not out["length"]:
            raise ValueError("native checksum output exceeds bound")
    return out


class Native(omapwrite.Native):
    def __init__(self, library=None):
        super().__init__(library)
        p, u64, s, i = C.c_void_p, C.c_uint64, C.c_size_t, C.c_int
        for name, args in {
            "read_op_checksum": [p, i, p, s, u64, s, s, p, s, C.POINTER(i)],
            "read_op_assert_version": [p, u64],
            "read_op_cmpext": [p, p, s, u64, C.POINTER(i)],
        }.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = args, None

    def seed(self, io, name, data, attributes, entries):
        code = self.lib.rados_remove(io, name)
        if code not in (0, -2):
            self.check(code)
        operation = self.lib.rados_create_write_op()
        if not operation:
            raise RuntimeError("native seed allocation failed")
        try:
            buffer = C.create_string_buffer(data, len(data) + 1)
            self.lib.rados_write_op_write_full(operation, buffer, len(data))
            _, keep = self.append(operation, "write", [{"op": "set", "name": n, "value": v, "flags": 0} for n, v in attributes])
            if entries:
                keys, kl, kb = omapwrite.omap.pointers([k for k, _ in entries])
                vals, vl, vb = omapwrite.omap.pointers([v for _, v in entries])
                self.lib.rados_write_op_omap_set2(operation, keys, vals, kl, vl, len(entries))
            stamp = Timespec(*SEED_MTIME)
            return self.lib.rados_write_op_operate2(operation, io, name, C.byref(stamp), 0)
        finally:
            self.lib.rados_release_write_op(operation)

    def execute(self, io, name, actions):
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native read-op allocation failed")
        slots, keep = [], []
        try:
            for action in actions:
                slot = {"action": action, "prval": C.c_int(SENTINEL)}
                slots.append(slot)
                keep.append(slot["prval"])
                prval = C.byref(slot["prval"])
                op = action["op"]
                if op == "read":
                    slot["buffer"] = C.create_string_buffer(bytes([0xcc]) * max(action["length"], 1), max(action["length"], 1))
                    slot["count"] = C.c_size_t(UNSET)
                    self.lib.rados_read_op_read(operation, action["offset"], action["length"], slot["buffer"], C.byref(slot["count"]), prval)
                elif op == "stat":
                    slot["size"], slot["stamp"] = C.c_uint64(UNSET), xattr.Timespec(-7, -11)
                    self.lib.rados_read_op_stat2(operation, C.byref(slot["size"]), C.byref(slot["stamp"]), prval)
                elif op == "assert":
                    self.lib.rados_read_op_assert_version(operation, action["version"])
                elif op == "cmpext":
                    slot["data"] = C.create_string_buffer(action["data"], len(action["data"]))
                    self.lib.rados_read_op_cmpext(operation, slot["data"], len(action["data"]), action["offset"], prval)
                elif op == "getxattrs":
                    slot["iterator"] = C.c_void_p()
                    self.lib.rados_read_op_getxattrs(operation, C.byref(slot["iterator"]), prval)
                elif op == "cmpxattr":
                    slot["value"] = C.create_string_buffer(action["value"], len(action["value"]) + 1)
                    self.lib.rados_read_op_cmpxattr(operation, action["name"], action["comparison"], slot["value"], len(action["value"]))
                elif op in ("keys", "vals"):
                    slot["iterator"], slot["more"] = C.c_void_p(), C.c_ubyte(2)
                    if op == "keys":
                        self.lib.rados_read_op_omap_get_keys2(operation, action["start_after"], action["max_return"], C.byref(slot["iterator"]), C.byref(slot["more"]), prval)
                    else:
                        self.lib.rados_read_op_omap_get_vals2(operation, action["start_after"], action["prefix"], action["max_return"], C.byref(slot["iterator"]), C.byref(slot["more"]), prval)
                elif op == "bykeys":
                    keys, lengths, buffers = omapwrite.omap.pointers(action["keys"])
                    keep.extend((keys, lengths, buffers))
                    slot["iterator"] = C.c_void_p()
                    self.lib.rados_read_op_omap_get_vals_by_keys2(operation, keys, len(action["keys"]), lengths, C.byref(slot["iterator"]), prval)
                elif op == "omapcmp":
                    self.lib.rados_read_op_omap_cmp2(operation, action["key"], action["comparison"], action["value"], len(action["key"]), len(action["value"]), prval)
                elif op == "exec":
                    slot["out"], slot["length"] = C.c_void_p(), C.c_size_t()
                    self.lib.rados_read_op_exec(operation, action["cls"], action["method"], action["input"], len(action["input"]),
                                                C.byref(slot["out"]), C.byref(slot["length"]), prval)
                else:
                    width = CHECKSUM_WIDTHS[action["type"]]
                    chunks = action["length"] // action["chunk_size"] if action["chunk_size"] else 1
                    size = 4 + chunks * width
                    slot["buffer"] = C.create_string_buffer(bytes([0xcc]) * size, size)
                    slot["seed"] = C.create_string_buffer(struct.pack("<Q" if width == 8 else "<I", action["seed"]), width)
                    slot.update(width=width, chunks=chunks)
                    self.lib.rados_read_op_checksum(operation, action["type"], slot["seed"], width, action["offset"], action["length"],
                                                    action["chunk_size"], slot["buffer"], size, prval)
            aggregate = int(self.lib.rados_read_op_operate(operation, io, name, 0))
            version = int(self.lib.rados_get_last_version(io))
            return {"aggregate": aggregate, "version": version, "operations": [self.output(slot) for slot in slots],
                    "operate_calls": 1, "raw_wire_per_op_results_available": False}
        finally:
            for slot in slots:
                if slot.get("iterator") is not None and slot["iterator"].value:
                    if slot["action"]["op"] == "getxattrs":
                        self.lib.rados_getxattrs_end(slot["iterator"])
                    else:
                        self.lib.rados_omap_get_end(slot["iterator"])
                if slot.get("out") is not None and slot["out"].value:
                    self.lib.rados_buffer_free(slot["out"])
            self.lib.rados_release_read_op(operation)

    def output(self, slot):
        action, op = slot["action"], slot["action"]["op"]
        exposed = op not in ("assert", "cmpxattr")
        row = {"op": op, "prval_available": exposed, "prval": slot["prval"].value if exposed else None}
        prval = row["prval"]
        if op == "read":
            raw, length = slot["buffer"].raw[:action["length"]], slot["count"].value
            row["count_unchanged"] = length == UNSET
            if not row["count_unchanged"] and length > action["length"]:
                raise ValueError("native read byte count exceeds its buffer")
            row["data"] = xattr.encode(b"" if row["count_unchanged"] else raw[:length])
        elif op == "stat":
            row.update(size=slot["size"].value, seconds=slot["stamp"].tv_sec, nanos=slot["stamp"].tv_nsec,
                       unchanged=slot["size"].value == UNSET and slot["stamp"].tv_sec == -7 and slot["stamp"].tv_nsec == -11)
        elif op == "getxattrs":
            row["attributes"] = self.entries(slot["iterator"]) if prval == 0 and slot["iterator"].value else None
        elif op in ("keys", "vals", "bykeys"):
            entries = None
            if prval == 0 and slot["iterator"].value:
                entries = self.omap_entries(slot["iterator"], op == "keys")
                if len(entries) > MAX_ENTRIES:
                    raise ValueError("native OMAP read exceeds bound")
            row["entries"] = entries
            if op != "bykeys":
                row["more"] = bool(slot["more"].value) if entries is not None and slot["more"].value in (0, 1) else None
        elif op == "exec":
            if slot["length"].value > MAX_CALL_OUTPUT:
                raise ValueError("native CALL output exceeds bound")
            row["output"] = xattr.encode(C.string_at(slot["out"].value, slot["length"].value) if slot["out"].value and slot["length"].value else b"")
        elif op == "checksum":
            raw = slot["buffer"].raw
            row["unchanged"] = raw == bytes([0xcc]) * len(raw)
            values = []
            if not row["unchanged"] and prval == 0:
                n = struct.unpack_from("<I", raw)[0]
                if n > slot["chunks"]:
                    raise ValueError("native checksum count exceeds bound")
                values = list(struct.unpack_from("<" + ("Q" if slot["width"] == 8 else "I") * n, raw, 4))
            row["values"] = values
        return row


def attributes(raw):
    if not isinstance(raw, list) or len(raw) > 16:
        raise ValueError("invalid seed attributes")
    out = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"name", "value"}:
            raise ValueError("invalid seed attribute")
        out.append((xattr.checked_name(b64(item["name"], xattr.MAX_NAME)), b64(item["value"], 4096)))
    return sorted(out)


def entries(raw):
    if not isinstance(raw, list) or len(raw) > MAX_ENTRIES:
        raise ValueError("invalid seed OMAP entries")
    out = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"key", "value"}:
            raise ValueError("invalid seed OMAP entry")
        out.append((b64(item["key"], 4096), b64(item["value"], 4096)))
    if len({k for k, _ in out}) != len(out):
        raise ValueError("duplicate seed OMAP key")
    return sorted(out)


@oracle_mode
def mode_readcompound(rados, cluster, request, out, native=None):
    """Seed one owned head, or run one native read-op vector on it."""
    if not isinstance(request, dict) or request.get("action") not in ("seed", "execute"):
        raise ValueError("readcompound takes action seed or execute")
    fields = {"seed": {"data", "attributes", "entries"}, "execute": {"operations"}}[request["action"]]
    if set(request) != fields | {"action", "slot", "object"}:
        raise ValueError("readcompound %s takes exactly slot, object and %s" % (request["action"], sorted(fields)))
    pool = pool_name(request["slot"])
    name, _ = object_request(request)
    if request["action"] == "seed":
        data, attrs, omap = b64(request["data"]), attributes(request["attributes"]), entries(request["entries"])
    else:
        if not isinstance(request["operations"], list) or not 0 < len(request["operations"]) <= MAX_ACTIONS:
            raise ValueError("readcompound runs 1-%d actions" % MAX_ACTIONS)
        actions = [validate(a) for a in request["operations"]]
    native = native if native is not None else Native(rados.lib)
    io = rados.ioctx(cluster, pool)
    try:
        if request["action"] == "seed":
            code = native.seed(io, name, data, attrs, omap)
            return {"read_compound": {"pool": pool, "object": request["object"], "action": "seed", "code": code,
                                      "state": native.snapshot(io, name, []) if code == 0 else None}}
        return {"read_compound": {"pool": pool, "object": request["object"], "action": "execute", "native": native.execute(io, name, actions),
                                  "scope": "one librados C read op; callback results are convenience outputs, not raw wire rvals"}}
    finally:
        rados.lib.rados_ioctx_destroy(io)

