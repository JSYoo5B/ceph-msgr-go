"""Owned-pool oracle mode nativeop: one native operation on one owned object.

Test-only; never product code. It shows what the pinned native client gets
for an operation an erasure-coded pool restricts, so a Go reply can be
compared with it. Each request runs exactly one of:

- action write: ONE rados_write_op built from the listed operations, in
  order, and one rados_write_op_operate2 with a fixed mtime and no flags
  (pinned librados C API, Ceph 20.2.4 commit
  7f793731f1b39eb4f465e960113d2363c311b964, src/include/rados/librados.h).
  Operations: writefull, write, append, zero, truncate, setxattr, create,
  delete, the OMAP writes set, remove, clear and rmrange, and call
  (rados_write_op_exec, a write-mode CALL).
- action read: ONE rados_read_op of a single read over the given extent with
  the operation flag balance (LIBRADOS_OPERATION_BALANCE_READS) or localize
  (LIBRADOS_OPERATION_LOCALIZE_READS). The Objecter picks the target; this
  mode does not observe which OSD served the read.
- action tool: the pinned rados CLI for the two operations the C API has no
  builder for: setomapheader (IoCtx::omap_set_header) and mapext
  (IoCtx::mapext over the whole object). A refusal is the exit status plus
  the errno of the cpp_strerror text.

The object's native snapshot (stat2, bytes, xattrs, user version) is taken
before and after, so a refused operation's "nothing changed" is this mode's
own observation. Nothing is retried. The C write op exposes a per-operation
result for call only; the aggregate is the operate call's return value.
"""
import ctypes as C
import re
import subprocess

import osd_omap_write_native as omapwrite
import osd_xattr_native as xattr
from osd_pool_oracle import object_request, oracle_mode, pool_name

WRITE_OPS = ("writefull", "write", "append", "zero", "truncate", "setxattr", "create", "delete", "set", "remove", "clear", "rmrange", "call")
FIELDS = {"writefull": {"data"}, "write": {"data", "offset"}, "append": {"data"}, "zero": {"offset", "length"}, "truncate": {"offset"},
          "setxattr": {"name", "value"}, "create": {"exclusive"}, "delete": set(), "set": {"entries"}, "remove": {"keys"}, "clear": set(),
          "rmrange": {"begin", "end"}, "call": {"class", "method", "input"}}
READ_FLAGS = {"balance": 1, "localize": 2}  # librados.h:121-122
TOOLS = ("setomapheader", "mapext")
MAX_OPERATIONS = 8
MAX_DATA = 64 << 10
MAX_EXTENT = 1 << 30
MAX_CLI_OUTPUT = 16 << 10
MTIME = (1700003000, 135792468)


def extent(value, label):
    if type(value) is not int or not 0 <= value <= MAX_EXTENT:
        raise ValueError("invalid native " + label)
    return value


def operations(raw):
    """The listed write operations with exactly their own fields, decoded and bounded."""
    if not isinstance(raw, list) or not 0 < len(raw) <= MAX_OPERATIONS:
        raise ValueError("nativeop write runs 1-%d operations" % MAX_OPERATIONS)
    result = []
    for action in raw:
        if not isinstance(action, dict) or action.get("op") not in WRITE_OPS:
            raise ValueError("unsupported native write operation")
        op = action["op"]
        if set(action) != FIELDS[op] | {"op"}:
            raise ValueError("native %s takes exactly %s" % (op, sorted(FIELDS[op])))
        decoded = {"op": op}
        if op in ("writefull", "write", "append"):
            decoded["data"] = xattr.binary(action["data"], MAX_DATA)
        if op in ("write", "zero", "truncate"):
            decoded["offset"] = extent(action["offset"], "offset")
        if op == "zero":
            decoded["length"] = extent(action["length"], "length")
        elif op == "setxattr":
            decoded.update(name=xattr.checked_name(xattr.binary(action["name"], xattr.MAX_NAME)), value=xattr.binary(action["value"], 4096))
        elif op == "create":
            if type(action["exclusive"]) is not bool:
                raise ValueError("native create exclusive must be a boolean")
            decoded["exclusive"] = action["exclusive"]
        elif op == "set":
            decoded["entries"] = omapwrite.entries(action["entries"], "set entry")
        elif op == "remove":
            decoded["keys"] = omapwrite.keys(action["keys"], "removal key")
        elif op == "rmrange":
            decoded.update(begin=xattr.binary(action["begin"], 4096), end=xattr.binary(action["end"], 4096))
        elif op == "call":
            decoded.update(cls=omapwrite.class_name(action["class"]), method=omapwrite.class_name(action["method"]),
                           input=xattr.binary(action["input"], 4096))
        result.append(decoded)
    return result


class Native(omapwrite.Native):
    def __init__(self, library=None):
        super().__init__(library)
        p, u64, s = C.c_void_p, C.c_uint64, C.c_size_t
        for name, arguments in {"write_op_append": [p, p, s], "write_op_zero": [p, u64, u64], "write_op_truncate": [p, u64]}.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, None

    def write(self, io, name, actions):
        """One write op of the actions; the aggregate and the CALL results."""
        operation = self.lib.rados_create_write_op()
        if not operation:
            raise RuntimeError("native write-op allocation failed")
        keep, calls = [], []
        try:
            for index, action in enumerate(actions):
                op = action["op"]
                if op in ("writefull", "write", "append", "setxattr"):
                    payload = action["value"] if op == "setxattr" else action["data"]
                    buffer = C.create_string_buffer(payload, len(payload) + 1)
                    keep.append(buffer)
                    if op == "writefull":
                        self.lib.rados_write_op_write_full(operation, buffer, len(payload))
                    elif op == "write":
                        self.lib.rados_write_op_write(operation, buffer, len(payload), action["offset"])
                    elif op == "append":
                        self.lib.rados_write_op_append(operation, buffer, len(payload))
                    else:
                        self.lib.rados_write_op_setxattr(operation, action["name"], buffer, len(payload))
                elif op == "zero":
                    self.lib.rados_write_op_zero(operation, action["offset"], action["length"])
                elif op == "truncate":
                    self.lib.rados_write_op_truncate(operation, action["offset"])
                elif op == "create":
                    self.lib.rados_write_op_create(operation, 1 if action["exclusive"] else 0, None)
                elif op == "delete":
                    self.lib.rados_write_op_remove(operation)
                elif op == "set":
                    k, kl, kb = omapwrite.omap.pointers([key for key, _ in action["entries"]])
                    v, vl, vb = omapwrite.omap.pointers([value for _, value in action["entries"]])
                    keep.extend((k, kl, kb, v, vl, vb))
                    self.lib.rados_write_op_omap_set2(operation, k, v, kl, vl, len(action["entries"]))
                elif op == "remove":
                    k, kl, kb = omapwrite.omap.pointers(action["keys"])
                    keep.extend((k, kl, kb))
                    self.lib.rados_write_op_omap_rm_keys2(operation, k, kl, len(action["keys"]))
                elif op == "clear":
                    self.lib.rados_write_op_omap_clear(operation)
                elif op == "rmrange":
                    self.lib.rados_write_op_omap_rm_range2(operation, action["begin"], len(action["begin"]), action["end"], len(action["end"]))
                else:
                    prval = C.c_int(999)
                    keep.append(prval)
                    calls.append((index, prval))
                    self.lib.rados_write_op_exec(operation, action["cls"], action["method"], action["input"], len(action["input"]), C.byref(prval))
            stamp = xattr.Timespec(*MTIME)
            aggregate = int(self.lib.rados_write_op_operate2(operation, io, name, C.byref(stamp), 0))
            return aggregate, [{"index": i, "prval": v.value} for i, v in calls]
        finally:
            self.lib.rados_release_write_op(operation)

    def flagged_read(self, io, name, offset, length, flags):
        """One read op of one read with the operation flags; no retry."""
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native read-op allocation failed")
        try:
            buffer, count, prval = C.create_string_buffer(max(length, 1)), C.c_size_t(0), C.c_int(999)
            self.lib.rados_read_op_read(operation, offset, length, buffer, C.byref(count), C.byref(prval))
            aggregate = int(self.lib.rados_read_op_operate(operation, io, name, flags))
            if count.value > length:
                raise ValueError("native read byte count exceeds its buffer")
            return {"aggregate": aggregate, "prval": prval.value, "data": xattr.encode(buffer.raw[:count.value] if aggregate >= 0 else b"")}
        finally:
            self.lib.rados_release_read_op(operation)


def run_tool(root, pool, tool, name, value, run=subprocess.run):
    """The pinned rados CLI once; a refusal keeps its exit status and errno."""
    command = ["rados", "--conf", str(root / "ceph.conf"), "--keyring", str(root / "keyring"), "--name", "client.test", "--pool", pool,
               tool, name.decode()] + ([value] if tool == "setomapheader" else [])
    result = run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if len(result.stdout) > MAX_CLI_OUTPUT or len(result.stderr) > MAX_CLI_OUTPUT:
        raise ValueError("native rados %s output exceeds oracle bound" % tool)
    stderr = result.stderr.decode(errors="replace")
    code = 0
    if result.returncode != 0:
        errno = re.search(r"\((\d+)\) ", stderr)
        code = -int(errno.group(1)) if errno else None
    return {"tool": tool, "exit": result.returncode, "code": code, "stderr": stderr[-512:]}


@oracle_mode
def mode_nativeop(rados, cluster, request, out, native=None, run=subprocess.run):
    """Run one native write op, flagged read or CLI tool on one owned object."""
    if not isinstance(request, dict) or request.get("action") not in ("write", "read", "tool"):
        raise ValueError("nativeop takes action write, read or tool")
    action = request["action"]
    fields = {"write": {"operations"}, "read": {"offset", "length", "flag"}, "tool": {"tool", "value"}}[action]
    if set(request) != fields | {"action", "slot", "object"}:
        raise ValueError("nativeop %s takes exactly slot, object and %s" % (action, sorted(fields)))
    pool = pool_name(request["slot"])
    name, _ = object_request(request)
    if action == "write":
        actions = operations(request["operations"])
    elif action == "read":
        if request["flag"] not in READ_FLAGS:
            raise ValueError("nativeop read takes flag balance or localize")
        offset, length = extent(request["offset"], "offset"), extent(request["length"], "length")
        if not 0 < length <= MAX_DATA:
            raise ValueError("invalid native read length")
    else:
        if request["tool"] not in TOOLS:
            raise ValueError("nativeop tool takes setomapheader or mapext")
        value = request["value"]
        if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9]{0,64}", value) or (request["tool"] == "setomapheader") != bool(value):
            raise ValueError("setomapheader takes a short lowercase value and mapext none")
    native = native if native is not None else Native(rados.lib)
    io = rados.ioctx(cluster, pool)
    try:
        result = {"pool": pool, "object": request["object"], "action": action, "before": native.snapshot(io, name, [])}
        if action == "write":
            aggregate, calls = native.write(io, name, actions)
            result.update(aggregate=aggregate, calls=calls, operate_calls=1)
        elif action == "read":
            result.update(read=native.flagged_read(io, name, offset, length, READ_FLAGS[request["flag"]]), flag=request["flag"])
        else:
            result.update(tool=run_tool(rados.root, pool, request["tool"], name, value, run))
        result["after"] = native.snapshot(io, name, [])
        return {"native_op": result}
    finally:
        rados.lib.rados_ioctx_destroy(io)
