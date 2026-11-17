#!/usr/bin/env python3
"""Closed native observer and twin for the Go manifest tier-operation test.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964, LGPL-2.1 librados;
never a product dependency. The Go test sends its own TIER_PROMOTE,
TIER_FLUSH and TIER_EVICT requests. For each of them this oracle issues the
same operation once on a separate native twin object, through the public C++
API the native tools use (ObjectWriteOperation::tier_promote,
ObjectReadOperation::tier_flush and tier_evict, resolved from the fixture
library's own dynamic symbol table as in the cache-tier checkpoint), with the
request flags of `rados tier-promote`, `rados tier-flush` and
`rados tier-evict`. It returns the signed completion value unchanged.

Topology: the two owned plain replicated pools of osd_pool_oracle.py. Slot 1
holds the manifest objects, slot 2 the chunk, redirect and dedup chunk
targets. Neither pool is a cache tier or has one; configure-dedup sets the
dedup options of slot 1 only (fingerprint sha256, fixed 16-byte chunks into
slot 2), which pool removal discards with the pool.

Observations of a slot-1 object never promote it: one read operation with
LIBRADOS_OPERATION_IGNORE_REDIRECT returns the local STAT2 and bytes (the
manifest handling returns early on that flag), and the C++ is_dirty read is
in the manifest pass-through list. A proxied read of one closed extent
without that flag is taken only when the caller names it: on a chunk the OSD
has marked missing it is served from the chunk target, and a read of a range
that is not wholly missing chunks would promote, so the extent is closed.

Each call reads one bounded JSON input (osd-tier-op-go-input.json) and writes
one bounded result (osd-tier-op-go-result.json); every call is also appended,
intent first, to the persistent receipt osd-tier-op-go-native.json. A native
tier operation label runs at most once: a repeated label, or an earlier call
left without a result, refuses the call and nothing is reissued.

Actions:
  bind             record the owned pools and check that both are plain
  configure-dedup  set the closed dedup options of slot 1 and read them back
  observe          local state of one slot-1 object, optionally a proxied extent
  native           one tier operation on one closed native twin object
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import re
import sys
import time

import osd_cache_tier_ops_oracle as cxxabi
import osd_pool_oracle as pool
import osd_set_chunk_oracle as chunk
from osd_oracle import Timespec

COMMIT = pool.SOURCE_COMMIT
CHECKPOINT = "tier-op-go-observer-v1"
INPUT, RESULT, RECEIPT = "osd-tier-op-go-input.json", "osd-tier-op-go-result.json", "osd-tier-op-go-native.json"
ACTIONS = ("bind", "configure-dedup", "observe", "native")
OPERATIONS = ("tier-promote", "tier-flush", "tier-evict")
# librados OPERATION_* values (librados.h) and their wire translation.
IGNORE_CACHE, IGNORE_OVERLAY, IGNORE_REDIRECT = 8, 32, 256
# rados.cc tier-flush/tier-evict: aio_operate with IGNORE_CACHE|IGNORE_OVERLAY;
# tier-promote: IoCtx::operate on an ObjectWriteOperation with no flag.
REQUEST_FLAGS = {"tier-promote": 0, "tier-flush": IGNORE_CACHE | IGNORE_OVERLAY, "tier-evict": IGNORE_CACHE | IGNORE_OVERLAY}
WIRE_FLAGS = {"tier-promote": 0x20, "tier-flush": 0x10 | 0x8000 | 0x20000, "tier-evict": 0x10 | 0x8000 | 0x20000}
# The closed twin objects a native tier operation may touch, and the objects
# an observation may read. Every name lives in slot 1.
TWINS = ("tier-absent", "tier-plain-native", "tier-chunk-native", "tier-redirect-native", "tier-dedup-native")
OBSERVED = TWINS + ("tier-plain-go", "tier-chunk-go", "tier-redirect-go", "tier-dedup-go")
# Closed dedup configuration of slot 1: CDC "fixed" with 16-byte chunks
# (do_cdc uses cbits(size)-1 bits, so 16 gives 16-byte chunks).
DEDUP = (("fingerprint_algorithm", "sha256"), ("dedup_chunk_algorithm", "fixed"), ("dedup_cdc_chunk_size", "16"))
LIMIT = 4096
UNKNOWN = chunk.UNKNOWN
SYMBOL_NS = re.compile(r"_ZN8librados(7v\d+_\d+_\d+)")


class Cxx:
    """Exact exported C++ entry points for the tier operations and is_dirty."""
    STRING = "RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"

    def __init__(self, lib, path):
        self.symbols = self.resolve(cxxabi.dynamic_symbols(Path(path).read_bytes()))
        self.library = str(path)
        p, i = C.c_void_p, C.c_int
        types = {"operate_read": ([p, C.POINTER(cxxabi.CxxString), p, p, i], i),
                 "operate_write": ([p, C.POINTER(cxxabi.CxxString), p, i], i),
                 "ioctx_ctor": ([p], None), "ioctx_dtor": ([p], None), "ioctx_from_c": ([p, p], None),
                 "operation_ctor": ([p], None), "operation_dtor": ([p], None),
                 "is-dirty": ([p, C.POINTER(C.c_bool), C.POINTER(i)], None),
                 "tier-promote": ([p], None), "tier-flush": ([p], None), "tier-evict": ([p], None)}
        self.fn = {}
        for role, name in self.symbols.items():
            fn = getattr(lib, name)
            fn.argtypes, fn.restype = types[role]
            self.fn[role] = fn

    @classmethod
    def resolve(cls, names):
        reads = sorted(name for name in names if cxxabi.OPERATE_READ.fullmatch(name))
        if len(reads) != 1:
            raise ValueError("native IoCtx::operate(read, flags) is missing or ambiguous: %r" % reads)
        prefix = "_ZN8librados" + cxxabi.OPERATE_READ.fullmatch(reads[0]).group(1)
        wanted = {
            "operate_read": reads[0],
            "operate_write": prefix + "5IoCtx7operateE" + cls.STRING + "PNS0_20ObjectWriteOperationEi",
            "ioctx_ctor": prefix + "5IoCtxC1Ev",
            "ioctx_dtor": prefix + "5IoCtxD1Ev",
            "ioctx_from_c": prefix + "5IoCtx18from_rados_ioctx_tEPvRS1_",
            "operation_ctor": prefix + "15ObjectOperationC1Ev",
            "operation_dtor": prefix + "15ObjectOperationD1Ev",
            "is-dirty": prefix + "19ObjectReadOperation8is_dirtyEPbPi",
            "tier-promote": prefix + "20ObjectWriteOperation12tier_promoteEv",
            "tier-flush": prefix + "19ObjectReadOperation10tier_flushEv",
            "tier-evict": prefix + "19ObjectReadOperation10tier_evictEv",
        }
        missing = sorted(role for role, name in wanted.items() if name not in names)
        if missing:
            hints = sorted(n for n in names if "tier_" in n or "is_dirty" in n or "IoCtx7operate" in n)[:32]
            raise ValueError("native C++ entry points missing %r; related exports %r" % (missing, hints))
        return wanted

    def run(self, ioctx, name, action, flags):
        """One C++ operation on a borrowed rados_ioctx_t; the signed result."""
        if action not in OPERATIONS + ("is-dirty",):
            raise ValueError("foreign C++ tier action")
        oid = name.encode()
        text = cxxabi.CxxString(oid, len(oid), len(oid), 0)
        cxx_io, operation = C.create_string_buffer(64), C.create_string_buffer(256)
        self.fn["ioctx_ctor"](C.addressof(cxx_io))
        try:
            self.fn["ioctx_from_c"](ioctx, C.addressof(cxx_io))
            self.fn["operation_ctor"](C.addressof(operation))
            try:
                if action == "is-dirty":
                    dirty, prval = C.c_bool(False), C.c_int(-99999)
                    self.fn["is-dirty"](C.addressof(operation), C.byref(dirty), C.byref(prval))
                    code = int(self.fn["operate_read"](C.addressof(cxx_io), C.byref(text), C.addressof(operation), None, flags))
                    result = {"code": code, "isdirty_prval": prval.value}
                    if code >= 0 and prval.value == 0:
                        result["dirty"] = bool(dirty.value)
                    return result
                self.fn[action](C.addressof(operation))
                if action == "tier-promote":
                    code = self.fn["operate_write"](C.addressof(cxx_io), C.byref(text), C.addressof(operation), flags)
                else:
                    code = self.fn["operate_read"](C.addressof(cxx_io), C.byref(text), C.addressof(operation), None, flags)
                # The C++ tier calls expose no per-operation return value.
                return {"code": int(code), "per_op_result_available": False}
            finally:
                self.fn["operation_dtor"](C.addressof(operation))
        finally:
            self.fn["ioctx_dtor"](C.addressof(cxx_io))


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > 512 << 10:
        raise ValueError("native tier-op output exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def validate_input(value):
    if not isinstance(value, dict) or value.get("action") not in ACTIONS:
        raise ValueError("closed tier-op action differs")
    action = value["action"]
    fields = {"bind": {"action", "source_pool_id", "chunk_pool_id"}, "configure-dedup": {"action"},
              "observe": {"action", "object", "extent"}, "native": {"action", "label", "operation", "object"}}[action]
    if set(value) != fields and not (action == "observe" and set(value) == fields - {"extent"}):
        raise ValueError("closed tier-op input fields differ")
    if action == "bind":
        for field in ("source_pool_id", "chunk_pool_id"):
            pool.integer(value[field], 1, (1 << 31) - 1, field)
        if value["source_pool_id"] == value["chunk_pool_id"]:
            raise ValueError("source and chunk pools must differ")
    if action == "observe":
        if value["object"] not in OBSERVED:
            raise ValueError("foreign observed object")
        extent = value.get("extent")
        if extent is not None and (not isinstance(extent, list) or len(extent) != 2
                                   or pool.integer(extent[0], 0, LIMIT, "offset") + pool.integer(extent[1], 1, LIMIT, "length") > LIMIT):
            raise ValueError("invalid proxied extent")
    if action == "native":
        if value["operation"] not in OPERATIONS or value["object"] not in TWINS:
            raise ValueError("native tier operation outside the closed twin set")
        if not isinstance(value["label"], str) or not re.fullmatch(r"[a-z0-9-]{1,64}", value["label"]):
            raise ValueError("invalid native label")
    return value


class Native:
    """Native librados access to the two owned pools; one identity."""

    def __init__(self, root):
        self.rados = pool.Rados(root, pool.xattr.Native())
        self.lib = self.rados.lib
        p, i, s, u64, char = C.c_void_p, C.c_int, C.c_size_t, C.c_uint64, C.c_char_p
        for name, (args, result) in {
            "read_op_stat2": ([p, C.POINTER(u64), C.POINTER(Timespec), C.POINTER(i)], None),
            "read_op_read": ([p, u64, s, char, C.POINTER(s), C.POINTER(i)], None),
            "get_instance_id": ([p], u64),
        }.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = args, result
        self.cluster = self.rados.connect()
        self.cxx = Cxx(self.lib, cxxabi.loaded_library())
        self.identity = {"name": "client.test", "global_id": int(self.lib.rados_get_instance_id(self.cluster))}

    def io(self, slot):
        return self.rados.ioctx(self.cluster, pool.pool_name(slot))

    def local(self, name, extent=None):
        """STAT2 and bytes with IGNORE_REDIRECT, is_dirty, and an optional proxied extent."""
        io = self.io(1)
        try:
            result = {}
            operation = self.lib.rados_create_read_op()
            if not operation:
                raise RuntimeError("native read allocation failed")
            try:
                size, stamp, stat_rval = C.c_uint64(), Timespec(), C.c_int(-99999)
                buf, length, read_rval = C.create_string_buffer(LIMIT + 1), C.c_size_t(), C.c_int(-99999)
                self.lib.rados_read_op_stat2(operation, C.byref(size), C.byref(stamp), C.byref(stat_rval))
                self.lib.rados_read_op_read(operation, 0, LIMIT + 1, buf, C.byref(length), C.byref(read_rval))
                code = int(self.lib.rados_read_op_operate(operation, io, name.encode(), IGNORE_REDIRECT))
                result.update(code=code, version=int(self.lib.rados_get_last_version(io)))
                if code == 0:
                    if stat_rval.value != 0 or read_rval.value < 0 or length.value > LIMIT or length.value != size.value:
                        raise ValueError("native local observation is inconsistent or unbounded")
                    result.update(size=size.value, seconds=stamp.tv_sec, nanos=stamp.tv_nsec,
                                  data=base64.b64encode(buf.raw[:length.value]).decode())
            finally:
                self.lib.rados_release_read_op(operation)
            if result["code"] == 0:
                result["dirty"] = self.cxx.run(io, name, "is-dirty", IGNORE_REDIRECT)
            if extent is not None and result["code"] == 0:
                operation = self.lib.rados_create_read_op()
                if not operation:
                    raise RuntimeError("native read allocation failed")
                try:
                    buf, length, read_rval = C.create_string_buffer(LIMIT + 1), C.c_size_t(), C.c_int(-99999)
                    self.lib.rados_read_op_read(operation, extent[0], extent[1], buf, C.byref(length), C.byref(read_rval))
                    code = int(self.lib.rados_read_op_operate(operation, io, name.encode(), 0))
                    entry = {"extent": list(extent), "code": code, "read_prval": read_rval.value}
                    if code >= 0 and read_rval.value >= 0 and length.value <= extent[1]:
                        entry["data"] = base64.b64encode(buf.raw[:length.value]).decode()
                    result["proxied"] = entry
                finally:
                    self.lib.rados_release_read_op(operation)
            return result
        finally:
            self.lib.rados_ioctx_destroy(io)

    def tier(self, name, operation):
        io = self.io(1)
        try:
            result = self.cxx.run(io, name, operation, REQUEST_FLAGS[operation])
            result["version"] = int(self.lib.rados_get_last_version(io))
            return result
        finally:
            self.lib.rados_ioctx_destroy(io)

    def close(self):
        self.lib.rados_shutdown(self.cluster)


def pool_options(rados, cluster, slot):
    descriptor = pool.find_pool(rados.dump(cluster), pool.pool_name(slot))
    if descriptor is None:
        raise ValueError("owned pool is missing")
    return descriptor


def bind(native, request):
    dump = native.rados.dump(native.cluster)
    chunk.check_pool(pool.find_pool(dump, pool.pool_name(1)), request["source_pool_id"])
    chunk.check_pool(pool.find_pool(dump, pool.pool_name(2)), request["chunk_pool_id"])
    return {"fsid": dump["fsid"], "epoch": dump["epoch"], "require_osd_release": dump.get("require_osd_release", ""),
            "source_pool_id": request["source_pool_id"], "chunk_pool_id": request["chunk_pool_id"]}


def configure_dedup(native, receipt):
    bound = receipt.get("bound")
    if not bound:
        raise ValueError("dedup configuration requires bound pools")
    source = pool_options(native.rados, native.cluster, 1)
    if source.get("pool") != bound["source_pool_id"] or (source.get("options") or {}).get("dedup_tier") not in (None, 0):
        raise ValueError("source pool identity or dedup state differs")
    steps = [("dedup_tier", pool.pool_name(2))] + list(DEDUP)
    applied = []
    for var, value in steps:
        code, _ = native.rados.mon(native.cluster, {"prefix": "osd pool set", "pool": pool.pool_name(1), "var": var, "val": value})
        applied.append({"var": var, "value": value, "code": code, "status": native.rados.last_status[:256]})
        if code < 0:
            return {"applied": applied, "configured": False}
    options = pool_options(native.rados, native.cluster, 1).get("options") or {}
    want = {"dedup_tier": bound["chunk_pool_id"], "fingerprint_algorithm": "sha256", "dedup_chunk_algorithm": "fixed", "dedup_cdc_chunk_size": 16}
    observed = {key: options.get(key) for key in want}
    return {"applied": applied, "options": observed, "configured": observed == want}


def main(args):
    if len(args) != 3:
        raise ValueError("usage: osd_tier_op_go_oracle.py ROOT OUT")
    root, out = Path(args[1]), Path(args[2])
    raw = (out / INPUT).read_bytes()
    if len(raw) > 4096:
        raise ValueError("tier-op input exceeds bound")
    request = validate_input(json.loads(raw))
    version = (out / "daemon-version").read_text().strip()
    if COMMIT not in version or "20.2.4" not in version:
        raise ValueError("tier-op oracle requires the pinned daemon")
    path = out / RECEIPT
    if path.exists():
        receipt = json.loads(path.read_bytes())
    else:
        receipt = {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "calls": [],
                   "native_api": {"request_flags": REQUEST_FLAGS, "wire_flags_source_derived": WIRE_FLAGS,
                                  "observe_flags": IGNORE_REDIRECT, "per_op_result_available": False}}
    calls = receipt["calls"]
    if any(call.get("state") != "completed" for call in calls):
        raise ValueError("an earlier tier-op call has no result; nothing is reissued")
    if request["action"] == "bind" and receipt.get("bound"):
        raise ValueError("tier-op pools were already bound")
    if request["action"] != "bind" and not receipt.get("bound"):
        raise ValueError("tier-op pools are not bound")
    if request["action"] == "native" and any(call["input"].get("label") == request["label"] for call in calls):
        raise ValueError("native tier label repeated; nothing is reissued: " + request["label"])
    call = {"input": request, "state": "intended"}
    calls.append(call)
    save = lambda: publish(path, receipt)
    save()
    native = None
    started = time.monotonic()
    try:
        native = Native(root)
        receipt.setdefault("native_api", {}).update(cxx_symbols=native.cxx.symbols, library=os.path.basename(native.cxx.library))
        receipt["native_identity"] = dict(native.identity)
        if request["action"] == "bind":
            result = bind(native, request)
            receipt["bound"] = result
        elif request["action"] == "configure-dedup":
            result = configure_dedup(native, receipt)
        elif request["action"] == "observe":
            extent = request.get("extent")
            result = native.local(request["object"], tuple(extent) if extent else None)
        else:
            result = native.tier(request["object"], request["operation"])
            result.update(request_flags=REQUEST_FLAGS[request["operation"]], wire_flags_source_derived=WIRE_FLAGS[request["operation"]])
    except BaseException as exc:
        call.update(state="unknown" if request["action"] == "native" else "failed", error=str(exc)[:1024])
        save()
        raise
    finally:
        if native is not None:
            native.close()
    call.update(state="completed", result=result, elapsed_ms=int((time.monotonic() - started) * 1000))
    if request["action"] == "native" and result["code"] in UNKNOWN:
        call["unknown_outcome"] = True
    save()
    publish(out / RESULT, {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "input": request,
                           "result": result, "native_identity": receipt["native_identity"]})


if __name__ == "__main__":
    main(sys.argv)
