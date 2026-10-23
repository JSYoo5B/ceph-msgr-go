#!/usr/bin/env python3
"""Closed native-only SET_CHUNK manifest checkpoint; never a product dependency.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964, LGPL-2.1 librados.
SET_CHUNK exists only in the public C++ API (ObjectReadOperation::set_chunk).
As in the cache-tier operation checkpoint, this oracle resolves the exported
librados.so.2 C++ entry points from the fixture library's own dynamic symbol
table and calls them through ctypes; it returns the signed completion value of
IoCtx::operate unchanged. Construction follows the pinned `rados set-chunk`
command (rados.cc): no request flag and the per-operation flag
CEPH_OSD_OP_FLAG_WITH_REFERENCE (0x80) on success paths.

Topology: the two owned plain replicated pools of osd_pool_oracle.py. Slot 1
holds the manifest source, slot 2 the chunk target. Neither pool is a tier, has
a tier, is erasure-coded or has a dedup tier; SET_CHUNK needs no pool option.

Independent observations, all native and separate from the operation:
- STAT2/READ/GETXATTRS of source and target plus a direct GETXATTR of the
  target's `chunk_refs` attribute, which cls_cas maintains (raw bytes retained
  and decoded here as chunk_refs_t);
- `cas.references_chunk` executed on the source, which counts the source's
  own chunk_map references to the named target (the OSD's
  get_manifest_ref_count); -ENOLINK means none.

Every operation publishes its intent first, is issued once and is never
reissued; an uncertain outcome stops the controller and skips cleanup.
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import struct
import sys
import time

import osd_cache_tier_ops_oracle as cxxabi
import osd_pool_oracle as pool
from osd_oracle import Timespec

COMMIT = pool.SOURCE_COMMIT
CHECKPOINT = "set-chunk-native-v1"
INPUT, RECEIPT = "osd-set-chunk-input.json", "osd-set-chunk-result.json"
SOURCE, ABSENT_SOURCE = "set-chunk-source", "set-chunk-absent-source"
TARGET, ABSENT_TARGET = "set-chunk-target", "set-chunk-absent-target"
SOURCE_OBJECTS, TARGET_OBJECTS = (SOURCE, ABSENT_SOURCE), (TARGET, ABSENT_TARGET)
SOURCE_DATA = b"S\x00\xff\x80set-chunk-source-bytes-01234"
TARGET_DATA = b"T\x00\xffset-chunk-target-bytes"
REFS_ATTR = b"chunk_refs"
WITH_REFERENCE = 0x80   # rados.h CEPH_OSD_OP_FLAG_WITH_REFERENCE
REQUEST_FLAGS = 0       # rados.cc set-chunk: io_ctx.operate(oid, &op, NULL)
SEED_MTIME = (1700001100, 0)
UNKNOWN = {-110, -104, -108, -125, -107}
ENOENT, EINVAL, ERANGE, ENODATA, ENOLINK, EOPNOTSUPP = -2, -22, -34, -61, -67, -95
CEPH_NOSNAP = (1 << 64) - 2
RELEASE_POLLS, RELEASE_INTERVAL = 20, 0.5
LIMIT = 512 << 10

# The closed SET_CHUNK sequence: (label, source object, src_offset, src_length,
# target object, tgt_offset, per-op flag, source-derived expected result).
# PrimaryLogPG.cc:7280-7400 checks, in order: tier pool, absent source,
# redirect manifest, decode, zero length, range, WITH_REFERENCE, erasure pool,
# chunk overlap, target placement, existing reference, then the cls_cas
# reference increment on the target. The absent-target control uses source
# offset 0: finish_set_manifest_refcount also records a failure under key 0,
# so a failure at another offset would leave the request without a reply.
REFUSALS = (
    ("absent-source", ABSENT_SOURCE, 0, 8, TARGET, 0, WITH_REFERENCE, ENOENT),
    ("zero-length", SOURCE, 0, 0, TARGET, 0, WITH_REFERENCE, EINVAL),
    ("out-of-range", SOURCE, len(SOURCE_DATA) - 4, 8, TARGET, 0, WITH_REFERENCE, ERANGE),
    ("without-reference", SOURCE, 0, 8, TARGET, 0, 0, EOPNOTSUPP),
    ("absent-target", SOURCE, 0, 8, ABSENT_TARGET, 0, WITH_REFERENCE, ENOENT),
)
FIRST = ("chunk-a", SOURCE, 8, 8, TARGET, 0, WITH_REFERENCE, 0)
OVERLAPS = (
    ("overlap-same", SOURCE, 8, 8, TARGET, 0, WITH_REFERENCE, EOPNOTSUPP),
    ("overlap-partial", SOURCE, 12, 8, TARGET, 0, WITH_REFERENCE, EOPNOTSUPP),
)
SECOND = ("chunk-b", SOURCE, 16, 8, TARGET, 8, WITH_REFERENCE, 0)
SEQUENCE = REFUSALS + (FIRST,) + OVERLAPS + (SECOND,)


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > LIMIT:
        raise ValueError("native SET_CHUNK receipt exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def validate_input(value):
    if not isinstance(value, dict) or set(value) != {"source_pool_id", "chunk_pool_id"}:
        raise ValueError("closed SET_CHUNK input fields differ")
    for field in ("source_pool_id", "chunk_pool_id"):
        pool.integer(value[field], 1, (1 << 31) - 1, field)
    if value["source_pool_id"] == value["chunk_pool_id"]:
        raise ValueError("source and chunk pools must differ")
    return value


def check_pool(descriptor, pool_id):
    """An owned plain replicated pool: no tier relation, dedup tier or EC."""
    if (descriptor is None or descriptor.get("pool") != pool_id or descriptor.get("type") != 1
            or descriptor.get("tier_of", -1) != -1 or descriptor.get("tiers", []) != []
            or descriptor.get("read_tier", -1) != -1 or descriptor.get("write_tier", -1) != -1
            or descriptor.get("cache_mode", "none") != "none"):
        raise ValueError("owned pool is not a plain replicated non-tier pool: %r" % (pool_id,))
    options = descriptor.get("options") or {}
    if options.get("dedup_tier") not in (None, 0) or options.get("dedup_chunk_algorithm") not in (None, ""):
        raise ValueError("owned pool carries a dedup configuration")


class Reader:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def take(self, size):
        if self.offset + size > len(self.data):
            raise ValueError("truncated chunk_refs encoding")
        value = self.data[self.offset:self.offset + size]
        self.offset += size
        return value

    def unpack(self, fmt):
        return struct.unpack("<" + fmt, self.take(struct.calcsize("<" + fmt)))[0]

    def string(self):
        return self.take(self.unpack("I")).decode("utf-8", "strict")

    def envelope(self, version, compat):
        v, c, length = self.unpack("B"), self.unpack("B"), self.unpack("I")
        if v != version or c != compat:
            raise ValueError("unexpected encoding version %d/%d" % (v, c))
        end = self.offset + length
        if end > len(self.data):
            raise ValueError("truncated encoding envelope")
        return end


def decode_refs(raw):
    """chunk_refs_t (cls_cas_internal.{h,cc}) in its by_object form only.

    Outer ENCODE_START(1,1), u8 type, then chunk_refs_by_object_t
    ENCODE_START(1,1) with a multiset<hobject_t>; each hobject_t is
    ENCODE_START(4,3): key, oid, snap, hash, max, nspace, pool.
    """
    r = Reader(raw)
    outer = r.envelope(1, 1)
    kind = r.unpack("B")
    if kind != 1:
        raise ValueError("chunk_refs_t type %d is not by_object" % kind)
    inner = r.envelope(1, 1)
    count = r.unpack("I")
    if count > 1024:
        raise ValueError("chunk_refs count exceeds bound")
    refs = []
    for _ in range(count):
        end = r.envelope(4, 3)
        ref = {"key": r.string(), "oid": r.string(), "snap": r.unpack("Q"), "hash": r.unpack("I"),
               "max": bool(r.unpack("B")), "namespace": r.string(), "pool": r.unpack("q")}
        if r.offset != end:
            raise ValueError("hobject_t encoding length differs")
        refs.append(ref)
    if r.offset != inner or inner != outer or outer != len(raw):
        raise ValueError("chunk_refs encoding length differs")
    return {"type": "by_object", "count": count, "refs": refs}


def encode_string(text):
    raw = text.encode()
    return struct.pack("<I", len(raw)) + raw


class Cxx:
    """Exact exported C++ entry points for ObjectReadOperation::set_chunk."""
    STRING = "RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
    BY_VALUE_STRING = "NSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"

    def __init__(self, lib, path):
        self.symbols = self.resolve(cxxabi.dynamic_symbols(Path(path).read_bytes()))
        self.library = str(path)
        p, i, u64 = C.c_void_p, C.c_int, C.c_uint64
        types = {"operate_read": ([p, C.POINTER(cxxabi.CxxString), p, p, i], i),
                 "ioctx_ctor": ([p], None), "ioctx_dtor": ([p], None), "ioctx_from_c": ([p, p], None),
                 "operation_ctor": ([p], None), "operation_dtor": ([p], None),
                 "set_chunk": ([p, u64, u64, p, C.POINTER(cxxabi.CxxString), u64, i], None)}
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
            "ioctx_ctor": prefix + "5IoCtxC1Ev",
            "ioctx_dtor": prefix + "5IoCtxD1Ev",
            "ioctx_from_c": prefix + "5IoCtx18from_rados_ioctx_tEPvRS1_",
            "operation_ctor": prefix + "15ObjectOperationC1Ev",
            "operation_dtor": prefix + "15ObjectOperationD1Ev",
            # set_chunk(uint64_t, uint64_t, const IoCtx&, std::string, uint64_t, int)
            "set_chunk": prefix + "19ObjectReadOperation9set_chunkEmmRKNS0_5IoCtxE" + cls.BY_VALUE_STRING + "mi",
        }
        missing = sorted(role for role, name in wanted.items() if name not in names)
        if missing:
            hints = sorted(n for n in names if "set_chunk" in n or "ObjectOperationC1" in n or "IoCtx7operate" in n)[:32]
            raise ValueError("native C++ entry points missing %r; related exports %r" % (missing, hints))
        return wanted

    def set_chunk(self, source_io, source, src_offset, src_length, target_io, target, tgt_offset, flag):
        """One ObjectReadOperation with a single SET_CHUNK; the signed result.

        The target name is passed by value; under the Itanium C++ ABI the
        caller owns that temporary, so a borrowed read-only view is safe: the
        callee only copies it into object_t.
        """
        oid, tid = source.encode(), target.encode()
        text = cxxabi.CxxString(oid, len(oid), len(oid), 0)
        target_text = cxxabi.CxxString(tid, len(tid), len(tid), 0)
        source_cxx, target_cxx, operation = C.create_string_buffer(64), C.create_string_buffer(64), C.create_string_buffer(256)
        self.fn["ioctx_ctor"](C.addressof(source_cxx))
        try:
            self.fn["ioctx_ctor"](C.addressof(target_cxx))
            try:
                self.fn["ioctx_from_c"](source_io, C.addressof(source_cxx))
                self.fn["ioctx_from_c"](target_io, C.addressof(target_cxx))
                self.fn["operation_ctor"](C.addressof(operation))
                try:
                    self.fn["set_chunk"](C.addressof(operation), src_offset, src_length, C.addressof(target_cxx),
                                         C.byref(target_text), tgt_offset, flag)
                    return int(self.fn["operate_read"](C.addressof(source_cxx), C.byref(text), C.addressof(operation),
                                                       None, REQUEST_FLAGS))
                finally:
                    self.fn["operation_dtor"](C.addressof(operation))
            finally:
                self.fn["ioctx_dtor"](C.addressof(target_cxx))
        finally:
            self.fn["ioctx_dtor"](C.addressof(source_cxx))


class Native:
    """Native librados access to the two owned pools; one identity."""

    def __init__(self, root):
        self.rados = pool.Rados(root, pool.xattr.Native())
        self.lib = self.rados.lib
        p, i, s, char = C.c_void_p, C.c_int, C.c_size_t, C.c_char_p
        self.lib.rados_exec.argtypes = [p, char, char, char, char, s, char, s]
        self.lib.rados_exec.restype = i
        self.lib.rados_get_instance_id.argtypes, self.lib.rados_get_instance_id.restype = [p], C.c_uint64
        self.cluster = self.rados.connect()
        self.cxx = Cxx(self.lib, cxxabi.loaded_library())
        self.identity = {"name": "client.test", "global_id": int(self.lib.rados_get_instance_id(self.cluster))}
        self.pools = {}

    def bind(self, request):
        dump = self.rados.dump(self.cluster)
        names = {"source": pool.pool_name(1), "chunk": pool.pool_name(2)}
        check_pool(pool.find_pool(dump, names["source"]), request["source_pool_id"])
        check_pool(pool.find_pool(dump, names["chunk"]), request["chunk_pool_id"])
        self.pools = names
        return {"fsid": dump["fsid"], "epoch": dump["epoch"], "require_osd_release": dump.get("require_osd_release", "")}

    def _io(self, role):
        if role not in self.pools:
            raise ValueError("foreign pool role")
        return self.rados.ioctx(self.cluster, self.pools[role])

    def observe(self, role, name):
        io = self._io(role)
        try:
            state = self.rados.native.snapshot(io, name.encode(), [REFS_ATTR] if role == "chunk" else [])
        finally:
            self.lib.rados_ioctx_destroy(io)
        return state

    def references(self, name, target):
        io = self._io("source")
        try:
            data = encode_string(target)
            out = C.create_string_buffer(64)
            code = int(self.lib.rados_exec(io, name.encode(), b"cas", b"references_chunk", data, len(data), out, 64))
            return {"code": code, "version": int(self.lib.rados_get_last_version(io))}
        finally:
            self.lib.rados_ioctx_destroy(io)

    def seed(self, role, name, data):
        io = self._io(role)
        operation = self.lib.rados_create_write_op()
        if not operation:
            self.lib.rados_ioctx_destroy(io)
            raise RuntimeError("native write allocation failed")
        try:
            buffer = C.create_string_buffer(data, len(data) + 1)
            self.lib.rados_write_op_write_full(operation, buffer, len(data))
            stamp = Timespec(*SEED_MTIME)
            code = int(self.lib.rados_write_op_operate2(operation, io, name.encode(), C.byref(stamp), 0))
            return {"code": code, "version": int(self.lib.rados_get_last_version(io))}
        finally:
            self.lib.rados_release_write_op(operation)
            self.lib.rados_ioctx_destroy(io)

    def remove(self, role, name):
        io = self._io(role)
        try:
            return {"code": int(self.lib.rados_remove(io, name.encode())), "version": int(self.lib.rados_get_last_version(io))}
        finally:
            self.lib.rados_ioctx_destroy(io)

    def set_chunk(self, name, src_offset, src_length, target, tgt_offset, flag):
        source_io, target_io = self._io("source"), None
        try:
            target_io = self._io("chunk")
            code = self.cxx.set_chunk(source_io, name, src_offset, src_length, target_io, target, tgt_offset, flag)
            return {"code": code, "version": int(self.lib.rados_get_last_version(source_io))}
        finally:
            if target_io is not None:
                self.lib.rados_ioctx_destroy(target_io)
            self.lib.rados_ioctx_destroy(source_io)

    def close(self):
        self.lib.rados_shutdown(self.cluster)


class Stop(Exception):
    """The controller stopped; the receipt names the reason."""


def refs_attribute(state):
    """The target's direct chunk_refs GETXATTR: its code, raw bytes and decoding."""
    if state.get("code") != 0:
        return {"object_code": state.get("code")}
    gets = state.get("gets") or []
    if len(gets) != 1 or base64.b64decode(gets[0]["name"]) != REFS_ATTR:
        raise Stop("chunk_refs observation missing")
    entry = {"code": gets[0]["code"]}
    if entry["code"] >= 0:
        raw = base64.b64decode(gets[0]["value"])
        entry["raw_b64"] = gets[0]["value"]
        try:
            entry["decoded"] = decode_refs(raw)
        except ValueError as error:
            raise Stop("chunk_refs decoding failed: %s" % error)
    return entry


class Controller:
    def __init__(self, native, request, receipt, save, sleep=time.sleep):
        self.native, self.request, self.receipt, self.save, self.sleep = native, request, receipt, save, sleep
        self.uncertain = False

    def persist(self):
        self.save()

    def require(self, condition, reason):
        if not condition:
            raise Stop(reason)

    def mutate(self, label, kind, details, execute, gate):
        """Issue one native mutation once, intent first; never reissued."""
        if any(step["label"] == label for step in self.receipt["steps"]):
            raise Stop("closed step label repeated: " + label)
        step = dict(details, label=label, kind=kind, state="intended", gate=list(gate))
        self.receipt["steps"].append(step)
        self.persist()
        started = time.monotonic()
        try:
            result = execute()
        except BaseException as exc:
            step.update(state="unknown", error=str(exc)[:1024])
            self.uncertain = True
            self.persist()
            raise Stop("native outcome unknown; will not repeat " + label) from exc
        step.update(result=result, elapsed_ms=int((time.monotonic() - started) * 1000))
        if result["code"] in UNKNOWN:
            step["state"] = "unknown"
            self.uncertain = True
            self.persist()
            raise Stop("native outcome unknown (%d); will not repeat %s" % (result["code"], label))
        step["state"] = "completed"
        self.persist()
        if result["code"] not in gate:
            raise Stop("%s returned %d outside gate %r" % (label, result["code"], list(gate)))
        return result

    def set_chunk(self, entry):
        label, source, offset, length, target, target_offset, flag, expected = entry
        details = {"object": source, "pool_role": "source", "src_offset": offset, "src_length": length,
                   "target_pool_role": "chunk", "target_object": target, "tgt_offset": target_offset,
                   "op_flags": flag, "request_flags": REQUEST_FLAGS, "expected_source_derived": expected}
        return self.mutate(label, "set-chunk", details,
                           lambda: self.native.set_chunk(source, offset, length, target, target_offset, flag), (expected,))

    def observe(self, label):
        """Independent native state of every closed object and both reference views."""
        states = {}
        try:
            for role, names in (("source", SOURCE_OBJECTS), ("chunk", TARGET_OBJECTS)):
                for name in names:
                    states[name] = self.native.observe(role, name)
            states["chunk_refs"] = refs_attribute(states[TARGET])
            states["references_chunk"] = self.native.references(SOURCE, TARGET)
        except Stop:
            raise
        except Exception as exc:
            raise Stop("native observation %s failed: %s" % (label, str(exc)[:512])) from exc
        self.receipt["observations"][label] = states
        self.persist()
        return states

    @staticmethod
    def holds(state, data):
        return state.get("code") == 0 and state.get("data") == base64.b64encode(data).decode() and state.get("version", 0) > 0

    @staticmethod
    def same(a, b, fields=("code", "data", "version", "seconds", "nanos", "attributes")):
        return all(a.get(field) == b.get(field) for field in fields)

    def refs(self, states, count):
        refs = states["chunk_refs"]
        if count == 0:
            return refs.get("code") == ENODATA
        decoded = refs.get("decoded") or {}
        sources = [(r["oid"], r["key"], r["namespace"], r["pool"], r["snap"], r["max"]) for r in decoded.get("refs", [])]
        expected = [(SOURCE, "", "", self.request["source_pool_id"], CEPH_NOSNAP, False)] * count
        return refs.get("code", -1) > 0 and decoded.get("count") == count and sources == expected

    def sequence(self):
        for role, name, data in (("source", SOURCE, SOURCE_DATA), ("chunk", TARGET, TARGET_DATA)):
            self.mutate("seed-" + name, "writefull", {"object": name, "pool_role": role, "length": len(data)},
                        lambda role=role, name=name, data=data: self.native.seed(role, name, data), (0,))
        before = self.observe("seeded")
        self.require(self.holds(before[SOURCE], SOURCE_DATA) and self.holds(before[TARGET], TARGET_DATA), "seeded bytes differ")
        self.require(before[ABSENT_SOURCE]["code"] == ENOENT and before[ABSENT_TARGET]["code"] == ENOENT, "absent controls exist")
        self.require(self.refs(before, 0) and before["references_chunk"]["code"] == ENOLINK, "seeded target already referenced")
        # 1. Refusals: each source-derived branch, no state change.
        for entry in REFUSALS:
            self.set_chunk(entry)
        refused = self.observe("after-refusals")
        for name in SOURCE_OBJECTS + TARGET_OBJECTS:
            self.require(self.same(refused[name], before[name]), "a refused SET_CHUNK changed " + name)
        self.require(self.refs(refused, 0) and refused["references_chunk"]["code"] == ENOLINK, "a refusal added a reference")
        # 2. First chunk: one reference on the target, source bytes untouched.
        self.set_chunk(FIRST)
        first = self.observe("after-chunk-a")
        self.require(self.holds(first[SOURCE], SOURCE_DATA) and self.holds(first[TARGET], TARGET_DATA), "SET_CHUNK changed object bytes")
        self.require(self.refs(first, 1) and first["references_chunk"]["code"] == 1, "first chunk reference differs")
        self.require(first[ABSENT_SOURCE]["code"] == ENOENT and first[ABSENT_TARGET]["code"] == ENOENT, "absent controls appeared")
        # 3. Overlapping chunk intervals are refused before any reference.
        for entry in OVERLAPS:
            self.set_chunk(entry)
        overlapped = self.observe("after-overlaps")
        for name in SOURCE_OBJECTS + TARGET_OBJECTS:
            self.require(self.same(overlapped[name], first[name]), "an overlapping SET_CHUNK changed " + name)
        self.require(self.refs(overlapped, 1) and overlapped["references_chunk"]["code"] == 1, "an overlap changed references")
        # 4. A second, disjoint chunk to the same target adds a second reference.
        self.set_chunk(SECOND)
        second = self.observe("after-chunk-b")
        self.require(self.holds(second[SOURCE], SOURCE_DATA) and self.holds(second[TARGET], TARGET_DATA), "second chunk changed object bytes")
        self.require(self.refs(second, 2) and second["references_chunk"]["code"] == 2, "second chunk reference differs")

    def release(self):
        """Delete the manifest source once and observe the references drop.

        Pinned _delete_oid drops every chunk reference on commit; cls_cas
        chunk_put_ref removes the target when its last reference goes. The
        polls are reads; the deletion is never reissued.
        """
        record = self.receipt["release"]
        self.mutate("delete-source", "delete", {"object": SOURCE, "pool_role": "source"},
                    lambda: self.native.remove("source", SOURCE), (0,))
        polls = []
        for index in range(RELEASE_POLLS):
            try:
                state = self.native.observe("chunk", TARGET)
            except Exception as exc:
                raise Stop("target observation after release failed: %s" % str(exc)[:512]) from exc
            entry = {"code": state.get("code"), "version": state.get("version", 0)}
            if state.get("code") == 0:
                entry["chunk_refs"] = refs_attribute(state)
            polls.append(entry)
            if state.get("code") == ENOENT:
                break
            self.sleep(RELEASE_INTERVAL)
        record.update(polls=polls, budget=[RELEASE_POLLS, RELEASE_INTERVAL],
                      target_removed_by_reference_release=bool(polls) and polls[-1]["code"] == ENOENT)
        source = self.native.observe("source", SOURCE)
        record["source_code_after_delete"] = source.get("code")
        self.persist()
        self.require(source.get("code") == ENOENT, "deleted source remains")
        self.require(record["target_removed_by_reference_release"], "target remained after its references were released")

    def cleanup(self):
        """Owned objects only; pools belong to the owned-pool oracle."""
        record = self.receipt["cleanup"]
        if self.uncertain:
            record.update(attempted=False, complete=False, reason="an uncertain native outcome stopped the controller")
            self.persist()
            return
        record["attempted"] = True
        self.persist()
        for role, names in (("source", SOURCE_OBJECTS), ("chunk", TARGET_OBJECTS)):
            for name in names:
                self.mutate("cleanup-" + name, "delete", {"object": name, "pool_role": role},
                            lambda role=role, name=name: self.native.remove(role, name), (0, ENOENT))
                state = self.native.observe(role, name)
                self.require(state.get("code") == ENOENT, "owned object remains after cleanup: " + name)
        record["complete"] = True
        self.persist()

    def run(self):
        try:
            self.sequence()
            self.release()
            self.receipt["sequence_complete"] = True
        except Stop as stop:
            self.receipt["stopped"] = str(stop)[:1024]
        self.persist()
        try:
            self.cleanup()
        except Stop as stop:
            self.receipt["cleanup"].update(complete=False, error=str(stop)[:1024])
        calls = all(step["state"] == "completed" for step in self.receipt["steps"])
        self.receipt["complete"] = bool(self.receipt.get("sequence_complete") and self.receipt["cleanup"].get("complete") and calls)
        self.persist()


def new_receipt(request):
    return {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "input": request,
            "native_only": True, "product_support_claimed": False, "complete": False, "sequence_complete": False,
            "objects": {"source": SOURCE, "absent_source": ABSENT_SOURCE, "target": TARGET, "absent_target": ABSENT_TARGET,
                        "source_data_b64": base64.b64encode(SOURCE_DATA).decode(),
                        "target_data_b64": base64.b64encode(TARGET_DATA).decode()},
            "native_api": {"cxx_symbols": {}, "library": "", "request_flags": REQUEST_FLAGS,
                           "with_reference_op_flag": WITH_REFERENCE, "native_client_may_resend": True},
            "steps": [], "observations": {}, "release": {}, "cleanup": {}}


def main(args):
    if len(args) != 3:
        raise ValueError("usage: osd_set_chunk_oracle.py ROOT OUT")
    root, out = Path(args[1]), Path(args[2])
    raw = (out / INPUT).read_bytes()
    if len(raw) > 4096:
        raise ValueError("SET_CHUNK input exceeds bound")
    request = validate_input(json.loads(raw))
    version = (out / "daemon-version").read_text().strip()
    if COMMIT not in version or "20.2.4" not in version:
        raise ValueError("SET_CHUNK oracle requires the pinned daemon")
    path = out / RECEIPT
    if path.exists():
        raise ValueError("SET_CHUNK receipt exists; the checkpoint is never rerun")
    receipt = new_receipt(request)
    save = lambda: publish(path, receipt)
    save()
    native = None
    try:
        native = Native(root)
        receipt["native_api"].update(cxx_symbols=native.cxx.symbols, library=os.path.basename(native.cxx.library))
        receipt["native_identity"] = dict(native.identity)
        receipt["identity"] = native.bind(request)
        save()
        Controller(native, request, receipt, save).run()
    except BaseException as exc:
        receipt["error"] = str(exc)[:2048]
        save()
        raise
    finally:
        if native is not None:
            native.close()
    # A stopped but coherent receipt exits zero: the consumer judges it, and
    # the owned-pool cleanup must remain selectable afterwards.
    print(json.dumps({"complete": receipt["complete"], "steps": len(receipt["steps"]), "stopped": receipt.get("stopped", "")}))


if __name__ == "__main__":
    main(sys.argv)
