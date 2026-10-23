#!/usr/bin/env python3
"""Closed native-only cache-tier operation checkpoint; never a product dependency.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964, LGPL-2.1 librados.
CACHE_FLUSH, CACHE_TRY_FLUSH and CACHE_EVICT exist only in the public C++ API
(ObjectReadOperation), as do ObjectWriteOperation::cache_pin/cache_unpin and
ObjectReadOperation::is_dirty. The trusted native runner has no Ceph SDK or
compiler, so this oracle calls those exported librados.so.2 C++ entry points
directly from the fixture image through ctypes. It resolves each symbol from
the library's own dynamic symbol table, records the resolved names, and returns
the signed completion value of IoCtx::operate unchanged. These C++ cache calls
expose no per-operation return value; STAT+READ observations and is_dirty keep
their per-operation results.

The vocabulary is separate from the IGNORE_CACHE probe and its 66-call matrix.
Three owned heads in the existing writeback tier fixture (no overlay, hit set,
snapshots or watchers) are touched; every call publishes its intent first, an
uncertain call stops the controller and nothing is reissued. Request flags are
librados OPERATION_* values. Their wire translation (IGNORE_CACHE 0x8000,
SKIPRWLOCKS 0x10000, IGNORE_OVERLAY 0x20000) is source-derived from
librados_cxx.cc translate_flags and rados.h:469-471; it is not observed here.
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import re
import struct
import sys
import time

import osd_ignore_cache_oracle as original

COMMIT, NAMESPACE = original.COMMIT, original.NAMESPACE
BASE, CHANGED, LIMIT = original.BASE, original.CHANGED, original.LIMIT
UNKNOWN = original.UNKNOWN
validate_plan = original.validate_plan
RECEIPT = "osd-cache-tier-ops-native.json"
CHECKPOINT = "cache-tier-ops-native-v1"
FLUSH, TRY, ABSENT = "tier-ops-flush", "tier-ops-try", "tier-ops-absent"
OBJECTS = (FLUSH, TRY, ABSENT)

IGNORE_CACHE, SKIPRWLOCKS, IGNORE_OVERLAY = 8, 16, 32
WIRE = {IGNORE_CACHE: 0x8000, SKIPRWLOCKS: 0x10000, IGNORE_OVERLAY: 0x20000}
# rados.cc:1393-1440 do_cache_flush / do_cache_try_flush / do_cache_evict.
FLUSH_FLAGS = IGNORE_CACHE | IGNORE_OVERLAY
TRY_FLAGS = IGNORE_CACHE | IGNORE_OVERLAY | SKIPRWLOCKS
EVICT_FLAGS = IGNORE_CACHE | IGNORE_OVERLAY | SKIPRWLOCKS
# The C API rados_cache_pin uses no request flag. This oracle selects
# IGNORE_CACHE so an absent tier head cannot be promoted before the handler.
PIN_FLAGS = IGNORE_CACHE
CACHE_ACTIONS = ("cache-flush", "cache-try-flush", "cache-evict", "cache-pin", "cache-unpin")
ACTIONS = CACHE_ACTIONS + ("is-dirty", "snapshot", "writefull", "delete")
TRY_DEFINITE = (0, -11, -16)
TRY_POLLS, TRY_INTERVAL = 10, 1.0


def wire_flags(flags):
    value = 0
    for bit, wire in WIRE.items():
        if flags & bit:
            value |= wire
    return value


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > 512 << 10:
        raise ValueError("native tier-ops receipt exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def dynamic_symbols(data):
    """Defined global/weak names from an ELF64 little-endian .dynsym."""
    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise ValueError("native library is not ELF64 little-endian")
    shoff = struct.unpack_from("<Q", data, 0x28)[0]
    shentsize, shnum = struct.unpack_from("<HH", data, 0x3A)
    if not shoff or shentsize != 64 or shoff + shnum * 64 > len(data):
        raise ValueError("native library lacks section headers")
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * 64) for i in range(shnum)]
    names = set()
    for section in sections:
        if section[1] != 11:  # SHT_DYNSYM
            continue
        if section[6] >= len(sections) or section[9] != 24:
            raise ValueError("invalid native dynamic symbol table")
        strings = sections[section[6]][4]
        for index in range(section[5] // 24):
            st_name, st_info, _, st_shndx, _, _ = struct.unpack_from("<IBBHQQ", data, section[4] + index * 24)
            if st_shndx == 0 or st_info >> 4 not in (1, 2):
                continue
            start = strings + st_name
            names.add(data[start:data.index(b"\0", start)].decode("ascii", "replace"))
    if not names:
        raise ValueError("native library exports no dynamic symbols")
    return names


OPERATE_READ = re.compile(r"_ZN8librados(7v\d+_\d+_\d+)5IoCtx7operateERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
                          r"PNS0_19ObjectReadOperationEPN4ceph6buffer(7v\d+_\d+_\d+)4listEi")


def resolve_symbols(names):
    """Exact exported C++ entry points, keyed by role."""
    reads = sorted(name for name in names if OPERATE_READ.fullmatch(name))
    if len(reads) != 1:
        raise ValueError("native IoCtx::operate(read, flags) is missing or ambiguous: %r" % reads)
    ns = OPERATE_READ.fullmatch(reads[0]).group(1)
    prefix = "_ZN8librados" + ns
    string = "RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
    wanted = {
        "operate_read": reads[0],
        "operate_write": prefix + "5IoCtx7operateE" + string + "PNS0_20ObjectWriteOperationEi",
        "ioctx_ctor": prefix + "5IoCtxC1Ev",
        "ioctx_dtor": prefix + "5IoCtxD1Ev",
        "ioctx_from_c": prefix + "5IoCtx18from_rados_ioctx_tEPvRS1_",
        "operation_ctor": prefix + "15ObjectOperationC1Ev",
        "operation_dtor": prefix + "15ObjectOperationD1Ev",
        "cache-flush": prefix + "19ObjectReadOperation11cache_flushEv",
        "cache-try-flush": prefix + "19ObjectReadOperation15cache_try_flushEv",
        "cache-evict": prefix + "19ObjectReadOperation11cache_evictEv",
        "is-dirty": prefix + "19ObjectReadOperation8is_dirtyEPbPi",
        "cache-pin": prefix + "20ObjectWriteOperation9cache_pinEv",
        "cache-unpin": prefix + "20ObjectWriteOperation11cache_unpinEv",
    }
    missing = sorted(role for role, name in wanted.items() if name not in names)
    if missing:
        hints = sorted(n for n in names if "ObjectOperation" in n or "IoCtx7operate" in n or "cache_" in n)[:64]
        raise ValueError("native C++ entry points missing %r; related exports %r" % (missing, hints))
    return wanted


class CxxString(C.Structure):
    """libstdc++ (_GLIBCXX_USE_CXX11_ABI=1) std::string, read-only borrowed view.

    Callees only take a const reference and copy it into object_t; this view
    is never destroyed or modified by native code.
    """
    _fields_ = [("data", C.c_char_p), ("size", C.c_size_t), ("capacity", C.c_size_t), ("padding", C.c_size_t)]


class Cxx:
    def __init__(self, lib, path):
        raw = Path(path).read_bytes()
        self.symbols = resolve_symbols(dynamic_symbols(raw))
        self.library = str(path)
        p, i = C.c_void_p, C.c_int
        types = {"operate_read": ([p, C.POINTER(CxxString), p, p, i], i),
                 "operate_write": ([p, C.POINTER(CxxString), p, i], i),
                 "ioctx_ctor": ([p], None), "ioctx_dtor": ([p], None), "ioctx_from_c": ([p, p], None),
                 "operation_ctor": ([p], None), "operation_dtor": ([p], None),
                 "is-dirty": ([p, C.POINTER(C.c_bool), C.POINTER(i)], None)}
        self.fn = {}
        for role, name in self.symbols.items():
            fn = getattr(lib, name)
            fn.argtypes, fn.restype = types.get(role, ([p], None))
            self.fn[role] = fn

    def run(self, ioctx, name, action, flags):
        """One C++ operation on a borrowed rados_ioctx_t; returns the signed result."""
        if action not in CACHE_ACTIONS + ("is-dirty",):
            raise ValueError("foreign C++ cache action")
        oid = name.encode()
        text = CxxString(oid, len(oid), len(oid), 0)
        cxx_io, operation = C.create_string_buffer(64), C.create_string_buffer(256)
        self.fn["ioctx_ctor"](C.addressof(cxx_io))
        try:
            self.fn["ioctx_from_c"](ioctx, C.addressof(cxx_io))
            self.fn["operation_ctor"](C.addressof(operation))
            try:
                result = {}
                if action == "is-dirty":
                    dirty, prval = C.c_bool(False), C.c_int(-99999)
                    self.fn["is-dirty"](C.addressof(operation), C.byref(dirty), C.byref(prval))
                    result["code"] = int(self.fn["operate_read"](C.addressof(cxx_io), C.byref(text), C.addressof(operation), None, flags))
                    result["isdirty_prval"] = prval.value
                    if result["code"] >= 0 and prval.value == 0:
                        result["dirty"] = bool(dirty.value)
                else:
                    self.fn[action](C.addressof(operation))
                    operate = "operate_write" if action in ("cache-pin", "cache-unpin") else "operate_read"
                    if operate == "operate_write":
                        result["code"] = int(self.fn[operate](C.addressof(cxx_io), C.byref(text), C.addressof(operation), flags))
                    else:
                        result["code"] = int(self.fn[operate](C.addressof(cxx_io), C.byref(text), C.addressof(operation), None, flags))
                    result["per_op_result_available"] = False
                return result
            finally:
                self.fn["operation_dtor"](C.addressof(operation))
        finally:
            self.fn["ioctx_dtor"](C.addressof(cxx_io))


def loaded_library(maps_path="/proc/self/maps"):
    pattern = re.compile(r"/librados\.so\.2(?:\.\d+)*$")
    with open(maps_path) as maps:
        paths = {line.split()[-1] for line in maps if pattern.search(line.rstrip())}
    if len(paths) != 1:
        raise ValueError("loaded native library path is missing or ambiguous")
    return paths.pop()


class Native(original.Native):
    # The closed head inventory; a subclass for another checkpoint narrows it.
    objects = OBJECTS

    def __init__(self, root, out, plan):
        super().__init__(root, out, plan)
        self.cxx = Cxx(self.lib, loaded_library())

    def execute(self, role, pool, name, action, flags, data=CHANGED):
        if (role not in ("observer", "rw", "ro", "base") or pool not in (self.plan["base"], self.plan["tier"])
                or name not in self.objects or action not in ACTIONS or data not in (BASE, CHANGED)
                or flags & ~(IGNORE_CACHE | SKIPRWLOCKS | IGNORE_OVERLAY)):
            raise ValueError("operation outside closed tier-ops checkpoint")
        if role == "observer" and action in CACHE_ACTIONS:
            raise ValueError("observer identity may not issue cache operations")
        if role != "observer" and action in ("writefull", "delete", "is-dirty"):
            raise ValueError("scoped identity may not seed, clean or observe dirty state")
        io = C.c_void_p()
        self.check(self.lib.rados_ioctx_create(self.cluster(role), pool.encode(), C.byref(io)))
        self.lib.rados_ioctx_set_namespace(io, NAMESPACE.encode())
        try:
            if action in CACHE_ACTIONS or action == "is-dirty":
                result = self.cxx.run(io, name, action, flags)
            elif action == "snapshot":
                operation = self.lib.rados_create_read_op()
                if not operation:
                    raise RuntimeError("native read allocation failed")
                buf, length, size, stamp = C.create_string_buffer(LIMIT), C.c_size_t(), C.c_uint64(), C.c_long()
                read_result, stat_result = C.c_int(-99999), C.c_int(-99999)
                try:
                    self.lib.rados_read_op_stat(operation, C.byref(size), C.byref(stamp), C.byref(stat_result))
                    self.lib.rados_read_op_read(operation, 0, LIMIT, buf, C.byref(length), C.byref(read_result))
                    result = {"code": int(self.lib.rados_read_op_operate(operation, io, name.encode(), flags)),
                              "stat_prval": stat_result.value, "read_prval": read_result.value}
                    if length.value > LIMIT:
                        raise ValueError("native output exceeds read budget")
                    if result["code"] >= 0:
                        result.update(bytes_b64=base64.b64encode(buf.raw[:length.value]).decode(), size=size.value, mtime=stamp.value)
                finally:
                    self.lib.rados_release_read_op(operation)
            else:
                operation = self.lib.rados_create_write_op()
                if not operation:
                    raise RuntimeError("native write allocation failed")
                try:
                    if action == "writefull":
                        buf = C.create_string_buffer(data)
                        self.lib.rados_write_op_write_full(operation, buf, len(data))
                    else:
                        self.lib.rados_write_op_remove(operation)
                    stamp = original.Timespec(1700000800, 0)
                    result = {"code": int(self.lib.rados_write_op_operate2(operation, io, name.encode(), C.byref(stamp), flags))}
                finally:
                    self.lib.rados_release_write_op(operation)
            result["version"] = int(self.lib.rados_get_last_version(io))
            return result
        finally:
            self.lib.rados_ioctx_destroy(io)


class Stop(Exception):
    """The controller stopped; the receipt names the reason."""


class Controller:
    def __init__(self, native, plan, receipt, save, sleep=time.sleep):
        self.native, self.plan, self.receipt, self.save, self.sleep = native, validate_plan(plan), receipt, save, sleep
        self.uncertain = False

    def persist(self):
        self.receipt.setdefault("native_identities", {}).update(self.native.identities)
        self.save()

    def call(self, label, role, pool, name, action, flags, gate=None, data=CHANGED):
        """Execute once. `gate` lists results that allow the sequence to continue."""
        if any(step["label"] == label for step in self.receipt["steps"]):
            raise Stop("closed step label repeated: " + label)
        step = {"label": label, "identity_role": role, "pool": pool, "namespace": NAMESPACE, "object": name,
                "action": action, "api_flags": flags, "wire_flags_source_derived": wire_flags(flags),
                "may_mutate": action in CACHE_ACTIONS + ("writefull", "delete") or (action == "snapshot" and flags & IGNORE_CACHE == 0),
                "state": "intended"}
        if gate is not None:
            step["gate"] = list(gate)
        self.receipt["steps"].append(step)
        self.persist()
        started = time.monotonic()
        try:
            result = self.native.execute(role, pool, name, action, flags, data)
        except BaseException as exc:
            step.update(state="unknown", error=str(exc)[:1024])
            self.uncertain = True
            self.persist()
            raise Stop("native outcome unknown; will not repeat " + label) from exc
        step["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        step["result"] = result
        identity = self.native.identities.get(role)
        if identity is not None:
            step["native_identity"] = dict(identity)
        if result["code"] in UNKNOWN:
            step["state"] = "unknown"
            self.uncertain = True
            self.persist()
            raise Stop("native outcome unknown (%d); will not repeat %s" % (result["code"], label))
        step["state"] = "completed"
        self.persist()
        if gate is not None and result["code"] not in gate:
            raise Stop("%s returned %d outside gate %r" % (label, result["code"], list(gate)))
        return result

    def snapshot(self, label, name, pools=("base", "tier")):
        states = {}
        for pool in pools:
            state = self.call(label + "-" + pool, "observer", self.plan[pool], name, "snapshot", IGNORE_CACHE, gate=(0, -2))
            if state["code"] == 0 and (state["stat_prval"] != 0 or state["read_prval"] != 0):
                raise Stop("snapshot per-operation result differs: " + label)
            states[pool] = state
        self.receipt["observations"][label] = states
        return states

    def dirty(self, label, name):
        result = self.call(label, "observer", self.plan["tier"], name, "is-dirty", IGNORE_CACHE, gate=(0, -2))
        if result["code"] == 0 and result.get("isdirty_prval") != 0:
            raise Stop("is_dirty per-operation result differs: " + label)
        return result

    @staticmethod
    def same(a, b, fields=("code", "bytes_b64", "size", "version", "mtime")):
        return all(a.get(field) == b.get(field) for field in fields)

    # CACHE_PIN/UNPIN are writes without user_modify: they keep the user
    # version but take the request mtime, so tier mtime is recorded, not frozen.
    PINNED_FIELDS = ("code", "bytes_b64", "size", "version")

    def require(self, condition, reason):
        if not condition:
            raise Stop(reason)

    def holds(self, state, data):
        return (state["code"] == 0 and state.get("bytes_b64") == base64.b64encode(data).decode()
                and state.get("size") == len(data) and state.get("version", 0) > 0)

    def sequence(self):
        plan, base, tier = self.plan, self.plan["base"], self.plan["tier"]
        rw, ro, other = "rw", "ro", "base"
        for pool in (base, tier):
            for name in OBJECTS:
                placed = self.native.placement(pool, name)
                placed.update(pool_name=pool, object=name)
                self.receipt["placements"].append(placed)
                self.persist()
        # 1. Seed A in the base, B in the tier, and observe both independently.
        for name in (FLUSH, TRY):
            self.call("seed-base-" + name, "observer", base, name, "writefull", IGNORE_CACHE, gate=(0,), data=BASE)
            self.call("seed-tier-" + name, "observer", tier, name, "writefull", IGNORE_CACHE, gate=(0,), data=CHANGED)
            seeded = self.snapshot("seeded-" + name, name)
            self.require(self.holds(seeded["base"], BASE) and self.holds(seeded["tier"], CHANGED), "seeded bytes differ: " + name)
            self.require(self.dirty("seeded-dirty-" + name, name).get("dirty") is True, "seeded tier head is not dirty: " + name)
        absent = self.snapshot("absent-initial", ABSENT)
        self.require(absent["base"]["code"] == -2 and absent["tier"]["code"] == -2, "absent control head exists")
        before = self.receipt["observations"]["seeded-" + FLUSH]
        self.call("evict-dirty", rw, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(-16,))
        self.require(self.same(self.snapshot("after-evict-dirty", FLUSH)["tier"], before["tier"]), "dirty eviction changed the tier")
        self.require(self.same(self.receipt["observations"]["after-evict-dirty"]["base"], before["base"]), "dirty eviction changed the base")
        # Scoped identities: refusals or cap-independent cache-mode results on an unpinned dirty head.
        self.call("scoped-ro-evict-dirty", ro, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(-1, -16))
        self.call("scoped-base-evict-tier", other, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(-1, -16))
        self.call("scoped-ro-pin-refused", ro, tier, FLUSH, "cache-pin", PIN_FLAGS, gate=(-1,))
        self.call("scoped-base-pin-refused", other, tier, FLUSH, "cache-pin", PIN_FLAGS, gate=(-1,))
        # 2. Pin, then reach the pinned refusal branches with correct flags.
        self.call("pin", rw, tier, FLUSH, "cache-pin", PIN_FLAGS, gate=(0,))
        self.call("pinned-flush", rw, tier, FLUSH, "cache-flush", FLUSH_FLAGS, gate=(-1,))
        self.call("pinned-try-flush", rw, tier, FLUSH, "cache-try-flush", TRY_FLAGS, gate=(-1,))
        self.call("pinned-evict", rw, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(-1,))
        # Wrong lock modes precede the pinned guard in the handler; while pinned
        # a skipped lock check still cannot start a flush.
        self.call("wrong-lock-flush-skiprwlocks", rw, tier, FLUSH, "cache-flush", FLUSH_FLAGS | SKIPRWLOCKS, gate=(-22, -1))
        self.call("wrong-lock-try-flush-locked", rw, tier, FLUSH, "cache-try-flush", TRY_FLAGS & ~SKIPRWLOCKS, gate=(-22, -1))
        self.call("scoped-ro-unpin-refused", ro, tier, FLUSH, "cache-unpin", PIN_FLAGS, gate=(-1,))
        pinned = self.snapshot("pinned", FLUSH)
        self.require(self.same(pinned["tier"], before["tier"], self.PINNED_FIELDS) and self.same(pinned["base"], before["base"]), "pinned refusals changed state")
        self.require(self.dirty("pinned-dirty", FLUSH).get("dirty") is True, "pinned head lost dirty state")
        self.call("unpin", rw, tier, FLUSH, "cache-unpin", PIN_FLAGS, gate=(0,))
        self.call("evict-dirty-unpinned", rw, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(-16,))
        unpinned = self.snapshot("unpinned", FLUSH)
        self.require(self.same(unpinned["tier"], before["tier"], self.PINNED_FIELDS) and self.same(unpinned["base"], before["base"]), "unpinned eviction changed state")
        # 3a. Blocking flush on its own head.
        self.call("flush", rw, tier, FLUSH, "cache-flush", FLUSH_FLAGS, gate=(0,))
        flushed = self.snapshot("flushed", FLUSH)
        self.require(self.holds(flushed["base"], CHANGED) and self.holds(flushed["tier"], CHANGED), "flush did not propagate tier bytes")
        self.require(self.dirty("flushed-dirty", FLUSH).get("dirty") is False, "flushed head remains dirty")
        # 3b. One try-flush on a separate head; observe, never reissue.
        try_before = self.receipt["observations"]["seeded-" + TRY]
        result = self.call("try-flush", rw, tier, TRY, "cache-try-flush", TRY_FLAGS)
        polls = []
        for index in range(TRY_POLLS):
            state = self.dirty("try-flush-dirty-%d" % index, TRY)
            polls.append(state.get("dirty"))
            if state.get("dirty") is False:
                break
            self.sleep(TRY_INTERVAL)
        tried = self.snapshot("try-flushed", TRY)
        self.receipt["try_flush"] = {"code": result["code"], "definite": result["code"] in TRY_DEFINITE,
                                     "dirty_observations": polls, "observation_budget": [TRY_POLLS, TRY_INTERVAL],
                                     "base_changed": not self.same(tried["base"], try_before["base"]), "reissued": False}
        self.persist()
        self.require(result["code"] in TRY_DEFINITE, "try-flush returned an unclassified result")
        # 4. Evict the verified clean, unpinned flushed head.
        self.call("evict-clean", rw, tier, FLUSH, "cache-evict", EVICT_FLAGS, gate=(0,))
        evicted = self.snapshot("evicted", FLUSH)
        self.require(evicted["tier"]["code"] == -2, "bypass read found the evicted tier head")
        self.require(self.same(evicted["base"], flushed["base"]), "eviction changed base bytes/version/mtime")
        promoted = self.call("ordinary-read-after-evict", rw, tier, FLUSH, "snapshot", 0, gate=(0,))
        self.require(self.holds(promoted, CHANGED), "ordinary tier read differs from flushed base bytes")
        after = self.snapshot("promoted", FLUSH)
        self.require(self.holds(after["tier"], CHANGED) and self.same(after["base"], flushed["base"]), "promotion state differs")
        self.require(self.dirty("promoted-dirty", FLUSH).get("dirty") is False, "promoted head is dirty")
        # 5. Absent-head controls; the do_op lookup precedes these handlers.
        self.call("absent-flush", rw, tier, ABSENT, "cache-flush", FLUSH_FLAGS, gate=(0, -2))
        self.call("absent-try-flush", rw, tier, ABSENT, "cache-try-flush", TRY_FLAGS, gate=(0, -2))
        self.call("absent-evict", rw, tier, ABSENT, "cache-evict", EVICT_FLAGS, gate=(0, -2))
        self.call("absent-pin", rw, tier, ABSENT, "cache-pin", PIN_FLAGS, gate=(-2,))
        self.call("absent-unpin", rw, tier, ABSENT, "cache-unpin", PIN_FLAGS, gate=(-2,))
        final = self.snapshot("absent-final", ABSENT)
        self.require(final["base"]["code"] == -2 and final["tier"]["code"] == -2, "absent controls created a head")

    def cleanup(self):
        """Owned-head cleanup records its own outcome; it is never replayed."""
        record = self.receipt["cleanup"]
        if self.uncertain:
            record.update(attempted=False, complete=False, reason="an uncertain native outcome stopped the controller")
            self.persist()
            return
        record["attempted"] = True
        self.persist()
        for pool in ("tier", "base"):
            for name in OBJECTS:
                self.call("cleanup-" + pool + "-" + name, "observer", self.plan[pool], name, "delete", IGNORE_CACHE, gate=(0, -2))
                gone = self.call("cleanup-absent-" + pool + "-" + name, "observer", self.plan[pool], name, "snapshot", IGNORE_CACHE, gate=(-2,))
                self.require(gone["code"] == -2, "owned head remains after cleanup")
        record["complete"] = True
        self.persist()

    def run(self):
        try:
            self.sequence()
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


def new_receipt(plan, symbols=None, library=None):
    return {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "plan": plan,
            "native_only": True, "product_support_claimed": False, "complete": False, "sequence_complete": False,
            "native_api": {"cxx_symbols": symbols or {}, "library": library or "",
                           "api_flags": {"IGNORE_CACHE": IGNORE_CACHE, "SKIPRWLOCKS": SKIPRWLOCKS, "IGNORE_OVERLAY": IGNORE_OVERLAY},
                           "wire_flags_source_derived": {"IGNORE_CACHE": WIRE[IGNORE_CACHE], "SKIPRWLOCKS": WIRE[SKIPRWLOCKS], "IGNORE_OVERLAY": WIRE[IGNORE_OVERLAY]},
                           "operation_flags": {"cache-flush": FLUSH_FLAGS, "cache-try-flush": TRY_FLAGS, "cache-evict": EVICT_FLAGS, "cache-pin": PIN_FLAGS, "cache-unpin": PIN_FLAGS},
                           "native_client_may_resend": True, "rados_osd_op_timeout_seconds": 20},
            "steps": [], "placements": [], "observations": {}, "cleanup": {}, "native_identities": {}}


def main(args):
    if len(args) != 3:
        raise ValueError("usage: osd_cache_tier_ops_oracle.py ROOT OUT")
    root, out = Path(args[1]), Path(args[2])
    raw = (out / "osd-ignore-cache-plan.json").read_bytes()
    if len(raw) > 16384:
        raise ValueError("plan exceeds bound")
    plan = validate_plan(json.loads(raw))
    path = out / RECEIPT
    if path.exists():
        raise ValueError("tier-ops receipt exists; the checkpoint is never rerun")
    receipt = new_receipt(plan)
    save = lambda: publish(path, receipt)
    save()
    native = None
    try:
        native = Native(root, out, plan)
        receipt["native_api"].update(cxx_symbols=native.cxx.symbols, library=os.path.basename(native.cxx.library))
        Controller(native, plan, receipt, save).run()
    except BaseException as exc:
        receipt["error"] = str(exc)[:2048]
        save()
        raise
    finally:
        if native is not None:
            native.close()
    # A stopped but coherent receipt exits zero: the consumer judges it, and
    # the fixture's own pool cleanup must remain selectable afterwards.
    print(json.dumps({"complete": receipt["complete"], "steps": len(receipt["steps"]), "stopped": receipt.get("stopped", "")}))


if __name__ == "__main__":
    main(sys.argv)
