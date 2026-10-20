#!/usr/bin/env python3
"""Closed read-only CHECKSUM compound C-API oracle; never raw wire prval proof.

One rados_read_op_operate per case. ASSERT_VER exposes no C prval. STAT's
convenience callback can turn untouched raw rval zero into -EIO; both the
callback result and sentinel buffers are retained without inventing raw data.
"""
import base64
import ctypes as C
from pathlib import Path
import struct
import sys

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_pool_oracle as pool
import osd_xattr_native as xattr

NAME = "osd-checksum-compound"
DATA = bytes([0, 255, 67, 72, 75, 0, 128, 13, 10, 17, 34, 51, 68, 85, 102, 119])
CASES = ("mixed", "distinct-seeds", "eof-prefix", "trimmed", "checksum-before-assert", "assert-first",
         "unaligned-after-read", "checksum-before-cmpext", "cmpext-first", "missing")
WIDTHS = {0: 4, 1: 8, 2: 4}
SENTINEL = -99999


def vector(case, version):
    if case not in CASES or type(version) is not int or not 0 < version < (1 << 64) - 1:
        raise ValueError("closed compound case/version differs")
    def checksum(kind, offset, length, chunk, seed):
        return dict(op="checksum", type=kind, offset=offset, length=length, chunk_size=chunk, seed=seed)
    def read(offset, length):
        return dict(op="read", offset=offset, length=length)
    stat = dict(op="stat")
    assertion = dict(op="assert", version=version + 1)
    wrong = bytearray(DATA[:4]); wrong[2] ^= 255
    cmp_good = dict(op="cmpext", offset=0, data=base64.b64encode(DATA[:4]).decode())
    cmp_bad = dict(op="cmpext", offset=0, data=base64.b64encode(wrong).decode())
    return {
        "mixed": [checksum(2, 0, 8, 4, 0x1234), stat, read(4, 8), checksum(1, 0, 16, 0, 0xfedcba9876543210)],
        "distinct-seeds": [checksum(0, 0, 8, 0, 0xffffffff), checksum(1, 8, 8, 4, 0x8000000000000001)],
        "eof-prefix": [checksum(2, 16, 8, 4, 1), read(0, 8), stat],
        "trimmed": [checksum(2, 12, 8, 0, 7), stat],
        "checksum-before-assert": [checksum(2, 0, 8, 4, 1), assertion, checksum(1, 0, 8, 0, 2), read(0, 4)],
        "assert-first": [assertion, checksum(2, 0, 8, 0, 1), stat],
        "unaligned-after-read": [read(0, 4), checksum(2, 12, 8, 8, 3), stat, checksum(0, 0, 8, 0, 4)],
        "checksum-before-cmpext": [checksum(2, 0, 8, 0, 5), cmp_bad, stat],
        "cmpext-first": [cmp_good, checksum(1, 0, 8, 0, 6), read(8, 8)],
        "missing": [stat, checksum(2, 0, 8, 0, 1), read(0, 4)],
    }[case]


def validate(raw):
    if not isinstance(raw, dict) or set(raw) != {"object", "case"} or raw["case"] not in CASES:
        raise ValueError("closed checksum compound input differs")
    return {"object": xattr.identity(raw["object"], "owned object"), "case": raw["case"]}


class Native(writes.Native):
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
        self.lib.rados_get_instance_id.argtypes, self.lib.rados_get_instance_id.restype = [p], u64

    def compound(self, io, name, operations):
        operation = self.lib.rados_create_read_op()
        if not operation:
            raise RuntimeError("native read-op allocation failed")
        outputs, keepalive = [], []
        try:
            for action in operations:
                entry = {"request": action, "prval_available": action["op"] != "assert"}
                prval = C.c_int(SENTINEL)
                if action["op"] == "checksum":
                    width = WIDTHS[action["type"]]
                    count = action["length"] // action["chunk_size"] if action["chunk_size"] else 1
                    size = 4 + count * width
                    output = C.create_string_buffer(bytes([0xcc]) * size, size)
                    seed = C.create_string_buffer(struct.pack("<Q" if width == 8 else "<I", action["seed"]), width)
                    self.lib.rados_read_op_checksum(operation, action["type"], seed, width, action["offset"], action["length"], action["chunk_size"], output, size, C.byref(prval))
                    entry.update(_buffer=output, _width=width, _count=count)
                    keepalive.extend((output, seed))
                elif action["op"] == "read":
                    output, length = C.create_string_buffer(bytes([0xcc]) * action["length"], action["length"]), C.c_size_t((1 << 63) - 1)
                    self.lib.rados_read_op_read(operation, action["offset"], action["length"], output, C.byref(length), C.byref(prval))
                    entry.update(_buffer=output, _length=length)
                    keepalive.extend((output, length))
                elif action["op"] == "stat":
                    size, stamp = C.c_uint64((1 << 63) - 1), xattr.Timespec(-7, -11)
                    self.lib.rados_read_op_stat2(operation, C.byref(size), C.byref(stamp), C.byref(prval))
                    entry.update(_size=size, _stamp=stamp)
                    keepalive.extend((size, stamp))
                elif action["op"] == "assert":
                    self.lib.rados_read_op_assert_version(operation, action["version"])
                elif action["op"] == "cmpext":
                    comparison = base64.b64decode(action["data"], validate=True)
                    data = C.create_string_buffer(comparison, len(comparison))
                    self.lib.rados_read_op_cmpext(operation, data, len(comparison), action["offset"], C.byref(prval))
                    keepalive.append(data)
                else:
                    raise ValueError("unreachable compound opcode")
                entry["_prval"] = prval
                keepalive.append(prval)
                outputs.append(entry)
            aggregate = int(self.lib.rados_read_op_operate(operation, io, name, 0))
            version = int(self.lib.rados_get_last_version(io))
            result = []
            for entry in outputs:
                action = entry["request"]
                row = {"request": action, "prval_available": entry["prval_available"], "callback_prval": entry["_prval"].value if entry["prval_available"] else None}
                if "_buffer" in entry:
                    raw = entry["_buffer"].raw
                    row["buffer_raw"] = base64.b64encode(raw).decode()
                    row["buffer_unchanged"] = raw == bytes([0xcc]) * len(raw)
                    data = b""
                    if action["op"] == "checksum" and not row["buffer_unchanged"] and row["callback_prval"] >= 0:
                        count = struct.unpack_from("<I", raw)[0]
                        if count > entry["_count"]:
                            raise ValueError("native checksum output count exceeds bound")
                        data = raw[:4 + count * entry["_width"]]
                        row["values"] = list(struct.unpack_from("<" + ("Q" if entry["_width"] == 8 else "I") * count, data, 4))
                    if action["op"] == "read":
                        length = entry["_length"].value
                        row["byte_count_unchanged"] = length == (1 << 63) - 1
                        if length > len(raw) and not row["byte_count_unchanged"]:
                            raise ValueError("native read byte-count exceeds bound")
                        row["bytes_read"] = length
                        data = b"" if row["byte_count_unchanged"] else raw[:length]
                    row["data"] = base64.b64encode(data).decode()
                    if action["op"] == "checksum":
                        row.setdefault("values", [])
                if action["op"] == "stat":
                    row.update(size=entry["_size"].value, seconds=entry["_stamp"].tv_sec, nanos=entry["_stamp"].tv_nsec)
                result.append(row)
            return {"aggregate": aggregate, "version": version, "operations": result,
                    "operate_calls": 1, "raw_wire_per_op_results_available": False,
                    "assert_version_prval_available": False, "returned_extent_union_available": False, "stat_callback_may_replace_raw_zero_with_EIO": True}
        finally:
            self.lib.rados_release_read_op(operation)


def collect(root, out):
    request = validate(omap.read_json(out / (NAME + "-input.json")))
    metadata = omap.read_json(out / "osd-oracle.json")
    names = writes.check_ownership(omap.read_json(out / (writes.OWNED + ".json")), metadata)
    if len(names) != 2 or request["object"] != names[1 if request["case"] == "missing" else 0]:
        raise ValueError("compound object is outside exact owned main/absent scope")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("compound oracle requires pinned Tentacle")
    native = Native()
    rados = pool.Rados(root, native)
    cluster = rados.connect(timeout=30)
    try:
        dump = rados.dump(cluster)
        descriptor = pool.find_pool(dump, metadata["pool"])
        if descriptor is None or descriptor.get("pool") != metadata["pool_id"] or descriptor.get("type") != 1:
            raise ValueError("compound target is not the owned replicated fixture pool")
        io = rados.ioctx(cluster, metadata["pool"])
        try:
            main_before = native.snapshot(io, names[0], [])
            before = native.snapshot(io, request["object"], [])
            if main_before["code"] != 0 or base64.b64decode(main_before["data"]) != DATA or main_before["version"] <= 0:
                raise ValueError("owned immutable binary16 baseline differs")
            operations = vector(request["case"], main_before["version"])
            result = native.compound(io, request["object"], operations)
            after = native.snapshot(io, request["object"], [])
            main_after = native.snapshot(io, names[0], [])
            if before != after or main_before != main_after:
                raise ValueError("read-only compound changed native state")
            result.update(source_commit=xattr.SOURCE_COMMIT, schema_version=1, case=request["case"], object=request["object"].decode(),
                          fixture={k: metadata[k] for k in ("pool", "pool_id", "object")}, before=before, after=after,
                          main_before=main_before, main_after=main_after,
                          native_identity={"name": "client.test", "global_id": int(native.lib.rados_get_instance_id(cluster))},
                          native_fsid=dump["fsid"], native_pool_epoch=dump["epoch"],
                          native_pool_type=descriptor["type"], native_client_may_resend_reads=True)
            omap.publish(out, NAME + "-result", result)
        finally:
            native.lib.rados_ioctx_destroy(io)
    finally:
        native.lib.rados_shutdown(cluster)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError("usage: checksum compound oracle ROOT OUT")
    collect(Path(sys.argv[1]), Path(sys.argv[2]))
