#!/usr/bin/env python3
"""Disposable native librados mutation/readback oracle, never product code.

Independently invokes APIs from the pinned Tentacle librados.h (LGPL-2.1).
The native comparison object is reset before every vector. No Go wire codec
generates native requests. Only comparison reads may be repeated by tests.
"""
import base64
import ctypes as C
import json
import os
import pathlib
import sys
import subprocess

from osd_oracle import Timespec


def collect(root, out):
    request = json.loads((out / "osd-mutation-input.json").read_bytes())
    metadata = json.loads((out / "osd-oracle.json").read_bytes())
    lib = C.CDLL("librados.so.2")
    ptr, char, integer, u64, size = C.c_void_p, C.c_char_p, C.c_int, C.c_uint64, C.c_size_t
    defs = {
        "create2": ([C.POINTER(ptr), char, char, u64], integer),
        "conf_read_file": ([ptr, char], integer),
        "conf_set": ([ptr, char, char], integer),
        "connect": ([ptr], integer),
        "ioctx_create": ([ptr, char, C.POINTER(ptr)], integer),
        "ioctx_destroy": ([ptr], None), "shutdown": ([ptr], None),
        "stat2": ([ptr, char, C.POINTER(u64), C.POINTER(Timespec)], integer),
        "get_last_version": ([ptr], u64),
        "read": ([ptr, char, ptr, size, u64], integer),
        "remove": ([ptr, char], integer),
        "write_full": ([ptr, char, char, size], integer),
        "create_write_op": ([], ptr), "release_write_op": ([ptr], None),
        "write_op_write": ([ptr, char, size, u64], None),
        "write_op_write_full": ([ptr, char, size], None),
        "write_op_append": ([ptr, char, size], None),
        "write_op_truncate": ([ptr, u64], None),
        "write_op_zero": ([ptr, u64, u64], None),
        "write_op_create": ([ptr, integer, char], None),
        "write_op_remove": ([ptr], None),
        "write_op_assert_version": ([ptr, u64], None),
        "write_op_cmpext": ([ptr, char, size, u64, C.POINTER(integer)], None),
        "write_op_set_flags": ([ptr, integer], None),
        "write_op_operate2": ([ptr, ptr, char, C.POINTER(Timespec), integer], integer),
    }
    for name, (args, result) in defs.items():
        fn = getattr(lib, "rados_" + name)
        fn.argtypes, fn.restype = args, result

    def check(result):
        if result < 0:
            raise RuntimeError("native oracle setup failed: " + str(result))

    def snapshot(io, name):
        length, mtime = u64(), Timespec()
        result = lib.rados_stat2(io, name, C.byref(length), C.byref(mtime))
        if result == -2:
            return {"code": -2, "data": "", "version": 0}
        check(result)
        if length.value > 1 << 20:
            raise ValueError("oracle object exceeds 1 MiB bound")
        version = lib.rados_get_last_version(io)
        buffer = C.create_string_buffer(length.value + 1)
        count = lib.rados_read(io, name, buffer, length.value, 0)
        check(count)
        if count != length.value:
            raise ValueError("native stat/read changed size")
        return {"code": 0, "data": base64.b64encode(buffer.raw[:count]).decode(),
                "version": version, "seconds": mtime.tv_sec, "nanos": mtime.tv_nsec}

    cluster, io = ptr(), ptr()
    check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
    try:
        check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        check(lib.rados_connect(cluster))
        check(lib.rados_ioctx_create(cluster, metadata["pool"].encode(), C.byref(io)))
        try:
            original = metadata["object"].encode()
            actual = snapshot(io, original)
            result = {"actual": actual}
            if request.get("placement", False):
                def ceph(*args):
                    return json.loads(subprocess.check_output(["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test", "-k", str(root / "keyring"), *args, "--format", "json"], timeout=10))
                mapped = ceph("osd", "map", metadata["pool"], metadata["object"])
                topology = ceph("osd", "dump")
                primary = mapped["acting_primary"]
                daemon = next(o for o in topology["osds"] if o["osd"] == primary)
                endpoint = next(a for a in daemon["public_addrs"]["addrvec"] if a["type"] == "v2")
                result["placement"] = {"pool_id": mapped["pool_id"], "object": mapped["objname"], "osd_id": primary, "epoch": topology["epoch"], "hash": int(mapped["raw_pgid"].split(".")[1], 16), "address": "v2:" + endpoint["addr"] + "/" + str(endpoint["nonce"])}
            if request.get("compare", False):
                mirror = original + b"-native-mutation"
                code = lib.rados_remove(io, mirror)
                if code != -2:
                    check(code)
                if request["exists"]:
                    baseline = base64.b64decode(request["baseline"], validate=True)
                    seed = lib.rados_create_write_op()
                    try:
                        lib.rados_write_op_write_full(seed, baseline, len(baseline))
                        seed_time = Timespec(1700000000, 123456789)
                        check(lib.rados_write_op_operate2(seed, io, mirror, C.byref(seed_time), 0))
                    finally:
                        lib.rados_release_write_op(seed)
                version = snapshot(io, mirror)["version"]
                op = lib.rados_create_write_op()
                perops = []
                try:
                    for action in request["operations"]:
                        code, offset, length = action["Code"], action["Offset"], action["Length"]
                        data = base64.b64decode(action.get("Data") or "", validate=True)
                        flags = action["Flags"]
                        if code == 0x2201:
                            lib.rados_write_op_write(op, data, len(data), offset)
                        elif code == 0x2202:
                            lib.rados_write_op_write_full(op, data, len(data))
                        elif code == 0x2206:
                            lib.rados_write_op_append(op, data, len(data))
                        elif code == 0x2203:
                            lib.rados_write_op_truncate(op, offset)
                        elif code == 0x2204:
                            lib.rados_write_op_zero(op, offset, length)
                        elif code == 0x220d:
                            lib.rados_write_op_create(op, flags & 1, None)
                        elif code == 0x2205:
                            lib.rados_write_op_remove(op)
                        elif code == 0x1208:
                            lib.rados_write_op_assert_version(op, version + request["version_delta"])
                        elif code == 0x1220:
                            rval = integer(999)
                            perops.append(rval)
                            lib.rados_write_op_cmpext(op, data, len(data), offset, C.byref(rval))
                        else:
                            raise ValueError("unsupported oracle opcode")
                        if flags:
                            lib.rados_write_op_set_flags(op, flags)
                    mtime = Timespec(request["seconds"], request["nanos"])
                    result["code"] = lib.rados_write_op_operate2(op, io, mirror, C.byref(mtime), 0)
                    result["comparisons"] = [v.value for v in perops]
                finally:
                    lib.rados_release_write_op(op)
                result["native"] = snapshot(io, mirror)
            temporary = out / "osd-mutation-result.tmp"
            temporary.write_text(json.dumps(result) + "\n")
            os.replace(temporary, out / "osd-mutation-result.json")
        finally:
            lib.rados_ioctx_destroy(io)
    finally:
        lib.rados_shutdown(cluster)


if __name__ == "__main__":
    collect(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
