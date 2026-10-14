#!/usr/bin/env python3
"""Read-only native before/after observations for mixed OSD vector tests.

This oracle does NOT reproduce a mixed read/write vector. Tentacle's librados
C write_op API cannot append READ/STAT operations. It independently observes
the committed object through librados stat2/read and its user version. The Go
test separately checks real server vector outputs against pinned PrimaryLogPG
semantics. No self-generated Go request or reply bytes enter this oracle.

API provenance: Ceph 20.2.4, commit
7f793731f1b39eb4f465e960113d2363c311b964, src/include/rados/librados.h
(LGPL-2.1); this ctypes adapter is independently written test-only code.
"""

import base64
import ctypes as C
import json
import os
import pathlib
import sys

from osd_oracle import Timespec


def collect(root, out):
    metadata = json.loads((out / "osd-oracle.json").read_bytes())
    request = json.loads((out / "osd-compound-input.json").read_bytes())
    label = request["label"]
    if label not in ("before", "after"):
        raise ValueError("invalid compound observation phase")
    lib = C.CDLL("librados.so.2")
    ptr, char, integer = C.c_void_p, C.c_char_p, C.c_int
    u64, size = C.c_uint64, C.c_size_t
    definitions = {
        "create2": ([C.POINTER(ptr), char, char, u64], integer),
        "conf_read_file": ([ptr, char], integer),
        "conf_set": ([ptr, char, char], integer),
        "connect": ([ptr], integer),
        "ioctx_create": ([ptr, char, C.POINTER(ptr)], integer),
        "ioctx_destroy": ([ptr], None), "shutdown": ([ptr], None),
        "stat2": ([ptr, char, C.POINTER(u64), C.POINTER(Timespec)], integer),
        "get_last_version": ([ptr], u64),
        "read": ([ptr, char, ptr, size, u64], integer),
    }
    for name, (arguments, result) in definitions.items():
        function = getattr(lib, "rados_" + name)
        function.argtypes, function.restype = arguments, result

    def check(code):
        if code < 0:
            raise RuntimeError("native compound observation failed: " + str(code))

    cluster, io = ptr(), ptr()
    check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
    try:
        check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        check(lib.rados_connect(cluster))
        check(lib.rados_ioctx_create(cluster, metadata["pool"].encode(), C.byref(io)))
        try:
            name = metadata["object"].encode()
            length, mtime = u64(), Timespec()
            code = lib.rados_stat2(io, name, C.byref(length), C.byref(mtime))
            if code == -2:
                snapshot = {"code": code, "data": "", "version": 0}
            else:
                check(code)
                if (length.value > 1 << 20 or not 0 <= mtime.tv_sec < 1 << 32
                        or not 0 <= mtime.tv_nsec < 10**9):
                    raise ValueError("native compound observation exceeds bounds")
                version = lib.rados_get_last_version(io)
                buffer = C.create_string_buffer(length.value + 1)
                count = lib.rados_read(io, name, buffer, length.value, 0)
                check(count)
                if count != length.value or lib.rados_get_last_version(io) != version:
                    raise ValueError("native stat/read changed during observation")
                snapshot = {"code": 0, "data": base64.b64encode(buffer.raw[:count]).decode(),
                            "version": version, "seconds": mtime.tv_sec,
                            "nanos": mtime.tv_nsec}
            result = {"label": label, "snapshot": snapshot}
            temporary = out / "osd-compound-result.tmp"
            temporary.write_text(json.dumps(result) + "\n")
            os.replace(temporary, out / "osd-compound-result.json")
        finally:
            lib.rados_ioctx_destroy(io)
    finally:
        lib.rados_shutdown(cluster)


if __name__ == "__main__":
    collect(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
