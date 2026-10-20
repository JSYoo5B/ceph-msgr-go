#!/usr/bin/env python3
"""Test-only independent Tentacle rados_checksum oracle for owned fixture objects.

Pinned Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964,
include/rados/librados.h:1585-1618 and librados/librados_c.cc:1444-1474
(LGPL-2.1; public header LGPL-2.1 or LGPL-3.0). No implementation copied.
Only reads the exclusive OMAP-write fixture ownership set. Native librados may
route/retry read operations; this does not add Go routing or replay behavior.
"""
import argparse
import base64
import ctypes as C
from pathlib import Path
import struct

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-checksum"
MAX_READ = 128 << 10
WIDTHS = {0: 4, 1: 8, 2: 4}  # Tentacle XXHASH32, XXHASH64, CRC32C.


def validate_request(raw):
    fields = {"object", "offset", "length", "chunk_size", "seed", "type"}
    if not isinstance(raw, dict) or set(raw) not in (fields, fields | {"snap"}):
        raise ValueError("invalid checksum oracle fields")
    request = {"object": xattr.identity(raw["object"], "owned object")}
    algorithm = omap.integer(raw["type"], 2, "type")
    for field, maximum in (("offset", (1 << 64)-1), ("length", MAX_READ), ("chunk_size", MAX_READ), ("seed", (1 << (8*WIDTHS[algorithm]))-1), ("type", 2)):
        request[field] = omap.integer(raw[field], maximum, field)
    if not request["length"] or request["offset"] + request["length"] >= 1 << 64:
        raise ValueError("only bounded checksum reads are supported")
    chunk = request["chunk_size"]
    if chunk and request["length"] % chunk:
        raise ValueError("unaligned checksum request")
    if "snap" in raw:
        request["snap"] = omap.integer(raw["snap"], (1 << 64)-3, "snap")
        if not request["snap"]:
            raise ValueError("omit snap for a head read")
    return request


class Native(xattr.Native):
    def __init__(self, library=None):
        super().__init__(library)
        fn = self.lib.rados_checksum
        fn.argtypes = [C.c_void_p, C.c_char_p, C.c_int, C.c_char_p, C.c_size_t,
                       C.c_size_t, C.c_uint64, C.c_size_t, C.c_void_p, C.c_size_t]
        fn.restype = C.c_int
        self.lib.rados_ioctx_snap_set_read.argtypes = [C.c_void_p, C.c_uint64]
        self.lib.rados_ioctx_snap_set_read.restype = None

    def selected_checksum(self, io, request):
        if "snap" not in request:
            return self.checksum(io, request)
        self.lib.rados_ioctx_snap_set_read(io, request["snap"])
        try:
            before = self.snapshot(io, request["object"], [])
            result = self.checksum(io, request)
            after = self.snapshot(io, request["object"], [])
            if before != after:
                raise ValueError("native checksum changed selected snapshot state")
            result.update(read_snap=request["snap"], selected_before=before, selected_after=after)
            return result
        finally:
            self.lib.rados_ioctx_snap_set_read(io, writes.SNAP_HEAD)

    def checksum(self, io, request):
        chunk, length = request["chunk_size"], request["length"]
        width = WIDTHS[request["type"]]
        value_format = "Q" if width == 8 else "I"
        count = length // chunk if chunk else 1
        size = 4 + width*count
        sentinel = bytes([0xcc])*size
        output = C.create_string_buffer(sentinel, size)
        code = self.lib.rados_checksum(io, request["object"], request["type"], struct.pack("<"+value_format, request["seed"]), width,
                                       length, request["offset"], chunk, output, size)
        raw = output.raw
        if code < 0 or raw == sentinel:
            data = b""
        else:
            observed = struct.unpack_from("<I", raw)[0]
            if observed > count:
                raise ValueError("native checksum count exceeds receipt bound")
            data = raw[:4+width*observed]
        return {"code": code, "data": base64.b64encode(data).decode(),
                "values": list(struct.unpack_from("<"+value_format*((len(data)-4)//width), data, 4)) if data else []}


def collect(root, out):
    request = validate_request(omap.read_json(out / (NAME+"-input.json")))
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("checksum oracle requires pinned Tentacle")
    metadata = omap.read_json(out / "osd-oracle.json")
    names = writes.check_ownership(omap.read_json(out / (writes.OWNED+".json")), metadata)
    if request["object"] not in names:
        raise ValueError("checksum object is outside fixture ownership")
    native = Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        before = native.snapshot(io, request["object"], [])
        result = native.selected_checksum(io, request)
        after = native.snapshot(io, request["object"], [])
    if before != after:
        raise ValueError("native checksum changed independently observed object state")
    result.update(source_commit=xattr.SOURCE_COMMIT, object=request["object"].decode(),
                  before=before, after=after, native_client_may_resend=True)
    omap.publish(out, NAME+"-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    collect(args.root, args.out)
