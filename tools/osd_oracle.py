#!/usr/bin/env python3
"""Test-only native stat and placement oracle for the explicit legacy OSD fixture."""

import ctypes
import hashlib
import json
import os
import pathlib
import re
import sys


class Timespec(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]


def native_stat(root, pool, object_name):
    # Independent native API preserves nanoseconds, unlike the Python binding's
    # time_t stat. Fixed Tentacle librados.h: rados_create2 (446), stat2 (1828).
    # https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados/librados.h#L1828
    lib = ctypes.CDLL("librados.so.2")
    ptr, char, integer = ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int
    definitions = {
        "rados_create2": ([ctypes.POINTER(ptr), char, char, ctypes.c_uint64], integer),
        "rados_conf_read_file": ([ptr, char], integer),
        "rados_conf_set": ([ptr, char, char], integer),
        "rados_connect": ([ptr], integer),
        "rados_ioctx_create": ([ptr, char, ctypes.POINTER(ptr)], integer),
        "rados_stat2": ([ptr, char, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(Timespec)], integer),
        "rados_ioctx_destroy": ([ptr], None),
        "rados_shutdown": ([ptr], None),
    }
    for name, (args, result) in definitions.items():
        function = getattr(lib, name)
        function.argtypes, function.restype = args, result

    def check(result):
        if result < 0:
            raise RuntimeError("Native OSD stat oracle failed with code " + str(result))

    cluster, ioctx = ptr(), ptr()
    check(lib.rados_create2(ctypes.byref(cluster), b"ceph", b"client.test", 0))
    try:
        check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        check(lib.rados_connect(cluster))
        check(lib.rados_ioctx_create(cluster, pool.encode(), ctypes.byref(ioctx)))
        try:
            size, mtime = ctypes.c_uint64(), Timespec()
            check(lib.rados_stat2(ioctx, object_name.encode(), ctypes.byref(size), ctypes.byref(mtime)))
            if size.value > 1024 * 1024 or mtime.tv_sec < 0 or not 0 <= mtime.tv_nsec < 10**9:
                raise ValueError("Native OSD stat exceeds fixture metadata bounds")
            return size.value, mtime.tv_sec, mtime.tv_nsec
        finally:
            lib.rados_ioctx_destroy(ioctx)
    finally:
        lib.rados_shutdown(cluster)


def collect(root, output, pool, object_name):
    mapped = json.loads((root / "osd.object-map.json").read_bytes())
    topology = json.loads((root / "osd.dump.json").read_bytes())
    crush = json.loads((root / "osd.crush.json").read_bytes())
    raw_pg, pg = mapped["raw_pgid"], mapped["pgid"]
    pattern = r"([0-9]+)\.([0-9a-f]+)"
    raw, actual = re.fullmatch(pattern, raw_pg), re.fullmatch(pattern, pg)
    if not raw or not actual or raw[1] != actual[1] or int(raw[2], 16) >= 1 << 32:
        raise ValueError("Invalid native PG metadata")
    pool_id = int(raw[1])
    descriptor = next(p for p in topology["pools"] if p["pool"] == pool_id)
    osd = next(o for o in topology["osds"] if o["osd"] == 0)
    addresses = osd["public_addrs"]["addrvec"]
    legacy_tunables = {"choose_local_tries": 2, "choose_local_fallback_tries": 5,
                       "choose_total_tries": 19, "chooseleaf_descend_once": 0,
                       "chooseleaf_vary_r": 0, "chooseleaf_stable": 0,
                       "straw_calc_version": 0}
    if (mapped["pool"] != pool or mapped["pool_id"] != pool_id or
            mapped["objname"] != object_name or mapped["epoch"] != topology["epoch"] or
            descriptor["pool_name"] != pool or
            mapped["acting"] != [0] or mapped["up"] != [0] or
            descriptor["size"] != 1 or descriptor["min_size"] != 1 or
            descriptor["pg_num"] != 1 or descriptor["pg_placement_num"] != 1 or
            {"hashpspool", "creating"} & set(descriptor.get("flags_names", "").split(",")) or
            topology.get("pg_upmap") or topology.get("pg_upmap_items") or topology.get("pg_upmap_primaries") or
            any(b["alg"] != "straw" for b in crush["buckets"]) or
            crush.get("choose_args") or
            any(crush["tunables"].get(name) != value for name, value in legacy_tunables.items())):
        raise ValueError("Native topology does not match the explicit legacy OSD fixture")
    if len(addresses) != 1 or addresses[0]["type"] != "v2" or addresses[0]["addr"] != "127.0.0.1:36900":
        raise ValueError("Unexpected native OSD CLIENT endpoint")
    endpoint = addresses[0]
    if type(endpoint["nonce"]) is not int or not 0 <= endpoint["nonce"] < 1 << 32:
        raise ValueError("Invalid native OSD endpoint nonce")
    size, seconds, nanos = native_stat(root, pool, object_name)
    data = (output / "osd-native-read.bin").read_bytes()
    if len(data) != size or data != (output / "osd-object.bin").read_bytes():
        raise ValueError("Independent native read/stat does not match the prepared object")
    metadata = {
        "pool": pool, "pool_id": pool_id, "object": object_name, "osd_id": 0,
        "epoch": topology["epoch"], "raw_pgid": raw_pg, "pgid": pg,
        "hash": int(raw[2], 16), "pg_seed": int(actual[2], 16),
        "address": "v2:" + endpoint["addr"] + "/" + str(endpoint["nonce"]),
        "endpoint": endpoint,
        "size": size, "mtime_seconds": seconds, "mtime_nanoseconds": nanos,
        "sha256": hashlib.sha256(data).hexdigest(),
        "legacy_tunables": crush["tunables"],
        "pool_flags": descriptor["flags"], "pool_flags_names": descriptor.get("flags_names", ""),
    }
    temporary = output / "osd-oracle.tmp"
    temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
    os.replace(temporary, output / "osd-oracle.json")


if __name__ == "__main__":
    root, output, pool, object_name = sys.argv[1:]
    collect(pathlib.Path(root), pathlib.Path(output), pool, object_name)
