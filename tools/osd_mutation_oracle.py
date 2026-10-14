#!/usr/bin/env python3
"""Disposable native librados mutation/readback oracle, never product code.

Independently invokes APIs from the pinned Tentacle librados.h (LGPL-2.1).
The native comparison object is reset before every vector. No Go wire codec
generates native requests. Only comparison reads may be repeated by tests.
"""
import base64
import ctypes as C
import json
import ipaddress
import os
import pathlib
import sys
import subprocess
import re
import time

from osd_oracle import Timespec


def collect_placement(command, pool_name, object_name, pool_id, *, attempts=5,
                      timeout=30, clock=time.monotonic):
    """Collect one internally consistent native routing observation.

    Only osd map/dump reads are repeated, and only when their epochs differ.
    One pair never combines an old acting set with a newer daemon address.
    The read budget bounds every subprocess and the total number of pairs.
    Mutation/readback requests are not replayed by this collector.
    """
    if type(attempts) is not int or not 1 <= attempts <= 5 or not 0 < timeout <= 30:
        raise ValueError("invalid native placement read budget")
    deadline = clock() + timeout

    def query(*arguments):
        remaining = deadline - clock()
        if remaining <= 0:
            raise ValueError("native placement read deadline exceeded")
        value = command(*arguments, timeout=min(10, remaining))
        if clock() > deadline:
            raise ValueError("native placement read deadline exceeded")
        if not isinstance(value, dict):
            raise ValueError("invalid native placement JSON shape")
        return value

    for _ in range(attempts):
        mapped = query("osd", "map", pool_name, object_name)
        topology = query("osd", "dump")
        epochs = (mapped.get("epoch"), topology.get("epoch"))
        if any(type(epoch) is not int or not 0 < epoch < 1 << 32 for epoch in epochs):
            raise ValueError("invalid native placement epoch")
        if (mapped.get("pool") != pool_name or mapped.get("objname") != object_name
                or type(mapped.get("pool_id")) is not int or mapped["pool_id"] != pool_id):
            raise ValueError("native placement object or pool identity changed")
        if epochs[0] != epochs[1]:
            continue
        primary = mapped.get("acting_primary")
        if type(primary) is not int or not 0 <= primary < 1 << 31:
            raise ValueError("invalid native acting primary")
        for field in ("up", "acting"):
            vector = mapped.get(field)
            if (not isinstance(vector, list) or not 1 <= len(vector) <= 64
                    or any(type(osd) is not int or not 0 <= osd < 1 << 31 for osd in vector)
                    or len(set(vector)) != len(vector)):
                raise ValueError("invalid native placement " + field + " set")
        if primary not in mapped["acting"] or primary not in mapped["up"]:
            raise ValueError("native acting primary absent from current routing sets")
        osds, pools = topology.get("osds"), topology.get("pools")
        if not isinstance(osds, list) or not isinstance(pools, list):
            raise ValueError("invalid native OSDMap topology")
        daemons = [o for o in osds if isinstance(o, dict) and type(o.get("osd")) is int and o["osd"] == primary]
        descriptors = [p for p in pools if isinstance(p, dict) and type(p.get("pool")) is int and p["pool"] == pool_id]
        if len(daemons) != 1 or len(descriptors) != 1:
            raise ValueError("native target OSD or pool is missing or ambiguous")
        daemon, pool = daemons[0], descriptors[0]
        if daemon.get("up") != 1 or daemon.get("in") != 1:
            raise ValueError("native target OSD is not up and in")
        if (pool.get("pool_name") != pool_name or pool.get("type") != 1
                or type(pool.get("size")) is not int or type(pool.get("min_size")) is not int
                or not 1 <= pool["min_size"] <= pool["size"] <= 64):
            raise ValueError("native target pool is not the expected replicated pool")
        public = daemon.get("public_addrs", {})
        if not isinstance(public, dict):
            raise ValueError("invalid native target address vector")
        addresses = public.get("addrvec", [])
        if not isinstance(addresses, list):
            raise ValueError("invalid native target address vector")
        endpoints = [a for a in addresses if isinstance(a, dict) and a.get("type") == "v2"]
        if len(endpoints) != 1:
            raise ValueError("native target must have one unambiguous v2 address")
        endpoint = endpoints[0]
        address = endpoint.get("addr")
        if (not isinstance(address, str) or len(address) > 256
                or type(endpoint.get("nonce")) is not int or not 0 <= endpoint["nonce"] < 1 << 32):
            raise ValueError("invalid native target endpoint metadata")
        host, separator, port = address.rpartition(":")
        if host.startswith("["):
            if not host.endswith("]") or ":" not in host:
                raise ValueError("invalid native target IPv6 address")
            host = host[1:-1]
        elif ":" in host:
            raise ValueError("native IPv6 endpoint requires brackets")
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            raise ValueError("invalid native target IP address") from None
        if not separator or not port.isdecimal() or not 1 <= int(port) <= 65535 or ip.is_unspecified:
            raise ValueError("invalid native target TCP address")
        raw_pg = mapped.get("raw_pgid")
        match = re.fullmatch(r"([0-9]+)\.([0-9a-f]+)", raw_pg) if isinstance(raw_pg, str) else None
        if not match or int(match[1]) != pool_id or int(match[2], 16) >= 1 << 32:
            raise ValueError("invalid native raw placement hash")
        return {"up": mapped["up"], "acting": mapped["acting"], "replicas": pool["size"],
                "min_size": pool["min_size"], "pool_id": pool_id, "object": object_name,
                "osd_id": primary, "epoch": epochs[0], "hash": int(match[2], 16),
                "address": "v2:" + address + "/" + str(endpoint["nonce"])}
    raise ValueError("native placement epochs did not converge within read budget")


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
                def ceph(*args, timeout):
                    data = subprocess.check_output(["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test", "-k", str(root / "keyring"), *args, "--format", "json"], timeout=timeout)
                    if len(data) > 1 << 20:
                        raise ValueError("native placement JSON exceeds 1 MiB")
                    return json.loads(data)
                result["placement"] = collect_placement(ceph, metadata["pool"], metadata["object"], metadata["pool_id"])
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
