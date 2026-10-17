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
import re
import time
import math

from osd_oracle import Timespec

MAX_INPUT_BYTES = 4 << 20
MAX_DATA_BYTES = 1 << 20
MAX_IDENTITY_BYTES = 4096
MAX_MON_OUTPUT_BYTES = 1 << 20
MAX_MON_DIAGNOSTIC_BYTES = 16 << 10


def fixture_identity(value, label, *, empty=False):
    if not isinstance(value, str) or "\0" in value or not empty and not value:
        raise ValueError("invalid native fixture " + label)
    if len(value.encode("utf-8")) > MAX_IDENTITY_BYTES:
        raise ValueError("native fixture " + label + " exceeds bound")
    return value


def bounded_json(path):
    with path.open("rb") as file:
        data = file.read(MAX_INPUT_BYTES + 1)
    if len(data) > MAX_INPUT_BYTES:
        raise ValueError("native mutation JSON exceeds input bound")
    return json.loads(data)


def bounded_data(encoded):
    # Existing Go []byte JSON uses null for nil; native nil/empty both length0.
    if encoded is None:
        encoded = ""
    if not isinstance(encoded, str) or len(encoded) > 4 * ((MAX_DATA_BYTES + 2) // 3):
        raise ValueError("invalid native mutation binary input")
    data = base64.b64decode(encoded, validate=True)
    if len(data) > MAX_DATA_BYTES:
        raise ValueError("native mutation binary input exceeds bound")
    return data


def validate_mutation_request(request, metadata):
    """Validate caller identity/vector before any native read or mirror reset."""
    if not isinstance(request, dict) or not isinstance(metadata, dict):
        raise ValueError("invalid native mutation JSON object")
    fixture_identity(metadata["pool"], "pool")
    if type(metadata["pool_id"]) is not int or not 0 <= metadata["pool_id"] < 1 << 63:
        raise ValueError("invalid native fixture pool ID")
    name = fixture_identity(request.get("object", metadata["object"]), "object")
    namespace = fixture_identity(request.get("namespace", ""), "namespace", empty=True)
    for field in ("compare", "placement", "exists"):
        if field in request and type(request[field]) is not bool:
            raise ValueError("invalid native mutation boolean")
    actions = request.get("operations", [])
    if not isinstance(actions, list) or len(actions) > 64 or request.get("compare", False) and not actions:
        raise ValueError("invalid native mutation operation count")
    total = 0
    supported = {0x2201, 0x2202, 0x2206, 0x2203, 0x2204, 0x220d, 0x2205, 0x1208, 0x1220}
    for action in actions:
        if not isinstance(action, dict) or type(action.get("Code")) is not int or action["Code"] not in supported:
            raise ValueError("unsupported native mutation opcode")
        for field in ("Offset", "Length"):
            if type(action.get(field)) is not int or not 0 <= action[field] < 1 << 64:
                raise ValueError("invalid native mutation extent")
        flags = action.get("Flags")
        allowed = (0, 1, 2, 3) if action["Code"] == 0x220d else (0, 2)
        if type(flags) is not int or flags not in allowed:
            raise ValueError("unsupported native mutation flags")
        data = bounded_data(action.get("Data"))
        total += len(data)
        if total > MAX_DATA_BYTES:
            raise ValueError("native mutation vector exceeds binary bound")
        if action["Code"] in (0x2201, 0x2202, 0x2206, 0x1220) and action["Length"] != len(data):
            raise ValueError("native mutation payload length differs from extent")
        if action["Code"] not in (0x2201, 0x2202, 0x2206, 0x1220) and data:
            raise ValueError("native mutation opcode does not carry data")
    if request.get("compare", False):
        if type(request.get("exists")) is not bool:
            raise ValueError("native mutation baseline existence is required")
        bounded_data(request.get("baseline"))
        for field, limit in (("seconds", 1 << 32), ("nanos", 10**9)):
            if type(request.get(field)) is not int or not 0 <= request[field] < limit:
                raise ValueError("invalid native mutation timestamp")
        delta = request.get("version_delta", 0)
        if type(delta) is not int or not -(1 << 63) <= delta < 1 << 63:
            raise ValueError("invalid native mutation version delta")
    return name, namespace


def native_mon_query(lib, cluster, *arguments, timeout):
    """Read placement through public librados, without CLI ASCII validation.

    Pinned librados.h:3910 and librados_c.cc:891-917 allocate both output
    buffers even on errors. MonClient.h:585-600 uses whole seconds; zero is
    unlimited. Bounds reject oversized native allocations before copying.
    """
    if arguments == ("osd", "dump"):
        command = {"prefix": "osd dump", "format": "json"}
    elif len(arguments) in (4, 5) and arguments[:2] == ("osd", "map"):
        command = {"prefix": "osd map", "format": "json",
                   "pool": fixture_identity(arguments[2], "pool"),
                   "object": fixture_identity(arguments[3], "object")}
        if len(arguments) == 5:
            command["nspace"] = fixture_identity(arguments[4], "namespace", empty=True)
    else:
        raise ValueError("unsupported native placement MON query")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 1:
        raise ValueError("native placement read deadline below one whole second")
    seconds = math.floor(min(10, timeout))
    code = lib.rados_conf_set(cluster, b"rados_mon_op_timeout", str(seconds).encode())
    if code < 0:
        raise RuntimeError("native placement timeout setup failed: " + str(code))
    encoded = json.dumps(command, ensure_ascii=False).encode("utf-8")
    commands = (C.c_char_p * 1)(encoded)
    output, diagnostic = C.c_void_p(), C.c_void_p()
    output_length, diagnostic_length = C.c_size_t(), C.c_size_t()
    try:
        code = lib.rados_mon_command(cluster, commands, 1, None, 0,
                                     C.byref(output), C.byref(output_length),
                                     C.byref(diagnostic), C.byref(diagnostic_length))
        if output_length.value > MAX_MON_OUTPUT_BYTES or diagnostic_length.value > MAX_MON_DIAGNOSTIC_BYTES:
            raise ValueError("native placement MON output exceeds bounded allocation receipt")
        if (output_length.value and not output.value or diagnostic_length.value and not diagnostic.value):
            raise ValueError("native placement MON returned a missing buffer")
        if code < 0:
            # Never copy native diagnostics or authentication material to receipts.
            raise RuntimeError("native placement MON query failed: " + str(code))
        value = json.loads(C.string_at(output, output_length.value))
        if not isinstance(value, dict):
            raise ValueError("invalid native placement JSON shape")
        return value
    finally:
        if output.value:
            lib.rados_buffer_free(output)
        if diagnostic.value:
            lib.rados_buffer_free(diagnostic)


def collect_placement(command, pool_name, object_name, pool_id, *, attempts=5,
                      timeout=30, clock=time.monotonic, namespace="", target_osd=None):
    """Collect one internally consistent native routing observation.

    Only osd map/dump reads are repeated, and only when their epochs differ.
    One pair never combines an old acting set with a newer daemon address.
    The read budget bounds every native query and the total number of pairs.
    Mutation/readback requests are not replayed by this collector.
    """
    if type(attempts) is not int or not 1 <= attempts <= 5 or not 0 < timeout <= 30:
        raise ValueError("invalid native placement read budget")
    fixture_identity(pool_name, "pool")
    fixture_identity(object_name, "object")
    fixture_identity(namespace, "namespace", empty=True)
    if type(pool_id) is not int or not 0 <= pool_id < 1 << 63:
        raise ValueError("invalid native placement pool ID")
    # Pinned MonCommands.h:650: osd map pool object [nspace]. OSDMonitor
    # :5918-5928 emits namespace/object as objname, not the bare object name.
    displayed_object = namespace + "/" + object_name if namespace else object_name
    map_arguments = ("osd", "map", pool_name, object_name) + ((namespace,) if namespace else ())
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
        mapped = query(*map_arguments)
        topology = query("osd", "dump")
        epochs = (mapped.get("epoch"), topology.get("epoch"))
        if any(type(epoch) is not int or not 0 < epoch < 1 << 32 for epoch in epochs):
            raise ValueError("invalid native placement epoch")
        if (mapped.get("pool") != pool_name or mapped.get("objname") != displayed_object
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
        # A caller may name another member of the acting set as the connection
        # target (replica reads); the primary is reported separately.
        target = primary
        if target_osd is not None:
            if type(target_osd) is not int or target_osd not in mapped["acting"]:
                raise ValueError("requested target OSD is not in the acting set")
            target = target_osd
        osds, pools = topology.get("osds"), topology.get("pools")
        if not isinstance(osds, list) or not isinstance(pools, list):
            raise ValueError("invalid native OSDMap topology")
        daemons = [o for o in osds if isinstance(o, dict) and type(o.get("osd")) is int and o["osd"] == target]
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
        mapped_pg = mapped.get("pgid")
        pg_match = re.fullmatch(r"([0-9]+)\.([0-9a-f]+)", mapped_pg) if isinstance(mapped_pg, str) else None
        if not pg_match or int(pg_match[1]) != pool_id or int(pg_match[2], 16) >= 1 << 32:
            raise ValueError("invalid native mapped placement group")
        return {"up": mapped["up"], "acting": mapped["acting"], "replicas": pool["size"],
                "min_size": pool["min_size"], "pool_id": pool_id, "object": object_name,
                "osd_id": target, "primary_id": primary, "epoch": epochs[0], "hash": int(match[2], 16),
                "namespace": namespace, "raw_pgid": raw_pg, "pgid": mapped_pg,
                "pg_seed": int(pg_match[2], 16),
                "address": "v2:" + address + "/" + str(endpoint["nonce"])}
    raise ValueError("native placement epochs did not converge within read budget")


def collect(root, out):
    request = bounded_json(out / "osd-mutation-input.json")
    metadata = bounded_json(out / "osd-oracle.json")
    object_name, namespace = validate_mutation_request(request, metadata)
    lib = C.CDLL("librados.so.2")
    ptr, char, integer, u64, size = C.c_void_p, C.c_char_p, C.c_int, C.c_uint64, C.c_size_t
    defs = {
        "create2": ([C.POINTER(ptr), char, char, u64], integer),
        "conf_read_file": ([ptr, char], integer),
        "conf_set": ([ptr, char, char], integer),
        "mon_command": ([ptr, C.POINTER(char), size, char, size,
                         C.POINTER(ptr), C.POINTER(size), C.POINTER(ptr), C.POINTER(size)], integer),
        "buffer_free": ([ptr], None),
        "connect": ([ptr], integer),
        "ioctx_create": ([ptr, char, C.POINTER(ptr)], integer),
        "ioctx_set_namespace": ([ptr, char], None),
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
            lib.rados_ioctx_set_namespace(io, namespace.encode())
            original = object_name.encode()
            actual = snapshot(io, original)
            result = {"actual": actual}
            if request.get("placement", False):
                def command(*args, timeout):
                    return native_mon_query(lib, cluster, *args, timeout=timeout)
                result["placement"] = collect_placement(command, metadata["pool"], object_name,
                                                         metadata["pool_id"], namespace=namespace)
            if request.get("compare", False):
                mirror = original + b"-native-mutation"
                code = lib.rados_remove(io, mirror)
                if code != -2:
                    check(code)
                if request["exists"]:
                    baseline = bounded_data(request.get("baseline"))
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
                        data = bounded_data(action.get("Data"))
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
                            target_version = version + request.get("version_delta", 0)
                            if not 0 <= target_version < 1 << 64:
                                raise ValueError("native comparison version exceeds u64")
                            lib.rados_write_op_assert_version(op, target_version)
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
