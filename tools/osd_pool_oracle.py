#!/usr/bin/env python3
"""Closed, disposable-fixture native oracle for extra owned replicated pools.

Test-only; never product code. It uses the public librados C API (LGPL-2.1,
Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964) through ctypes
and creates at most two pools named ceph-msgr-owned-pool-1/-2 so that tests can
observe cross-pool, namespace and pool-snapshot behavior independently of the
Go client. Modes: create, seed, observe, place, snapshot, selfmanaged, quota, poolinfo, clusterstat, cleanup. Every mode
reads one bounded JSON input and writes one bounded JSON result; native
mutations are never retried and credentials/keys are never copied.
"""
import base64
import ctypes as C
import json
import os
import pathlib
import sys

import osd_mutation_oracle as mutation
import osd_xattr_native as xattr
from osd_oracle import Timespec

SOURCE_COMMIT = xattr.SOURCE_COMMIT
MODES = ("create", "seed", "observe", "place", "snapshot", "selfmanaged", "quota", "poolinfo", "clusterstat", "cleanup")
SLOTS = (1, 2)
MAX_OUTPUT = 512 << 10
MAX_MON_OUTPUT = 4 << 20
FIXED_MTIME = Timespec(1700000500, 123456789)


def pool_name(slot):
    if type(slot) is not int or slot not in SLOTS:
        raise ValueError("pool slot must be 1 or 2")
    return "ceph-msgr-owned-pool-%d" % slot


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("invalid " + label)
    return value


def b64(value, limit):
    return xattr.binary(value, limit)


def read_input(out):
    return xattr.read_json_bounded(out / "osd-pool-input.json")


def publish(out, result):
    data = json.dumps(result).encode() + b"\n"
    if len(data) > MAX_OUTPUT:
        raise ValueError("pool oracle output exceeds 512 KiB")
    temporary = out / "osd-pool-result.tmp"
    temporary.write_bytes(data)
    os.replace(temporary, out / "osd-pool-result.json")


class ClusterStat(C.Structure):
    _fields_ = [("kb", C.c_uint64), ("kb_used", C.c_uint64), ("kb_avail", C.c_uint64), ("num_objects", C.c_uint64)]


class Rados:
    def __init__(self, root, native):
        self.native, self.lib, self.root = native, native.lib, root
        p, i, u64, char, s = C.c_void_p, C.c_int, C.c_uint64, C.c_char_p, C.c_size_t
        for name, (args, result) in {
            "mon_command": ([p, C.POINTER(char), s, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)], i),
            "buffer_free": ([p], None),
            "ioctx_snap_create": ([p, char], i),
            "ioctx_snap_remove": ([p, char], i),
            "ioctx_snap_lookup": ([p, char, C.POINTER(u64)], i),
            "ioctx_snap_set_read": ([p, u64], None),
            "cluster_stat": ([p, C.POINTER(ClusterStat)], i),
            "ioctx_selfmanaged_snap_create": ([p, C.POINTER(u64)], i),
            "ioctx_selfmanaged_snap_remove": ([p, u64], i),
            "write_full": ([p, char, p, s], i),
        }.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = args, result

    def connect(self, timeout=45):
        cluster = C.c_void_p()
        self.native.check(self.lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
        try:
            self.native.check(self.lib.rados_conf_read_file(cluster, os.fsencode(self.root / "ceph.conf")))
            self.native.check(self.lib.rados_conf_set(cluster, b"keyring", os.fsencode(self.root / "keyring")))
            self.native.check(self.lib.rados_conf_set(cluster, b"rados_osd_op_timeout", str(timeout).encode()))
            self.native.check(self.lib.rados_conf_set(cluster, b"rados_mon_op_timeout", b"30"))
            self.native.check(self.lib.rados_connect(cluster))
        except BaseException:
            self.lib.rados_shutdown(cluster)
            raise
        return cluster

    def mon(self, cluster, command):
        encoded = json.dumps(command, ensure_ascii=False).encode("utf-8")
        commands = (C.c_char_p * 1)(encoded)
        output, diagnostic = C.c_void_p(), C.c_void_p()
        output_length, diagnostic_length = C.c_size_t(), C.c_size_t()
        try:
            code = self.lib.rados_mon_command(cluster, commands, 1, None, 0, C.byref(output), C.byref(output_length),
                                              C.byref(diagnostic), C.byref(diagnostic_length))
            if output_length.value > MAX_MON_OUTPUT or diagnostic_length.value > 64 << 10:
                raise ValueError("native MON output exceeds bound")
            text = C.string_at(output, output_length.value) if output.value else b""
            # The MON's status text explains refusals; it holds no credentials.
            self.last_status = C.string_at(diagnostic, diagnostic_length.value).decode(errors="replace")[:512] if diagnostic.value else ""
            return code, text
        finally:
            if output.value:
                self.lib.rados_buffer_free(output)
            if diagnostic.value:
                self.lib.rados_buffer_free(diagnostic)

    def ioctx(self, cluster, name, namespace=""):
        io = C.c_void_p()
        self.native.check(self.lib.rados_ioctx_create(cluster, name.encode(), C.byref(io)))
        self.lib.rados_ioctx_set_namespace(io, namespace.encode())
        return io

    def dump(self, cluster):
        code, text = self.mon(cluster, {"prefix": "osd dump", "format": "json"})
        if code < 0:
            raise RuntimeError("native osd dump failed: %d" % code)
        return json.loads(text)


def find_pool(dump, name):
    matches = [p for p in dump["pools"] if p.get("pool_name") == name]
    if len(matches) > 1:
        raise ValueError("ambiguous native pool")
    return matches[0] if matches else None


EC_PROFILE = "ceph-msgr-owned-ec"


def mode_create(rados, cluster, request, out):
    name = pool_name(request["slot"])
    pg = integer(request["pg_num"], 1, 32, "pg_num")
    erasure = request.get("erasure", False)
    if type(erasure) is not bool:
        raise ValueError("erasure must be boolean")
    hashpspool = request["hashpspool"]
    if type(hashpspool) is not bool:
        raise ValueError("hashpspool must be boolean")
    if find_pool(rados.dump(cluster), name):
        raise RuntimeError("owned pool already exists")
    if erasure:
        # k=2, m=1 over three OSDs with an OSD failure domain; overwrites need
        # BlueStore. The MON derives size 3 and min_size k+1 itself.
        size, minimum = 3, None
        profile = ["k=2", "m=1", "crush-failure-domain=osd"]
        steps = [{"prefix": "osd erasure-code-profile set", "name": EC_PROFILE, "profile": profile, "force": True},
                 {"prefix": "osd pool create", "pool": name, "pg_num": pg, "pgp_num": pg, "pool_type": "erasure", "erasure_code_profile": EC_PROFILE},
                 {"prefix": "osd pool set", "pool": name, "var": "allow_ec_overwrites", "val": "true"}]
    else:
        size = integer(request["size"], 1, 3, "size")
        minimum = integer(request["min_size"], 1, size, "min_size")
        steps = [{"prefix": "osd pool create", "pool": name, "pg_num": pg, "pgp_num": pg, "pool_type": "replicated"},
                 {"prefix": "osd pool set", "pool": name, "var": "size", "val": str(size), "yes_i_really_mean_it": True},
                 {"prefix": "osd pool set", "pool": name, "var": "min_size", "val": str(minimum)}]
    for command in steps:
        code, _ = rados.mon(cluster, command)
        if code < 0:
            raise RuntimeError("native pool step %s failed: %d %s" % (command["prefix"], code, rados.last_status))
    io = rados.ioctx(cluster, name)
    try:
        # A write blocks until every PG is active; a bounded timeout fails it.
        probe = b"ceph-msgr-owned-pool-probe"
        data = C.create_string_buffer(b"p", 2)
        rados.native.check(rados.lib.rados_write_full(io, probe, data, 1))
        rados.native.check(rados.lib.rados_remove(io, probe))
    finally:
        rados.lib.rados_ioctx_destroy(io)
    pool = find_pool(rados.dump(cluster), name)
    if minimum is None and pool is not None:
        minimum = pool["min_size"]
    if pool is None or bool(pool["flags"] & 1) != hashpspool or pool["size"] != size or pool["min_size"] != minimum or pool["pg_num"] != pg or (pool["type"] == 3) != erasure:
        raise RuntimeError("native pool does not match the requested closed shape")
    rule = None
    code, text = rados.mon(cluster, {"prefix": "osd crush rule dump", "format": "json"})
    if code == 0:
        rules = [r for r in json.loads(text) if r.get("rule_id") == pool.get("crush_rule")]
        rule = rules[0] if rules else None
    return {"pool": name, "pool_id": pool["pool"], "flags": pool["flags"], "size": size, "min_size": minimum,
            "pg_num": pg, "epoch": rados.dump(cluster)["epoch"], "pool_type": pool["type"], "crush_rule": rule}


def seed_object(rados, io, name, data, attributes):
    lib, native = rados.lib, rados.native
    code = lib.rados_remove(io, name)
    if code not in (0, -2):
        native.check(code)
    op = lib.rados_create_write_op()
    if not op:
        raise RuntimeError("native seed allocation failed")
    try:
        buffer = C.create_string_buffer(data, len(data) + 1)
        lib.rados_write_op_write_full(op, buffer, len(data))
        actions = [dict(op="set", name=n, value=v, flags=0) for n, v in attributes]
        _, buffers = native.append(op, "write", actions)
        stamp = Timespec(FIXED_MTIME.tv_sec, FIXED_MTIME.tv_nsec)
        native.check(lib.rados_write_op_operate2(op, io, name, C.byref(stamp), 0))
    finally:
        lib.rados_release_write_op(op)


def object_request(request):
    name = xattr.identity(request["object"], "object")
    namespace = request.get("namespace", "")
    xattr.identity(namespace, "namespace", empty=True)
    return name, namespace


def mode_seed(rados, cluster, request, out):
    pool = pool_name(request["slot"])
    name, namespace = object_request(request)
    data = b64(request["data"], 1 << 20)
    attributes = []
    for item in request["attributes"]:
        if set(item) != {"name", "value"}:
            raise ValueError("invalid seed attribute")
        attributes.append((xattr.checked_name(b64(item["name"], xattr.MAX_NAME)), b64(item["value"], xattr.MAX_VALUE)))
    if len(attributes) > 16:
        raise ValueError("too many seed attributes")
    io = rados.ioctx(cluster, pool, namespace)
    try:
        seed_object(rados, io, name, data, sorted(attributes))
        return {"pool": pool, "object": request["object"], "state": rados.native.snapshot(io, name, [])}
    finally:
        rados.lib.rados_ioctx_destroy(io)


def mode_observe(rados, cluster, request, out):
    pool = pool_name(request["slot"])
    name, namespace = object_request(request)
    io = rados.ioctx(cluster, pool, namespace)
    try:
        snap_id = request.get("snap_id")
        if snap_id:
            rados.lib.rados_ioctx_snap_set_read(io, integer(snap_id, 1, (1 << 64) - 3, "snap_id"))
        snap = request.get("snap")
        if snap:
            snap_id = C.c_uint64()
            rados.native.check(rados.lib.rados_ioctx_snap_lookup(io, snap.encode(), C.byref(snap_id)))
            rados.lib.rados_ioctx_snap_set_read(io, snap_id.value)
        return {"pool": pool, "object": request["object"], "state": rados.native.snapshot(io, name, [])}
    finally:
        rados.lib.rados_ioctx_destroy(io)


def mode_place(rados, cluster, request, out):
    pool = pool_name(request["slot"])
    name, namespace = object_request(request)
    dump = rados.dump(cluster)
    descriptor = find_pool(dump, pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    target = request.get("osd")
    return {"placement": mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace, target_osd=target)}


def mode_snapshot(rados, cluster, request, out):
    pool = pool_name(request["slot"])
    snap = request["name"]
    xattr.identity(snap, "snapshot")
    action = request["action"]
    if action not in ("create", "remove"):
        raise ValueError("invalid pool snapshot action")
    io = rados.ioctx(cluster, pool)
    try:
        function = rados.lib.rados_ioctx_snap_create if action == "create" else rados.lib.rados_ioctx_snap_remove
        rados.native.check(function(io, snap.encode()))
        result = {"pool": pool, "name": snap, "action": action}
        if action == "create":
            snap_id = C.c_uint64()
            rados.native.check(rados.lib.rados_ioctx_snap_lookup(io, snap.encode(), C.byref(snap_id)))
            result["snap_id"] = snap_id.value
        return result
    finally:
        rados.lib.rados_ioctx_destroy(io)


def mode_selfmanaged(rados, cluster, request, out):
    pool = pool_name(request["slot"])
    action = request["action"]
    io = rados.ioctx(cluster, pool)
    try:
        if action == "create":
            snap_id = C.c_uint64()
            code = rados.lib.rados_ioctx_selfmanaged_snap_create(io, C.byref(snap_id))
            rados.native.check(code)
            return {"pool": pool, "action": action, "snap_id": snap_id.value}
        if action == "remove":
            snap = integer(request["snap_id"], 1, (1 << 64) - 3, "snap_id")
            rados.native.check(rados.lib.rados_ioctx_selfmanaged_snap_remove(io, snap))
            return {"pool": pool, "action": action, "snap_id": snap}
        raise ValueError("invalid self-managed snapshot action")
    finally:
        rados.lib.rados_ioctx_destroy(io)


def mode_clusterstat(rados, cluster, request, out):
    # rados_cluster_stat uses the same MStatfs request as the Go client's Statfs.
    stat = ClusterStat()
    rados.native.check(rados.lib.rados_cluster_stat(cluster, C.byref(stat)))
    return {"kb": stat.kb, "kb_used": stat.kb_used, "kb_avail": stat.kb_avail, "num_objects": stat.num_objects}


def mode_quota(rados, cluster, request, out):
    name = pool_name(request["slot"])
    limit = integer(request["max_objects"], 0, 1 << 20, "max_objects")
    code, _ = rados.mon(cluster, {"prefix": "osd pool set-quota", "pool": name, "field": "max_objects", "val": str(limit)})
    if code < 0:
        raise RuntimeError("native quota change failed: %d" % code)
    return {"pool": name, "max_objects": limit}


def mode_poolinfo(rados, cluster, request, out):
    name = pool_name(request["slot"])
    dump = rados.dump(cluster)
    pool = find_pool(dump, name)
    if pool is None:
        raise RuntimeError("owned pool is missing")
    return {"pool": name, "pool_id": pool["pool"], "flags": pool["flags"], "flag_names": pool.get("flags_names", ""),
            "epoch": dump["epoch"], "quota_max_objects": pool.get("quota_max_objects"),
            "snap_seq": pool.get("snap_seq"), "snap_mode": pool.get("snap_mode"), "pool_snaps": pool.get("pool_snaps", []),
            "removed_snaps": pool.get("removed_snaps"), "removed_snaps_queue": pool.get("removed_snaps_queue", [])}


def mode_cleanup(rados, cluster, request, out):
    removed = []
    code, _ = rados.mon(cluster, {"prefix": "config set", "who": "mon", "name": "mon_allow_pool_delete", "value": "true"})
    if code < 0:
        raise RuntimeError("native pool deletion could not be enabled: %d" % code)
    for slot in SLOTS:
        name = pool_name(slot)
        if find_pool(rados.dump(cluster), name) is None:
            removed.append({"pool": name, "existed": False})
            continue
        code, _ = rados.mon(cluster, {"prefix": "osd pool rm", "pool": name, "pool2": name, "yes_i_really_really_mean_it": True})
        if code < 0:
            raise RuntimeError("native pool removal failed: %d" % code)
        removed.append({"pool": name, "existed": True})
    # The owned erasure-code profile is unused once its pools are gone.
    rados.mon(cluster, {"prefix": "osd erasure-code-profile rm", "name": EC_PROFILE})
    gone = all(find_pool(rados.dump(cluster), pool_name(slot)) is None for slot in SLOTS)
    return {"removed": removed, "pools_removed": gone}


HANDLERS = {"create": mode_create, "seed": mode_seed, "observe": mode_observe, "place": mode_place,
            "snapshot": mode_snapshot, "selfmanaged": mode_selfmanaged, "quota": mode_quota, "poolinfo": mode_poolinfo, "clusterstat": mode_clusterstat, "cleanup": mode_cleanup}


def main(argv):
    if len(argv) != 4 or argv[1] not in MODES:
        raise SystemExit("usage: osd_pool_oracle.py MODE ROOT OUT")
    mode, root, out = argv[1], pathlib.Path(argv[2]), pathlib.Path(argv[3])
    request = {} if mode == "cleanup" else read_input(out)
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or SOURCE_COMMIT not in version:
        raise ValueError("pool oracle requires the pinned Tentacle daemon")
    rados = Rados(root, xattr.Native())
    cluster = rados.connect()
    try:
        result = HANDLERS[mode](rados, cluster, request, out)
    finally:
        rados.lib.rados_shutdown(cluster)
    result.update(schema_version=1, source_commit=SOURCE_COMMIT, mode=mode, fixture_daemon_version=version)
    publish(out, result)


if __name__ == "__main__":
    main(sys.argv)
