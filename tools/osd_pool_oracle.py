#!/usr/bin/env python3
"""Closed, disposable-fixture native oracle for extra owned replicated pools.

Test-only; never product code. It uses the public librados C API (LGPL-2.1,
Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964) through ctypes
and creates at most two pools named ceph-msgr-owned-pool-1/-2 so that tests can
observe cross-pool, namespace and pool-snapshot behavior independently of the
Go client. create also makes an erasure-coded pool of a closed width: k2m1
(profile ceph-msgr-owned-ec) or k2m2 (its own ceph-msgr-owned-ec-k2m2 profile,
isa, never overwritten, optionally optimized with its nonprimary shards read
back); cleanup removes both profiles.
Modes: create, seed, observe, place, snapshot, selfmanaged, quota, poolinfo, clusterstat, versions, service, servicewatch, fullratios, osdflags, inconsistent, cleanup.
servicemetadata prepares/restores the native selftest module and reads only a
fixed get_metadata expression for the exclusive MGR service fixture.
servicecompare independently registers a native service with the Go caller's
fields, observes exact status replacements and confirms removal after shutdown.
mgrstate projects native MON MgrMap identity and flags without contacting a MGR.
servicewatch polls the native MON `service dump` and MGR `service status` until a
named service daemon matches the expected metadata/status/task status, or is
absent, and reports the last native observation; it registers nothing.
inconsistent runs the native `rados list-inconsistent-obj` for one owned-pool
PG and reports its interval and object names.
fullratios changes the cluster-wide nearfull/backfillfull/full ratios of the
disposable fixture; the caller restores the values it reported first.
osdflags sets or unsets only the norecover and nobackfill OSDMap flags; the
caller unsets them again. Every mode
reads one bounded JSON input and writes one bounded JSON result; native
mutations are never retried and credentials/keys are never copied.
"""
import base64
import ctypes as C
import json
import os
import pathlib
import re
import subprocess
import sys

import osd_mutation_oracle as mutation
import osd_xattr_native as xattr
from osd_oracle import Timespec

SOURCE_COMMIT = xattr.SOURCE_COMMIT
MODES = ("create", "seed", "observe", "place", "snapshot", "selfmanaged", "quota", "poolinfo", "clusterstat", "versions", "mgrstate", "service", "servicewatch", "servicecompare", "servicemetadata", "fullratios", "osdflags", "inconsistent", "cleanup")
RECOVERY_FLAGS = ("norecover", "nobackfill")
RATIO_COMMANDS = (("nearfull", "osd set-nearfull-ratio"), ("backfillfull", "osd set-backfillfull-ratio"), ("full", "osd set-full-ratio"))
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
            "mgr_command": ([p, C.POINTER(char), s, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)], i),
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
        return self._command(self.lib.rados_mon_command, cluster, command)

    def mgr(self, cluster, command):
        return self._command(self.lib.rados_mgr_command, cluster, command)

    def _command(self, function, cluster, command):
        encoded = json.dumps(command, ensure_ascii=False).encode("utf-8")
        commands = (C.c_char_p * 1)(encoded)
        output, diagnostic = C.c_void_p(), C.c_void_p()
        output_length, diagnostic_length = C.c_size_t(), C.c_size_t()
        try:
            code = function(cluster, commands, 1, None, 0, C.byref(output), C.byref(output_length),
                            C.byref(diagnostic), C.byref(diagnostic_length))
            if output_length.value > MAX_MON_OUTPUT or diagnostic_length.value > 64 << 10:
                raise ValueError("native command output exceeds bound")
            text = C.string_at(output, output_length.value) if output.value else b""
            # The daemon's status text explains refusals; it holds no credentials.
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
# Closed erasure-code widths. k2m1 keeps its original profile name, plugin
# choices and force-set behaviour. A wider width owns a distinct profile that
# is never force-overwritten, so it cannot change the profile another owned
# pool still uses; it names its plugin and, when optimized, its
# nonprimary_shards are read back against the pinned derivation.
EC_WIDTHS = {
    "k2m1": {"profile": EC_PROFILE, "k": 2, "m": 1, "plugins": ("", "jerasure", "isa"), "optimized": True, "force": True},
    "k2m2": {"profile": "ceph-msgr-owned-ec-k2m2", "k": 2, "m": 2, "plugins": ("isa",), "optimized": True, "force": False},
}
EC_PROFILES = tuple(width["profile"] for width in EC_WIDTHS.values())
# Pinned OSDMonitor::prepare_pool_size: an EC pool's MON-derived min_size.
def ec_min_size(k, m):
    return k + min(1, m - 1)


# Pinned OSDMonitor::enable_pool_ec_optimizations: the data raw shards other
# than the first may not be primary. isa defines no chunk mapping, so raw shard
# and shard are equal; parity shards and shard 0 stay primary-eligible.
def ec_nonprimary_shards(k, optimized):
    return set(range(1, k)) if optimized else set()


def parse_nonprimary(value):
    return {int(n) for n in re.findall(r"[0-9]+", str(value or ""))}


def ec_profile_get(rados, cluster, name):
    code, text = rados.mon(cluster, {"prefix": "osd erasure-code-profile get", "name": name, "format": "json"})
    if code < 0:
        return code, None
    profile = json.loads(text)
    if not isinstance(profile, dict) or not all(type(k) is str and type(v) is str for k, v in profile.items()):
        raise ValueError("invalid native erasure-code profile")
    return code, profile


def check_ec_profile(profile, width, plugin):
    """Independent read-back of the owned profile after setting it."""
    if (profile is None or profile.get("k") != str(width["k"]) or profile.get("m") != str(width["m"])
            or profile.get("crush-failure-domain") != "osd" or plugin and profile.get("plugin") != plugin):
        raise RuntimeError("native erasure-code profile differs from the requested width: %r" % (profile,))


def check_ec_rule(rule):
    """An erasure rule that selects independent OSDs (failure domain osd)."""
    steps = rule.get("steps") if isinstance(rule, dict) else None
    if (not isinstance(steps, list) or rule.get("type") != 3
            or not any(isinstance(s, dict) and s.get("op") in ("choose_indep", "chooseleaf_indep") and s.get("type") == "osd" for s in steps)):
        raise RuntimeError("native pool rule is not an OSD-domain erasure rule: %r" % (rule,))


def mode_create(rados, cluster, request, out):
    name = pool_name(request["slot"])
    pg = integer(request["pg_num"], 1, 32, "pg_num")
    erasure = request.get("erasure", False)
    if type(erasure) is not bool:
        raise ValueError("erasure must be boolean")
    width_name = request.get("width", "k2m1" if erasure else "")
    if erasure and width_name not in EC_WIDTHS or not erasure and width_name != "":
        raise ValueError("width must be k2m1 or k2m2 and needs erasure")
    width = EC_WIDTHS.get(width_name)
    optimized = request.get("optimized", False)
    if type(optimized) is not bool or optimized and not (erasure and width["optimized"]):
        raise ValueError("optimized must be boolean and needs an erasure width that admits it")
    # Empty keeps the cluster default plugin (isa in Tentacle).
    plugin = request.get("plugin", "")
    if plugin not in ("", "jerasure", "isa") or plugin and not erasure or erasure and plugin not in width["plugins"]:
        raise ValueError("plugin must be one the erasure width admits and needs erasure")
    hashpspool = request["hashpspool"]
    if type(hashpspool) is not bool:
        raise ValueError("hashpspool must be boolean")
    if find_pool(rados.dump(cluster), name):
        raise RuntimeError("owned pool already exists")
    if erasure:
        # OSD failure domain; overwrites need BlueStore. The MON derives the
        # pool's size (k+m) and min_size itself; neither is lowered here.
        size, minimum = width["k"] + width["m"], None
        if not width["force"]:
            code, _ = ec_profile_get(rados, cluster, width["profile"])
            if code != -2:
                raise RuntimeError("owned erasure-code profile %s already exists (%d); refusing to overwrite it" % (width["profile"], code))
            minimum = ec_min_size(width["k"], width["m"])
        profile = ["k=%d" % width["k"], "m=%d" % width["m"], "crush-failure-domain=osd"] + (["plugin=" + plugin] if plugin else [])
        setter = {"prefix": "osd erasure-code-profile set", "name": width["profile"], "profile": profile}
        if width["force"]:
            setter["force"] = True
        steps = [setter,
                 {"prefix": "osd pool create", "pool": name, "pg_num": pg, "pgp_num": pg, "pool_type": "erasure", "erasure_code_profile": width["profile"]},
                 {"prefix": "osd pool set", "pool": name, "var": "allow_ec_overwrites", "val": "true"}]
        if optimized:
            # Tentacle's optimized EC (OSDMonitor::enable_pool_ec_optimizations):
            # irreversible, restricts which shards may be primary and stores
            # pg_temp primary-first.
            steps.append({"prefix": "osd pool set", "pool": name, "var": "allow_ec_optimizations", "val": "true"})
    else:
        size = integer(request["size"], 1, 3, "size")
        minimum = integer(request["min_size"], 1, size, "min_size")
        steps = [{"prefix": "osd pool create", "pool": name, "pg_num": pg, "pgp_num": pg, "pool_type": "replicated"},
                 {"prefix": "osd pool set", "pool": name, "var": "size", "val": str(size), "yes_i_really_mean_it": True},
                 {"prefix": "osd pool set", "pool": name, "var": "min_size", "val": str(minimum)}]
    ec_profile = None
    for command in steps:
        code, _ = rados.mon(cluster, command)
        if code < 0:
            raise RuntimeError("native pool step %s failed: %d %s" % (command["prefix"], code, rados.last_status))
        if command["prefix"] == "osd erasure-code-profile set":
            _, ec_profile = ec_profile_get(rados, cluster, width["profile"])
            check_ec_profile(ec_profile, width, plugin)
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
        raise RuntimeError("native pool does not match the requested closed shape: %r" % (
            None if pool is None else {key: pool.get(key) for key in ("type", "size", "min_size", "pg_num", "flags")},))
    overwrites = None
    if erasure:
        code, text = rados.mon(cluster, {"prefix": "osd pool get", "pool": name, "var": "allow_ec_optimizations", "format": "json"})
        if code < 0 or json.loads(text).get("allow_ec_optimizations") is not optimized:
            raise RuntimeError("native pool EC optimization state differs: %d %s" % (code, text))
        code, text = rados.mon(cluster, {"prefix": "osd pool get", "pool": name, "var": "allow_ec_overwrites", "format": "json"})
        overwrites = json.loads(text).get("allow_ec_overwrites") if code == 0 else None
        if not width["force"] and (overwrites is not True or pool.get("erasure_code_profile") != width["profile"]
                                   or "ec_overwrites" not in pool.get("flags_names", "").split(",")):
            raise RuntimeError("native pool overwrite flag or profile differs: %r %r" % (overwrites, pool.get("erasure_code_profile")))
        if not width["force"] and parse_nonprimary(pool.get("nonprimary_shards")) != ec_nonprimary_shards(width["k"], optimized):
            raise RuntimeError("native pool nonprimary_shards %r differ from the pinned derivation for optimized=%r"
                               % (pool.get("nonprimary_shards"), optimized))
    rule = None
    code, text = rados.mon(cluster, {"prefix": "osd crush rule dump", "format": "json"})
    if code == 0:
        rules = [r for r in json.loads(text) if r.get("rule_id") == pool.get("crush_rule")]
        rule = rules[0] if rules else None
    if erasure and not width["force"]:
        check_ec_rule(rule)
    result = {"pool": name, "pool_id": pool["pool"], "flags": pool["flags"], "size": size, "min_size": minimum,
              "pg_num": pg, "epoch": rados.dump(cluster)["epoch"], "pool_type": pool["type"], "crush_rule": rule,
              "ec_optimized": optimized, "nonprimary_shards": pool.get("nonprimary_shards")}
    if erasure:
        result.update(width=width_name, erasure_code_profile=pool.get("erasure_code_profile"), ec_profile=ec_profile,
                      ec_overwrites=overwrites, flag_names=pool.get("flags_names", ""))
    return result


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


def mode_versions(rados, cluster, request, out):
    # Map epochs as the native CLI path reports them: each is the committed
    # version of the MON service MMonGetVersion reads.
    result = {}
    for key, prefix in (("osdmap", "osd dump"), ("monmap", "mon dump"), ("fsmap", "fs dump")):
        code, text = rados.mon(cluster, {"prefix": prefix, "format": "json"})
        if code < 0:
            raise RuntimeError("native %s failed: %d" % (prefix, code))
        result[key] = json.loads(text)["epoch"]
    return {"epochs": result}


def mode_mgrstate(rados, cluster, request, out):
    # MON-only observation remains usable while MGR activation is disabled.
    code, text = rados.mon(cluster, {"prefix": "mgr dump", "format": "json"})
    if code != 0:
        raise RuntimeError("native MGR state observation failed: %d" % code)
    raw = json.loads(text)
    state = {key: raw[key] for key in ("epoch", "flags", "available", "active_name", "active_gid")}
    integer(state["epoch"], 1, (1 << 32) - 1, "MGR epoch")
    integer(state["flags"], 0, (1 << 64) - 1, "MGR flags")
    integer(state["active_gid"], 0, (1 << 64) - 1, "MGR GID")
    if (type(state["available"]) is not bool or type(state["active_name"]) is not str
            or len(state["active_name"]) > 255 or "\0" in state["active_name"]
            or bool(state["active_gid"]) != bool(state["active_name"])
            or state["available"] and not state["active_gid"]):
        raise ValueError("inconsistent native MGR identity")
    return {"manager_map": state}


def mode_service(rados, cluster, request, out):
    # Register a service daemon as a native client does (an RGW or mirror
    # daemon uses the same call), keep the connection long enough for the MGR
    # to publish it, then read the MON's service map dump.
    import time
    daemon = request["daemon"]
    xattr.identity(daemon, "daemon")
    hold = integer(request["hold_seconds"], 1, 60, "hold_seconds")
    rados.lib.rados_service_register.argtypes = [C.c_void_p, C.c_char_p, C.c_char_p, C.c_char_p]
    rados.lib.rados_service_register.restype = C.c_int
    metadata = b"ceph_msgr_role\x00oracle\x00ceph_msgr_daemon\x00" + daemon.encode() + b"\x00\x00"
    rados.native.check(rados.lib.rados_service_register(cluster, b"ceph-msgr", daemon.encode(), metadata))
    # Hold the registration for the whole window so a concurrent watcher sees
    # it; record the first MON dump that lists the daemon.
    deadline = time.monotonic() + hold
    dump = None
    while time.monotonic() < deadline:
        if dump is None:
            code, text = rados.mon(cluster, {"prefix": "service dump", "format": "json"})
            if code == 0:
                current = json.loads(text)
                if daemon in current.get("services", {}).get("ceph-msgr", {}).get("daemons", {}):
                    dump = current
        time.sleep(1)
    return {"service_dump": dump}


SERVICE_NAME = re.compile(r"[A-Za-z0-9._-]{1,64}")


def string_map(value, label):
    if type(value) is not dict or len(value) > 16 or not all(
            type(k) is str and type(v) is str and len(k) <= 64 and len(v) <= 256 for k, v in value.items()):
        raise ValueError("invalid " + label)
    return value


def mode_servicewatch(rados, cluster, request, out):
    # Observe a service daemon registered by another client (the Go client under
    # test) through the native MON service map and the MGR's service status.
    import time
    service, daemon = request["service"], request["daemon"]
    if type(service) is not str or type(daemon) is not str or not SERVICE_NAME.fullmatch(service) or not SERVICE_NAME.fullmatch(daemon):
        raise ValueError("invalid service or daemon name")
    if request["expect"] not in ("present", "absent"):
        raise ValueError("invalid expectation")
    wait = integer(request["wait_seconds"], 1, 90, "wait_seconds")
    stable = integer(request.get("stable_seconds", 0), 0, 10, "stable_seconds")
    if stable >= wait:
        raise ValueError("stable_seconds must be shorter than wait_seconds")
    want_metadata = string_map(request.get("metadata", {}), "metadata")
    want_status = string_map(request.get("status", {}), "status")
    want_task = string_map(request.get("task_status", {}), "task_status")
    exact_status = request.get("exact_status", False)
    if type(exact_status) is not bool:
        raise ValueError("invalid exact_status")
    after_beacon = request.get("after_beacon", "")
    if type(after_beacon) is not str or len(after_beacon) > 64 or "\0" in after_beacon:
        raise ValueError("invalid after_beacon")
    deadline = time.monotonic() + wait
    observed = {}
    matched_since, observations = None, 0
    while True:
        code, text = rados.mon(cluster, {"prefix": "service dump", "format": "json"})
        dump = json.loads(text) if code == 0 else {}
        entry = dump.get("services", {}).get(service, {}).get("daemons", {}).get(daemon)
        status_entry, status_code = None, None
        if entry is not None:
            status_code, status_text = rados.mgr(cluster, {"prefix": "service status", "format": "json"})
            if status_code == 0:
                document = json.loads(status_text)
                status_entry = document.get("service_status", document).get(service, {}).get(daemon)
        observed = {"dump_code": code, "dump_epoch": dump.get("epoch"), "entry": entry,
                    "status_code": status_code, "status_entry": status_entry}
        if request["expect"] == "absent":
            matched = code == 0 and entry is None
        else:
            status_matches = (status_entry is not None and status_entry.get("status") == want_status
                              if exact_status else (not want_status or (status_entry is not None
                                  and all(status_entry.get("status", {}).get(k) == v for k, v in want_status.items()))))
            beacon = status_entry.get("last_beacon") if status_entry is not None else None
            beacon_matches = not after_beacon or (type(beacon) is str and beacon > after_beacon)
            matched = (entry is not None
                       and all(entry.get("metadata", {}).get(k) == v for k, v in want_metadata.items())
                       and all(entry.get("task_status", {}).get(k) == v for k, v in want_task.items())
                       and status_matches and beacon_matches)
        observations += 1
        now = time.monotonic()
        if not matched:
            matched_since = None
        elif matched_since is None:
            matched_since = now
        duration = 0 if matched_since is None else now - matched_since
        settled = matched and duration >= stable
        if settled or now >= deadline:
            return dict(observed, matched=settled, expect=request["expect"],
                        matched_for_seconds=duration, observations=observations)
        time.sleep(1)


def service_dictionary(value, label):
    value = string_map(value, label)
    if any(not k or "\0" in k or "\0" in v for k, v in value.items()):
        raise ValueError("invalid native C string dictionary")
    return b"".join(k.encode() + b"\0" + value[k].encode() + b"\0" for k in sorted(value)) + b"\0"


def mode_servicecompare(rados, cluster, request, out):
    # A separate native registration handle is shut down exactly once. The
    # original handle remains an independent MON/MGR observer of its removal.
    import time
    service, daemon = request["service"], request["daemon"]
    if type(service) is not str or type(daemon) is not str or not SERVICE_NAME.fullmatch(service) or not SERVICE_NAME.fullmatch(daemon):
        raise ValueError("invalid service or daemon name")
    metadata = service_dictionary(request["metadata"], "metadata")
    statuses = request["statuses"]
    if type(statuses) is not dict or set(statuses) != {"initial", "replacement", "empty"} or statuses["empty"] != {}:
        raise ValueError("invalid comparison status phases")
    encoded = {phase: service_dictionary(statuses[phase], "status") for phase in statuses}
    register, update = rados.lib.rados_service_register, rados.lib.rados_service_update_status
    register.argtypes, register.restype = [C.c_void_p, C.c_char_p, C.c_char_p, C.c_char_p], C.c_int
    update.argtypes, update.restype = [C.c_void_p, C.c_char_p], C.c_int
    native = rados.connect()
    result = {"metadata": {}, "statuses": {}}
    try:
        rados.native.check(register(native, service.encode(), daemon.encode(), metadata))
        for phase in ("initial", "replacement", "empty"):
            rados.native.check(update(native, encoded[phase]))
            observed = mode_servicewatch(rados, cluster, {
                "service": service, "daemon": daemon, "expect": "present", "wait_seconds": 20,
                "metadata": request["metadata"], "status": statuses[phase], "exact_status": True}, out)
            if not observed["matched"]:
                raise RuntimeError("native comparison status did not match: " + phase)
            entry = observed["entry"]
            if phase == "initial":
                # librados also collects system metadata. Retain only fields
                # supplied for comparison, never the native host inventory.
                result.update(gid=entry["gid"], start_epoch=entry["start_epoch"], dump_epoch=observed["dump_epoch"])
                result["metadata"] = {k: entry["metadata"][k] for k in request["metadata"]}
            elif entry["gid"] != result["gid"] or entry["start_epoch"] != result["start_epoch"]:
                raise RuntimeError("native registration identity changed during comparison")
            result["statuses"][phase] = observed["status_entry"]["status"]
    finally:
        rados.lib.rados_shutdown(native)
    # The void shutdown API is not a removal acknowledgement. Preserve an
    # independent post-return observation and allow the native beacon grace.
    started = time.monotonic()
    code, text = rados.mon(cluster, {"prefix": "service dump", "format": "json"})
    if code != 0:
        raise RuntimeError("native post-shutdown observation failed: %d" % code)
    entry = json.loads(text).get("services", {}).get(service, {}).get("daemons", {}).get(daemon)
    result["present_after_shutdown"] = entry is not None
    if entry is not None and entry["gid"] != result["gid"]:
        raise RuntimeError("native identity changed after shutdown")
    gone = mode_servicewatch(rados, cluster, {"service": service, "daemon": daemon,
        "expect": "absent", "wait_seconds": 90, "stable_seconds": 3}, out)
    result["removed"] = gone["matched"]
    result["absence_seconds"] = gone["matched_for_seconds"]
    result["removal_seconds"] = time.monotonic() - started
    if not result["removed"]:
        raise RuntimeError("native registration remains after shutdown")
    return {"comparison": result}


def mode_servicemetadata(rados, cluster, request, out):
    # get_metadata is not exposed by cli_api. The disposable fixture alone
    # enables the upstream selftest module, using only a fixed read expression.
    # No source text or arbitrary command is accepted from the requester.
    import time
    action = request["action"]
    if action in ("prepare", "restore"):
        code, text = rados.mon(cluster, {"prefix": "mgr module ls", "format": "json"})
        if code != 0:
            raise RuntimeError("native module inventory failed: %d" % code)
        enabled = "selftest" in json.loads(text)["enabled_modules"]
        if action == "restore":
            if enabled:
                code, _ = rados.mon(cluster, {"prefix": "mgr module disable", "module": "selftest"})
                if code != 0:
                    raise RuntimeError("native selftest restoration failed: %d" % code)
            code, text = rados.mon(cluster, {"prefix": "mgr module ls", "format": "json"})
            if code != 0 or "selftest" in json.loads(text)["enabled_modules"]:
                raise RuntimeError("native selftest removal was not observed")
            return {"restored": True}
        if not enabled:
            code, _ = rados.mon(cluster, {"prefix": "mgr module enable", "module": "selftest"})
            if code != 0:
                raise RuntimeError("native selftest preparation failed: %d" % code)
        deadline = time.monotonic() + 45
        while True:
            code, _ = rados.mgr(cluster, {"prefix": "mgr self-test python-version"})
            if code == 0:
                return {"ready": True, "module_enabled_by_test": not enabled}
            if time.monotonic() >= deadline:
                raise RuntimeError("native selftest command did not become ready: %d" % code)
            time.sleep(1)
    if action != "observe":
        raise ValueError("invalid metadata action")
    service, daemon = request["service"], request["daemon"]
    if type(service) is not str or type(daemon) is not str or not SERVICE_NAME.fullmatch(service) or not SERVICE_NAME.fullmatch(daemon):
        raise ValueError("invalid service or daemon name")
    wanted = string_map(request["metadata"], "metadata")
    if "hostname" in wanted:
        raise ValueError("hostname is reserved by the native metadata projection")
    wait = integer(request["wait_seconds"], 1, 45, "wait_seconds")
    expression = "print(__import__('json').dumps(mgr.get_metadata(%r, %r)))" % (service, daemon)
    deadline, observations = time.monotonic() + wait, 0
    while True:
        code, text = rados.mgr(cluster, {"prefix": "mgr self-test eval", "s": expression})
        metadata = json.loads(text) if code == 0 else None
        observations += 1
        # The native projection adds hostname separately. All caller-supplied
        # metadata must match exactly, including removal of previous keys.
        matched = (type(metadata) is dict
                   and {k: v for k, v in metadata.items() if k != "hostname"} == wanted)
        if matched or time.monotonic() >= deadline:
            return {"matched": matched, "metadata_code": code,
                    "daemon_metadata": metadata, "observations": observations}
        time.sleep(1)


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


def mode_fullratios(rados, cluster, request, out):
    wanted = request.get("set")
    if wanted is not None:
        if not isinstance(wanted, dict) or set(wanted) != {name for name, _ in RATIO_COMMANDS}:
            raise ValueError("set needs exactly nearfull, backfillfull and full")
        values = [wanted[name] for name, _ in RATIO_COMMANDS]
        if any(type(v) is not float or not 0 < v <= 1 for v in values) or not values[0] <= values[1] <= values[2]:
            raise ValueError("ratios must be ordered floats in (0, 1]")
        # Raise full first and lower it last so the order never inverts.
        order = RATIO_COMMANDS if wanted["full"] <= rados.dump(cluster)["full_ratio"] else tuple(reversed(RATIO_COMMANDS))
        for name, prefix in order:
            code, _ = rados.mon(cluster, {"prefix": prefix, "ratio": wanted[name]})
            if code < 0:
                raise RuntimeError("native %s failed: %d %s" % (prefix, code, rados.last_status))
    dump = rados.dump(cluster)
    owned = {}
    for slot in SLOTS:
        pool = find_pool(dump, pool_name(slot))
        if pool is not None:
            owned[str(slot)] = pool.get("flags_names", "")
    full = sorted(o["osd"] for o in dump.get("osds", []) if isinstance(o, dict) and "full" in o.get("state", []))
    return {"nearfull_ratio": dump["nearfull_ratio"], "backfillfull_ratio": dump["backfillfull_ratio"],
            "full_ratio": dump["full_ratio"], "epoch": dump["epoch"], "full_osds": full, "owned_flags": owned}


def mode_osdflags(rados, cluster, request, out):
    action = request.get("action")
    if action not in ("set", "unset", "show"):
        raise ValueError("action must be set, unset or show")
    if action != "show":
        for flag in RECOVERY_FLAGS:
            code, _ = rados.mon(cluster, {"prefix": "osd " + action, "key": flag})
            if code < 0:
                raise RuntimeError("native osd %s %s failed: %d %s" % (action, flag, code, rados.last_status))
    dump = rados.dump(cluster)
    flags = [f for f in str(dump.get("flags", "")).split(",") if f]
    return {"epoch": dump["epoch"], "osdmap_flags": flags}


def mode_inconsistent(rados, cluster, request, out):
    pgid = request.get("pgid")
    if not isinstance(pgid, str) or not re.fullmatch(r"[0-9]{1,10}\.[0-9a-f]{1,8}", pgid):
        raise ValueError("pgid must be <pool>.<hex seed>")
    dump = rados.dump(cluster)
    owned = {find_pool(dump, pool_name(slot))["pool"] for slot in SLOTS if find_pool(dump, pool_name(slot))}
    if int(pgid.split(".")[0]) not in owned:
        raise ValueError("pgid is not in an owned pool")
    result = {"pgid": pgid}
    for key, command in (("objects", "list-inconsistent-obj"), ("snapsets", "list-inconsistent-snapset")):
        proc = subprocess.run(["rados", "-c", str(rados.root / "ceph.conf"), "--keyring", str(rados.root / "keyring"), "--id", "test",
                               command, pgid, "--format=json"], stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
        if proc.returncode != 0:
            text = proc.stderr.decode("utf-8", "replace")[-300:]
            result[key] = {"code": -2 if "No scrub information" in text else -1, "stderr": text}
            continue
        raw = json.loads(proc.stdout)
        if key == "objects":
            names = [item.get("object", {}).get("name", "") for item in raw.get("inconsistents", [])]
        else:
            names = [item.get("name", "") for item in raw.get("inconsistents", [])]
        result[key] = {"code": 0, "interval": raw.get("epoch"), "names": names}
    return result


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
    # Every owned erasure-code profile is unused once its pools are gone.
    # Removal is not retried; a profile still readable afterwards is reported.
    remaining = []
    for profile in EC_PROFILES:
        rados.mon(cluster, {"prefix": "osd erasure-code-profile rm", "name": profile})
        code, _ = ec_profile_get(rados, cluster, profile)
        if code != -2:
            remaining.append(profile)
    gone = all(find_pool(rados.dump(cluster), pool_name(slot)) is None for slot in SLOTS)
    return {"removed": removed, "pools_removed": gone, "ec_profiles_remaining": remaining}


HANDLERS = {"create": mode_create, "seed": mode_seed, "observe": mode_observe, "place": mode_place,
            "snapshot": mode_snapshot, "selfmanaged": mode_selfmanaged, "quota": mode_quota, "poolinfo": mode_poolinfo, "clusterstat": mode_clusterstat, "versions": mode_versions, "mgrstate": mode_mgrstate, "service": mode_service, "servicewatch": mode_servicewatch, "servicecompare": mode_servicecompare, "servicemetadata": mode_servicemetadata, "fullratios": mode_fullratios, "osdflags": mode_osdflags, "inconsistent": mode_inconsistent, "cleanup": mode_cleanup}


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
