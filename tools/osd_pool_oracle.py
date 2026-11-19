#!/usr/bin/env python3
"""Closed, disposable-fixture native oracle for extra owned replicated pools.

Test-only; never product code. It uses the public librados C API (LGPL-2.1,
Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964) through ctypes
and creates at most two pools named ceph-msgr-owned-pool-1/-2 so that tests can
observe cross-pool, namespace and pool-snapshot behavior independently of the
Go client. create also makes an erasure-coded pool of a closed width: k2m1
(profile ceph-msgr-owned-ec) or k2m2 (its own ceph-msgr-owned-ec-k2m2 profile,
isa, never overwritten, optionally optimized with its nonprimary shards read
back); cleanup removes only what create recorded as created in this run
(osd-pool-claims.json in the output directory): the pools, the CRUSH rule the
MON made with an owned erasure-coded pool and the profiles.

Each mode is declared only beside its handler: a function mode_<name>
marked @oracle_mode, in this file or in its own module
osd_pool_mode_<name>_oracle.py beside it. A mode's description is the comment
or docstring of its handler. Every mode reads one bounded JSON input and
writes one bounded JSON result; native mutations are never retried and
credentials/keys are never copied.
"""
import base64
import ctypes as C
import errno
import importlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time

import osd_mutation_oracle as mutation
import osd_objecter_requests_oracle as objecter_requests
import osd_ops_in_flight_oracle as ops_in_flight
import osd_pg_history_oracle as history
import osd_pg_interval_oracle as pg_interval
import osd_shard_history_oracle as shard_history
import osd_xattr_native as xattr
from osd_oracle import Timespec

SOURCE_COMMIT = xattr.SOURCE_COMMIT
HANDLERS = {}
MODE_NAME = re.compile(r"[a-z][a-z0-9]{0,31}")
MODE_MODULE = re.compile(r"osd_pool_mode_([a-z][a-z0-9]{0,31})_oracle")


def oracle_mode(function):
    """Register function, named mode_<name>, as the handler of mode <name>.

    A mode is declared only beside its handler: the decorator line directly
    above `def mode_<name>(`, in this file or in its own module
    osd_pool_mode_<name>_oracle.py next to it. The fixture reads the same
    two lines from the staged sources to admit a requested mode
    (integration/fixture/control_native.go), so no list names the modes.
    """
    name = function.__name__[len("mode_"):]
    if not function.__name__.startswith("mode_") or not MODE_NAME.fullmatch(name):
        raise ValueError("owned-pool oracle mode handler must be named mode_<name>: %r" % function.__name__)
    if name in HANDLERS:
        raise ValueError("owned-pool oracle mode declared twice: %s" % name)
    HANDLERS[name] = function
    return function


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
            "osd_command": ([p, i, C.POINTER(char), s, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)], i),
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

    def osd(self, cluster, osd, command, limit):
        """One raw command to one explicit OSD; never resent by this oracle.

        Returns (code, output, status, oversized_length). Output above limit
        is not copied; its native length is reported instead.
        """
        encoded = json.dumps(command, ensure_ascii=False).encode("utf-8")
        commands = (C.c_char_p * 1)(encoded)
        output, diagnostic = C.c_void_p(), C.c_void_p()
        output_length, diagnostic_length = C.c_size_t(), C.c_size_t()
        try:
            code = self.lib.rados_osd_command(cluster, osd, commands, 1, None, 0, C.byref(output), C.byref(output_length),
                                              C.byref(diagnostic), C.byref(diagnostic_length))
            if diagnostic_length.value > 64 << 10:
                raise ValueError("native OSD command status exceeds bound")
            status = C.string_at(diagnostic, diagnostic_length.value).decode(errors="replace")[:512] if diagnostic.value else ""
            if output_length.value > limit:
                return code, b"", status, output_length.value
            text = C.string_at(output, output_length.value) if output.value and output_length.value else b""
            return code, text, status, None
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


class UnreadPresence(RuntimeError):
    """A presence read that neither found the object nor reported it absent."""

    def __init__(self, kind, name, code):
        super().__init__("native read of %s %s failed: %d; its presence is unknown" % (kind, name, code))
        self.code = code


def ec_profile_present(rados, cluster, name):
    """Whether the MON reports the profile: a successful read is present,
    ENOENT is absent, and any other reply raises UnreadPresence."""
    code, _ = rados.mon(cluster, {"prefix": "osd erasure-code-profile get", "name": name, "format": "json"})
    if code >= 0:
        return True
    if code == -errno.ENOENT:
        return False
    raise UnreadPresence("erasure-code profile", name, code)


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


@oracle_mode
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
    if erasure and name in crush_rules(rados, cluster):
        # The MON names an erasure-coded pool's own rule after the pool. A
        # rule of that name that predates the pool is not this run's.
        raise RuntimeError("CRUSH rule %s already exists; refusing to reuse it for the owned pool" % name)
    profile_absent = False
    if erasure:
        # OSD failure domain; overwrites need BlueStore. The MON derives the
        # pool's size (k+m) and min_size itself; neither is lowered here.
        size, minimum = width["k"] + width["m"], None
        # A read that is neither present nor absent refuses the create, so
        # the set below never runs against a profile of unknown origin.
        profile_absent = not ec_profile_present(rados, cluster, width["profile"])
        if not width["force"]:
            if not profile_absent:
                raise RuntimeError("owned erasure-code profile %s already exists; refusing to overwrite it" % width["profile"])
            minimum = ec_min_size(width["k"], width["m"])
        elif not profile_absent and width["profile"] not in read_claims(out)["ec_profiles"]:
            # The forced set may only replace a profile this run created.
            raise RuntimeError("erasure-code profile %s exists and this run did not create it; refusing to overwrite it" % width["profile"])
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
        # What this step may create was absent just before it; whatever of
        # it a successful read shows afterwards is claimed for the cleanup,
        # even when the step reports a failure, and nothing else is. A read
        # that fails claims nothing: the name stays unclaimed and the read
        # error fails the create.
        may_create = []
        if command["prefix"] == "osd erasure-code-profile set" and profile_absent:
            may_create.append(("ec_profiles", width["profile"]))
        if command["prefix"] == "osd pool create":
            may_create += [("pools", name)] + ([("crush_rules", name)] if erasure else [])
        try:
            code, _ = rados.mon(cluster, command)
        finally:
            unread = []
            for kind, item in may_create:
                try:
                    present = claim_present(rados, cluster, kind, item)
                except (RuntimeError, ValueError) as error:
                    unread.append("%s %s (%s)" % (kind, item, error))
                    continue
                if present:
                    record_claim(out, kind, item)
            if unread:
                raise RuntimeError("native pool step %s: presence after the step could not be read, left unclaimed: %s"
                                   % (command["prefix"], "; ".join(unread)))
        if code < 0:
            raise RuntimeError("native pool step %s failed: %d %s" % (command["prefix"], code, rados.last_status))
        if command["prefix"] == "osd erasure-code-profile set":
            code, ec_profile = ec_profile_get(rados, cluster, width["profile"])
            if code < 0:
                raise RuntimeError("native erasure-code profile read-back failed: %d" % code)
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


@oracle_mode
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


@oracle_mode
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


@oracle_mode
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


# pghistory takes one bounded primary PG-history snapshot for an owned object:
# placement before, the PG `log` command to that primary OSD, placement after,
# then the fail-closed parse in osd_pg_history_oracle.py. It reports where
# exact request identities occur on that primary; it never establishes
# absence.
@oracle_mode
def mode_pghistory(rados, cluster, request, out):
    """Bracketed primary PG-history snapshot; presence calibration only.

    The PG `log` command (pinned OSD.cc:2775-2825) is answered only by the
    PG's primary, so the snapshot says nothing about replica or shard-local
    history. Both brackets must be identical or the receipt is incomplete.
    """
    if set(request) - {"slot", "object", "namespace", "targets"}:
        raise ValueError("unknown pghistory fields")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    targets = request.get("targets")
    if not isinstance(targets, list) or not 1 <= len(targets) <= history.MAX_TARGETS:
        raise ValueError("pghistory needs 1 to %d targets" % history.MAX_TARGETS)
    for target in targets:
        history.target_identity(target)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    def place():
        return mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)

    before = place()
    # Pinned OSD.cc:4458-4463 registers the `pg <pgid> log` form with a pgid.
    code, raw, status, oversized = rados.osd(cluster, before["primary_id"],
                                             {"prefix": "pg", "pgid": before["pgid"], "cmd": "log", "format": "json"},
                                             history.MAX_LOG_BYTES)
    after = place()
    receipt = history.snapshot(before, after, code, raw, status, targets, oversized)
    receipt["osd"] = before["primary_id"]
    receipt["pgid"] = before["pgid"]
    return {"history": receipt}


def explicit_osd(request):
    """The optional OSD a snapshot asks instead of the bracketed primary."""
    if "osd" not in request:
        return None
    return integer(request["osd"], 0, (1 << 31) - 1, "osd")


# opsinflight takes one bounded snapshot of the op tracker of an owned
# object's primary: placement before, `dump_ops_in_flight` to that primary OSD, placement after,
# then the fail-closed parse in osd_ops_in_flight_oracle.py. It reports the
# in-flight ops of one client entity and incarnation; it never establishes
# absence.
@oracle_mode
def mode_opsinflight(rados, cluster, request, out):
    """Bracketed snapshot of the primary's in-flight ops; presence only.

    The owned object's primary answers ``dump_ops_in_flight`` from its own op
    tracker (pinned OSD.cc:2854-2878). An explicit "osd" asks that OSD
    instead, for example a former primary after a map change. Both brackets
    must be identical or the receipt is incomplete.
    """
    if set(request) - {"slot", "object", "namespace", "entity", "osd"}:
        raise ValueError("unknown opsinflight fields")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    ops_in_flight.client_entity(request.get("entity"))
    target = explicit_osd(request)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    def place():
        return mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)

    before = place()
    osd = before["primary_id"] if target is None else target
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "dump_ops_in_flight", "format": "json"},
                                             ops_in_flight.MAX_OPS_BYTES)
    after = place()
    receipt = ops_in_flight.snapshot(before, after, code, raw, status, request["entity"], oversized)
    receipt["osd"] = osd
    receipt["pgid"] = before["pgid"]
    return {"ops_in_flight": receipt}


# objecterrequests takes one bounded snapshot of a primary's internal
# Objecter, which reads COPY_FROM sources: placement before,
# `objecter_requests` to that primary OSD, placement after, then the
# fail-closed parse in osd_objecter_requests_oracle.py. It reports the ops of one
# source object; it never establishes absence.
@oracle_mode
def mode_objecterrequests(rados, cluster, request, out):
    """Bracketed snapshot of the primary's internal Objecter; presence only.

    The owned object's primary answers ``objecter_requests`` from the
    Objecter it reads a COPY_FROM source with (pinned Objecter.cc:405-409,
    4808-4844). An explicit "osd" asks that OSD instead. Both brackets must
    be identical or the receipt is incomplete.
    """
    if set(request) - {"slot", "object", "namespace", "source", "osd"}:
        raise ValueError("unknown objecterrequests fields")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    objecter_requests.source_identity(request.get("source"))
    target = explicit_osd(request)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    def place():
        return mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)

    before = place()
    osd = before["primary_id"] if target is None else target
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "objecter_requests", "format": "json"},
                                             objecter_requests.MAX_REQUESTS_BYTES)
    after = place()
    receipt = objecter_requests.snapshot(before, after, code, raw, status, request["source"], oversized)
    receipt["osd"] = osd
    receipt["pgid"] = before["pgid"]
    return {"objecter_requests": receipt}


SHARD_BEFORE = "osd-shard-history-before.json"
SHARD_CAPTURE = "osd-shard-history-capture.json"
SHARD_DUMP = re.compile(r"osd-shard-history-s[0-7]-(info|log)\.json")


def read_bounded(path, limit):
    with path.open("rb") as file:
        data = file.read(limit + 1)
    if len(data) > limit:
        raise ValueError("%s exceeds %d bytes" % (path.name, limit))
    return data


# shardhistory brackets the fixture's shard-local capture of an owned
# object's PG: phase before stores the placement the capture reads its PG and acting vector
# from; phase after reads the placement again and judges the capture record
# and each acting shard's offline dumps in osd_shard_history_oracle.py.
@oracle_mode
def mode_shardhistory(rados, cluster, request, out):
    """Placement brackets and judgement of one shard-local capture.

    Phase before stores the native placement of the object; the fixture's
    capture takes its PG and ordered acting vector from that stored record.
    Phase after reads the placement again after the last offline dump and
    requires the two complete records to be identical. Neither phase copies a
    store or contacts an OSD; presence in each shard's own log is reported,
    absence is never established.
    """
    phase = request.get("phase")
    if phase not in ("before", "after") or set(request) - {"phase", "slot", "object", "namespace", "targets"}:
        raise ValueError("shardhistory needs phase before or after and no unknown field")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    targets = request.get("targets")
    if phase == "before":
        if targets is not None:
            raise ValueError("shardhistory targets belong to phase after")
    else:
        if not isinstance(targets, list) or not 1 <= len(targets) <= history.MAX_TARGETS:
            raise ValueError("shardhistory needs 1 to %d targets" % history.MAX_TARGETS)
        for target in targets:
            history.target_identity(target)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    stored = out / SHARD_BEFORE
    if phase == "before":
        # A capture or bracket left by an earlier run must not be judged again.
        for stale in (stored, out / SHARD_CAPTURE):
            stale.unlink(missing_ok=True)
        placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)
        temporary = out / (SHARD_BEFORE + ".tmp")
        temporary.write_bytes(json.dumps(placement).encode())
        os.replace(temporary, stored)
        return {"placement": placement}
    before = read_bounded(stored, shard_history.MAX_RECORD_BYTES)
    capture = read_bounded(out / SHARD_CAPTURE, shard_history.MAX_RECORD_BYTES)
    after = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)

    def dumps(name, limit):
        if not isinstance(name, str) or not SHARD_DUMP.fullmatch(name):
            raise ValueError("invalid shard dump name")
        return read_bounded(out / name, limit)

    # Only the pool's own nonprimary shards may show a log head newer than
    # their last entry; an unreadable set admits none.
    try:
        nonprimary = shard_history.parse_written_shards(descriptor.get("nonprimary_shards"), 0)
    except shard_history.HistoryError:
        nonprimary = []
    receipt = shard_history.snapshot(before, after, capture, dumps, targets, nonprimary)
    # One bracket admits one judgement.
    stored.unlink()
    return {"shard_history": receipt}


INTERVAL_SECONDS = 30
INTERVAL_PAUSE = .5


def pg_observer(rados, cluster, request):
    """The owned pool's map entry and the two reads of one PG observation.

    place reads one placement record of the owned object, or says why none
    could be read. observe reads one placement record, sends `pg query` to
    the primary it names (or to the given one when there is no record) and
    reads one more placement record (osd_pg_interval_oracle.observation).
    Neither sends anything that changes the cluster.
    """
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    def place():
        try:
            return mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace, timeout=10)
        except (ValueError, RuntimeError) as error:
            # A PG between two intervals has no complete routing record.
            return "no placement: " + str(error)[:160]

    def observe(pgid, primary):
        first = place()
        if isinstance(first, dict):
            pgid, primary = first["pgid"], first["primary_id"]
        code, raw, status, oversized = rados.osd(cluster, primary, {"prefix": "pg", "pgid": pgid, "cmd": "query", "format": "json"},
                                                 pg_interval.MAX_QUERY_BYTES)
        return pg_interval.observation(first, code, raw, status, oversized, place())

    return descriptor, place, observe


# pgstate reads two placement records around one `pg query` of the
# primary, judges nothing and sends no command.
@oracle_mode
def mode_pgstate(rados, cluster, request, out, clock=time.monotonic):
    """One read-only observation of the PG of an owned object.

    Two placement records around one `pg query` of the primary the first
    record names, as pginterval observes it, without any judgement and
    without any command that changes the cluster. A PG whose placement
    cannot be read is reported with the reason and no query.
    """
    if set(request) - {"slot", "object", "namespace"}:
        raise ValueError("pgstate takes only slot, object and namespace")
    _, place, observe = pg_observer(rados, cluster, request)
    started = clock()
    first = place()
    if not isinstance(first, dict):
        seen = {"before": None, "after": None, "code": None, "placement_failure": first}
    else:
        seen = observe(first["pgid"], first["primary_id"])
    return {"pg_state": dict(seen, elapsed_ms=int((clock() - started) * 1000))}


# A pinned `rados listwatchers` line (src/tools/rados/rados.cc).
WATCHER_LINE = re.compile(r"watcher=(\S+) client\.([0-9]+) cookie=([0-9]+)")


# watchers runs the pinned `rados listwatchers` for one owned object and
# reports its entries.
@oracle_mode
def mode_watchers(rados, cluster, request, out, run=subprocess.run):
    """The native watcher list of one owned object (`rados listwatchers`).

    Read-only: the pinned CLI sends one LIST_WATCHERS read to the object's
    primary. Output beyond its bounds or a line of another shape fails.
    """
    if set(request) - {"slot", "object", "namespace"}:
        raise ValueError("watchers takes only slot, object and namespace")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    args = ["rados", "-c", str(rados.root / "ceph.conf"), "-n", "client.test", "--keyring", str(rados.root / "keyring"), "-p", pool]
    if namespace:
        args += ["-N", namespace]
    result = run(args + ["listwatchers", request["object"]], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if result.returncode != 0 or len(result.stdout) > 65536 or len(result.stderr) > 4096:
        raise RuntimeError("native listwatchers failed (%d) or exceeded its output bound" % result.returncode)
    entries = []
    for line in result.stdout.decode().splitlines():
        match = WATCHER_LINE.fullmatch(line)
        if not match:
            raise ValueError("unexpected pinned listwatchers output")
        entries.append({"address": match[1], "global_id": int(match[2]), "cookie": int(match[3])})
    return {"watchers": {"pool": pool, "object": request["object"], "namespace": namespace, "entries": entries}}


# pginterval ends the current interval of the PG of one owned object with one
# `pg repeer` command, which only stores a temporary mapping of that PG that
# the primary asks to drop again, and reads the placement and the primary's
# `pg query` until osd_pg_interval_oracle.py judges the PG settled in a newer
# interval with its original placement. Action show sends no command and
# reports one observation of the same kind; action absent sends none either
# and judges one observation of the PG active without the member of one acting
# position, whose OSD the caller took out through the fixture's daemon
# control; action replaced sends none and judges one observation of the PG
# active under the member the map makes primary after the caller took the
# primary's own OSD out the same way.
@oracle_mode
def mode_pginterval(rados, cluster, request, out, clock=time.monotonic, sleep=time.sleep):
    """One owned interval change of the PG of an owned object, or one observation of it.

    Action repeer requires a clean erasure-coded PG without a pg_temp, sends
    one `pg repeer` command, which is never resent, and observes until the
    PG is settled in a newer interval or the time bound is used up. Action
    show observes once and judges a clean PG on the recorded placement.
    Action absent observes once and judges the PG active without the member
    of one acting position of the recorded placement. Action replaced
    observes once and judges the PG active under the primary that the map
    names without the recorded primary's member. Each reports the last
    observation and its judgement; a PG that did not settle is reported, not
    raised.
    """
    action = request.get("action")
    allowed = {"action", "slot", "object", "namespace"} | {"show": {"origin"}, "absent": {"origin", "shard", "since"},
                                                           "replaced": {"origin", "since"}}.get(action, set())
    if action not in ("repeer", "show", "absent", "replaced") or set(request) - allowed:
        raise ValueError("pginterval needs action repeer, show, absent or replaced and no unknown field")
    descriptor, place, observe = pg_observer(rados, cluster, request)
    started = clock()
    if action in ("show", "absent", "replaced"):
        origin = request.get("origin")
        if not isinstance(origin, dict) or not isinstance(origin.get("pgid"), str) or type(origin.get("primary_id")) is not int:
            raise ValueError("pginterval %s needs the placement recorded before the command" % action)
        if action == "absent" and (type(request.get("shard")) is not int or type(request.get("since")) is not int):
            raise ValueError("pginterval absent needs the absent acting position and the recorded interval start")
        if action == "replaced" and type(request.get("since")) is not int:
            raise ValueError("pginterval replaced needs the recorded interval start")
        seen = observe(origin["pgid"], origin["primary_id"])
        now = seen["before"]
        if action == "absent":
            failures = pg_interval.absent_failures(origin, request["shard"], request["since"], seen)
        elif action == "replaced":
            # The shards that cannot lead, as the pool's own map entry lists
            # them; an unreadable list admits no observation.
            try:
                nonprimary = shard_history.parse_written_shards(descriptor.get("nonprimary_shards"), 0)
            except shard_history.HistoryError:
                nonprimary = None
            failures = pg_interval.replaced_failures(origin, request["since"], nonprimary, seen)
        elif now is None:
            failures = ["no placement record: " + seen["placement_failure"]]
        else:
            failures = pg_interval.origin_failures(now, seen) + [
                "placement %s is %r; it was %r before the command" % (key, now.get(key), origin.get(key))
                for key in pg_interval.PLACEMENT_KEYS if now.get(key) != origin.get(key)]
        return {"pg_interval": {"action": action, "observation": seen, "failures": failures, "settled": not failures, "observations": 1,
                                "elapsed_ms": int((clock() - started) * 1000)}}
    origin = place()
    if not isinstance(origin, dict):
        raise RuntimeError("the PG has no placement before the command: " + origin)
    first = observe(origin["pgid"], origin["primary_id"])
    receipt = {"action": action, "origin": origin, "origin_observation": first, "observations": 0, "settled": False}
    failures = pg_interval.origin_failures(origin, first)
    if failures:
        # Nothing was sent.
        receipt.update(failures=failures, command_sent=False, elapsed_ms=int((clock() - started) * 1000))
        return {"pg_interval": receipt}
    sent = {"prefix": "pg repeer", "pgid": origin["pgid"]}
    code, _ = rados.mon(cluster, sent)
    receipt.update(command=sent, command_sent=True, command_code=code, command_status=rados.last_status)
    if code != 0:
        receipt.update(failures=["pg repeer returned %d: %s" % (code, rados.last_status[:200])], elapsed_ms=int((clock() - started) * 1000))
        return {"pg_interval": receipt}
    deadline = clock() + INTERVAL_SECONDS
    while True:
        seen = observe(origin["pgid"], origin["primary_id"])
        receipt["observations"] += 1
        failures = pg_interval.unsettled(origin, first["query"], seen)
        if not failures or clock() >= deadline:
            break
        sleep(INTERVAL_PAUSE)
    receipt.update(observation=seen, failures=failures, settled=not failures, elapsed_ms=int((clock() - started) * 1000))
    return {"pg_interval": receipt}


@oracle_mode
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


@oracle_mode
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


@oracle_mode
def mode_clusterstat(rados, cluster, request, out):
    # rados_cluster_stat uses the same MStatfs request as the Go client's Statfs.
    stat = ClusterStat()
    rados.native.check(rados.lib.rados_cluster_stat(cluster, C.byref(stat)))
    return {"kb": stat.kb, "kb_used": stat.kb_used, "kb_avail": stat.kb_avail, "num_objects": stat.num_objects}


@oracle_mode
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


# mgrstate projects native MON MgrMap identity and flags without contacting a
# MGR.
@oracle_mode
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


@oracle_mode
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


# servicewatch polls the native MON `service dump` and MGR `service status`
# until a named service daemon matches the expected metadata/status/task
# status, or is absent, and reports the last native observation; it registers
# nothing.
@oracle_mode
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


# servicecompare independently registers a native service with the Go caller's
# fields, observes exact status replacements and confirms removal after
# shutdown.
@oracle_mode
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


# servicemetadata prepares/restores the native selftest module and reads only
# a fixed get_metadata expression for the exclusive MGR service fixture.
@oracle_mode
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


@oracle_mode
def mode_quota(rados, cluster, request, out):
    name = pool_name(request["slot"])
    limit = integer(request["max_objects"], 0, 1 << 20, "max_objects")
    code, _ = rados.mon(cluster, {"prefix": "osd pool set-quota", "pool": name, "field": "max_objects", "val": str(limit)})
    if code < 0:
        raise RuntimeError("native quota change failed: %d" % code)
    return {"pool": name, "max_objects": limit}


@oracle_mode
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


# fullratios changes the cluster-wide nearfull/backfillfull/full ratios of the
# disposable fixture; the caller restores the values it reported first.
@oracle_mode
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


# osdflags sets or unsets only the norecover and nobackfill OSDMap flags; the
# caller unsets them again.
@oracle_mode
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


# inconsistent runs the native `rados list-inconsistent-obj` for one
# owned-pool PG and reports its interval and object names.
@oracle_mode
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


# The central entry the cleanup opens for pool removal. It is restored to the
# state it had before (absent, or its earlier value), never left behind.
POOL_DELETE_SECTION, POOL_DELETE_OPTION = "mon", "mon_allow_pool_delete"


def json_mon(rados, cluster, command, label):
    code, text = rados.mon(cluster, dict(command, format="json"))
    if code < 0:
        raise RuntimeError("native %s failed: %d" % (label, code))
    return json.loads(text)


def pool_delete_entry(rados, cluster):
    """The MON section's unmasked mon_allow_pool_delete entry, or None."""
    entries = json_mon(rados, cluster, {"prefix": "config dump"}, "config dump")
    found = [e for e in entries if isinstance(e, dict) and e.get("section") == POOL_DELETE_SECTION
             and e.get("name") == POOL_DELETE_OPTION and not e.get("mask")]
    if len(found) > 1:
        raise RuntimeError("native config dump lists %s twice" % POOL_DELETE_OPTION)
    return found[0].get("value") if found else None


def crush_rules(rados, cluster):
    rules = json_mon(rados, cluster, {"prefix": "osd crush rule ls"}, "crush rule ls")
    if not isinstance(rules, list) or not all(type(r) is str for r in rules):
        raise ValueError("invalid native CRUSH rule list")
    return rules


# The objects mode_create created, by kind. mode_create records each one
# only after it observed the object present where it had been absent just
# before the step that created it; mode_cleanup removes only these. The
# record lives in the fixture output directory, which every oracle run of
# the fixture shares.
CLAIMS_FILE = "osd-pool-claims.json"
CLAIM_KINDS = ("pools", "crush_rules", "ec_profiles")
MAX_CLAIMS = 4 << 10


def claimable(kind):
    """The owned names a claim of each kind may hold."""
    return set(EC_PROFILES) if kind == "ec_profiles" else {pool_name(slot) for slot in SLOTS}


def read_claims(out):
    path = pathlib.Path(out) / CLAIMS_FILE
    try:
        with path.open("rb") as file:
            data = file.read(MAX_CLAIMS + 1)
    except FileNotFoundError:
        return {kind: [] for kind in CLAIM_KINDS}
    if len(data) > MAX_CLAIMS:
        raise ValueError("owned-pool claims exceed 4 KiB")
    claims = json.loads(data)
    # A record that is not exactly this shape removes nothing.
    if (not isinstance(claims, dict) or set(claims) != set(CLAIM_KINDS)
            or not all(isinstance(claims[k], list) and all(type(n) is str and n in claimable(k) for n in claims[k]) for k in CLAIM_KINDS)):
        raise ValueError("invalid owned-pool claims")
    return claims


def write_claims(out, claims):
    temporary = pathlib.Path(out) / (CLAIMS_FILE + ".tmp")
    temporary.write_bytes(json.dumps(claims).encode() + b"\n")
    os.replace(temporary, pathlib.Path(out) / CLAIMS_FILE)


def record_claim(out, kind, name):
    claims = read_claims(out)
    if name not in claims[kind]:
        claims[kind].append(name)
        write_claims(out, claims)


def take_claims(out):
    """Read the claims and drop the record before anything is removed, so a
    later cleanup never removes what an earlier test's run left behind."""
    claims = read_claims(out)
    try:
        os.remove(pathlib.Path(out) / CLAIMS_FILE)
    except FileNotFoundError:
        pass
    return claims


def claim_present(rados, cluster, kind, name):
    """Present or absent from a successful native read; a failed read of
    either kind raises instead of answering."""
    if kind == "pools":
        return find_pool(rados.dump(cluster), name) is not None
    if kind == "crush_rules":
        return name in crush_rules(rados, cluster)
    return ec_profile_present(rados, cluster, name)


def remove_owned_pools(rados, cluster, claimed, removed):
    for slot in SLOTS:
        name = pool_name(slot)
        existed = find_pool(rados.dump(cluster), name) is not None
        removed.append({"pool": name, "existed": existed, "claimed": name in claimed})
        if not existed or name not in claimed:
            continue
        code, _ = rados.mon(cluster, {"prefix": "osd pool rm", "pool": name, "pool2": name, "yes_i_really_really_mean_it": True})
        if code < 0:
            raise RuntimeError("native pool removal failed: %d" % code)


@oracle_mode
def mode_cleanup(rados, cluster, request, out):
    """Remove what mode_create claimed in this run and nothing else: the
    owned pools it created, the CRUSH rule the MON created with an owned
    erasure-coded pool and the owned erasure-code profiles it created. Pool
    removal enables the central mon_allow_pool_delete entry only while a
    claimed pool is removed and restores its prior state on every path. An
    owned name present without a claim is reported, never removed, and an
    owned pool of either kind keeps pools_removed false; a survivor is
    reported, not retried. An owned profile whose read fails is reported
    in ec_profiles_unread, apart from survivors."""
    claims = take_claims(out)
    removed = []
    prior = pool_delete_entry(rados, cluster)
    claimed_pools = [p for p in claims["pools"] if find_pool(rados.dump(cluster), p) is not None]
    restored = True
    if claimed_pools:
        try:
            if prior != "true":
                code, _ = rados.mon(cluster, {"prefix": "config set", "who": POOL_DELETE_SECTION, "name": POOL_DELETE_OPTION, "value": "true"})
                if code < 0:
                    raise RuntimeError("native pool deletion could not be enabled: %d" % code)
            remove_owned_pools(rados, cluster, set(claimed_pools), removed)
        finally:
            if prior is None:
                code, _ = rados.mon(cluster, {"prefix": "config rm", "who": POOL_DELETE_SECTION, "name": POOL_DELETE_OPTION})
            elif prior != "true":
                code, _ = rados.mon(cluster, {"prefix": "config set", "who": POOL_DELETE_SECTION, "name": POOL_DELETE_OPTION, "value": prior})
            else:
                code = 0
            # The restore is confirmed by an independent read, not by its code.
            restored = code >= 0 and pool_delete_entry(rados, cluster) == prior
    else:
        remove_owned_pools(rados, cluster, set(), removed)
    # No owned pool may remain, claimed or not: an unclaimed one is a
    # leftover this cleanup leaves, or a create whose claim went missing.
    gone = all(find_pool(rados.dump(cluster), pool_name(slot)) is None for slot in SLOTS)
    present_rules = set(crush_rules(rados, cluster))
    for rule in sorted(set(claims["crush_rules"]) & present_rules):
        rados.mon(cluster, {"prefix": "osd crush rule rm", "name": rule})
    present_rules = set(crush_rules(rados, cluster))
    rules_remaining = sorted(set(claims["crush_rules"]) & present_rules)
    # A claimed profile is unused once the claimed pools are gone. Removal
    # is not retried; a profile still readable afterwards is reported.
    for profile in EC_PROFILES:
        if profile in claims["ec_profiles"]:
            rados.mon(cluster, {"prefix": "osd erasure-code-profile rm", "name": profile})
    # A profile whose read failed is neither a survivor nor gone: it is
    # reported with its code and fails the caller.
    present_profiles, unread_profiles = set(), {}
    for profile in EC_PROFILES:
        try:
            if ec_profile_present(rados, cluster, profile):
                present_profiles.add(profile)
        except UnreadPresence as error:
            unread_profiles[profile] = error.code
    remaining = sorted(present_profiles & set(claims["ec_profiles"]))
    unclaimed = {"pools": sorted(p["pool"] for p in removed if p["existed"] and not p["claimed"]),
                 "crush_rules": sorted(claimable("crush_rules") & present_rules - set(claims["crush_rules"])),
                 "ec_profiles": sorted(present_profiles - set(claims["ec_profiles"]))}
    return {"removed": removed, "claimed": claims, "unclaimed_present": unclaimed, "pools_removed": gone,
            "ec_profiles_remaining": remaining, "ec_profiles_unread": unread_profiles, "crush_rules_remaining": rules_remaining,
            "pool_delete_config_restored": restored, "pool_delete_config_prior": prior}


def load_mode_modules(directory=pathlib.Path(__file__).resolve().parent):
    """Import every osd_pool_mode_<name>_oracle.py beside this file. Each
    module declares exactly its own mode <name> with @oracle_mode."""
    # Run as a script this module is __main__; a mode module's
    # `import osd_pool_oracle` must register into this same module.
    this = sys.modules[__name__]
    sys.modules.setdefault("osd_pool_oracle", this)
    if sys.modules["osd_pool_oracle"] is not this:
        raise RuntimeError("owned-pool oracle loaded twice")
    for path in sorted(directory.glob("osd_pool_mode_*_oracle.py")):
        match = MODE_MODULE.fullmatch(path.stem)
        if match is None:
            raise ValueError("owned-pool oracle mode module name is not osd_pool_mode_<name>_oracle.py: %s" % path.name)
        if path.stem in sys.modules:
            # Its own import is in progress (it imported this module first);
            # its decorator registers the mode when that import continues.
            continue
        before = set(HANDLERS)
        importlib.import_module(path.stem)
        if set(HANDLERS) - before != {match.group(1)}:
            raise ValueError("%s must declare exactly its own mode %s" % (path.name, match.group(1)))


load_mode_modules()


def main(argv):
    if len(argv) != 4 or argv[1] not in HANDLERS:
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
