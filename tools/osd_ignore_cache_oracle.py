#!/usr/bin/env python3
"""Closed native-only cache-tier fixture proof; never a product dependency.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964,
LGPL-2.1 librados C API. Operation flag 8 becomes wire IGNORE_CACHE 0x8000.
No retry of an uncertain operation. Whiteout is observed through subsequent
native reads, not a decoded object_info FLAG_WHITEOUT assertion.
Reads use a two-op STAT+READ vector with zero successful per-op results;
WRITEFULL and DELETE each use one op. Inputs and raw reads are at most 4 KiB.
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import re
import sys

import osd_mutation_oracle as mutation

COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
NAMESPACE = "cache-probe"
OBJECTS = ("miss", "write-normal", "write-ignore", "delete-normal", "delete-ignore")
BASE = b"base\x00\xff\x80\ncache-proof"
CHANGED = b"tier\x00\x80\xff\nreplacement"
LIMIT = 4096
UNKNOWN = {-110, -104, -108, -125, -107}


def validate_plan(plan):
    if not isinstance(plan, dict) or set(plan) != {"source_commit", "fsid", "token", "base", "tier", "namespace", "clients"}:
        raise ValueError("invalid closed cache probe plan")
    token = plan["token"]
    if plan["source_commit"] != COMMIT or not isinstance(token, str) or not re.fullmatch("[0-9a-f]{12}", token):
        raise ValueError("invalid cache probe provenance/token")
    if not isinstance(plan["fsid"], str) or not re.fullmatch("[0-9a-fA-F-]{36}", plan["fsid"]):
        raise ValueError("invalid fixture FSID")
    if plan["base"] != "ceph-msgr-cache-base-" + token or plan["tier"] != "ceph-msgr-cache-tier-" + token or plan["namespace"] != NAMESPACE:
        raise ValueError("foreign cache probe target")
    roles = {r: "client.cache-" + r + "-" + token for r in ("rw", "ro", "base")}
    if plan["clients"] != roles:
        raise ValueError("foreign cache probe identity")
    return plan


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > 256 << 10:
        raise ValueError("native cache receipt exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


class Timespec(C.Structure):
    _fields_ = [("tv_sec", C.c_long), ("tv_nsec", C.c_long)]


class Native:
    def __init__(self, root, out, plan):
        self.root, self.out, self.plan = root, out, plan
        self.lib = C.CDLL("librados.so.2")
        p, s, i, u64, char = C.c_void_p, C.c_size_t, C.c_int, C.c_uint64, C.c_char_p
        signatures = {
            "create2": ([C.POINTER(p), char, char, u64], i),
            "conf_read_file": ([p, char], i), "conf_set": ([p, char, char], i),
            "connect": ([p], i), "shutdown": ([p], None),
            "cluster_fsid": ([p, p, s], i),
            "get_instance_id": ([p], u64),
            "mon_command": ([p, C.POINTER(char), s, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)], i),
            "buffer_free": ([p], None),
            "ioctx_create": ([p, char, C.POINTER(p)], i), "ioctx_destroy": ([p], None),
            "ioctx_set_namespace": ([p, char], None), "get_last_version": ([p], u64),
            "create_read_op": ([], p), "release_read_op": ([p], None),
            "read_op_read": ([p, u64, s, p, C.POINTER(s), C.POINTER(i)], None),
            "read_op_stat": ([p, C.POINTER(u64), C.POINTER(C.c_long), C.POINTER(i)], None),
            "read_op_operate": ([p, p, char, i], i),
            "create_write_op": ([], p), "release_write_op": ([p], None),
            "write_op_write_full": ([p, p, s], None), "write_op_remove": ([p], None),
            "write_op_operate2": ([p, p, char, C.POINTER(Timespec), i], i),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(self.lib, "rados_" + name)
            fn.argtypes, fn.restype = args, result
        self.clusters = {}
        self.identities = {}

    @staticmethod
    def check(code):
        if code < 0:
            raise RuntimeError("native setup failed with code %d" % code)

    def cluster(self, role):
        if role in self.clusters:
            return self.clusters[role]
        identity = "client.test" if role == "observer" else self.plan["clients"][role]
        keyring = self.out / ("admin.keyring" if role == "observer" else "osd-ignore-cache-" + role + ".keyring")
        cluster = C.c_void_p()
        self.check(self.lib.rados_create2(C.byref(cluster), b"ceph", identity.encode(), 0))
        try:
            self.check(self.lib.rados_conf_read_file(cluster, os.fsencode(self.root / "ceph.conf")))
            self.check(self.lib.rados_conf_set(cluster, b"keyring", os.fsencode(keyring)))
            self.check(self.lib.rados_conf_set(cluster, b"rados_osd_op_timeout", b"20"))
            self.check(self.lib.rados_conf_set(cluster, b"rados_mon_op_timeout", b"20"))
            self.check(self.lib.rados_connect(cluster))
            fsid = C.create_string_buffer(64)
            self.check(self.lib.rados_cluster_fsid(cluster, fsid, len(fsid)))
            if fsid.value.decode() != self.plan["fsid"]:
                raise ValueError("native client joined a foreign fixture")
            gid = int(self.lib.rados_get_instance_id(cluster))
            if gid <= 0:
                raise ValueError("native client lacks a global ID")
            self.identities[role] = {"name": identity, "global_id": gid}
        except BaseException:
            self.lib.rados_shutdown(cluster)
            raise
        self.clusters[role] = cluster
        return cluster

    def execute(self, role, pool, name, op, flags, namespace=NAMESPACE, data=CHANGED):
        if role not in ("observer", "rw", "ro", "base") or pool not in (self.plan["base"], self.plan["tier"]) or name not in OBJECTS or op not in ("read", "write", "delete") or flags not in (0, 8) or namespace not in (NAMESPACE, "cache-foreign") or len(data) > LIMIT:
            raise ValueError("operation outside closed cache proof")
        io = C.c_void_p()
        self.check(self.lib.rados_ioctx_create(self.cluster(role), pool.encode(), C.byref(io)))
        self.lib.rados_ioctx_set_namespace(io, namespace.encode())
        try:
            if op == "read":
                operation = self.lib.rados_create_read_op()
                if not operation:
                    raise RuntimeError("native read allocation failed")
                buf, length, size, stamp = C.create_string_buffer(LIMIT), C.c_size_t(), C.c_uint64(), C.c_long()
                read_result, stat_result = C.c_int(-99999), C.c_int(-99999)
                try:
                    self.lib.rados_read_op_stat(operation, C.byref(size), C.byref(stamp), C.byref(stat_result))
                    self.lib.rados_read_op_read(operation, 0, LIMIT, buf, C.byref(length), C.byref(read_result))
                    code = self.lib.rados_read_op_operate(operation, io, name.encode(), flags)
                    if length.value > LIMIT:
                        raise ValueError("native output exceeds read budget")
                    result = {"code": code, "read_prval": read_result.value, "stat_prval": stat_result.value}
                    if code >= 0:
                        result.update(bytes_b64=base64.b64encode(buf.raw[:length.value]).decode(), size=size.value, mtime=stamp.value)
                finally:
                    self.lib.rados_release_read_op(operation)
            else:
                operation = self.lib.rados_create_write_op()
                if not operation:
                    raise RuntimeError("native write allocation failed")
                try:
                    if op == "write":
                        buf = C.create_string_buffer(data)
                        self.lib.rados_write_op_write_full(operation, buf, len(data))
                    else:
                        self.lib.rados_write_op_remove(operation)
                    stamp = Timespec(1700000800, 0)
                    result = {"code": self.lib.rados_write_op_operate2(operation, io, name.encode(), C.byref(stamp), flags)}
                finally:
                    self.lib.rados_release_write_op(operation)
            result["version"] = int(self.lib.rados_get_last_version(io))
            if result["code"] in UNKNOWN:
                raise RuntimeError("native outcome unknown: %d; operation must not be repeated" % result["code"])
            return result
        finally:
            self.lib.rados_ioctx_destroy(io)

    def close(self):
        for cluster in self.clusters.values():
            self.lib.rados_shutdown(cluster)

    def placement(self, pool, name):
        cluster = self.cluster("observer")
        def command(*args, timeout):
            return mutation.native_mon_query(self.lib, cluster, *args, timeout=timeout)
        mapped = command("osd", "map", pool, name, NAMESPACE, timeout=10)
        result = mutation.collect_placement(command, pool, name, mapped["pool_id"], namespace=NAMESPACE)
        if result["pool_type"] != 1 or result["replicas"] != 1 or result["acting"] != [0]:
            raise ValueError("native target differs from the owned single replicated OSD")
        result["pool_name"] = pool
        return result


def run(native, plan, receipt, save):
    base_states = {}
    snapshot_fields = ("bytes_b64", "size", "version", "mtime")
    def call(label, role, pool, name, op="read", flags=8, expected=0, data=CHANGED, namespace=NAMESPACE):
        step = {"label": label, "identity_role": role, "pool": pool, "namespace": namespace, "object": name, "operation": op, "capi_flags": flags, "state": "intended", "may_mutate": op != "read" or pool == plan["tier"] and flags == 0}
        receipt["steps"].append(step)
        save()
        try:
            result = native.execute(role, pool, name, op, flags, namespace, data)
        except BaseException:
            step["state"] = "unknown"
            save()
            raise
        step.update(state="completed", result=result)
        save()
        if result["code"] != expected:
            raise RuntimeError("%s expected code %d got %d" % (label, expected, result["code"]))
        if op == "read" and result["code"] >= 0 and (result["read_prval"] != 0 or result["stat_prval"] != 0):
            raise RuntimeError("STAT+READ per-operation result differs: " + label)
        return result

    def observe(label, pool, name, expected_data):
        result = call(label, "observer", pool, name, expected=-2 if expected_data is None else 0)
        if expected_data is not None:
            if result.get("bytes_b64") != base64.b64encode(expected_data).decode() or result.get("size") != len(expected_data) or result["version"] == 0:
                raise RuntimeError("native bytes/size/version differ: " + label)
            if pool == plan["base"] and name in base_states and any(result.get(field) != base_states[name].get(field) for field in snapshot_fields):
                raise RuntimeError("cache action changed the base object bytes/size/version/mtime: " + label)
        return result

    base, tier = plan["base"], plan["tier"]
    receipt["placements"] = []
    for pool in (base, tier):
        for name in OBJECTS:
            receipt["placements"].append(native.placement(pool, name))
            save()
    for name in OBJECTS:
        call("seed-base-" + name, "observer", base, name, "write", data=BASE)
        base_states[name] = observe("base-before-" + name, base, name, BASE)
        observe("tier-before-" + name, tier, name, None)
    call("ignore-cache-miss", "rw", tier, "miss", expected=-2)
    observe("miss-still-absent", tier, "miss", None)
    call("normal-read-promotes", "rw", tier, "miss", flags=0)
    observe("promoted-tier", tier, "miss", BASE)
    observe("promoted-base-unchanged", base, "miss", BASE)
    for name, flags in (("write-normal", 0), ("write-ignore", 8)):
        call(name, "rw", tier, name, "write", flags)
        observe(name + "-tier", tier, name, CHANGED)
        observe(name + "-base-unchanged", base, name, BASE)
    for name, flags in (("delete-normal", 0), ("delete-ignore", 8)):
        call(name + "-promote", "rw", tier, name, flags=0)
        call(name, "rw", tier, name, "delete", flags)
        observe(name + "-local-absent", tier, name, None)
        call(name + "-subsequent-normal-read", "rw", tier, name, flags=0, expected=-2 if flags == 0 else 0)
        observe(name + "-base-preserved", base, name, BASE)
        observe(name + "-tier-behavior", tier, name, None if flags == 0 else BASE)
    before = observe("auth-before", tier, "write-ignore", CHANGED)
    call("scoped-ro-read", "ro", tier, "write-ignore")
    call("scoped-ro-write-refused", "ro", tier, "write-ignore", "write", expected=-1)
    call("base-only-tier-read-refused", "base", tier, "write-ignore", expected=-1)
    call("tier-rw-base-read-refused", "rw", base, "write-ignore", expected=-1)
    call("tier-rw-foreign-namespace-refused", "rw", tier, "write-ignore", expected=-1, namespace="cache-foreign")
    call("base-only-base-read", "base", base, "write-ignore")
    after = observe("auth-after", tier, "write-ignore", CHANGED)
    if any(before.get(field) != after.get(field) for field in snapshot_fields):
        raise RuntimeError("authorization refusals changed target bytes/size/version/mtime")
    # Remove only this probe's fixed heads with IGNORE_CACHE. This also removes
    # the normal DELETE whiteout without promoting or proxying it to the base.
    for pool in (tier, base):
        for name in OBJECTS:
            call("cleanup-" + pool + "-" + name, "observer", pool, name, "delete", 8)
            observe("cleanup-absent-" + pool + "-" + name, pool, name, None)
    receipt["complete"] = True
    receipt["native_identities"] = native.identities
    receipt["whiteout_evidence"] = "subsequent native read behavior only; raw FLAG_WHITEOUT was not observed"
    save()


def main(args):
    if len(args) != 3:
        raise ValueError("usage: osd_ignore_cache_oracle.py ROOT OUT")
    root, out = map(Path, args[1:])
    raw = (out / "osd-ignore-cache-plan.json").read_bytes()
    if len(raw) > 16384:
        raise ValueError("plan exceeds bound")
    plan = validate_plan(json.loads(raw))
    receipt = {"schema_version": 1, "source_commit": COMMIT, "plan": plan, "complete": False, "steps": []}
    save = lambda: publish(out / "osd-ignore-cache-native.json", receipt)
    save()
    native = Native(root, out, plan)
    try:
        run(native, plan, receipt, save)
    except BaseException as exc:
        receipt["error"] = str(exc)
        save()
        raise
    finally:
        native.close()
    print(json.dumps({"complete": True, "steps": len(receipt["steps"])}))


if __name__ == "__main__":
    main(sys.argv)
