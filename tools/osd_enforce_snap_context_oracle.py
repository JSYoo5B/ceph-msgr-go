#!/usr/bin/env python3
"""Closed pool-snapshot observations; C API never sets ENFORCE_SNAPC.

The only native write is an ordinary WRITEFULL with ORDERSNAP (C flag 512).
Two fixed heads and one existing pool snapshot belong to the disposable pool
oracle. Intent is durable before the native write; uncertain writes never replay.
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import subprocess
import sys

import osd_pool_oracle as pool

SOURCE = pool.SOURCE_COMMIT
HEADS = ("enforce-snap-control", "enforce-snap-enforced")
SNAP_NAME = "enforce-snap-S"
CHANGED = b"C\x00\xffenforced-snapshot"
HEAD = (1 << 64) - 2
LIMIT = 512 << 10


def validate(value):
    if not isinstance(value, dict) or set(value) != {"object", "snap_id", "pool_id"}:
        raise ValueError("closed snapshot oracle fields differ")
    if value["object"] not in HEADS:
        raise ValueError("foreign snapshot object")
    for field, low, high in (("snap_id", 1, HEAD - 1), ("pool_id", 1, (1 << 31) - 1)):
        pool.integer(value[field], low, high, field)
    return value


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > LIMIT:
        raise ValueError("snapshot receipt exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def ordinary_once(path, identity, execute):
    """A second invocation returns a completed result, never repeats a write."""
    if path.exists():
        raw = path.read_bytes()
        if len(raw) > LIMIT:
            raise ValueError("snapshot write receipt exceeds bound")
        previous = json.loads(raw)
        if previous.get("identity") != identity or previous.get("state") != "completed":
            raise RuntimeError("foreign or uncertain native write; no replay")
        return previous["result"]
    entry = {"identity": identity, "state": "intended", "operation": "WRITEFULL", "capi_flags": 512,
             "wire_enforce_snapc": False}
    publish(path, entry)
    try:
        result = execute()
    except BaseException as exc:
        entry.update(state="unknown", error=str(exc)[:1024])
        publish(path, entry)
        raise
    entry.update(result=result, state="completed" if result["code"] == 0 else "unknown-or-failed")
    publish(path, entry)
    if entry["state"] != "completed":
        raise RuntimeError("native write did not establish success; no replay")
    return result


def observe(rados, cluster, root, request):
    name = request["object"]
    io = rados.ioctx(cluster, pool.pool_name(1))
    states = {}
    try:
        for label, snap in (("head", HEAD), ("snapshot", request["snap_id"])):
            rados.lib.rados_ioctx_snap_set_read(io, snap)
            states[label] = rados.native.snapshot(io, name.encode(), [])
    finally:
        rados.lib.rados_ioctx_destroy(io)
    argv = ["rados", "--conf", str(root / "ceph.conf"), "--keyring", str(root / "keyring"),
            "--name", "client.test", "--pool", pool.pool_name(1), "listsnaps", name, "--format", "json"]
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if result.returncode != 0 or len(result.stdout) > 64 << 10 or len(result.stderr) > 16 << 10:
        raise RuntimeError("bounded native LISTSNAPS failed")
    listing = json.loads(result.stdout)
    if listing.get("name") != name or listing.get("seq") != request["snap_id"]:
        raise ValueError("native clone listing identity differs")
    states["list_snaps"] = listing
    states["list_snaps_raw"] = base64.b64encode(result.stdout).decode()
    return states


def main(argv):
    if len(argv) != 4 or argv[1] not in ("observe", "ordinary-write"):
        raise ValueError("usage: oracle ACTION ROOT OUT")
    action, root, out = argv[1], Path(argv[2]), Path(argv[3])
    raw = (out / "osd-enforce-snap-context-input.json").read_bytes()
    if len(raw) > 4096:
        raise ValueError("snapshot input exceeds bound")
    request = validate(json.loads(raw))
    version = (out / "daemon-version").read_text().strip()
    if SOURCE not in version or "20.2.4" not in version:
        raise ValueError("snapshot oracle requires pinned daemon")
    rados = pool.Rados(root, pool.xattr.Native())
    lib = rados.lib
    lib.rados_get_instance_id.argtypes, lib.rados_get_instance_id.restype = [C.c_void_p], C.c_uint64
    cluster = rados.connect()
    try:
        dump = rados.dump(cluster)
        descriptor = pool.find_pool(dump, pool.pool_name(1))
        if (descriptor is None or descriptor.get("pool") != request["pool_id"] or descriptor.get("type") != 1
                or descriptor.get("snap_mode") != "pool" or descriptor.get("snap_seq") != request["snap_id"]
                or [(s.get("snapid"), s.get("name")) for s in descriptor.get("pool_snaps", [])] != [(request["snap_id"], SNAP_NAME)]):
            raise ValueError("owned pool/snapshot identity differs")
        identity = {"source_commit": SOURCE, "fsid": dump["fsid"], **request}
        native_identity = {"name": "client.test", "global_id": int(lib.rados_get_instance_id(cluster))}
        if not native_identity["global_id"]:
            raise ValueError("native identity missing")
        result = {"schema_version": 1, "source_commit": SOURCE, "action": action, "identity": identity,
                  "native_identity": native_identity, "pool_epoch": dump["epoch"],
                  "native_enforce_flag_available": False}
        if action == "ordinary-write":
            if request["object"] != HEADS[0]:
                raise ValueError("native write is limited to control head")
            def execute():
                io = rados.ioctx(cluster, pool.pool_name(1))
                operation = lib.rados_create_write_op()
                if not operation:
                    lib.rados_ioctx_destroy(io)
                    raise RuntimeError("native write allocation failed")
                try:
                    data = C.create_string_buffer(CHANGED)
                    lib.rados_write_op_write_full(operation, data, len(CHANGED))
                    stamp = pool.Timespec(1700000902, 22)
                    code = int(lib.rados_write_op_operate2(operation, io, request["object"].encode(), C.byref(stamp), 512))
                    return {"code": code, "version": int(lib.rados_get_last_version(io)), "native_identity": native_identity}
                finally:
                    lib.rados_release_write_op(operation)
                    lib.rados_ioctx_destroy(io)
            result["ordinary_write"] = ordinary_once(out / "osd-enforce-snap-context-native-write.json", identity, execute)
        result.update(observe(rados, cluster, root, request))
        publish(out / "osd-enforce-snap-context-result.json", result)
    finally:
        lib.rados_shutdown(cluster)


if __name__ == "__main__":
    main(sys.argv)
