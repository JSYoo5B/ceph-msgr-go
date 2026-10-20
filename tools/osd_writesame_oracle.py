#!/usr/bin/env python3
"""Closed test-only public C++ WRITESAME oracle on two owned objects.

C API seeds and observes state; the separate C++ compound operation executes
WRITESAME. Logical outputs, per-op native results and durability are never
invented. Self-managed snapshot ids come from the existing owned-snapshot
fixture. No Go message bytes are input to the native execution.
"""
import argparse
import ctypes as C
import json
from pathlib import Path
import subprocess

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-writesame"
BASELINE = b"owned-writesame-baseline\x00\xff"
MAX_UINT64 = (1 << 64)-1


def request(raw):
    fields={"phase","object","mirror","exists","action","offset","length","pattern","flags","snap"}
    if not isinstance(raw,dict) or set(raw)!=fields:
        raise ValueError("invalid writesame fields")
    if raw["phase"] not in ("seed","execute","observe") or raw["action"] not in ("plain","append","guard","failok","snapshot") or type(raw["exists"]) is not bool:
        raise ValueError("invalid closed writesame action")
    result=dict(raw)
    for field in ("object","mirror"): result[field]=xattr.identity(raw[field],field)
    if result["object"]==result["mirror"]: raise ValueError("mirror must be distinct")
    result["pattern"]=xattr.binary(raw["pattern"],256)
    for field,limit in (("offset",1<<32),("length",4096),("flags",2),("snap",MAX_UINT64-2)):
        result[field]=omap.integer(raw[field],limit,field)
    if not result["pattern"] or result["length"]%len(result["pattern"]) or result["flags"] not in (0,2) or (result["action"]=="failok")!=(result["flags"]==2) or (result["action"]=="snapshot")!=(result["snap"]>0):
        raise ValueError("invalid closed writesame pattern/context")
    return result


def seed(native, io, name, exists):
    code = native.lib.rados_remove(io, name)
    if code not in (0, -2):
        native.check(code)
    if not exists:
        return
    op = native.lib.rados_create_write_op()
    if not op:
        raise RuntimeError("native seed allocation failed")
    try:
        data = C.create_string_buffer(BASELINE, len(BASELINE)+1)
        native.lib.rados_write_op_write_full(op, data, len(BASELINE))
        attrs = [(b"marker", b"before-marker"), (b"remove", b"remove-me")]
        _, buffers = native.append(op, "write", [dict(op="set", name=n, value=v, flags=0) for n, v in attrs])
        stamp = xattr.Timespec(1700000000, 123456789)
        native.check(native.lib.rados_write_op_operate2(op, io, name, C.byref(stamp), 0))
    finally:
        native.lib.rados_release_write_op(op)


def execute(root, out, metadata, req):
    args = [str(out / "osd_writesame_oracle"), str(root / "ceph.conf"), str(root / "keyring"),
            metadata["pool"], req["mirror"].decode(), req["action"], str(req["offset"]),
            str(req["length"]), req["pattern"].hex(), str(req["flags"]),str(req["snap"])]
    try:
        observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    except subprocess.TimeoutExpired as error:
        omap.publish(out, NAME+"-result", dict(failed=True, native_execution=dict(timed_out=True, stderr=(error.stderr or b"")[:4096].decode(errors="replace"))))
        raise RuntimeError("native WRITESAME timed out") from error
    execution = dict(returncode=observed.returncode, stdout_bytes=len(observed.stdout), stderr=observed.stderr[:4096].decode(errors="replace"))
    if observed.returncode or len(observed.stdout) > 4096 or len(observed.stderr) > 4096:
        omap.publish(out, NAME+"-result", dict(failed=True, native_execution=execution))
        raise RuntimeError("native WRITESAME execution failed")
    result = json.loads(observed.stdout)
    if not isinstance(result, dict) or set(result) != {"code", "version", "raw_writesame_per_op_result_available"} or type(result["code"]) is not int or not -(1 << 31) <= result["code"] < 1 << 31 or result["raw_writesame_per_op_result_available"] is not False:
        raise ValueError("invalid native WRITESAME result")
    omap.integer(result["version"], MAX_UINT64, "native version")
    result["execution"] = execution
    return result


def run(root, out):
    req = request(omap.read_json(out / (NAME+"-input.json")))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("WRITESAME oracle requires pinned Tentacle")
    owned = writes.check_ownership(omap.read_json(out / (writes.OWNED+".json")), metadata)
    if req["object"] not in owned or req["mirror"] not in owned:
        raise ValueError("WRITESAME names are outside fixture ownership")
    if req["snap"] and req["snap"] not in writes.owned_snaps(out,metadata):
        raise ValueError("snapshot is outside fixture ownership")
    native = xattr.Native()
    native.lib.rados_ioctx_snap_set_read.argtypes=[C.c_void_p,C.c_uint64]
    native.lib.rados_ioctx_snap_set_read.restype=None
    with xattr.client_connection(native, root, metadata, "test") as io:
        if req["phase"] == "seed":
            for name in (req["object"], req["mirror"]):
                seed(native, io, name, req["exists"])
        before = native.snapshot(io, req["object"], [])
        mirror_before = native.snapshot(io, req["mirror"], [])
        result = execute(root, out, metadata, req) if req["phase"] == "execute" else None
        after = native.snapshot(io, req["object"], [])
        mirror_after = native.snapshot(io, req["mirror"], [])
        clones={}
        if req["snap"] and req["phase"]=="observe":
            native.lib.rados_ioctx_snap_set_read(io,req["snap"])
            try:
                clones["clone"]=native.snapshot(io,req["object"],[])
                clones["mirror_clone"]=native.snapshot(io,req["mirror"],[])
            finally:
                native.lib.rados_ioctx_snap_set_read(io,MAX_UINT64-1)
        if before != after:
            raise RuntimeError("native mirror comparison altered the Go subject")
    omap.publish(out, NAME+"-result", dict(source_commit=xattr.SOURCE_COMMIT, phase=req["phase"],
                 object=req["object"].decode(), mirror=req["mirror"].decode(), before=before, after=after,
                 mirror_before=mirror_before, mirror_after=mirror_after, native=result, native_client_may_resend=True, **clones))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
