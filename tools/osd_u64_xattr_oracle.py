#!/usr/bin/env python3
"""Closed test-only U64 CMPXATTR oracle on two fixture-owned object names.

Native C++ ObjectOperation::cmpxattr(uint64_t) is required: the C comparison
ABI selects STRING only. Seeding/independent state observations use librados C.
No Go protocol bytes, stored-value conversion or native per-op cmp result are
invented. The helper's aggregate result and observed effects are the oracle.
"""
import argparse
import ctypes as C
import json
from pathlib import Path
import subprocess

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-u64-xattr"
BASELINE = b"owned-u64-baseline\x00\xff"
MAX_UINT64 = (1 << 64)-1


def request(raw):
    fields = {"phase", "object", "mirror", "stored", "exists", "action", "comparison", "operand", "flags"}
    if not isinstance(raw, dict) or set(raw) != fields:
        raise ValueError("invalid U64 comparison fields")
    if raw["phase"] not in ("seed", "execute", "observe") or raw["action"] not in ("read", "write", "staged-write"):
        raise ValueError("invalid closed U64 comparison action")
    if type(raw["exists"]) is not bool:
        raise ValueError("exists must be boolean")
    result = dict(raw)
    for field in ("object", "mirror"):
        result[field] = xattr.identity(raw[field], field)
    if result["object"] == result["mirror"]:
        raise ValueError("independent U64 oracle requires distinct owned names")
    result["stored"] = None if raw["stored"] is None else xattr.binary(raw["stored"], 128)
    result["comparison"] = omap.integer(raw["comparison"], 6, "comparison")
    result["operand"] = omap.integer(raw["operand"], MAX_UINT64, "operand")
    result["flags"] = omap.integer(raw["flags"], 2, "flags")
    if not result["comparison"] or result["flags"] not in (0, 2):
        raise ValueError("invalid U64 comparison operator or flag")
    return result


def seed(native, io, name, stored, exists):
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
        if stored is not None:
            attrs.append((b"number", stored))
        _, buffers = native.append(op, "write", [dict(op="set", name=n, value=v, flags=0) for n, v in attrs])
        stamp = xattr.Timespec(1700000000, 123456789)
        native.check(native.lib.rados_write_op_operate2(op, io, name, C.byref(stamp), 0))
    finally:
        native.lib.rados_release_write_op(op)


def execute(root, out, metadata, req):
    args = [str(out / "osd_u64_xattr_oracle"), str(root / "ceph.conf"), str(root / "keyring"),
            metadata["pool"], req["mirror"].decode(), req["action"], str(req["comparison"]),
            str(req["operand"]), str(req["flags"]), str(int(req["exists"]))]
    try:
        observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    except subprocess.TimeoutExpired as error:
        omap.publish(out, NAME+"-result", dict(failed=True, native_execution=dict(timed_out=True, stderr=(error.stderr or b"")[:4096].decode(errors="replace"))))
        raise RuntimeError("native U64 comparison timed out") from error
    execution = dict(returncode=observed.returncode, stdout_bytes=len(observed.stdout), stderr=observed.stderr[:4096].decode(errors="replace"))
    if observed.returncode or len(observed.stdout) > 4096 or len(observed.stderr) > 4096:
        omap.publish(out, NAME+"-result", dict(failed=True, native_execution=execution))
        raise RuntimeError("native U64 comparison execution failed")
    result = json.loads(observed.stdout)
    if not isinstance(result, dict) or set(result) != {"code", "version", "raw_cmp_per_op_result_available"} or type(result["code"]) is not int or not -(1 << 31) <= result["code"] < 1 << 31 or result["raw_cmp_per_op_result_available"] is not False:
        raise ValueError("invalid native U64 comparison result")
    omap.integer(result["version"], MAX_UINT64, "native version")
    result["execution"] = execution
    return result


def run(root, out):
    req = request(omap.read_json(out / (NAME+"-input.json")))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("U64 oracle requires pinned Tentacle")
    owned = writes.check_ownership(omap.read_json(out / (writes.OWNED+".json")), metadata)
    if req["object"] not in owned or req["mirror"] not in owned:
        raise ValueError("U64 comparison names are outside fixture ownership")
    native = xattr.Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        if req["phase"] == "seed":
            for name in (req["object"], req["mirror"]):
                seed(native, io, name, req["stored"], req["exists"])
        before = native.snapshot(io, req["object"], [])
        mirror_before = native.snapshot(io, req["mirror"], [])
        result = execute(root, out, metadata, req) if req["phase"] == "execute" else None
        after = native.snapshot(io, req["object"], [])
        mirror_after = native.snapshot(io, req["mirror"], [])
        if before != after:
            raise RuntimeError("native mirror comparison altered the Go subject")
    omap.publish(out, NAME+"-result", dict(source_commit=xattr.SOURCE_COMMIT, phase=req["phase"],
                 object=req["object"].decode(), mirror=req["mirror"].decode(), before=before, after=after,
                 mirror_before=mirror_before, mirror_after=mirror_after, native=result, native_client_may_resend=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
