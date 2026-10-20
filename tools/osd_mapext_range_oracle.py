#!/usr/bin/env python3
"""Exact-range head/snapshot IoCtx::mapext observer for owned test objects.

Native mapext returns an extent count and does not update IoCtx last_version.
Version evidence therefore comes from independent selected-snapshot stat2,
never from a claimed mapext reply version. No native product dependency.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

MAX_UINT64 = (1 << 64)-1
MAX_EXTENTS = 4096


def request(raw):
    fields = {"object", "offset", "length"}
    if not isinstance(raw, dict) or set(raw) not in (fields, fields | {"snap"}):
        raise ValueError("invalid exact-range mapext fields")
    name = xattr.identity(raw["object"], "owned object")
    offset = omap.integer(raw["offset"], MAX_UINT64, "offset")
    length = omap.integer(raw["length"], MAX_UINT64, "length")
    if not length or offset + length > MAX_UINT64:
        raise ValueError("zero or wrapping mapext range")
    snap = None
    if "snap" in raw:
        snap = omap.integer(raw["snap"], MAX_UINT64-2, "snap")
        if not snap:
            raise ValueError("omit snap for a head read")
    return name, offset, length, snap


def validate_observation(result, offset, length):
    if not isinstance(result, dict) or set(result) != {"code", "extents"}:
        raise ValueError("invalid native mapext output fields")
    code, extents = result["code"], result["extents"]
    if type(code) is not int or not -(1 << 31) <= code < 1 << 31:
        raise ValueError("invalid native mapext signed result")
    if not isinstance(extents, list) or len(extents) > MAX_EXTENTS:
        raise ValueError("native mapext count exceeds bound")
    end = offset
    for extent in extents:
        if not isinstance(extent, dict) or set(extent) != {"offset", "length"}:
            raise ValueError("invalid native extent fields")
        start = omap.integer(extent["offset"], MAX_UINT64, "extent offset")
        size = omap.integer(extent["length"], MAX_UINT64, "extent length")
        if not size or start < end or start + size > offset + length:
            raise ValueError("invalid native extent ordering or range")
        end = start + size
    if code >= 0 and code != len(extents) or code < 0 and extents:
        raise ValueError("native mapext result/count differs")
    return code


def observe_selected(native, io, name, snap, execute):
    if snap is not None:
        native.lib.rados_ioctx_snap_set_read(io, snap)
    try:
        before = native.snapshot(io, name, [])
        result = execute()
        after = native.snapshot(io, name, [])
        if before != after:
            raise RuntimeError("native mapext changed selected object state")
        return before, result, after
    finally:
        if snap is not None:
            native.lib.rados_ioctx_snap_set_read(io, writes.SNAP_HEAD)


def execute_native(args, out):
    env = dict(os.environ, CEPH_MSGR_NATIVE_MAPEXT_TRACE="1")
    try:
        observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, env=env)
    except subprocess.TimeoutExpired as error:
        observed = subprocess.CompletedProcess(args, -124, error.stdout or b"", error.stderr or b"")
    execution = dict(returncode=observed.returncode, stdout_bytes=len(observed.stdout), stderr_bytes=len(observed.stderr),
                     stderr=observed.stderr[:4096].decode("utf-8", errors="replace"))
    if observed.returncode or len(observed.stdout) > 1 << 20 or len(observed.stderr) > 4096:
        execution["stdout_prefix"] = observed.stdout[:512].decode("utf-8", errors="replace")
        omap.publish(out, "osd-mapext-range-result", dict(failed=True, native_execution=execution))
        raise RuntimeError("native mapext execution failed: " + json.dumps(execution))
    return json.loads(observed.stdout), execution


def run(root, out):
    name, offset, length, snap = request(omap.read_json(out / "osd-mapext-range-input.json"))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("mapext oracle requires pinned Tentacle")
    owned = writes.check_ownership(omap.read_json(out / (writes.OWNED + ".json")), metadata)
    if name not in owned:
        raise ValueError("mapext requires a fixture-owned object")
    native = writes.Native()
    args = [str(out / "osd_mapext_range_oracle"), str(root / "ceph.conf"), str(root / "keyring"),
            metadata["pool"], name.decode(), str(offset), str(length)] + ([str(snap)] if snap is not None else [])
    with xattr.client_connection(native, root, metadata, "test") as io:
        before = native.snapshot(io, name, [])
        selected_before, observation, selected_after = observe_selected(native, io, name, snap, lambda: execute_native(args, out))
        result, execution = observation
        code = validate_observation(result, offset, length)
        after = native.snapshot(io, name, [])
        if before != after:
            raise RuntimeError("native mapext changed head state")
    result.update(native_api_code=code, code=0 if code >= 0 else code,
                  source_commit=xattr.SOURCE_COMMIT, object=name.decode(), offset=offset, length=length, snap=snap,
                  before=before, after=after, selected_before=selected_before, selected_after=selected_after,
                  version=selected_before["version"], version_source="independent selected-snapshot stat2",
                  native_same_range=True, native_execution=execution)
    omap.publish(out, "osd-mapext-range-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("observe",))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
