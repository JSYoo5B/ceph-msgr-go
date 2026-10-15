#!/usr/bin/env python3
"""Test-only exact-range native C++ sparse-read observer on owned fixture objects.

Ceph v20.2.4 public LGPL-2.1 API (no copied implementation). The C++ helper
executes IoCtx::sparse_read; C librados snapshots independently preserve object
bytes, mtime, xattrs and version. No mutations, retries or placement logic.
"""
import argparse
from pathlib import Path
import subprocess
import json
import os

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

MAX_UINT64 = (1 << 64) - 1
MAX_LENGTH = 1 << 20


def request(raw):
    if not isinstance(raw, dict) or set(raw) != {"object", "offset", "length"}:
        raise ValueError("invalid sparse read input fields")
    name = xattr.identity(raw["object"], "object")
    offset = omap.integer(raw["offset"], MAX_UINT64, "offset")
    length = omap.integer(raw["length"], MAX_LENGTH, "length")
    if not length or offset + length > MAX_UINT64:
        raise ValueError("zero or overflowing sparse read range")
    return name, offset, length


def validate_observation(result, offset, length):
    if not isinstance(result, dict) or set(result) != {"code", "extents", "data_hex", "version"}:
        raise ValueError("invalid native sparse read fields")
    code = result["code"]
    if type(code) is not int or not -(1 << 31) <= code < 1 << 31:
        raise ValueError("invalid native signed result")
    extents = result["extents"]
    if not isinstance(extents, list) or len(extents) > 4096:
        raise ValueError("native sparse read extent count exceeds bound")
    text = result["data_hex"]
    if not isinstance(text, str) or len(text) > MAX_LENGTH * 2 or len(text) % 2 or any(c not in "0123456789abcdef" for c in text):
        raise ValueError("invalid native sparse read bytes")
    data = bytes.fromhex(text)
    total, end = 0, offset
    for e in extents:
        if not isinstance(e, dict) or set(e) != {"offset", "length"}:
            raise ValueError("invalid native sparse read extent")
        off = omap.integer(e["offset"], MAX_UINT64, "extent offset")
        size = omap.integer(e["length"], MAX_UINT64, "extent length")
        if not size or off < end or off + size > offset + length:
            raise ValueError("invalid native sparse read range/order")
        total += size
        end = off + size
    if total != len(data) or code >= 0 and code != len(extents) or code < 0 and (extents or data):
        raise ValueError("native sparse read extent/byte/result mismatch")
    omap.integer(result["version"], MAX_UINT64, "version")
    return data


def native_execution(args):
    env = dict(os.environ, CEPH_MSGR_NATIVE_SPARSE_READ_TRACE="1")
    timed_out = False
    try:
        observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, env=env)
    except subprocess.TimeoutExpired as error:
        timed_out = True
        observed = subprocess.CompletedProcess(args, -124, error.stdout or b"", error.stderr or b"")
    receipt = dict(returncode=observed.returncode, timed_out=timed_out, stdout_bytes=len(observed.stdout),
                   stderr_bytes=len(observed.stderr), stderr=observed.stderr[:4096].decode("utf-8", errors="replace"),
                   stderr_truncated=len(observed.stderr) > 4096, stdout_exceeds_bound=len(observed.stdout) > 3 << 20)
    failed = observed.returncode != 0 or receipt["stdout_exceeds_bound"] or receipt["stderr_truncated"]
    if failed:
        receipt["stdout_prefix"] = observed.stdout[:512].decode("utf-8", errors="replace")
    return observed, receipt, failed


def run(root, out):
    name, offset, length = request(omap.read_json(out / "osd-sparse-read-input.json"))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("sparse read oracle requires pinned Tentacle")
    owned = writes.check_ownership(omap.read_json(out / (writes.OWNED + ".json")), metadata)
    if name not in owned:
        raise ValueError("sparse read requires fixture-owned object")
    native = xattr.Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        before = native.snapshot(io, name, [])
        args = [str(out / "osd_sparse_read_oracle"), str(root / "ceph.conf"), str(root / "keyring"),
                metadata["pool"], name.decode(), str(offset), str(length)]
        observed, execution, failed = native_execution(args)
        if failed:
            omap.publish(out, "osd-sparse-read-result", dict(source_commit=xattr.SOURCE_COMMIT,
                         object=name.decode(), offset=offset, length=length, failed=True, native_execution=execution))
            raise RuntimeError("native sparse read execution failed: " + json.dumps(execution))
        result = json.loads(observed.stdout)
        data = validate_observation(result, offset, length)
        after = native.snapshot(io, name, [])
        if before != after:
            raise RuntimeError("native sparse read altered object state")
    result.update(source_commit=xattr.SOURCE_COMMIT, object=name.decode(), offset=offset, length=length,
                  data=xattr.encode(data), before=before, after=after, native_same_range=True, native_execution=execution)
    omap.publish(out, "osd-sparse-read-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("observe",))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
