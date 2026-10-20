#!/usr/bin/env python3
"""Test-only pinned native rados mapext oracle, limited to owned fixture objects.

The CLI calls C++ IoCtx::mapext on the complete head object (offset 0, UINT64_MAX).
For a subrange the receipt explicitly labels the intersection model; it does not
claim that the native CLI sent a partial MAPEXT. Full-range comparisons are
identical independent native and Go operations. Native stat/read observations
before and after retain bytes, nanosecond mtime and user version. No mutation,
routing, socket interception or retry is implemented by this helper.
"""
import argparse
from pathlib import Path
import re
import subprocess

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

MAX_UINT64 = (1 << 64) - 1
MAX_EXTENTS = 4096


def request(raw):
    if not isinstance(raw, dict) or set(raw) != {"object", "offset", "length"}:
        raise ValueError("invalid extent map input fields")
    name = xattr.identity(raw["object"], "object")
    offset = omap.integer(raw["offset"], MAX_UINT64, "offset")
    length = omap.integer(raw["length"], MAX_UINT64, "length")
    if not length or offset + length > MAX_UINT64:
        raise ValueError("zero or overflowing extent map range")
    return name, offset, length


def parse_map(data):
    if len(data) > 256 << 10:
        raise ValueError("native map output exceeds bound")
    result, end = [], 0
    for line in data.decode("ascii").splitlines():
        if not re.fullmatch(r"[0-9a-f]+\t[0-9a-f]+", line):
            raise ValueError("invalid pinned native extent map line")
        offset, length = [int(word, 16) for word in line.split("\t")]
        if not length or offset + length > MAX_UINT64 or result and offset < end or len(result) >= MAX_EXTENTS:
            raise ValueError("invalid native extent map ordering or range")
        result.append(dict(offset=offset, length=length))
        end = offset + length
    return result


def intersect(extents, offset, length):
    result = []
    for extent in extents:
        lo, hi = max(extent["offset"], offset), min(extent["offset"] + extent["length"], offset + length)
        if lo < hi:
            result.append(dict(offset=lo, length=hi - lo))
    return result


def run(root, out):
    name, offset, length = request(omap.read_json(out / "osd-mapext-input.json"))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("extent map oracle requires pinned Tentacle")
    owned = writes.check_ownership(omap.read_json(out / (writes.OWNED + ".json")), metadata)
    if name not in owned:
        raise ValueError("extent map observation requires a fixture-owned object")
    native = xattr.Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        before = native.snapshot(io, name, [])
        args = ["rados", "-c", str(root / "ceph.conf"), "-n", "client.test", "--keyring", str(root / "keyring"),
                "-p", metadata["pool"], "mapext", name.decode()]
        observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if len(observed.stdout) > 256 << 10 or len(observed.stderr) > 4096:
            raise ValueError("native CLI output exceeds bound")
        code = 0
        full = []
        if observed.returncode:
            # Only independently verified absence is admitted; other failures
            # remain oracle failures rather than guessed native errno values.
            if before["code"] != -2 or b"(2)" not in observed.stderr:
                raise RuntimeError("native mapext failed unexpectedly")
            code = -2
        else:
            if before["code"] != 0:
                raise RuntimeError("native CLI and stat disagree on existence")
            full = parse_map(observed.stdout)
        after = native.snapshot(io, name, [])
        if before != after:
            raise RuntimeError("read-only native extent map changed object state")
    exact = offset == 0 and length == MAX_UINT64
    result = dict(source_commit=xattr.SOURCE_COMMIT, object=name.decode(), code=code,
                  offset=offset, length=length, native_full_extents=full,
                  extents=intersect(full, offset, length), before=before, after=after,
                  native_same_range=exact, range_scope="identical full-range native MAPEXT" if exact else "intersection of independently observed native full extent map",
                  cli_stdout=observed.stdout.decode(), cli_stderr=observed.stderr.decode())
    omap.publish(out, "osd-mapext-result", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("observe",))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
