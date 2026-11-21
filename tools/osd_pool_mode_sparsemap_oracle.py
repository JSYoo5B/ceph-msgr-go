"""Owned-pool oracle mode sparsemap: one object's allocation map at a snapshot.

Test-only; never product code. One read-only observation of an owned pool
object at its head or at a pool snapshot: native stat and bytes through the
public librados C API after rados_ioctx_snap_set_read, then the pinned
`rados mapext` CLI, which calls C++ IoCtx::mapext over the complete object
(offset 0, UINT64_MAX) at the snapshot it selects with `-s` (Ceph 20.2.4
commit 7f793731f1b39eb4f465e960113d2363c311b964, src/tools/rados/rados.cc
"mapext" and the --snap handling before it), then the same stat and bytes
again. The OSD answers MAPEXT from the store's fiemap of the object it
resolved for the read snap (src/osd/PrimaryLogPG.cc do_osd_ops MAPEXT), the
same object a SPARSE_READ at that snap reads. The map is logical and
store-specific; it says nothing about physical allocation. Only absence that
native stat also reports is admitted as a failure; any other CLI failure
fails the oracle. It judges nothing and sends nothing that changes the
cluster.
"""
import ctypes as C
import re
import subprocess

import osd_mapext_oracle as mapext
import osd_xattr_native as xattr
from osd_pool_oracle import oracle_mode, object_request, pool_name

MAX_STDOUT = 256 << 10
MAX_STDERR = 4096
SELECTED = re.compile(r"selected snap ([0-9]+) '([^'\n]*)'")


def parse(stdout, snap):
    """The pinned CLI output: one `selected snap` line at a snapshot, then the map."""
    if len(stdout) > MAX_STDOUT:
        raise ValueError("native mapext output exceeds bound")
    lines = stdout.decode("ascii").split("\n")
    selected = None
    if snap is not None:
        match = SELECTED.fullmatch(lines[0]) if lines else None
        if match is None or match[2] != snap:
            raise ValueError("native mapext did not select the requested snapshot")
        selected = int(match[1])
        lines = lines[1:]
    return selected, mapext.parse_map("\n".join(lines).encode("ascii"))


def observe(rados, io, name, snap_id):
    rados.lib.rados_ioctx_snap_set_read(io, snap_id)
    return rados.native.snapshot(io, name, [])


@oracle_mode
def mode_sparsemap(rados, cluster, request, out, run=subprocess.run):
    """The native full allocation map, bytes and stat of one owned object.

    At the head, or at the pool snapshot named by snap. Read-only; it fails
    when the object changed between the observations around the CLI call.
    """
    if set(request) - {"slot", "object", "snap"}:
        raise ValueError("sparsemap takes only slot, object and snap")
    pool = pool_name(request["slot"])
    name, _ = object_request(request)
    snap = request.get("snap") or None
    if snap is not None:
        xattr.identity(snap, "snapshot")
    io = rados.ioctx(cluster, pool)
    try:
        snap_id = (1 << 64) - 2
        if snap is not None:
            found = C.c_uint64()
            rados.native.check(rados.lib.rados_ioctx_snap_lookup(io, snap.encode(), C.byref(found)))
            snap_id = found.value
        before = observe(rados, io, name, snap_id)
        args = ["rados", "-c", str(rados.root / "ceph.conf"), "-n", "client.test", "--keyring", str(rados.root / "keyring"), "-p", pool]
        if snap is not None:
            args += ["-s", snap]
        result = run(args + ["mapext", request["object"]], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if len(result.stdout) > MAX_STDOUT or len(result.stderr) > MAX_STDERR:
            raise RuntimeError("native mapext exceeded its output bound")
        code, selected, full = 0, None, []
        if result.returncode:
            if before["code"] != -2 or b"(2)" not in result.stderr:
                raise RuntimeError("native mapext failed unexpectedly (%d)" % result.returncode)
            code = -2
            if snap is not None:
                selected, _ = parse(result.stdout, snap)
        else:
            if before["code"] != 0:
                raise RuntimeError("native mapext and stat disagree on existence")
            selected, full = parse(result.stdout, snap)
        if snap is not None and selected != snap_id:
            raise ValueError("native mapext selected another snap id")
        after = observe(rados, io, name, snap_id)
        if after != before:
            raise RuntimeError("read-only native observation changed the object")
    finally:
        rados.lib.rados_ioctx_destroy(io)
    return {"sparse_map": {"pool": pool, "object": request["object"], "snap": snap or "", "snap_id": snap_id,
                           "code": code, "native_full_extents": full, "state": before, "unchanged": True,
                           "scope": "pinned rados mapext over the complete object at the selected snapshot; native stat and read around it"}}
