"""Owned-pool oracle mode omapstate: one owned head's complete native state.

Test-only; never product code. It reads one head of an owned pool with the
pinned librados C API and the pinned rados CLI, sending nothing that changes
the cluster: bytes, stat2 mtime, user version and xattrs
(tools/osd_xattr_native.py snapshot), every OMAP entry in one bounded page
with its completeness flag plus an explicit by-keys read of the empty key
(tools/osd_omap_native.py state), and the raw OMAP header through
`rados getomapheader` (tools/osd_omap_write_native.py HeaderCLI, because the
C API has no header read). A stat2 before and after the CLI read must agree,
so the separate reads describe one object state (full_state). An absent
object is reported with code -2 and no OMAP or header. Bounds are those of
the reused readers: 128 entries and 128 KiB of OMAP keys and values, a
64 KiB header and 1 MiB of data. A page that is not complete fails.

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):
src/include/rados/librados.h rados_read_op_omap_get_vals2 and
rados_read_op_omap_get_vals_by_keys2, src/tools/rados/rados.cc
getomapheader.
"""
from osd_pool_oracle import object_request, oracle_mode, pool_name
import osd_omap_write_native as omapwrite


def omap_state(native, io, name, cli):
    """The full native state of one head: data, xattrs, OMAP and header."""
    return native.full_state(io, name, [], cli)


@oracle_mode
def mode_omapstate(rados, cluster, request, out, native=None, cli=None):
    """Observe one owned head's bytes, xattrs, OMAP entries and header."""
    if not set(request) <= {"slot", "object", "namespace"} or not {"slot", "object"} <= set(request):
        raise ValueError("omapstate takes exactly slot, object and an optional namespace")
    pool = pool_name(request["slot"])
    name, namespace = object_request(request)
    native = native if native is not None else omapwrite.Native(rados.lib)
    cli = cli if cli is not None else omapwrite.HeaderCLI(rados.root, {"pool": pool}, namespace)
    io = rados.ioctx(cluster, pool, namespace)
    try:
        return {"pool": pool, "object": request["object"], "omap_state": omap_state(native, io, name, cli)}
    finally:
        rados.lib.rados_ioctx_destroy(io)
