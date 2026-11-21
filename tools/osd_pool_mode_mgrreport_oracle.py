"""Owned-pool oracle mode mgrreport: what MON committed from MGR reports.

Test-only; never product code. One read-only observation through the public
librados C API of the pinned Ceph 20.2.4 (commit
7f793731f1b39eb4f465e960113d2363c311b964), on the oracle's own MON session:
the MgrMap before and after (`mgr dump`: epoch, active GID and name,
standby GIDs), `rados_cluster_stat` (the MON's MStatfsReply, which
MgrStatMonitor::preprocess_statfs answers from the committed PGMapDigest,
ceph/ceph/blob/v20.2.4/src/mon/MgrStatMonitor.cc), and the `health detail`
checks whose code starts with the requested prefix (HealthMonitor gathers
the MGR stat service's committed checks, the ones the last accepted
MMonMgrReport carried; ceph/ceph/blob/v20.2.4/src/mon/HealthMonitor.cc).
The test's Go MGR sends the reports; this oracle never talks to a MGR and
sends nothing that changes the cluster.
"""
import ctypes as C
import json
import re

from osd_pool_oracle import ClusterStat, oracle_mode

MAX_CHECKS = 16
PREFIX = re.compile(r"[A-Z][A-Z0-9_]{3,63}")


def manager(dump):
    """The roles of one native `mgr dump`."""
    return {"epoch": dump["epoch"], "active_gid": dump["active_gid"], "active_name": dump["active_name"],
            "available": dump["available"], "standby_gids": sorted(s["gid"] for s in dump.get("standbys", []))}


def checks(detail, prefix):
    """The checks of one native `health detail` whose code starts with prefix."""
    out = {}
    for code, check in detail.get("checks", {}).items():
        if not code.startswith(prefix):
            continue
        out[code] = {"severity": check["severity"], "summary": check["summary"]["message"],
                     "count": check["summary"]["count"], "detail": [d["message"] for d in check.get("detail", [])]}
    if len(out) > MAX_CHECKS:
        raise ValueError("more matching health checks than the bound")
    return out


@oracle_mode
def mode_mgrreport(rados, cluster, request, out):
    """Native MgrMap roles, cluster_stat and the prefixed health checks."""
    if set(request) != {"prefix"}:
        raise ValueError("mgrreport takes only prefix")
    prefix = request["prefix"]
    if type(prefix) is not str or not PREFIX.fullmatch(prefix):
        raise ValueError("invalid mgrreport prefix")

    def mon(command):
        code, raw = rados.mon(cluster, command)
        if code != 0:
            raise RuntimeError("native %s failed: %s %s" % (command["prefix"], code, rados.last_status))
        return json.loads(raw)

    before = manager(mon({"prefix": "mgr dump", "format": "json"}))
    stat = ClusterStat()
    rados.native.check(rados.lib.rados_cluster_stat(cluster, C.byref(stat)))
    health = checks(mon({"prefix": "health", "detail": "detail", "format": "json"}), prefix)
    after = manager(mon({"prefix": "mgr dump", "format": "json"}))
    return {"mgr_report": {"before": before, "after": after, "health": health,
                           "cluster_stat": {"kb": stat.kb, "kb_used": stat.kb_used, "kb_avail": stat.kb_avail,
                                            "num_objects": stat.num_objects}}}
