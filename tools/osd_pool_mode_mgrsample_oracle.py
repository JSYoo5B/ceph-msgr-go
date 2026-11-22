"""Owned-pool oracle mode mgrsample: native samples a MGR report is compared with.

Test-only; never product code. One read-only observation, all through the
public librados C API of the pinned Ceph 20.2.4 (commit
7f793731f1b39eb4f465e960113d2363c311b964), none through a MGR: the MON's
`osd dump` (map epoch, and each OSD's up_from), the placement of one owned
object (`osd map` with the matching dump, osd_mutation_oracle.py), the
primary's own `pg query` for that PG (its info.stats: reported epoch and
sequence, state, up and acting sets and primaries, and the object, byte and
write counts of its stat_sum), and the primary's `perf dump` values of the
requested counter paths. A stock MGR would aggregate the same pg_stat_t and
perf counters from the OSD's own MPGStats and MMgrReport; here the test's Go
MGR receives those messages and compares them with this sample. The `osd
dump` is read before and after so a caller can tell whether the map moved
during the sample. It sends nothing that changes the cluster.
"""
import json

import osd_mutation_oracle as mutation
from osd_pool_oracle import find_pool, object_request, oracle_mode, pool_name

MAX_PERF_BYTES = 4 << 20
MAX_QUERY_BYTES = 4 << 20
MAX_COUNTERS = 16
STAT_SUM = ("num_objects", "num_bytes", "num_write", "num_write_kb", "num_read")


def osd_epochs(dump):
    """The map epoch and each OSD's up_from of one native `osd dump`."""
    return {"epoch": dump["epoch"], "up_from": {str(o["osd"]): o["up_from"] for o in dump["osds"]}}


def counter_values(perf, paths):
    """The requested `<section>.<name>` paths of a native `perf dump`.

    A scalar counter is its integer; a LONGRUNAVG counter is its avgcount and
    its sum as printed. A missing path is reported as null.
    """
    out = {}
    for path in paths:
        section, _, name = path.partition(".")
        value = perf.get(section, {}).get(name) if name else None
        if isinstance(value, dict):
            value = {"avgcount": value.get("avgcount"), "sum": value.get("sum")}
        elif value is not None and type(value) is not int:
            raise ValueError("native perf counter %s is not an integer: %r" % (path, value))
        out[path] = value
    return out


def pg_stats(document):
    """The info.stats fields this sample reports from one native `pg query`."""
    stats = document["info"]["stats"]
    return {"pgid": document["info"]["pgid"], "state": stats["state"], "reported_epoch": stats["reported_epoch"],
            "reported_seq": stats["reported_seq"], "up": stats["up"], "acting": stats["acting"],
            "up_primary": stats["up_primary"], "acting_primary": stats["acting_primary"],
            "stat_sum": {key: stats["stat_sum"][key] for key in STAT_SUM}}


@oracle_mode
def mode_mgrsample(rados, cluster, request, out):
    """Native OSD map, placement, primary pg query and perf counters."""
    if set(request) - {"slot", "object", "namespace", "counters"}:
        raise ValueError("mgrsample takes only slot, object, namespace and counters")
    counters = request.get("counters", [])
    if type(counters) is not list or len(counters) > MAX_COUNTERS or not all(type(c) is str and 0 < len(c) <= 128 for c in counters):
        raise ValueError("invalid mgrsample counters")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    before = rados.dump(cluster)
    descriptor = find_pool(before, pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace)
    primary = placement["primary_id"]
    code, raw, status, oversized = rados.osd(cluster, primary, {"prefix": "pg", "pgid": placement["pgid"], "cmd": "query", "format": "json"}, MAX_QUERY_BYTES)
    if code != 0 or oversized is not None:
        raise RuntimeError("native pg query failed: %s %s" % (code, status))
    query = pg_stats(json.loads(raw))
    code, raw, status, oversized = rados.osd(cluster, primary, {"prefix": "perf dump", "format": "json"}, MAX_PERF_BYTES)
    if code != 0 or oversized is not None:
        raise RuntimeError("native perf dump failed: %s %s" % (code, status))
    values = counter_values(json.loads(raw), counters)
    after = rados.dump(cluster)
    return {"sample": {"before": osd_epochs(before), "after": osd_epochs(after), "placement": placement,
                       "pg": query, "counters": values}}
