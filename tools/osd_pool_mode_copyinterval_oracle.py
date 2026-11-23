"""Owned-pool oracle mode copyinterval: the interval of one owned PG, replicated or erasure-coded.

Test-only; never product code. It reads one placement record of an owned
object, sends one read-only ``pg <pgid> query`` to the primary that record
names, and reads one more placement record. It changes nothing and judges
nothing; the copy tests judge the observation (copyIntervalHeld,
integration/osd_copyfrom_interval_test.go).

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):

- src/osd/PrimaryLogPG.cc:1055-1075 answers ``pg <pgid> query`` with
  ``PeeringState::dump_peering_state`` (src/osd/PeeringState.cc:4559-4606):
  ``state``, the OSD's map ``epoch``, ``up``, ``acting`` and the primary's own
  ``info``.
- src/osd/osd_types.cc:3693-3696 (``pg_info_t::dump``) prints ``pgid`` as an
  spg_t, ``<pgid>`` for a replicated PG and ``<pgid>s<shard>`` for a shard of
  an erasure-coded one, and :3579-3600 (``pg_history_t::dump``) prints
  ``same_interval_since`` inside ``history``. Every interval change,
  including one that comes back to the same sets, sets it to the epoch of the
  new interval (PeeringState::start_peering_interval), so it only grows.

Unlike the pgstate mode, which parses the shard records of an erasure-coded
primary, this mode reads only the fields an interval witness needs and so
also answers for a replicated PG. A refused, oversized or unparsable reply is
reported with its reason, never raised.
"""
from osd_pool_oracle import oracle_mode, pg_observer
import osd_pg_history_oracle as history

MAX_QUERY_BYTES = 256 << 10


def _epoch(value):
    return value if type(value) is int and 0 < value < 1 << 32 else None


def _vector(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 64 or any(type(osd) is not int or not 0 <= osd < 1 << 31 for osd in value):
        return None
    return value


def parse_query(code, raw, status, oversized):
    """(query, failure) of one ``pg query`` reply: the state, the primary's
    map epoch, its sets, the pgid its info names and its interval start."""
    if oversized is not None:
        return None, "pg query reply of %d bytes exceeds the bound" % oversized
    if type(code) is not int or code != 0:
        return None, "pg query returned %s: %s" % (code, str(status)[:200])
    try:
        document = history.strict_document(raw, MAX_QUERY_BYTES)
    except history.HistoryError as error:
        return None, str(error)[:200]
    if not isinstance(document, dict):
        return None, "native output is not a pg query reply"
    info = document.get("info")
    past = info.get("history") if isinstance(info, dict) else None
    query = {"state": document.get("state"), "epoch": _epoch(document.get("epoch")), "up": _vector(document.get("up")),
             "acting": _vector(document.get("acting")), "pgid": info.get("pgid") if isinstance(info, dict) else None,
             "same_interval_since": _epoch(past.get("same_interval_since")) if isinstance(past, dict) else None}
    if not isinstance(query["state"], str) or not 0 < len(query["state"]) <= 200:
        return None, "pg query has no usable state"
    if not isinstance(query["pgid"], str) or not 0 < len(query["pgid"]) <= 40:
        return None, "pg query info has no pgid"
    missing = [name for name in ("epoch", "up", "acting", "same_interval_since") if query[name] is None]
    if missing:
        return None, "pg query has no usable " + ", ".join(missing)
    return query, None


@oracle_mode
def mode_copyinterval(rados, cluster, request, out):
    """Two placement records around one `pg query` of the primary of an owned object's PG."""
    if set(request) - {"slot", "object", "namespace"}:
        raise ValueError("copyinterval takes only slot, object and namespace")
    _, place, _ = pg_observer(rados, cluster, request)
    first = place()
    seen = {"before": first if isinstance(first, dict) else None, "after": None, "code": None}
    if not isinstance(first, dict):
        seen["placement_failure"] = str(first)[:200]
        return {"copy_interval": seen}
    code, raw, status, oversized = rados.osd(cluster, first["primary_id"], {"prefix": "pg", "pgid": first["pgid"], "cmd": "query", "format": "json"},
                                             MAX_QUERY_BYTES)
    seen["code"] = code
    query, failure = parse_query(code, raw, status, oversized)
    if failure:
        seen["query_failure"] = failure
    else:
        seen["query"] = query
    second = place()
    if isinstance(second, dict):
        seen["after"] = second
    else:
        seen["placement_failure"] = str(second)[:200]
    return {"copy_interval": seen}
