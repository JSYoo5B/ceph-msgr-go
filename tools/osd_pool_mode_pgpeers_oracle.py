"""Owned-pool oracle mode pgpeers: one `pg query` of a replicated owned PG.

Test-only; never product code. It sends one `pg <pgid> query` to the OSD
the caller names as the PG's primary, and reports the PG state, up and
acting sets, the primary's own last_update and last_complete and, for every
peer_info entry, the peer and its last_update and last_complete. A peer
whose last_complete is behind its last_update still lacks objects of the
log between them: pg_info_t's last_complete is the newest version up to
which every object is present (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964, src/osd/osd_types.h pg_info_t and
src/osd/osd_types.cc pg_info_t::dump). The pgid must name a PG of an owned
pool. It judges nothing and sends nothing that changes the cluster.
"""
import json
import re

from osd_pool_oracle import SLOTS, find_pool, integer, oracle_mode, pool_name

MAX_QUERY_BYTES = 4 << 20
PGID = re.compile(r"[0-9]{1,10}\.[0-9a-f]{1,8}")
EVERSION = re.compile(r"[0-9]{1,10}'[0-9]{1,20}")


def eversion(value, label):
    if not isinstance(value, str) or not EVERSION.fullmatch(value):
        raise ValueError("pg query %s is not an eversion" % label)
    return value


def info(document, label):
    if not isinstance(document, dict):
        raise ValueError("pg query %s is not a pg_info_t dump" % label)
    return {"last_update": eversion(document.get("last_update"), label + " last_update"),
            "last_complete": eversion(document.get("last_complete"), label + " last_complete")}


def parse(raw):
    document = json.loads(raw)
    if not isinstance(document, dict) or not {"state", "up", "acting", "info", "peer_info"} <= set(document):
        raise ValueError("native output is not a pg query reply")
    if not isinstance(document["state"], str) or not 0 < len(document["state"]) <= 200:
        raise ValueError("pg query has no usable state")
    for key in ("up", "acting"):
        if not isinstance(document[key], list) or not all(type(i) is int for i in document[key]):
            raise ValueError("pg query has no usable %s set" % key)
    peers = document["peer_info"]
    if not isinstance(peers, list) or len(peers) > 64:
        raise ValueError("pg query has no usable peer_info array")
    parsed = {"state": document["state"], "up": document["up"], "acting": document["acting"], "info": info(document["info"], "info"), "peers": []}
    for peer in peers:
        if not isinstance(peer, dict) or not isinstance(peer.get("peer"), str) or len(peer["peer"]) > 40:
            raise ValueError("pg query has an unexpected peer_info entry")
        parsed["peers"].append(dict(info(peer, "peer_info"), peer=peer["peer"]))
    return parsed


@oracle_mode
def mode_pgpeers(rados, cluster, request, out):
    """The state and the peers' completeness of one owned replicated PG."""
    if set(request) != {"pgid", "osd"}:
        raise ValueError("pgpeers takes exactly pgid and osd")
    pgid, osd = request["pgid"], integer(request["osd"], 0, 1 << 20, "osd")
    if not isinstance(pgid, str) or not PGID.fullmatch(pgid):
        raise ValueError("pgid must be <pool>.<hex seed>")
    dump = rados.dump(cluster)
    owned = {p["pool"]: p for p in (find_pool(dump, pool_name(slot)) for slot in SLOTS) if p}
    pool = owned.get(int(pgid.split(".")[0]))
    if pool is None or pool.get("type") != 1:
        raise ValueError("pgid is not a PG of an owned replicated pool")
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "pg", "pgid": pgid, "cmd": "query", "format": "json"}, MAX_QUERY_BYTES)
    if oversized is not None:
        raise ValueError("pg query reply of %d bytes exceeds the bound" % oversized)
    if code != 0:
        raise RuntimeError("native pg query failed: %d %s" % (code, status[:200]))
    return {"pg_peers": dict(parse(raw), pgid=pgid, osd=osd)}
