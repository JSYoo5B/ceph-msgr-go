"""Owned-pool oracle mode snaptrim: where a removed snapshot of an owned pool is.

Test-only; never product code. One read-only observation, sending nothing
that changes the cluster: the native `osd dump` (the pool's snap_seq and its
entry of the OSDMap removed_snaps_queue), one placement record and `pg query`
of the primary of an owned object's PG, replicated or erasure-coded (its
snap_trimq and info.purged_snaps,
Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964,
src/osd/PrimaryLogPG.cc do_command "query", src/osd/osd_types.cc
pg_info_t::dump), and `rados listsnaps` of up to eight owned objects.

A removed self-managed snapshot is first in the removed_snaps_queue and in the
snap_trimq of each PG; the trimmer then moves it to the PG's purged_snaps
(PrimaryLogPG.cc SnapTrimmer AwaitAsyncWork); the MON removes it from the
queue once the MGR digest reports it purged (src/mon/OSDMonitor.cc
try_prune_purged_snaps), and the PG then drops it from purged_snaps
(src/osd/PG.cc on_active_advmap). This mode reports those sets as snap ids and
judges nothing.
"""
import json
import re
import subprocess

import osd_pg_history_oracle as history
from osd_pool_oracle import find_pool, integer, object_request, oracle_mode, pool_name
import osd_mutation_oracle as mutation

MAX_QUERY = 256 << 10
MAX_SNAPS = 1024
MAX_OBJECTS = 8
# snapid_t prints in hexadecimal; interval_set prints "[start~length,...]".
TRIMQ = re.compile(r"\[((?:[0-9a-f]+~[0-9a-f]+)(?:,[0-9a-f]+~[0-9a-f]+)*)?\]")


def expand(intervals):
    """Expand (start, length) pairs into a sorted, bounded list of snap ids."""
    snaps = []
    for start, length in intervals:
        if start < 1 or length < 1 or len(snaps) + length > MAX_SNAPS:
            raise ValueError("snap interval outside its bound")
        snaps.extend(range(start, start + length))
    if snaps != sorted(set(snaps)):
        raise ValueError("snap intervals overlap or are out of order")
    return snaps


def trimq(text):
    """The pinned `snap_trimq` stream form, as snap ids."""
    match = TRIMQ.fullmatch(text) if type(text) is str else None
    if match is None:
        raise ValueError("unexpected snap_trimq form")
    if not match.group(1):
        return []
    return expand([tuple(int(part, 16) for part in item.split("~")) for item in match.group(1).split(",")])


def purged(entries):
    """pg_info_t::dump purged_snaps: [{"start": hex, "length": hex}, ...]."""
    if type(entries) is not list:
        raise ValueError("purged_snaps is not a list")
    pairs = []
    for entry in entries:
        if type(entry) is not dict or set(entry) != {"start", "length"} or type(entry["start"]) is not str or type(entry["length"]) is not str:
            raise ValueError("unexpected purged_snaps entry")
        pairs.append((int(entry["start"], 16), int(entry["length"], 16)))
    return expand(pairs)


def queue(dump, pool):
    """The pool's OSDMap::dump removed_snaps_queue entry, as snap ids."""
    entries = [e for e in dump.get("removed_snaps_queue", []) if e.get("pool") == pool]
    if len(entries) > 1:
        raise ValueError("ambiguous removed_snaps_queue entry")
    if not entries:
        return []
    return expand([(integer(i["begin"], 1, (1 << 64) - 3, "begin"), integer(i["length"], 1, MAX_SNAPS, "length")) for i in entries[0]["snaps"]])


def query_state(document):
    """The trim state of one primary `pg query` document."""
    info = document.get("info")
    if type(info) is not dict or type(document.get("state")) is not str or type(document.get("epoch")) is not int:
        raise ValueError("pg query lacks its info, state or epoch")
    snaps = trimq(document.get("snap_trimq"))
    if document.get("snap_trimq_len") != len(snaps):
        raise ValueError("snap_trimq_len differs from snap_trimq")
    return {"pgid": info.get("pgid"), "state": document["state"], "epoch": document["epoch"],
            "snap_trimq": snaps, "purged_snaps": purged(info.get("purged_snaps"))}


def query_pgid(placement):
    """The PG id the primary's `pg query` reports: an erasure-coded PG's
    info.pgid is the spg_t of the primary's own shard, "<pgid>s<shard>"
    (src/osd/osd_types.h spg_t operator<<); a replicated PG's has no shard."""
    shard = placement["pg_shard"]
    return placement["pgid"] if shard < 0 else "%ss%d" % (placement["pgid"], shard)


@oracle_mode
def mode_snaptrim(rados, cluster, request, out, run=subprocess.run):
    """Where removed snapshots of an owned pool are, for one object's PG.

    Read-only: `osd dump`, one placement record, the primary's `pg query`
    and `rados listsnaps` of the listed owned objects. It judges nothing.
    """
    if set(request) - {"slot", "object", "namespace", "objects"}:
        raise ValueError("snaptrim takes only slot, object, namespace and objects")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    # A JSON null is an empty list (a Go nil slice).
    objects = request.get("objects") or []
    if type(objects) is not list or len(objects) > MAX_OBJECTS:
        raise ValueError("snaptrim lists at most eight objects")
    for name in objects:
        object_request({"object": name})
    dump = rados.dump(cluster)
    descriptor = find_pool(dump, pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace, timeout=10)
    code, raw, status, oversized = rados.osd(cluster, placement["primary_id"], {"prefix": "pg", "pgid": placement["pgid"], "cmd": "query", "format": "json"}, MAX_QUERY)
    if code != 0 or oversized is not None:
        raise RuntimeError("native pg query failed (%d): %s" % (code, status))
    state = query_state(history.strict_document(raw, MAX_QUERY))
    if state["pgid"] != query_pgid(placement):
        raise ValueError("pg query answered for another PG")
    listings = {}
    for name in objects:
        args = ["rados", "-c", str(rados.root / "ceph.conf"), "-n", "client.test", "--keyring", str(rados.root / "keyring"), "-p", pool]
        if namespace:
            args += ["-N", namespace]
        result = run(args + ["listsnaps", name, "--format=json"], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if len(result.stdout) > 65536 or len(result.stderr) > 4096:
            raise RuntimeError("native listsnaps exceeded its output bound")
        entry = {"exit": result.returncode, "stderr": result.stderr.decode(errors="replace")[:512]}
        if result.returncode == 0:
            entry["json"] = json.loads(result.stdout)
        listings[name] = entry
    return {"snap_trim": dict(state, osdmap_epoch=dump["epoch"], pool_id=descriptor["pool"], snap_seq=descriptor["snap_seq"],
                              removed_snaps_queue=queue(dump, descriptor["pool"]), primary=placement["primary_id"],
                              primary_shard=placement["pg_shard"], listsnaps=listings)}
