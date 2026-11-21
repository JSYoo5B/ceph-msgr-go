"""Owned-pool oracle mode osdstate: one OSD's entry in the native OSD map.

Test-only; never product code. One read-only `osd dump` through the native
client, sending nothing that changes the cluster. It reports the map epoch
and, for the requested OSD, its up and in states and its up_from, up_thru
and down_at epochs.

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):
OSDMap::dump prints the epoch and every existing OSD through dump_osd, with
"up" and "in" as integers and the osd_info_t epochs beside them
(src/osd/OSDMap.cc:4137-4145, :4015-4030, osd_info_t::dump :73-81). When an
incremental map marks an up OSD down, OSDMap::apply_incremental sets that
OSD's down_at to the new epoch, and when it marks an OSD up it sets up_from
(src/osd/OSDMap.cc:2484-2521). An OSD whose up_from and down_at are the same
in two dumps, and which both dumps show up, was not marked down by any map
between them. It judges nothing else.
"""
from osd_pool_oracle import json_mon, oracle_mode

EPOCHS = ("up_from", "up_thru", "down_at")


def osd_entry(dump, osd):
    """The projection of osd's entry of a native osd dump, with the epoch."""
    if not isinstance(dump, dict) or type(dump.get("epoch")) is not int or not isinstance(dump.get("osds"), list):
        raise ValueError("native osd dump has no epoch or OSD list")
    found = [e for e in dump["osds"] if isinstance(e, dict) and e.get("osd") == osd]
    if len(found) != 1:
        raise RuntimeError("native osd dump lists osd.%d %d times" % (osd, len(found)))
    entry = found[0]
    if any(entry.get(k) not in (0, 1) or type(entry.get(k)) is not int for k in ("up", "in")):
        raise ValueError("native osd dump entry of osd.%d has no integer up and in states" % osd)
    if any(type(entry.get(k)) is not int or entry[k] < 0 for k in EPOCHS):
        raise ValueError("native osd dump entry of osd.%d has no epochs %s" % (osd, ", ".join(EPOCHS)))
    state = {"epoch": dump["epoch"], "osd": osd, "up": entry["up"] == 1, "in": entry["in"] == 1}
    state.update((k, entry[k]) for k in EPOCHS)
    return state


@oracle_mode
def mode_osdstate(rados, cluster, request, out):
    """Read one OSD's state and epochs from one native osd dump."""
    if set(request) != {"osd"} or type(request["osd"]) is not int or request["osd"] < 0:
        raise ValueError("osdstate takes only a non-negative integer osd")
    return {"osd_state": osd_entry(json_mon(rados, cluster, {"prefix": "osd dump"}, "osd dump"), request["osd"])}
