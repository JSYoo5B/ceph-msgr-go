"""Owned-pool oracle mode copystaging: one complete listing of one PG on one OSD.

Test-only; never product code. It sends two read-only commands to one
explicit OSD and changes nothing: ``bluestore collections`` and
``bluestore list <pgid>_head GHMIN <max>``. It reports how many objects the
listing holds, whether it is complete, and every temporary object in it.

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):

- src/os/bluestore/BlueAdmin.cc:24-38 registers both commands on the OSD's
  admin socket, which an OSD also answers for a command sent to it
  (src/osd/OSD.cc:7379-7393). BlueAdmin.cc:72-80 prints one collection per
  line; BlueAdmin.cc:81-118 lists at most ``max`` objects of one collection
  from ``start`` to the end, one printed ghobject_t per line, and prints
  nothing when the listing fails.
- src/osd/PrimaryLogPG.cc:8954-8962 (generate_temp_object) names the
  temporary object of a multi-chunk copy ``temp_<pgid>_<role>_<gid>_<n>``,
  and src/common/hobject.h:304-309 (make_temp_hobject) keeps the target's
  hash and puts it in pool -2-<pool>, which sorts before every object of the
  PG's pool, so a listing from GHMIN reaches the temporary objects first.
  src/common/hobject.cc:523-535 prints an unsharded ghobject_t without
  generation as ``#<hobject>#``, the hobject starting with its pool.

A listing of fewer than ``max`` entries is the collection's whole committed
content at that moment: it shows which temporary objects exist, and an
absent one is absent from that OSD's store then, not from any other OSD or
from the PG's history. A refused or unparsable reply is reported as
incomplete, never raised, so a failed observation does not stop the fixture.
"""
import hashlib
import re

from osd_pool_oracle import integer, oracle_mode

MAX_LIST = 4096
MAX_COLLECTIONS_BYTES = 1 << 20
MAX_LIST_BYTES = 4 << 20
MAX_TEMP = 64
PGID = re.compile(r"(0|[1-9][0-9]{0,18})\.(0|[1-9a-f][0-9a-f]{0,7})")
ENTRY = re.compile(r"#(-?(?:0|[1-9][0-9]{0,18})):[^\n]*#")


def parse_listing(raw):
    """(objects, temporary entries) of one listing, or ValueError."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("native object list is not UTF-8") from None
    objects, temporary = 0, []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        match = ENTRY.fullmatch(line)
        if not match:
            raise ValueError("native object list has an unparsable entry")
        objects += 1
        if int(match[1]) <= -2:
            temporary.append(line)
    if len(temporary) > MAX_TEMP:
        raise ValueError("native object list holds more temporary objects than the bound")
    return objects, temporary


@oracle_mode
def mode_copystaging(rados, cluster, request, out):
    """List one PG collection of one OSD completely; read-only."""
    if set(request) != {"osd", "pgid"}:
        raise ValueError("copystaging takes exactly osd and pgid")
    osd = integer(request["osd"], 0, (1 << 31) - 1, "osd")
    if not isinstance(request["pgid"], str) or not PGID.fullmatch(request["pgid"]):
        raise ValueError("invalid copystaging pgid")
    collection = request["pgid"] + "_head"
    receipt = {"osd": osd, "pgid": request["pgid"], "collection": collection, "max": MAX_LIST, "complete": False,
               "objects": 0, "temp": [], "failures": []}
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "bluestore collections"}, MAX_COLLECTIONS_BYTES)
    receipt["collections_code"] = code
    if code != 0 or oversized is not None:
        receipt["failures"].append("native bluestore collections returned %s (%s): %s" % (code, oversized, str(status)[:200]))
        return {"copy_staging": receipt}
    names = {line.strip() for line in raw.decode("utf-8", "replace").split("\n") if line.strip()}
    if collection not in names:
        receipt["failures"].append("the OSD's store has no collection " + collection)
        return {"copy_staging": receipt}
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "bluestore list", "collection": collection, "start": "GHMIN", "max": MAX_LIST},
                                             MAX_LIST_BYTES)
    receipt["list_code"] = code
    if code != 0 or oversized is not None:
        receipt["failures"].append("native bluestore list returned %s (%s): %s" % (code, oversized, str(status)[:200]))
        return {"copy_staging": receipt}
    receipt["sha256"] = hashlib.sha256(raw).hexdigest()
    try:
        receipt["objects"], receipt["temp"] = parse_listing(raw)
    except ValueError as error:
        receipt["failures"].append(str(error))
        return {"copy_staging": receipt}
    # BlueAdmin.cc prints nothing for a failed listing; the PG metadata
    # object is always present, so an empty listing is not a complete one.
    if receipt["objects"] == 0:
        receipt["failures"].append("the listing is empty")
    elif receipt["objects"] >= MAX_LIST:
        receipt["failures"].append("the listing reached its bound")
    receipt["complete"] = not receipt["failures"]
    return {"copy_staging": receipt}
