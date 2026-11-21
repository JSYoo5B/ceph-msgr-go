"""Owned-pool oracle mode copychunk: one OSD's runtime osd_copyfrom_max_chunk.

Test-only; never product code. A COPY_FROM primary reads its source in
chunks of at most osd_copyfrom_max_chunk bytes (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964, src/osd/PrimaryLogPG.h
get_copy_chunk_size, read by _copy_some at every chunk,
src/osd/PrimaryLogPG.cc:9564-9600), so lowering it on the destination
primary makes a small source a multi-chunk copy. The commands go to one
explicit OSD: ``config get`` and ``config set`` of
src/common/ceph_context.cc:593-620 and 756-758, which change only that
daemon's runtime value; a restarted OSD starts from its configuration again.

Action lower records the OSD's effective value as it was in
osd-copy-chunk-claim-<osd>.json in the fixture output directory, refuses
unless that value is above LIMIT, sets LIMIT once and reads it back. It
sets nothing while a claim of that OSD is recorded. Action restore reads the
effective value; only when it differs from the recorded one it sets the
recorded value once, reads it back, and drops the claim when the readback
matches. Without a claim it changes nothing. Action show only reads. No
command is retried, and a refused command is reported, not raised.
"""
import json
import os
import pathlib

from osd_pool_oracle import integer, oracle_mode

OPTION = "osd_copyfrom_max_chunk"
LIMIT = 32 << 10
MAX_REPLY = 4 << 10
MAX_CLAIM = 1 << 10


def claim_path(out, osd):
    return pathlib.Path(out) / ("osd-copy-chunk-claim-%d.json" % osd)


def read_claim(out, osd):
    """The recorded prior value as {"osd", "prior"}, or None."""
    try:
        with claim_path(out, osd).open("rb") as file:
            data = file.read(MAX_CLAIM + 1)
    except FileNotFoundError:
        return None
    claim = json.loads(data) if len(data) <= MAX_CLAIM else None
    if not isinstance(claim, dict) or set(claim) != {"osd", "prior"} or claim["osd"] != osd or type(claim["prior"]) is not int or claim["prior"] <= 0:
        raise ValueError("invalid %s claim" % OPTION)
    return claim


def effective(rados, cluster, osd):
    """(value or None, failure text or None) from one `config get`."""
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "config get", "var": OPTION, "format": "json"}, MAX_REPLY)
    if code != 0 or oversized is not None:
        return None, "native config get returned %s (%s): %s" % (code, oversized, str(status)[:200])
    try:
        document = json.loads(raw)
        value = int(document[OPTION]) if isinstance(document, dict) and set(document) == {OPTION} else None
    except (ValueError, TypeError):
        value = None
    if value is None or value <= 0:
        return None, "native config get has no positive %s" % OPTION
    return value, None


def assign(rados, cluster, osd, value):
    """The failure text of one `config set`, or None."""
    code, _, status, oversized = rados.osd(cluster, osd, {"prefix": "config set", "var": OPTION, "val": [str(value)], "format": "json"}, MAX_REPLY)
    if code != 0 or oversized is not None:
        return "native config set returned %s (%s): %s" % (code, oversized, str(status)[:200])
    return None


@oracle_mode
def mode_copychunk(rados, cluster, request, out):
    """Lower one OSD's osd_copyfrom_max_chunk to LIMIT, or restore it."""
    if set(request) != {"osd", "action"} or request["action"] not in ("lower", "restore", "show"):
        raise ValueError("copychunk takes exactly osd and action lower, restore or show")
    osd = integer(request["osd"], 0, (1 << 31) - 1, "osd")
    action = request["action"]
    claim = read_claim(out, osd)
    receipt = {"osd": osd, "action": action, "limit": LIMIT, "claimed": claim is not None, "failures": []}
    value, failure = effective(rados, cluster, osd)
    receipt["value"] = value
    if failure:
        receipt["failures"].append(failure)
    if action == "show" or failure:
        return {"copy_chunk": receipt}
    if action == "lower":
        if claim is not None:
            receipt["failures"].append("%s of osd.%d is already claimed; restore it first" % (OPTION, osd))
            return {"copy_chunk": receipt}
        receipt["prior"] = value
        if value <= LIMIT:
            receipt["failures"].append("the effective %s %d is not above %d" % (OPTION, value, LIMIT))
            return {"copy_chunk": receipt}
        temporary = claim_path(out, osd).with_suffix(".tmp")
        temporary.write_bytes(json.dumps({"osd": osd, "prior": value}).encode() + b"\n")
        os.replace(temporary, claim_path(out, osd))
        receipt["claimed"], receipt["set"] = True, True
        failure = assign(rados, cluster, osd, LIMIT)
        if failure is None:
            receipt["value"], failure = effective(rados, cluster, osd)
        if failure:
            receipt["failures"].append(failure)
        elif receipt["value"] != LIMIT:
            receipt["failures"].append("the readback %d is not %d" % (receipt["value"], LIMIT))
        receipt["lowered"] = not receipt["failures"]
        return {"copy_chunk": receipt}
    if claim is None:
        receipt["restored"] = False
        return {"copy_chunk": receipt}
    prior = receipt["prior"] = claim["prior"]
    receipt["set"] = value != prior
    if receipt["set"]:
        failure = assign(rados, cluster, osd, prior)
        if failure is None:
            receipt["value"], failure = effective(rados, cluster, osd)
        if failure:
            receipt["failures"].append(failure)
    receipt["restored"] = not receipt["failures"] and receipt["value"] == prior
    if receipt["restored"]:
        os.remove(claim_path(out, osd))
        receipt["claimed"] = False
    return {"copy_chunk": receipt}
