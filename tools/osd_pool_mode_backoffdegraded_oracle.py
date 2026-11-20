"""Owned-pool oracle mode backoffdegraded: the OSDs' osd_backoff_on_degraded entry.

Test-only; never product code. With the option set, a primary answers a
write-ordered op of a client that negotiated RADOS_BACKOFF on an object
degraded on an acting shard with an object-scope BLOCK and drops the op,
instead of parking it (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964, src/osd/PrimaryLogPG.cc do_op,
is_degraded_or_backfilling_object). The pinned default is false
(src/common/options/global.yaml.in), and OSDs read it on every op, so the
central entry takes effect without a restart.

Action enable records the central entry of the osd section as it was
(its value, or that there was none) in osd-backoff-degraded-claim.json in
the fixture output directory, then sets it to true; it refuses while a
claim is recorded. Action restore puts the recorded entry back, `config rm`
for none, confirms it with an independent `config dump` and drops the claim;
without a claim it changes nothing and reports that. Action show only reads.
No command is retried.
"""
import json
import os
import pathlib

from osd_pool_oracle import json_mon, oracle_mode

SECTION, OPTION = "osd", "osd_backoff_on_degraded"
CLAIM = "osd-backoff-degraded-claim.json"
MAX_CLAIM = 1 << 10


def entry(entries):
    """The osd section's unmasked osd_backoff_on_degraded value, or None."""
    if not isinstance(entries, list):
        raise ValueError("native config dump is not a list")
    found = [e for e in entries if isinstance(e, dict) and e.get("section") == SECTION and e.get("name") == OPTION and not e.get("mask")]
    if len(found) > 1:
        raise RuntimeError("native config dump lists %s twice" % OPTION)
    if found and type(found[0].get("value")) is not str:
        raise ValueError("native config entry has no string value")
    return found[0]["value"] if found else None


def read_claim(out):
    """The recorded prior entry as {"prior": value or None}, or None."""
    path = pathlib.Path(out) / CLAIM
    try:
        with path.open("rb") as file:
            data = file.read(MAX_CLAIM + 1)
    except FileNotFoundError:
        return None
    claim = json.loads(data) if len(data) <= MAX_CLAIM else None
    if not isinstance(claim, dict) or set(claim) != {"prior"} or not (claim["prior"] is None or type(claim["prior"]) is str):
        raise ValueError("invalid osd_backoff_on_degraded claim")
    return claim


def current(rados, cluster):
    return entry(json_mon(rados, cluster, {"prefix": "config dump"}, "config dump"))


@oracle_mode
def mode_backoffdegraded(rados, cluster, request, out):
    """Set the osd section's osd_backoff_on_degraded to true, or restore it."""
    if set(request) != {"action"} or request["action"] not in ("enable", "restore", "show"):
        raise ValueError("backoffdegraded takes only action enable, restore or show")
    action = request["action"]
    claim = read_claim(out)
    if action == "show":
        return {"backoff_degraded": {"action": action, "value": current(rados, cluster), "claimed": claim is not None}}
    if action == "enable":
        if claim is not None:
            raise RuntimeError("osd_backoff_on_degraded is already claimed; restore it first")
        prior = current(rados, cluster)
        temporary = pathlib.Path(out) / (CLAIM + ".tmp")
        temporary.write_bytes(json.dumps({"prior": prior}).encode() + b"\n")
        os.replace(temporary, pathlib.Path(out) / CLAIM)
        code, _ = rados.mon(cluster, {"prefix": "config set", "who": SECTION, "name": OPTION, "value": "true"})
        if code < 0:
            raise RuntimeError("native config set %s failed: %d %s" % (OPTION, code, rados.last_status))
        return {"backoff_degraded": {"action": action, "prior": prior, "value": current(rados, cluster), "claimed": True}}
    if claim is None:
        return {"backoff_degraded": {"action": action, "value": current(rados, cluster), "claimed": False, "restored": False}}
    prior = claim["prior"]
    if prior is None:
        code, _ = rados.mon(cluster, {"prefix": "config rm", "who": SECTION, "name": OPTION})
    else:
        code, _ = rados.mon(cluster, {"prefix": "config set", "who": SECTION, "name": OPTION, "value": prior})
    now = current(rados, cluster)
    restored = code >= 0 and now == prior
    if restored:
        os.remove(pathlib.Path(out) / CLAIM)
    return {"backoff_degraded": {"action": action, "prior": prior, "value": now, "claimed": True, "restored": restored}}
