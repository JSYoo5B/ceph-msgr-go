"""Owned-pool oracle mode pglogbounds: the OSDs' PG-log and duplicate bounds.

Test-only; never product code. In the pinned Ceph 20.2.4 source
(7f793731f1b39eb4f465e960113d2363c311b964) a primary computes how far to trim
its PG log on every write from the target entry count
(src/osd/OSD.cc:9675-9694 OSDService::get_target_pg_log_entries: the per-OSD
budget split over its PGs, clamped to osd_max_pg_log_entries and then raised
to osd_min_pg_log_entries) and trims only when at least osd_pg_log_trim_min
entries are over it (src/osd/PeeringState.cc:5065-5148, calc_trim_to and
calc_trim_to_aggressive). A trimmed entry, and each of its extra request IDs
with the entry's own version, becomes a duplicate record when it is within
osd_pg_log_dups_tracked versions of the newest entry, and the oldest
duplicates are dropped while more than osd_pg_log_dups_tracked remain
(src/osd/PGLog.cc:55-160, IndexedLog::trim). All four are read from the
configuration at each call (src/common/options/global.yaml.in, no startup
flag), so central entries take effect without a restart once each OSD has
applied them.

Action lower records, in osd-pglog-bounds-claim.json in the fixture output
directory, the central entry of each option in the osd section as it was
(its value, or that there was none), the up OSDs and each one's effective
values; it refuses while a claim is recorded. It then sets the entries of
BOUNDS once each and reads every up OSD's effective values (`config get`,
src/common/ceph_context.cc) until all of them show BOUNDS, within a bound.
Action restore puts every recorded entry back once (`config rm` for none),
confirms them with an independent `config dump`, waits until every recorded
OSD shows its recorded effective values again and drops the claim; without a
claim it changes nothing and reports that. Action show only reads. No
command that changes the cluster is repeated.
"""
import json
import os
import pathlib
import time

from osd_pool_oracle import json_mon, oracle_mode

SECTION = "osd"
# Every write past four entries trims, a trimmed entry becomes a duplicate
# record while it is within twelve versions of the newest entry, and at most
# twelve duplicate records stay.
BOUNDS = (("osd_min_pg_log_entries", 4), ("osd_max_pg_log_entries", 4), ("osd_pg_log_trim_min", 1), ("osd_pg_log_dups_tracked", 12))
OPTIONS = tuple(name for name, _ in BOUNDS)
CLAIM = "osd-pglog-bounds-claim.json"
MAX_CLAIM = 8 << 10
MAX_REPLY = 4 << 10
MAX_OSDS = 16
SETTLE_SECONDS = 30
SETTLE_PAUSE = .5


def entries(rados, cluster, options=OPTIONS):
    """The osd section's unmasked central entry of each option, or None."""
    dump = json_mon(rados, cluster, {"prefix": "config dump"}, "config dump")
    if not isinstance(dump, list):
        raise ValueError("native config dump is not a list")
    found = {}
    for name in options:
        matches = [e for e in dump if isinstance(e, dict) and e.get("section") == SECTION and e.get("name") == name and not e.get("mask")]
        if len(matches) > 1:
            raise RuntimeError("native config dump lists %s twice" % name)
        if matches and type(matches[0].get("value")) is not str:
            raise ValueError("native config entry %s has no string value" % name)
        found[name] = matches[0]["value"] if matches else None
    return found


def up_osds(rados, cluster):
    """The IDs of the OSDs the native map has up and in, ascending."""
    osds = sorted(o["osd"] for o in rados.dump(cluster).get("osds", []) if isinstance(o, dict) and type(o.get("osd")) is int and o.get("up") == 1 and o.get("in") == 1)
    if not 1 <= len(osds) <= MAX_OSDS:
        raise RuntimeError("the native map has %d up OSDs" % len(osds))
    return osds


def effective(rados, cluster, osd, options=OPTIONS):
    """({option: value} or None, failure text or None) from `config get` of every option."""
    values = {}
    for name in options:
        code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "config get", "var": name, "format": "json"}, MAX_REPLY)
        if code != 0 or oversized is not None:
            return None, "osd.%d config get %s returned %s (%s): %s" % (osd, name, code, oversized, str(status)[:200])
        try:
            document = json.loads(raw)
            value = int(document[name]) if isinstance(document, dict) and set(document) == {name} else None
        except (ValueError, TypeError):
            value = None
        if value is None or value < 0:
            return None, "osd.%d config get has no %s" % (osd, name)
        values[name] = value
    return values, None


def settle(rados, cluster, want, clock, sleep, options=OPTIONS):
    """Read every OSD of want ({osd: {option: value}}) until each shows its values.

    Returns ({osd: last values or None}, failures). Only reads are repeated.
    """
    deadline = clock() + SETTLE_SECONDS
    while True:
        seen, failures = {}, []
        for osd in sorted(want):
            values, failure = effective(rados, cluster, osd, options)
            seen[str(osd)] = values
            if failure:
                failures.append(failure)
            elif values != want[osd]:
                failures.append("osd.%d shows %s; want %s" % (osd, values, want[osd]))
        if not failures or clock() >= deadline:
            return seen, failures
        sleep(SETTLE_PAUSE)


def read_claim(out, claim=CLAIM, options=OPTIONS):
    """The recorded {"prior", "effective"} claim, or None."""
    path = pathlib.Path(out) / claim
    try:
        with path.open("rb") as file:
            data = file.read(MAX_CLAIM + 1)
    except FileNotFoundError:
        return None
    claim = json.loads(data) if len(data) <= MAX_CLAIM else None
    if not isinstance(claim, dict) or set(claim) != {"prior", "effective"} or not isinstance(claim["prior"], dict) or set(claim["prior"]) != set(options):
        raise ValueError("invalid PG-log bounds claim")
    if any(not (v is None or type(v) is str) for v in claim["prior"].values()):
        raise ValueError("invalid PG-log bounds claim")
    recorded = claim["effective"]
    if not isinstance(recorded, dict) or not 1 <= len(recorded) <= MAX_OSDS:
        raise ValueError("invalid PG-log bounds claim")
    for osd, values in recorded.items():
        if not osd.isdigit() or not isinstance(values, dict) or set(values) != set(options) or any(type(v) is not int for v in values.values()):
            raise ValueError("invalid PG-log bounds claim")
    return claim


@oracle_mode
def mode_pglogbounds(rados, cluster, request, out, clock=time.monotonic, sleep=time.sleep):
    """Lower the osd section's PG-log and duplicate bounds to BOUNDS, or restore them."""
    if set(request) != {"action"} or request["action"] not in ("lower", "restore", "show"):
        raise ValueError("pglogbounds takes only action lower, restore or show")
    return {"pglog_bounds": owned_settings(rados, cluster, request["action"], out, BOUNDS, CLAIM, clock, sleep)}


def owned_settings(rados, cluster, action, out, bounds, claim_name, clock, sleep):
    """Action lower, restore or show of the osd section's entries of bounds, recorded in claim_name.

    The receipt is the one this module's docstring describes for BOUNDS.
    """
    options = tuple(name for name, _ in bounds)
    claim = read_claim(out, claim_name, options)
    receipt = {"action": action, "bounds": dict(bounds), "claimed": claim is not None, "failures": []}
    if action == "show":
        osds = up_osds(rados, cluster)
        receipt["entries"] = entries(rados, cluster, options)
        receipt["effective"] = {str(osd): effective(rados, cluster, osd, options)[0] for osd in osds}
        return receipt
    if action == "lower":
        if claim is not None:
            raise RuntimeError("the %s are already claimed; restore them first" % ", ".join(options))
        prior, osds = entries(rados, cluster, options), up_osds(rados, cluster)
        recorded = {}
        for osd in osds:
            values, failure = effective(rados, cluster, osd, options)
            if failure:
                raise RuntimeError(failure)
            recorded[str(osd)] = values
        receipt.update(prior=prior, prior_effective=recorded)
        temporary = pathlib.Path(out) / (claim_name + ".tmp")
        temporary.write_bytes(json.dumps({"prior": prior, "effective": recorded}).encode() + b"\n")
        os.replace(temporary, pathlib.Path(out) / claim_name)
        receipt["claimed"] = True
        for name, value in bounds:
            code, _ = rados.mon(cluster, {"prefix": "config set", "who": SECTION, "name": name, "value": str(value)})
            if code < 0:
                receipt["failures"].append("native config set %s returned %d: %s" % (name, code, rados.last_status[:200]))
                break
        receipt["entries"] = entries(rados, cluster, options)
        if receipt["entries"] != {name: str(value) for name, value in bounds}:
            receipt["failures"].append("the central entries read back as %s" % receipt["entries"])
        if not receipt["failures"]:
            receipt["effective"], failures = settle(rados, cluster, {osd: dict(bounds) for osd in osds}, clock, sleep, options)
            receipt["failures"].extend(failures)
        receipt["lowered"] = not receipt["failures"]
        return receipt
    if claim is None:
        receipt["restored"] = False
        return receipt
    prior = receipt["prior"] = claim["prior"]
    # Reverse order: the maximum goes back before the minimum.
    for name in reversed(options):
        if prior[name] is None:
            code, _ = rados.mon(cluster, {"prefix": "config rm", "who": SECTION, "name": name})
        else:
            code, _ = rados.mon(cluster, {"prefix": "config set", "who": SECTION, "name": name, "value": prior[name]})
        if code < 0:
            receipt["failures"].append("native restore of %s returned %d: %s" % (name, code, rados.last_status[:200]))
    receipt["entries"] = entries(rados, cluster, options)
    if receipt["entries"] != prior:
        receipt["failures"].append("the central entries read back as %s; recorded %s" % (receipt["entries"], prior))
    if not receipt["failures"]:
        receipt["effective"], failures = settle(rados, cluster, {int(osd): values for osd, values in claim["effective"].items()}, clock, sleep, options)
        receipt["failures"].extend(failures)
    receipt["restored"] = not receipt["failures"]
    if receipt["restored"]:
        os.remove(pathlib.Path(out) / claim_name)
        receipt["claimed"] = False
    return receipt
