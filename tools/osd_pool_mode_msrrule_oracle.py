"""Owned-pool oracle mode msrrule: one owned CRUSH MSR erasure rule and its client baseline.

Test-only; never product code. A CRUSH rule of an MSR type makes the OSDMap
require CRUSH_MSR from every client (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964, src/osd/OSDMap.cc get_features and
src/crush/CrushWrapper.h has_msr_rules), and the MON accepts such a map only
once require_min_compat_client is at least squid (src/mon/OSDMonitor.cc
validate_crush_against_features, OSDMap::get_min_compat_client). The rule
has no pool: it exists only to raise the map's client requirement on a
disposable cluster.

- action probe: with the client baseline as the fixture left it, sets the
  owned erasure-code profile (k=2, m=2, OSD failure domain, two OSDs per
  failure domain, which ErasureCode::create_rule turns into an MSR rule),
  then sends the rule creation and the baseline change WITHOUT
  yes_i_really_mean_it once each and reports their codes and status texts.
  Neither is repeated, and the rule must still be absent afterwards.
- action apply: raises require_min_compat_client to squid with
  yes_i_really_mean_it, creates the rule and reads it back.
- action restore: removes the rule and the profile this run recorded an
  intent for, sets require_min_compat_client back to the recorded value and
  confirms all three by independent reads. Without a record it removes
  nothing.

The prior baseline and what this run may have changed are kept in the
fixture output directory, so restore works from a later oracle run. The
record is an intent, written BEFORE each mutating command: the probe first
proves the owned rule and profile absent, so once an intent is recorded
anything under those names is this run's, whether or not the command's reply
or the read after it arrived. Restore therefore never trusts a read taken
around the mutation. It sends the removal for every recorded intent (both
removals answer 0 for an absent name), sets the baseline back when it
differs, and reports restored only from reads taken afterwards; a read that
fails raises and keeps the record for the next restore.

Apply takes an optional "lose": "readback". It then raises right after the
rule creation was sent, before any read, which is the outcome of a lost
reply or read as this mode sees it. The test uses it to run the restore
after an uncertain mutation against the real MON.
"""
import json
import os
import pathlib

from osd_pool_oracle import crush_rules, ec_profile_present, json_mon, oracle_mode

PROFILE = "ceph-msgr-owned-ec-msr"
RULE = "ceph-msgr-owned-msr-rule"
PROFILE_SETTINGS = ["k=2", "m=2", "crush-failure-domain=osd", "crush-osds-per-failure-domain=2", "crush-num-failure-domains=2"]
TARGET = "squid"
# The baselines a restore may set again: the releases below the target that
# pinned OSDMap::get_min_compat_client can report.
BASELINES = ("jewel", "luminous", "mimic", "nautilus", "octopus", "pacific", "quincy", "reef")
MSR_RULE_TYPES = (4, 5)  # crush.h CRUSH_RULE_TYPE_MSR_FIRSTN, CRUSH_RULE_TYPE_MSR_INDEP
STATE_FILE = "osd-pool-msrrule.json"
MAX_STATE = 1 << 10


def read_state(out):
    path = pathlib.Path(out) / STATE_FILE
    try:
        with path.open("rb") as file:
            data = file.read(MAX_STATE + 1)
    except FileNotFoundError:
        return None
    if len(data) > MAX_STATE:
        raise ValueError("msrrule state exceeds 1 KiB")
    state = json.loads(data)
    if (not isinstance(state, dict) or set(state) != {"prior", "profile", "rule", "raised"} or state["prior"] not in BASELINES
            or not all(type(state[key]) is bool for key in ("profile", "rule", "raised"))):
        raise ValueError("invalid msrrule state")
    return state


def write_state(out, state):
    temporary = pathlib.Path(out) / (STATE_FILE + ".tmp")
    temporary.write_bytes(json.dumps(state).encode() + b"\n")
    os.replace(temporary, pathlib.Path(out) / STATE_FILE)


def baseline(rados, cluster):
    """The map's require_min_compat_client and its epoch."""
    dump = rados.dump(cluster)
    value = dump.get("require_min_compat_client")
    if type(value) is not str or type(dump.get("epoch")) is not int:
        raise ValueError("native osd dump lacks require_min_compat_client or its epoch")
    return value, dump["epoch"]


def rule_dump(rados, cluster):
    """The owned rule as the MON dumps it, or None when it is absent."""
    rules = json_mon(rados, cluster, {"prefix": "osd crush rule dump", "format": "json"}, "crush rule dump")
    found = [r for r in rules if isinstance(r, dict) and r.get("rule_name") == RULE]
    if len(found) > 1:
        raise ValueError("native CRUSH rule dump lists the owned MSR rule twice")
    return found[0] if found else None


def refusal(rados, cluster, command):
    code, _ = rados.mon(cluster, command)
    return {"code": code, "status": rados.last_status}


def intend(out, state, key):
    """Record, before the command that may do it, that this run may create
    the rule or the profile or change the baseline."""
    state[key] = True
    write_state(out, state)


@oracle_mode
def mode_msrrule(rados, cluster, request, out):
    """Probe, apply or restore the owned MSR rule and the client baseline."""
    if (not isinstance(request, dict) or request.get("action") not in ("probe", "apply", "restore")
            or set(request) - {"action", "lose"} or "lose" in request and (request["action"] != "apply" or request["lose"] != "readback")):
        raise ValueError("msrrule takes exactly action probe, apply or restore; apply also takes lose=readback")
    action = request["action"]
    state = read_state(out)
    if action == "probe":
        if state is not None:
            raise RuntimeError("msrrule probe runs once, before apply")
        prior, _ = baseline(rados, cluster)
        if prior not in BASELINES:
            raise RuntimeError("the fixture's require_min_compat_client %r is not below %s" % (prior, TARGET))
        if RULE in crush_rules(rados, cluster) or ec_profile_present(rados, cluster, PROFILE):
            raise RuntimeError("the owned MSR rule or profile already exists; refusing to reuse it")
        state = {"prior": prior, "profile": False, "rule": False, "raised": False}
        write_state(out, state)
        intend(out, state, "profile")
        code, _ = rados.mon(cluster, {"prefix": "osd erasure-code-profile set", "name": PROFILE, "profile": PROFILE_SETTINGS})
        if code < 0:
            raise RuntimeError("native MSR erasure-code profile failed: %d %s" % (code, rados.last_status))
        # Both guarded commands are expected to be refused, but either may
        # apply; the intent covers that before the reply is known.
        intend(out, state, "rule")
        create = refusal(rados, cluster, {"prefix": "osd crush rule create-erasure", "name": RULE, "profile": PROFILE})
        intend(out, state, "raised")
        raise_plain = refusal(rados, cluster, {"prefix": "osd set-require-min-compat-client", "version": TARGET})
        after, epoch = baseline(rados, cluster)
        return {"msr_rule": {"action": action, "prior": prior, "rule_create": create, "baseline_change": raise_plain,
                             "rule_present": RULE in crush_rules(rados, cluster), "baseline": after, "epoch": epoch}}
    if action == "apply":
        # The probe's refused steps must have left the map as it was; that
        # is read here, not taken from the record.
        if (state is None or not state["profile"] or RULE in crush_rules(rados, cluster) or not ec_profile_present(rados, cluster, PROFILE)
                or baseline(rados, cluster)[0] != state["prior"]):
            raise RuntimeError("msrrule apply needs a probe that left the profile and nothing else")
        intend(out, state, "raised")
        code, _ = rados.mon(cluster, {"prefix": "osd set-require-min-compat-client", "version": TARGET, "yes_i_really_mean_it": True})
        if code < 0:
            raise RuntimeError("native require_min_compat_client %s failed: %d %s" % (TARGET, code, rados.last_status))
        intend(out, state, "rule")
        code, _ = rados.mon(cluster, {"prefix": "osd crush rule create-erasure", "name": RULE, "profile": PROFILE})
        if request.get("lose") == "readback":
            raise RuntimeError("injected: the reply and the read after the MSR rule creation are lost")
        if code < 0:
            raise RuntimeError("native MSR rule creation failed: %d %s" % (code, rados.last_status))
        rule = rule_dump(rados, cluster)
        if rule is None or rule.get("type") not in MSR_RULE_TYPES or not any(
                isinstance(step, dict) and step.get("op") == "choosemsr" for step in rule.get("steps", [])):
            raise RuntimeError("the owned rule is not an MSR rule: %r" % (rule,))
        after, epoch = baseline(rados, cluster)
        if after != TARGET:
            raise RuntimeError("require_min_compat_client is %r after the change" % after)
        return {"msr_rule": {"action": action, "prior": state["prior"], "rule": rule, "baseline": after, "epoch": epoch}}
    if state is None:
        value, epoch = baseline(rados, cluster)
        return {"msr_rule": {"action": action, "owned": False, "rule_present": RULE in crush_rules(rados, cluster),
                             "profile_present": ec_profile_present(rados, cluster, PROFILE), "baseline": value, "epoch": epoch, "restored": True}}
    # Every recorded intent is undone whatever a read around the mutation
    # showed. The rule goes first: the MON refuses a baseline below what the
    # map still needs, and a profile a rule was made from stays removable.
    if state["rule"]:
        rados.mon(cluster, {"prefix": "osd crush rule rm", "name": RULE})
    if state["profile"]:
        rados.mon(cluster, {"prefix": "osd erasure-code-profile rm", "name": PROFILE})
    rule_present, profile_present = RULE in crush_rules(rados, cluster), ec_profile_present(rados, cluster, PROFILE)
    value, epoch = baseline(rados, cluster)
    if state["raised"] and value != state["prior"] and not rule_present:
        rados.mon(cluster, {"prefix": "osd set-require-min-compat-client", "version": state["prior"], "yes_i_really_mean_it": True})
        value, epoch = baseline(rados, cluster)
    restored = not rule_present and not profile_present and value == state["prior"]
    if restored:
        try:
            os.remove(pathlib.Path(out) / STATE_FILE)
        except FileNotFoundError:
            pass
    return {"msr_rule": {"action": action, "owned": True, "prior": state["prior"], "rule_present": rule_present, "profile_present": profile_present,
                         "baseline": value, "epoch": epoch, "restored": restored}}
