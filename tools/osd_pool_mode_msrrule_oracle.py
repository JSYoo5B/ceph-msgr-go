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
- action restore: removes the rule and the profile this run created, sets
  require_min_compat_client back to the recorded value and confirms all three
  by independent reads. It removes nothing this run did not create.

What this run created and the prior baseline are kept in the fixture output
directory, so restore works from a later oracle run.
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


@oracle_mode
def mode_msrrule(rados, cluster, request, out):
    """Probe, apply or restore the owned MSR rule and the client baseline."""
    if not isinstance(request, dict) or set(request) != {"action"} or request["action"] not in ("probe", "apply", "restore"):
        raise ValueError("msrrule takes exactly action probe, apply or restore")
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
        try:
            code, _ = rados.mon(cluster, {"prefix": "osd erasure-code-profile set", "name": PROFILE, "profile": PROFILE_SETTINGS})
        finally:
            state["profile"] = ec_profile_present(rados, cluster, PROFILE)
            write_state(out, state)
        if code < 0:
            raise RuntimeError("native MSR erasure-code profile failed: %d %s" % (code, rados.last_status))
        try:
            create = refusal(rados, cluster, {"prefix": "osd crush rule create-erasure", "name": RULE, "profile": PROFILE})
        finally:
            state["rule"] = RULE in crush_rules(rados, cluster)
            write_state(out, state)
        raise_plain = refusal(rados, cluster, {"prefix": "osd set-require-min-compat-client", "version": TARGET})
        after, epoch = baseline(rados, cluster)
        state["raised"] = after != prior
        write_state(out, state)
        return {"msr_rule": {"action": action, "prior": prior, "rule_create": create, "baseline_change": raise_plain,
                             "rule_present": state["rule"], "baseline": after, "epoch": epoch}}
    if action == "apply":
        if state is None or state["rule"] or state["raised"] or not state["profile"]:
            raise RuntimeError("msrrule apply needs a probe that left the profile and nothing else")
        state["raised"] = True
        write_state(out, state)
        code, _ = rados.mon(cluster, {"prefix": "osd set-require-min-compat-client", "version": TARGET, "yes_i_really_mean_it": True})
        if code < 0:
            raise RuntimeError("native require_min_compat_client %s failed: %d %s" % (TARGET, code, rados.last_status))
        try:
            code, _ = rados.mon(cluster, {"prefix": "osd crush rule create-erasure", "name": RULE, "profile": PROFILE})
        finally:
            state["rule"] = RULE in crush_rules(rados, cluster)
            write_state(out, state)
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
    if state["rule"]:
        rados.mon(cluster, {"prefix": "osd crush rule rm", "name": RULE})
    if state["profile"]:
        rados.mon(cluster, {"prefix": "osd erasure-code-profile rm", "name": PROFILE})
    rule_present, profile_present = RULE in crush_rules(rados, cluster), ec_profile_present(rados, cluster, PROFILE)
    if state["raised"] and not rule_present:
        # The MON refuses a baseline below what the map still needs, so the
        # rule goes first; the change back is confirmed by the read below.
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
