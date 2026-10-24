#!/usr/bin/env python3
"""Closed, test-only native observations of one MON-local runtime option.

Every request targets one owned MON by bare name and opens a fresh independently
authenticated librados handle. A named-target ``mon_command`` is sent to that
daemon as a Tell command, so the observation and the Go client use the same
daemon-local path. Each observation records only: the daemon identity
(``mon_status`` name, rank and state, read before and after), the daemon's
effective ``log_max_recent`` (``config get``) and that option's entry from
``config diff get`` (its override presence/value and final value). A full
configuration dump is never requested or persisted.

Run under an external timeout: pinned Python mon_command ignores its timeout
argument. Reads may be repeated by the caller; restoration is submitted only
once. The restoration comes from this worker's own initial receipt: unset the
runtime override when none existed, otherwise set the one original override
again. No arbitrary command, option, value, target or output path is accepted,
and a baseline outside the bound is neither recorded nor restored.
"""

import base64
import json
import pathlib
import re
import sys

from mgr_command_outcome_oracle import bounded_json, summary
from mon_command_outcome_oracle import atomic_write, result


PHASES = {"initial", "applied", "overwritten", "recovered", "restore", "restored"}
TARGETS = {"a", "b", "c"}
PREFIX = "native-mon-tell-no-replay-"
OPTION = "log_max_recent"
# log_max_recent is a positive in-memory crash-log retention count. The cell
# only raises it by two slots: a baseline above CEILING - 2 is refused so the
# candidate values never exceed CEILING and no retained evidence is truncated.
CEILING = 20000
LOWEST, HIGHEST_BASELINE = 1, CEILING - 2


def validate(raw_id, token, phase, target):
    if (not raw_id.isascii() or not raw_id.isdecimal()
            or not 0 < int(raw_id) < 1 << 63
            or not re.fullmatch(r"[0-9a-f]{32}", token) or phase not in PHASES
            or target not in TARGETS):
        raise ValueError("Invalid owned MON Tell observation")
    return int(raw_id)


def journal(output, value):
    # Only closed owned-option views reach this metadata file: never a full
    # configuration, a monitor map or credentials.
    with (output / "mon-tell-outcome-native.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")


def number(value):
    """Return the canonical decimal text of a bounded integer or digit string."""
    if type(value) is int and 0 <= value < 1 << 63:
        return str(value)
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,18}", value):
        return str(int(value))
    return None


def in_bound(text):
    return text is not None and LOWEST <= int(text) <= HIGHEST_BASELINE


def daemon_identity(code, payload, status):
    observed = summary(code, payload, status)
    value = bounded_json(code, payload)
    observed["name"] = observed["rank"] = observed["state"] = None
    if isinstance(value, dict):
        if isinstance(value.get("name"), str) and len(value["name"]) <= 64:
            observed["name"] = value["name"]
        if type(value.get("rank")) is int and 0 <= value["rank"] < 1 << 31:
            observed["rank"] = value["rank"]
        if isinstance(value.get("state"), str) and len(value["state"]) <= 32:
            observed["state"] = value["state"]
    return observed


def option_diff(code, payload, status):
    """Keep only the owned option's diff entry; refuse a wider reply."""
    observed = summary(code, payload, status)
    value = bounded_json(code, payload)
    observed["valid"] = False
    observed["entry"] = None
    if isinstance(value, dict) and isinstance(value.get("diff"), dict):
        diff = value["diff"]
        if set(diff) - {OPTION}:
            raise ValueError("Native diff reply exceeds the owned option")
        observed["valid"] = True
        entry = diff.get(OPTION)
        if isinstance(entry, dict):
            sources = sorted(name for name in entry if isinstance(name, str) and len(name) <= 32)
            observed["entry"] = {
                "sources": sources,
                "override_present": "override" in entry,
                "override": number(entry["override"]) if "override" in entry else None,
                "final": number(entry["final"]) if "final" in entry else None,
            }
    return observed


def effective_value(observed):
    config = observed["config_get"]
    if config["code"] != 0:
        return None
    value = bounded_json(0, base64.b64decode(config["data"]))
    return number(value.get(OPTION)) if isinstance(value, dict) else None


def views(client, target):
    def tell(command):
        return client.mon_command(json.dumps(command, sort_keys=True), b"", target=target)

    before = daemon_identity(*tell({"prefix": "mon_status", "format": "json"}))
    effective = result(*tell({"prefix": "config get", "var": OPTION, "format": "json"}))
    diff = option_diff(*tell({"prefix": "config diff get", "var": OPTION, "format": "json"}))
    after = daemon_identity(*tell({"prefix": "mon_status", "format": "json"}))
    return {"daemon_before": before, "config_get": effective, "config_diff": diff,
            "daemon_after": after}


def restorable(observed, target):
    # Only an in-bound effective value that is either not overridden at runtime
    # or one runtime override equal to it can be restored by one closed mutation.
    for key in ("daemon_before", "daemon_after"):
        identity = observed[key]
        if identity["code"] != 0 or identity["name"] != target or identity["rank"] is None:
            return None
    effective = effective_value(observed)
    diff = observed["config_diff"]
    if diff["code"] != 0 or not diff["valid"] or not in_bound(effective):
        return None
    entry = diff["entry"]
    if entry is None or not entry["override_present"]:
        return {"present": False, "effective": effective}
    if entry["override"] == effective and entry["final"] in (None, effective):
        return {"present": True, "value": effective, "effective": effective}
    return None


def native_handle(fixture_root):
    import rados

    return rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                       conf={"keyring": str(fixture_root / "keyring")})


def native_views(fixture_root, target):
    with native_handle(fixture_root) as client:
        return views(client, target)


def restoration_command(baseline):
    if not isinstance(baseline, dict) or baseline.get("present") not in (True, False):
        raise ValueError("Invalid recorded baseline")
    if baseline["present"]:
        value = number(baseline.get("value"))
        if not in_bound(value) or baseline.get("value") != value:
            raise ValueError("Recorded baseline is outside the bound")
        return {"prefix": "config set", "var": OPTION, "val": [value], "format": "json"}
    return {"prefix": "config unset", "var": OPTION, "format": "json"}


def native_restore(fixture_root, target, baseline):
    command = restoration_command(baseline)
    with native_handle(fixture_root) as client:
        return result(*client.mon_command(json.dumps(command, sort_keys=True), b"", target=target))


def collect(fixture_root, output, raw_id, token, phase, target):
    request_id = validate(raw_id, token, phase, target)
    owner = output / (PREFIX + token + ".baseline.json")
    receipt = {"request_id": request_id, "token": token, "phase": phase, "target": target}
    if phase == "initial":
        observed = native_views(fixture_root, target)
        receipt["views"] = observed
        baseline = restorable(observed, target)
        receipt["baseline"] = baseline
        if baseline is not None:
            # A repeated initial request cannot replace the first baseline.
            with owner.open("x", encoding="utf-8") as handle:
                json.dump({"token": token, "target": target, "baseline": baseline}, handle)
    else:
        recorded = json.loads(owner.read_text(encoding="utf-8"))
        if (recorded.get("token") != token or recorded.get("target") != target
                or not isinstance(recorded.get("baseline"), dict)):
            raise ValueError("MON Tell outcome has no matching baseline receipt")
        receipt["baseline"] = recorded["baseline"]
        if phase == "restore":
            restoration_command(recorded["baseline"])
            started = output / (PREFIX + token + ".restore-started.json")
            # Claim the one allowed restoration before crossing the native call.
            # A timeout leaves this marker uncertain; no retry may repeat it.
            with started.open("x", encoding="utf-8") as handle:
                json.dump(receipt, handle)
            journal(output, dict(receipt, state="restore-started-outcome-unknown"))
            restored = native_restore(fixture_root, target, recorded["baseline"])
            atomic_write(output / (PREFIX + token + ".restore-mutation.json"), restored)
            journal(output, dict(receipt, state="restore-returned", restoration=restored))
            receipt["restoration"] = restored
        receipt["views"] = native_views(fixture_root, target)
    atomic_write(output / (PREFIX + token + "." + phase + ".json"), receipt)
    journal(output, receipt)


def main():
    if len(sys.argv) != 7:
        raise ValueError("Expected fixture root, output, request ID, token, phase and target")
    root, output, raw_id, token, phase, target = sys.argv[1:]
    collect(pathlib.Path(root), pathlib.Path(output), raw_id, token, phase, target)


def diagnostic(error):
    # A bounded class name and message with every path-like token masked, so a
    # failed owned observation is attributable without private locations.
    text = re.sub(r"\S*/\S*", "<path>", str(error))
    text = re.sub(r"[^\x20-\x7e]", "?", text)[:200]
    return type(error).__name__ + (": " + text if text else "")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # The started/mutation receipts distinguish uncertain and returned restore.
        raise SystemExit("Owned native MON Tell observation failed: " + diagnostic(error)) from None
