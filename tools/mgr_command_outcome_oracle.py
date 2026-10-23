#!/usr/bin/env python3
"""Closed, test-only native observations of the MGR balancer-mode outcome.

Each request opens one fresh independently authenticated librados handle and
records three views: the active balancer module's mode (mgr_command
``balancer status``), the MON's effective ``config get`` for who=mgr and
key=mgr/balancer/mode, and the explicit configuration entries carrying that
option name (from ``config dump``). The active MGR name and GID are read
before and after those views so the caller can bracket them.

Run under an external timeout: pinned Python mon_command/mgr_command ignore
their timeout arguments. Reads may be repeated by the caller; restoration is
submitted only once. The restoration target comes from this worker's own
initial receipt: remove the option entry when none existed, otherwise set the
one original who=mgr value again. No arbitrary command, option, value or
output path is accepted.
"""

import base64
import hashlib
import json
import pathlib
import re
import sys

from mon_command_outcome_oracle import atomic_write, result


PHASES = {"initial", "applied", "overwritten", "recovered", "restore", "restored"}
PREFIX = "native-mgr-no-replay-"
OPTION = "mgr/balancer/mode"
# Pinned balancer/module.py enum_allowed for the mode option.
MODES = {"none", "crush-compat", "upmap", "read", "upmap-read"}
METADATA_LIMIT = 4 << 20


def validate(raw_id, token, phase):
    if (not raw_id.isascii() or not raw_id.isdecimal()
            or not 0 < int(raw_id) < 1 << 63
            or not re.fullmatch(r"[0-9a-f]{32}", token) or phase not in PHASES):
        raise ValueError("Invalid owned MGR command observation")
    return int(raw_id)


def journal(output, value):
    # Only closed balancer-mode views reach this metadata file. Full config
    # dumps and MgrMaps stay in memory; credentials are never collected.
    with (output / "mgr-command-outcome-native.jsonl").open("a", encoding="utf-8") as target:
        target.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")


def bounded_json(code, payload):
    if code != 0 or not isinstance(payload, bytes) or len(payload) > METADATA_LIMIT:
        return None
    try:
        return json.loads(payload)
    except (ValueError, UnicodeError):
        return None


def summary(code, payload, status):
    # Keep the signed code, status and a digest of large metadata replies whose
    # unrelated contents are deliberately not persisted.
    if (type(code) is not int or not -(1 << 31) <= code < 1 << 31
            or not isinstance(payload, bytes) or len(payload) > METADATA_LIMIT
            or not isinstance(status, str) or len(status.encode("utf-8")) > 8192):
        raise ValueError("Native metadata result exceeds its bound")
    return {"code": code, "data_bytes": len(payload),
            "data_sha256": hashlib.sha256(payload).hexdigest(), "status": status}


def manager_identity(code, payload, status):
    observed = summary(code, payload, status)
    value = bounded_json(code, payload)
    observed["active_name"] = observed["active_gid"] = observed["epoch"] = observed["available"] = None
    if isinstance(value, dict):
        if isinstance(value.get("active_name"), str) and len(value["active_name"]) <= 64:
            observed["active_name"] = value["active_name"]
        for field in ("active_gid", "epoch"):
            if type(value.get(field)) is int and 0 <= value[field] < 1 << 64:
                observed[field] = value[field]
        if type(value.get("available")) is bool:
            observed["available"] = value["available"]
    return observed


def option_entries(code, payload, status):
    observed = summary(code, payload, status)
    value = bounded_json(code, payload)
    entries = None
    if isinstance(value, list):
        entries = []
        for item in value:
            if not isinstance(item, dict) or item.get("name") != OPTION:
                continue
            entry = {field: item.get(field) for field in ("section", "name", "value", "mask")}
            if any(not isinstance(field, str) or len(field) > 256 for field in entry.values()):
                raise ValueError("Native balancer-mode configuration entry is malformed")
            entries.append(entry)
        entries.sort(key=lambda entry: (entry["section"], entry["mask"], entry["value"]))
    observed["entries"] = entries
    return observed


def views(client):
    def mon(command):
        return client.mon_command(json.dumps(command, sort_keys=True), b"")

    before = manager_identity(*mon({"prefix": "mgr dump", "format": "json"}))
    status = result(*client.mgr_command(json.dumps({"prefix": "balancer status", "format": "json"}), b""))
    effective = result(*mon({"prefix": "config get", "who": "mgr", "key": OPTION}))
    explicit = option_entries(*mon({"prefix": "config dump", "format": "json"}))
    after = manager_identity(*mon({"prefix": "mgr dump", "format": "json"}))
    return {"manager_before": before, "balancer_status": status, "config_get": effective,
            "config_entries": explicit, "manager_after": after}


def balancer_mode(observed):
    status = observed["balancer_status"]
    if status["code"] != 0:
        return None
    try:
        value = json.loads(base64.b64decode(status["data"]))
    except (ValueError, UnicodeError):
        return None
    mode = value.get("mode") if isinstance(value, dict) else None
    return mode if mode in MODES else None


def restorable(observed):
    # Only an absent entry or one unmasked who=mgr entry can be restored by one
    # closed native mutation. Anything else is recorded but never claimed.
    entries = observed["config_entries"]["entries"]
    if observed["config_entries"]["code"] != 0 or entries is None or balancer_mode(observed) is None:
        return None
    if not entries:
        return {"present": False}
    if len(entries) == 1 and entries[0]["section"] == "mgr" and entries[0]["mask"] == "":
        return {"present": True, "value": entries[0]["value"]}
    return None


def native_handle(fixture_root):
    import rados

    return rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                       conf={"keyring": str(fixture_root / "keyring")})


def native_views(fixture_root):
    # Each call owns a fresh independently authenticated native handle.
    with native_handle(fixture_root) as client:
        return views(client)


def native_restore(fixture_root, baseline):
    command = {"prefix": "config rm", "who": "mgr", "name": OPTION}
    if baseline["present"]:
        command = {"prefix": "config set", "who": "mgr", "name": OPTION, "value": baseline["value"]}
    with native_handle(fixture_root) as client:
        return result(*client.mon_command(json.dumps(command, sort_keys=True), b""))


def collect(fixture_root, output, raw_id, token, phase):
    request_id = validate(raw_id, token, phase)
    owner = output / (PREFIX + token + ".baseline.json")
    receipt = {"request_id": request_id, "token": token, "phase": phase}
    if phase == "initial":
        observed = native_views(fixture_root)
        receipt["views"] = observed
        baseline = restorable(observed)
        receipt["baseline"] = baseline
        if baseline is not None:
            # A repeated initial request cannot replace the first baseline.
            with owner.open("x", encoding="utf-8") as target:
                json.dump({"token": token, "baseline": baseline}, target)
    else:
        recorded = json.loads(owner.read_text(encoding="utf-8"))
        if recorded.get("token") != token or not isinstance(recorded.get("baseline"), dict):
            raise ValueError("MGR command outcome has no matching baseline receipt")
        receipt["baseline"] = recorded["baseline"]
        if phase == "restore":
            started = output / (PREFIX + token + ".restore-started.json")
            # Claim the one allowed restoration before crossing the native call.
            # A timeout leaves this marker uncertain; no retry may repeat it.
            with started.open("x", encoding="utf-8") as target:
                json.dump(receipt, target)
            journal(output, dict(receipt, state="restore-started-outcome-unknown"))
            restored = native_restore(fixture_root, recorded["baseline"])
            atomic_write(output / (PREFIX + token + ".restore-mutation.json"), restored)
            journal(output, dict(receipt, state="restore-returned", restoration=restored))
            receipt["restoration"] = restored
        receipt["views"] = native_views(fixture_root)
    atomic_write(output / (PREFIX + token + "." + phase + ".json"), receipt)
    journal(output, receipt)


def main():
    if len(sys.argv) != 6:
        raise ValueError("Expected fixture root, output, request ID, token and phase")
    root, output, raw_id, token, phase = sys.argv[1:]
    collect(pathlib.Path(root), pathlib.Path(output), raw_id, token, phase)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Do not expose arbitrary native exception text or private paths. The
        # started/mutation receipts distinguish uncertain and returned restore.
        raise SystemExit("Owned native MGR command observation failed") from None
