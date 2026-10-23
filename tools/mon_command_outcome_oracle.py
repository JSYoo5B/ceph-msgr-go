#!/usr/bin/env python3
"""Closed, test-only native observations of one owned MON config-key.

Run under an external timeout: pinned Python mon_command ignores its timeout
argument. Reads may be repeated by the caller; cleanup is submitted only once.
No arbitrary command, native set operation, key or output path is accepted.
"""

import base64
import json
import os
import pathlib
import re
import sys


PHASES = {"initial", "applied", "overwritten", "recovered", "cleanup"}
PREFIX = "native-mon-no-replay-"


def validate(raw_id, token, phase):
    if (not raw_id.isascii() or not raw_id.isdecimal()
            or not 0 < int(raw_id) < 1 << 63
            or not re.fullmatch(r"[0-9a-f]{32}", token) or phase not in PHASES):
        raise ValueError("Invalid owned MON command observation")
    return int(raw_id)


def atomic_write(path, value):
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                         encoding="utf-8")
    os.replace(temporary, path)


def journal(output, value):
    # Only closed owned-key results reach this metadata file. In particular no
    # daemon configuration, credentials or unrelated key values are collected.
    with (output / "mon-command-outcome-native.jsonl").open("a", encoding="utf-8") as target:
        target.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")


def result(code, payload, status):
    if (type(code) is not int or not -(1 << 31) <= code < 1 << 31
            or not isinstance(payload, bytes) or len(payload) > 64 << 10
            or not isinstance(status, str) or len(status.encode("utf-8")) > 8192):
        raise ValueError("Native MON result exceeds its bound")
    return {"code": code, "data": base64.b64encode(payload).decode("ascii"),
            "status": status}


def native_command(fixture_root, prefix, key):
    import rados

    # Each call owns a fresh independently authenticated native handle, including
    # the absence check after cleanup. Keep the signed code and raw output.
    with rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                     conf={"keyring": str(fixture_root / "keyring")}) as client:
        return result(*client.mon_command(json.dumps({"prefix": prefix, "key": key}), b""))


def collect(fixture_root, output, raw_id, token, phase):
    request_id = validate(raw_id, token, phase)
    key = PREFIX + token
    owner = output / (key + ".owner.json")
    receipt = {"request_id": request_id, "token": token, "phase": phase}
    if phase == "initial":
        observed = native_command(fixture_root, "config-key get", key)
        receipt["result"] = observed
        if observed["code"] == -2:
            # An existing key is never claimed or later removed. A repeated
            # initial request cannot replace an earlier ownership decision.
            with owner.open("x", encoding="utf-8") as target:
                json.dump({"token": token, "key": key}, target)
    else:
        if json.loads(owner.read_text(encoding="utf-8")) != {"token": token, "key": key}:
            raise ValueError("MON command key has no matching ownership receipt")
        if phase == "cleanup":
            started = output / (key + ".cleanup-started.json")
            # Claim the one allowed cleanup before crossing the native call.
            # A timeout leaves this marker uncertain; no retry may repeat rm.
            with started.open("x", encoding="utf-8") as target:
                json.dump(receipt, target)
            journal(output, dict(receipt, state="cleanup-started-outcome-unknown"))
            removed = native_command(fixture_root, "config-key rm", key)
            atomic_write(output / (key + ".cleanup-mutation.json"), removed)
            journal(output, dict(receipt, state="cleanup-returned", removal=removed))
            receipt["removal"] = removed
        receipt["result"] = native_command(fixture_root, "config-key get", key)
    atomic_write(output / (key + "." + phase + ".json"), receipt)
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
        # started/mutation receipts distinguish uncertain and returned cleanup.
        raise SystemExit("Owned native MON command observation failed") from None
