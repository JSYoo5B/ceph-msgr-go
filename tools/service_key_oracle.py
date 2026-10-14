#!/usr/bin/env python3
"""Validate and preserve public metadata from rotating-key readiness probes.

This observes local MonMap epochs and fresh native MGR commands. It cannot
identify the Go client's ticket generation. No key or opaque ticket is read.
Shapes follow pinned Ceph 20.2.4 mon_status, MgrMap and VersionHook output:
7f793731f1b39eb4f465e960113d2363c311b964, src/mon/Monitor.cc,
src/mgr/MgrMap.cc and src/common/admin_socket.cc (LGPL-2.1).
This bounded metadata validator is independently written test-only code.
"""
import datetime
import json
import pathlib
import sys


def validate(value, phase, mon, rank, map_epoch, auth_epoch, manager, manager_id):
    record = {"phase": phase, "mon": mon, "rank": rank,
              "required_map_epoch": map_epoch, "required_auth_epoch": auth_epoch,
              "required_manager": manager, "required_manager_id": manager_id}
    if not isinstance(value, dict):
        return record, False
    if phase == "mon-status":
        local = value.get("monmap", {})
        if not isinstance(local, dict):
            return record, False
        record.update({k: value.get(k) for k in ("name", "rank")})
        record.update({"map_epoch": local.get("epoch"), "auth_epoch": local.get("auth_epoch")})
        accepted = (value.get("name") == mon and type(value.get("rank")) is int
                    and value["rank"] == rank and type(local.get("epoch")) is int
                    and local["epoch"] >= map_epoch and type(local.get("auth_epoch")) is int
                    and local["auth_epoch"] >= auth_epoch)
    elif phase == "mgr-map":
        record.update({k: value.get(k) for k in ("available", "active_name", "active_gid", "epoch")})
        accepted = (value.get("available") is True and value.get("active_name") == manager
                    and type(value.get("active_gid")) is int and value["active_gid"] == manager_id)
    elif phase == "mgr-version":
        record.update({k: value.get(k) for k in ("version", "release", "release_type")})
        accepted = (value.get("release") == "tentacle" and isinstance(value.get("version"), str)
                    and value["version"].startswith("20.2."))
    else:
        raise ValueError("unknown service-key probe phase")
    return record, accepted


def collect(source, history, request_id, phase, mon, rank, map_epoch, auth_epoch, manager, manager_id, command_code=0):
    args = (phase, mon, rank, map_epoch, auth_epoch, manager, manager_id)
    data = source.read_bytes()
    if len(data) > 1 << 20:
        raise ValueError("service-key oracle input exceeds 1 MiB")
    try:
        record, accepted = validate(json.loads(data), *args)
    except (ValueError, UnicodeError):
        record, accepted = validate(None, *args)
        record["error"] = "invalid native JSON"
    accepted = accepted and command_code == 0
    record.update({"request_id": request_id, "accepted": accepted, "command_code": command_code,
                   "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    with history.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record) + "\n")
    return accepted


if __name__ == "__main__":
    source, history, request_id, phase, mon, rank, map_epoch, auth_epoch, manager, manager_id, command_code = sys.argv[1:]
    ok = collect(pathlib.Path(source), pathlib.Path(history), request_id, phase, mon,
                 int(rank), int(map_epoch), int(auth_epoch), manager, int(manager_id), int(command_code))
    raise SystemExit(0 if ok else 1)
