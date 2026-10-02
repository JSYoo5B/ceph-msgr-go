#!/usr/bin/env python3
"""Validate native Ceph CLI metadata for the isolated mapped-address fixture.

This development-only gate reads fixed JSON files, never credentials. Ceph
configuration uses hex mapped literals; dotted mapped JSON is metadata only.
"""

import ipaddress
import json
from pathlib import Path
import sys


FSID = "80bbab73-69c1-4a0c-a746-4271357750b8"
MON_PORTS = {"a": 33300, "b": 33301, "c": 33302}
MGR_PORTS = {"a": 36800, "b": 36801}
MAX_JSON_BYTES = 1024 * 1024


def object_value(value, label):
    if not isinstance(value, dict):
        raise ValueError(label + " must be a JSON object")
    return value


def unsigned(value, maximum, label, minimum=0):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(label + " must be an integer in the expected range")
    return value


def mapped_endpoint(vector, port, label):
    addrs = object_value(vector, label).get("addrvec")
    if not isinstance(addrs, list) or len(addrs) != 1:
        raise ValueError(label + " must contain exactly one v2 address")
    addr = object_value(addrs[0], label)
    text = addr.get("addr")
    if addr.get("type") != "v2" or not isinstance(text, str):
        raise ValueError(label + " must contain a v2 numeric address")
    host, separator, number = text.rpartition(":")
    if (not separator or not host.startswith("[") or not host.endswith("]")
            or "%" in host or not number.isascii() or not number.isdecimal()):
        raise ValueError(label + " must contain bracketed IPv6 and a numeric port")
    try:
        ip = ipaddress.IPv6Address(host[1:-1])
    except ValueError:
        raise ValueError(label + " has an invalid IPv6 address") from None
    if ip.ipv4_mapped != ipaddress.IPv4Address("127.0.0.1") or int(number) != port:
        raise ValueError(label + " has the wrong mapped address or port")
    unsigned(addr.get("nonce"), (1 << 32) - 1, label + " nonce")
    return text


def validate(status, mon, mgr, pg):
    status = object_value(status, "status")
    mon = object_value(mon, "mon dump")
    mgr = object_value(mgr, "mgr dump")
    pg = object_value(pg, "pg stat")
    if status.get("fsid") != FSID or mon.get("fsid") != FSID:
        raise ValueError("status and mon dump must identify the fixture FSID")
    if unsigned(mon.get("min_mon_release"), 255, "min_mon_release") != 20:
        raise ValueError("mon dump must require Tentacle release 20")
    mons = mon.get("mons")
    if not isinstance(mons, list) or len(mons) != 3:
        raise ValueError("mon dump must contain the three fixture monitors")
    mon_endpoints = {}
    for entry in mons:
        entry = object_value(entry, "monitor")
        name = entry.get("name")
        if not isinstance(name, str) or name not in MON_PORTS or name in mon_endpoints:
            raise ValueError("mon dump has unexpected or duplicate monitor names")
        mon_endpoints[name] = mapped_endpoint(entry.get("public_addrs"), MON_PORTS[name], "monitor " + name)
    name = mgr.get("active_name")
    if mgr.get("available") is not True or not isinstance(name, str) or name not in MGR_PORTS:
        raise ValueError("mgr dump must identify an available fixture manager")
    gid = unsigned(mgr.get("active_gid"), (1 << 64) - 1, "active_gid", minimum=1)
    mgr_endpoint = mapped_endpoint(mgr.get("active_addrs"), MGR_PORTS[name], "manager " + name)
    standbys = mgr.get("standbys")
    if not isinstance(standbys, list) or len(standbys) != 1:
        raise ValueError("mgr dump must contain one standby")
    standby = object_value(standbys[0], "standby")
    if standby.get("name") != ("b" if name == "a" else "a"):
        raise ValueError("mgr dump has the wrong standby name")
    standby_gid = unsigned(standby.get("gid"), (1 << 64) - 1, "standby gid", minimum=1)
    if standby_gid == gid:
        raise ValueError("active and standby managers must have distinct global IDs")
    if type(pg.get("pg_ready")) is not bool:
        raise ValueError("pg stat must contain boolean pg_ready")
    pg_summary = object_value(pg.get("pg_summary"), "pg_summary")
    unsigned(pg_summary.get("num_pgs"), (1 << 64) - 1, "num_pgs")
    return {"fsid": FSID, "mon_endpoints": sorted(mon_endpoints.values()),
            "mgr_endpoint": mgr_endpoint, "mgr_name": name, "mgr_global_id": gid}


def read_json(path):
    with path.open("rb") as source:
        data = source.read(MAX_JSON_BYTES + 1)
    if len(data) > MAX_JSON_BYTES:
        raise ValueError(path.name + " exceeds the metadata size limit")
    try:
        return json.loads(data)
    except (ValueError, UnicodeError):
        raise ValueError(path.name + " is invalid JSON") from None


def main(argv):
    if len(argv) != 1:
        print("usage: mapped_address_oracle.py OUTPUT_DIRECTORY", file=sys.stderr)
        return 2
    out = Path(argv[0])
    try:
        summary = validate(*(read_json(out / ("mapped-oracle-" + name + ".json"))
                             for name in ("status", "mon", "mgr", "pg")))
        (out / "mapped-oracle-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError) as error:
        print("Mapped-address oracle failed: " + str(error), file=sys.stderr)
        return 1
    print("Native Ceph mapped-address oracle: " + json.dumps(summary, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
