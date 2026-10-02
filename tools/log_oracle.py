#!/usr/bin/env python3
"""Select one independently generated native Ceph cluster-log entry.

Development-only: retain no other cluster logs or credentials in diagnostics.
The schema comes from pinned LogEntry.dump, not this project's wire decoder.
"""

import json
import pathlib
import re
import sys


LIMIT = 1024 * 1024


def select_entry(data, sentinel):
    if len(data) > LIMIT:
        raise ValueError("native log oracle exceeds its metadata limit")
    if not re.fullmatch(r"ceph-msgr-log-oracle-[0-9a-f]{32}", sentinel):
        raise ValueError("invalid native log oracle sentinel")
    try:
        entries = json.loads(data)
    except (ValueError, UnicodeError):
        raise ValueError("native log oracle is invalid JSON") from None
    if not isinstance(entries, list) or len(entries) > 64:
        raise ValueError("native log oracle requires a bounded entry array")
    matched = [entry for entry in entries if isinstance(entry, dict) and entry.get("message") == sentinel]
    if len(matched) != 1:
        raise ValueError("native log oracle requires exactly one sentinel entry")
    entry = matched[0]
    if (entry.get("name") != "client.test" or entry.get("priority") != "[INF]"
            or entry.get("channel") != "cluster" or type(entry.get("seq")) is not int
            or entry["seq"] != 0 or not isinstance(entry.get("rank"), str)
            or not re.fullmatch(r"client\.(?:\?|[0-9]{1,19})", entry["rank"])):
        raise ValueError("native log oracle lost the injected client entry")
    if entry["rank"] != "client.?" and int(entry["rank"].split(".")[1]) >= 1 << 63:
        raise ValueError("native log oracle client rank is out of range")
    if (not isinstance(entry.get("stamp"), str)
            or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]{8}\.[0-9]{6,9}(?:[+-][0-9]{4}|Z)", entry["stamp"])):
        raise ValueError("native log oracle requires the original timestamp")
    addrs = entry.get("addrs")
    if (not isinstance(addrs, dict) or not isinstance(addrs.get("addrvec"), list)
            or not 1 <= len(addrs["addrvec"]) <= 64):
        raise ValueError("native log oracle requires source address metadata")
    for address in addrs["addrvec"]:
        if (not isinstance(address, dict) or address.get("type") not in ("v1", "v2", "any")
                or not isinstance(address.get("addr"), str) or not address["addr"]
                or len(address["addr"]) > 128 or type(address.get("nonce")) is not int
                or not 0 <= address["nonce"] < 1 << 32):
            raise ValueError("native log oracle has invalid source address metadata")
    return {"sentinel": sentinel, "entry": entry}


def main():
    root = pathlib.Path(sys.argv[1])
    sentinel = (root / "log-oracle-sentinel.txt").read_text().strip()
    with pathlib.Path(sys.argv[2]).open("rb") as source:
        data = source.read(LIMIT + 1)
    oracle = select_entry(data, sentinel)
    (root / "log-oracle.json").write_text(json.dumps(oracle, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        raise SystemExit(str(error)) from None
