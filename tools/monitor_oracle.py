#!/usr/bin/env python3
"""Development-only native MonMap oracle for the disposable fixture."""

import json
import os
import pathlib
import sys

import rados

root, output = (pathlib.Path(value) for value in sys.argv[1:])
with rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                 conf={"keyring": str(root / "keyring")}) as client:
    code, payload, _ = client.mon_command(json.dumps({"prefix": "mon dump", "format": "json"}), b"")
if code != 0:
    raise SystemExit("Native monitor-map oracle command failed")
full = json.loads(payload)
metadata = {name: full[name] for name in ("epoch", "fsid")}
metadata["mons"] = [{name: item[name] for name in ("name", "rank")}
                    for item in sorted(full["mons"], key=lambda item: item["rank"])]
temporary = output / "monitor-oracle.tmp"
temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
os.replace(temporary, output / "monitor-oracle.json")
