#!/usr/bin/env python3
"""Development-only native MgrMap metadata oracle for the disposable fixture."""

import json
import os
import pathlib
import sys

import rados

root, output = (pathlib.Path(value) for value in sys.argv[1:])
with rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                 conf={"keyring": str(root / "keyring")}) as client:
    code, payload, _ = client.mon_command(json.dumps({"prefix": "mgr dump", "format": "json"}), b"")
if code != 0:
    raise SystemExit("Native manager-map oracle command failed")
full = json.loads(payload)
metadata = {name: full[name] for name in ("epoch", "available", "active_name", "active_gid")}
metadata["standbys"] = sorted(({"name": item["name"], "gid": item["gid"]}
                                for item in full["standbys"]), key=lambda item: item["gid"])
temporary = output / "manager-oracle.tmp"
temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
os.replace(temporary, output / "manager-oracle.json")
