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
metadata["modules"] = full["modules"]
metadata["services"] = full["services"]
# Fixed Tentacle common/options.h formatter names. Only the independent
# comparison copy changes representation; product snapshots keep wire codes.
# "unknown" is deliberately rejected because the formatter loses the code.
types = {name: code for code, name in enumerate(("uint", "int", "str", "float", "bool", "addr", "addrvec", "uuid", "size", "secs", "millisecs"))}
levels = {"basic": 0, "advanced": 1, "dev": 2}
metadata["available_modules"] = []
for module in full["available_modules"]:
    entry = {name: module[name] for name in ("name", "can_run", "error_string")}
    entry["module_options"] = {}
    for key, option in module["module_options"].items():
        descriptor = dict(option)
        descriptor["type"] = types[option["type"]]
        descriptor["level"] = levels[option["level"]]
        entry["module_options"][key] = descriptor
    metadata["available_modules"].append(entry)
temporary = output / "manager-oracle.tmp"
temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
os.replace(temporary, output / "manager-oracle.json")
