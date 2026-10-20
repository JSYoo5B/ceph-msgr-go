#!/usr/bin/env python3
"""Development-only native Ceph CLI oracle for daemon-local command schemas."""

import json
import os
import pathlib
import subprocess
import sys


def collect(root, output):
    import rados

    with rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                     conf={"keyring": str(root / "keyring")}) as client:
        code, payload, _ = client.mon_command(json.dumps({"prefix": "mgr dump", "format": "json"}), b"")
    if code != 0:
        raise ValueError("Native MGR map read failed")
    manager = json.loads(payload)
    if not manager["available"] or manager["active_name"] not in ("a", "b"):
        raise ValueError("Native MGR map has no active fixture manager")
    base = ["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test",
            "-k", str(root / "keyring"), "tell"]
    roles = [("mon-" + name, "mon." + name) for name in ("a", "b", "c")]
    roles.append(("mgr", "mgr." + manager["active_name"]))
    catalogs = {}
    for role, target in roles:
        response = subprocess.run(base + [target, "get_command_descriptions", "--format", "json"],
                                  check=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=5)
        catalog = json.loads(response.stdout)
        if not isinstance(catalog, dict) or not catalog:
            raise ValueError("Native Tell catalog is not a nonempty object")
        catalogs[role] = catalog
    for role, catalog in catalogs.items():
        filename = "tell-command-oracle-" + role + ".json"
        temporary = output / (filename + ".tmp")
        temporary.write_text(json.dumps(catalog, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, output / filename)
    metadata = {"manager_name": manager["active_name"], "manager_id": manager["active_gid"],
                "entries": {role: len(catalog) for role, catalog in catalogs.items()}}
    temporary = output / "tell-command-oracle-summary.tmp"
    temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
    os.replace(temporary, output / "tell-command-oracle-summary.json")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Expected disposable fixture root and output directory")
    collect(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
