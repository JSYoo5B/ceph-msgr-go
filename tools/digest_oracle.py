#!/usr/bin/env python3
"""Development-only health and named-MON status oracle for the fixture."""

import json
import os
import pathlib
import subprocess
import sys

import rados

root, output = (pathlib.Path(value) for value in sys.argv[1:3])
name, label = sys.argv[3:]
with rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                 conf={"keyring": str(root / "keyring")}) as client:
    code, payload, _ = client.mon_command(json.dumps({"prefix": "health", "detail": "detail",
                                                     "format": "json"}), b"")
if code != 0:
    raise SystemExit("Native health-detail oracle command failed")
status = subprocess.run(["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test",
                         "-k", str(root / "keyring"), "tell", "mon." + name,
                         "mon_status", "--format", "json"], check=True,
                        stdout=subprocess.PIPE, timeout=10).stdout
metadata = {"name": name, "label": label, "health": json.loads(payload),
            "mon_status": json.loads(status)}
temporary = output / "digest-oracle.tmp"
temporary.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
os.replace(temporary, output / "digest-oracle.json")
