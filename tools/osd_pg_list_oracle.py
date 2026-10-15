#!/usr/bin/env python3
"""Pinned native CLI inventory of one explicit PG, independently of Go.

Tentacle src/tools/rados/rados.cc:2490-2565 uses native nobjects_begin(pg)
(PGNLS), then native object-PG checking, preserving names/namespaces/locators.
This compares the complete PG inventory, not individual wire page boundaries.
The fixture pool and explicit namespace are read only; no automatic mutation
or retry occurs. LGPL-2.1 source referenced, no implementation copied.
"""
import argparse
import json
from pathlib import Path
import subprocess

import osd_omap_native as omap
import osd_xattr_native as xattr

MAX_ENTRIES = 4096
MAX_OUTPUT = 512 << 10


def request(raw):
    if not isinstance(raw, dict) or set(raw) != {"pg_seed", "namespace"}:
        raise ValueError("invalid PG list input fields")
    pg = omap.integer(raw["pg_seed"], (1 << 32) - 1, "PG seed")
    ns = xattr.identity(raw["namespace"], "namespace", empty=True)
    if ns == b"\x01":
        raise ValueError("native PG CLI oracle supports one explicit namespace")
    return pg, ns.decode()


def parse_listing(data, namespace):
    if len(data) > MAX_OUTPUT:
        raise ValueError("native PG list output exceeds bound")
    raw = json.loads(data)
    if not isinstance(raw, list) or len(raw) > MAX_ENTRIES:
        raise ValueError("invalid native PG list inventory")
    result = []
    for item in raw:
        if not isinstance(item, dict) or set(item) - {"namespace", "name", "locator"} or not {"namespace", "name"} <= set(item):
            raise ValueError("invalid native PG list entry")
        if item["namespace"] != namespace or not isinstance(item["name"], str) or not isinstance(item.get("locator", ""), str):
            raise ValueError("native PG list namespace or identity differs")
        result.append(dict(namespace=item["namespace"], object=item["name"], key=item.get("locator", "")))
    if len({(e["namespace"], e["object"], e["key"]) for e in result}) != len(result):
        raise ValueError("duplicate native PG list entry")
    return result


def run(root, out):
    pg, namespace = request(omap.read_json(out / "osd-pg-list-input.json"))
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("PG list oracle requires pinned Tentacle")
    pool_id = omap.integer(metadata["pool_id"], (1 << 63) - 1, "pool ID")
    args = ["rados", "-c", str(root / "ceph.conf"), "-n", "client.test", "--keyring", str(root / "keyring"),
            "-p", metadata["pool"], "--pgid", f"{pool_id}.{pg:x}", "--namespace", namespace, "--format", "json", "ls"]
    observed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if observed.returncode or len(observed.stderr) > 4096:
        raise RuntimeError("native PG list CLI failed")
    entries = parse_listing(observed.stdout, namespace)
    omap.publish(out, "osd-pg-list-result", dict(source_commit=xattr.SOURCE_COMMIT, pool_id=pool_id, pg_seed=pg,
                 namespace=namespace, entries=entries, cli_stdout=observed.stdout.decode(),
                 scope="native complete explicit-PG inventory; page boundaries are not compared"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("observe",))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.root, args.out)
