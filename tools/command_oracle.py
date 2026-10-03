#!/usr/bin/env python3
"""Development-only native command-catalog oracle for the Tentacle fixture.

Uses Python librados inside the disposable Ceph image. Product Go packages
must never import, invoke, build, or install this helper. Invoke under shell
`timeout`: Tentacle Python mon_command/mgr_command ignore their timeout arg.
"""

import json
import os
import pathlib
import sys


PREFIXES = {
    "mon": ("status", "config get"),
    "mgr": ("pg stat", "balancer status", "iostat"),
}
LABELS = {"initial", "renewal", "learned-mon", "mgr-replacement"}


def canonical_catalog(payload):
    """Keep every native descriptor and field, normalizing object key order."""
    value = json.loads(payload)
    if not isinstance(value, dict) or not value:
        raise ValueError("Native command catalog is not a nonempty JSON object")
    if any(not isinstance(entry, dict) or not isinstance(entry.get("sig"), list)
           for entry in value.values()):
        raise ValueError("Native command catalog lacks descriptor signatures")
    return value, (json.dumps(value, ensure_ascii=True, sort_keys=True,
                             separators=(",", ":")) + "\n").encode("utf-8")


def command_prefix(signature):
    """Ceph sig contains leading literal tokens followed by argument dicts."""
    literals = []
    for part in signature:
        if not isinstance(part, str):
            break
        literals.append(part)
    return " ".join(literals)


def role_metadata(catalog, role):
    matches = {prefix: [] for prefix in PREFIXES[role]}
    for tag, entry in catalog.items():
        prefix = command_prefix(entry["sig"])
        if prefix in matches:
            matches[prefix].append(tag)
    # Exact module-dependent requirements belong in the integration test after
    # the pinned image's enabled catalog has been independently observed.
    return {"entries": len(catalog),
            "selected": {prefix: sorted(tags) for prefix, tags in matches.items()}}


def atomic_write(output, filename, payload, request_id):
    temporary = output / (filename + "." + str(request_id) + ".tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, output / filename)


def collect(fixture_root, output, request_id, label):
    import rados

    # A new independent native client reads both services. No object I/O is
    # performed and no product state, command cache, or Go codec is consulted.
    request = json.dumps({"prefix": "get_command_descriptions", "format": "json"})
    catalog_data = {}
    with rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                     conf={"keyring": str(fixture_root / "keyring")}) as client:
        for role in ("mon", "mgr"):
            command = client.mon_command if role == "mon" else client.mgr_command
            code, payload, status = command(request, b"")
            if code != 0:
                raise ValueError("Native " + role.upper() + " command catalog returned code " + str(code))
            catalog, canonical = canonical_catalog(payload)
            catalog_data[role] = (catalog, canonical)

    # Publish only once both authenticated native reads succeeded. The caller's
    # acknowledgement is the barrier before Go integration reads any files.
    metadata = {"request_id": request_id, "label": label}
    for role, (catalog, canonical) in catalog_data.items():
        atomic_write(output, "command-oracle-" + role + ".json", canonical, request_id)
        metadata[role] = role_metadata(catalog, role)
    encoded = (json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    atomic_write(output, "command-oracle-summary.json", encoded, request_id)


def main():
    if len(sys.argv) != 5:
        raise ValueError("Expected fixture root, output directory, request ID, label")
    fixture_root, output, raw_id, label = sys.argv[1:]
    if not raw_id.isascii() or not raw_id.isdecimal() or label not in LABELS:
        raise ValueError("Invalid native command oracle request")
    request_id = int(raw_id)
    if request_id < 0 or (request_id == 0 and label != "initial"):
        raise ValueError("Invalid native command oracle request ID")
    collect(pathlib.Path(fixture_root), pathlib.Path(output), request_id, label)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError):
        # Descriptor failures must not turn unrelated credentials/config into
        # diagnostic output. Full command descriptions contain schema only.
        raise SystemExit("Native command description oracle failed") from None
