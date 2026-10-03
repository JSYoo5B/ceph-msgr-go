#!/usr/bin/env python3
"""Test-only native validation of received OSDMap blobs; never product code."""

import hashlib
import json
import pathlib
import subprocess
import sys

# Exact OSD CLIENT feature policy from the pinned Tentacle communication client.
# No CRUSH admission bits, PGPOOL3 or OSDENC are added for this oracle.
FEATURES = sum(1 << bit for bit in (1, 2, 4, 5, 8, 9, 15, 23, 28, 42, 57, 59, 61))
# ceph-dencoder adds RESERVED itself before encoding. Its set_features parser
# uses signed atoll, so pass the peer mask without the local bit-63 control flag.


def native(*arguments):
    result = subprocess.run(arguments, check=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=10)
    if len(result.stdout) > 2**20:
        raise ValueError("Native map metadata exceeds the test oracle bound")
    return result.stdout


def collect(output):
    manifest = json.loads((output / "osd-map-manifest.json").read_bytes())
    fsid = manifest["fsid"]
    records = manifest["blobs"]
    if not isinstance(fsid, str) or not records or len(records) > 64:
        raise ValueError("Invalid map oracle manifest")
    evidence = []
    for record in records:
        kind, epoch = record["kind"], record["epoch"]
        if kind not in ("full", "incremental") or type(epoch) is not int or not 0 < epoch < 2**32:
            raise ValueError("Invalid map blob kind or epoch")
        path = output / ("osd-map-" + kind + "-" + str(epoch) + ".bin")
        data = path.read_bytes()
        if not data or len(data) > 2**20:
            raise ValueError("Map blob exceeds the disposable oracle bound")
        type_name = "OSDMap" if kind == "full" else "OSDMap::Incremental"
        decoded = json.loads(native("ceph-dencoder", "type", type_name,
                                    "import", str(path), "decode", "dump_json"))
        if decoded["fsid"] != fsid or decoded["epoch"] != epoch:
            raise ValueError("Native decoded map identity differs from the wire envelope")
        encoded = path.with_suffix(".native")
        native("ceph-dencoder", "type", type_name, "import", str(path), "decode",
               "set_features", str(FEATURES), "encode", "export", str(encoded))
        if encoded.read_bytes() != data:
            raise ValueError("Native encoding with negotiated features differs from received map bytes")
        evidence.append({"kind": kind, "epoch": epoch, "bytes": len(data),
                         "sha256": hashlib.sha256(data).hexdigest()})
    result = {"fsid": fsid, "blobs": evidence, "native_feature_encoding_match": True}
    (output / "osd-map-native-oracle.json").write_text(json.dumps(result) + "\n", encoding="utf-8")


if __name__ == "__main__":
    collect(pathlib.Path(sys.argv[1]))
