#!/usr/bin/env python3
"""Test-only native validation of received OSDMap blobs; never product code."""

import hashlib
import json
import pathlib
import subprocess
import sys

# Fixed test-client peer masks, matching internal/session/handshake.go.
# The MON-only CRUSH admission bits do not establish map application support.
# https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/OSDMonitor.cc#L4609
# Native MON/OSD senders re-encode maps with canonical features intersected with
# peer features; this oracle does not substitute Ceph's all-features mask.
FEATURES = sum(1 << bit for bit in (1, 2, 4, 5, 8, 9, 15, 23, 28, 42, 57, 59, 61))
# MON now negotiates the implemented PGID64 wire capability (9), but not the
# OSD object-locator capability (8). PGPOOL3/OSDMAP_ENC/INCSUBOSDMAP remain absent.
MON_FEATURES = (FEATURES & ~(1 << 8)) | sum(
    1 << bit for bit in (18, 25, 41, 48, 58))
PEER_FEATURES = {"osd": FEATURES, "mon": MON_FEATURES}
# ceph-dencoder adds RESERVED itself before encoding. Its set_features parser
# uses signed atoll; pass only peer bits, without RESERVED (62) or retired bit 63.
# https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/tools/ceph-dencoder/ceph_dencoder.cc#L160


def native(*arguments):
    try:
        result = subprocess.run(arguments, check=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=10)
    except subprocess.CalledProcessError as error:
        # Only ceph-dencoder's bounded textual diagnostic is exposed. Do not
        # include argv, stdout, or raw map bytes in the functional error report.
        stderr = error.stderr or b""
        try:
            diagnostic = stderr[:4096].decode("utf-8")
            if any(ord(c) < 32 and c not in "\t\r\n" for c in diagnostic):
                diagnostic = "non-text native diagnostic omitted"
        except UnicodeDecodeError:
            diagnostic = "non-text native diagnostic omitted"
        if len(stderr) > 4096:
            diagnostic += " [truncated]"
        raise ValueError("Native map validation exited with code " + str(error.returncode)
                         + ": " + (diagnostic.strip() or "no stderr diagnostic")) from None
    if len(result.stdout) > 2**20:
        raise ValueError("Native map metadata exceeds the test oracle bound")
    return result.stdout


def collect(output):
    manifest = json.loads((output / "osd-map-manifest.json").read_bytes())
    fsid = manifest["fsid"]
    records = manifest["blobs"]
    source = manifest.get("source", "osd")
    if not isinstance(source, str) or source not in PEER_FEATURES:
        raise ValueError("Invalid fixed map oracle source")
    features = PEER_FEATURES[source]
    if not isinstance(fsid, str) or not isinstance(records, list) or not records or len(records) > 64:
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
        # Standalone OSDMap classes invoke their encoder here. A MOSDMap message
        # dencoder can reuse an imported payload and would not prove encoding.
        native("ceph-dencoder", "type", type_name, "import", str(path), "decode",
               "set_features", str(features), "encode", "export", str(encoded))
        if encoded.read_bytes() != data:
            raise ValueError("Native encoding with negotiated features differs from received map bytes")
        evidence.append({"kind": kind, "epoch": epoch, "bytes": len(data),
                         "sha256": hashlib.sha256(data).hexdigest()})
    result = {"fsid": fsid, "blobs": evidence, "native_feature_encoding_match": True}
    # Preserve the existing OSD manifest/evidence schema when source is omitted.
    if "source" in manifest:
        result["source"] = source
    (output / "osd-map-native-oracle.json").write_text(json.dumps(result) + "\n", encoding="utf-8")


if __name__ == "__main__":
    collect(pathlib.Path(sys.argv[1]))
