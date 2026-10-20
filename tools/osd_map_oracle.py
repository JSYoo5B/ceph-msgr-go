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
MAX_MAP_BYTES = 2**20
# ceph-dencoder adds RESERVED itself before encoding. Its set_features parser
# uses signed atoll; pass only peer bits, without RESERVED (62) or retired bit 63.
# https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/tools/ceph-dencoder/ceph_dencoder.cc#L160


def native(*arguments):
    try:
        result = subprocess.run(arguments, check=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=10)
    except subprocess.CalledProcessError as error:
        # Only the native tool's bounded textual diagnostic is exposed. Do not
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
    if len(result.stdout) > MAX_MAP_BYTES:
        raise ValueError("Native map metadata exceeds the test oracle bound")
    return result.stdout


def bounded_map(path, label):
    # Bound allocation even if a native tool unexpectedly writes a large file.
    with path.open("rb") as stream:
        data = stream.read(MAX_MAP_BYTES + 1)
    if not data or len(data) > MAX_MAP_BYTES:
        raise ValueError(label + " exceeds the disposable oracle bound")
    return data


def decode_identity(path, type_name, fsid, epoch, label):
    decoded = json.loads(native("ceph-dencoder", "type", type_name,
                                "import", str(path), "decode", "dump_json"))
    if decoded["fsid"] != fsid or decoded["epoch"] != epoch:
        raise ValueError(label + " identity differs from the wire envelope")


def collect(output, fixture_root):
    evidence_path = output / "osd-map-native-oracle.json"
    # A failed later validation must not retain evidence from an earlier call.
    evidence_path.unlink(missing_ok=True)
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
        data = bounded_map(path, "Map blob")
        type_name = "OSDMap" if kind == "full" else "OSDMap::Incremental"
        decode_identity(path, type_name, fsid, epoch, "Native decoded map")
        encode_input = path
        if kind == "full":
            # A classic full map has an unordered blocklist. Decoding its wire
            # representation can reorder entries, so it is not a valid encoder
            # input for byte identity. Fetch the same epoch's native canonical
            # map, then mirror the server's canonical -> peer encoding instead.
            # OSDMonitor.cc:5497/5645 returns get_version_full(epoch) directly;
            # reencode_full_map at :4638 decodes that canonical representation.
            # https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/OSDMonitor.cc#L4638
            canonical = path.with_suffix(".canonical")
            canonical.unlink(missing_ok=True)
            native("ceph", "-c", str(fixture_root / "ceph.conf"),
                   "-n", "client.test", "-k", str(fixture_root / "keyring"),
                   "osd", "getmap", str(epoch), "-o", str(canonical))
            bounded_map(canonical, "Canonical native map")
            decode_identity(canonical, type_name, fsid, epoch, "Canonical native map")
            encode_input = canonical
        # Incremental classic containers are ordered maps/sets/vectors, including
        # new_blocklist (OSDMap.h:415). Embedded fullmap/crush buffers are copied
        # unchanged by encode_classic (OSDMap.cc:490-491), not decoded again.
        encoded = path.with_suffix(".native")
        encoded.unlink(missing_ok=True)
        # Standalone OSDMap classes invoke their encoder here. A MOSDMap message
        # dencoder can reuse an imported payload and would not prove encoding.
        native("ceph-dencoder", "type", type_name, "import", str(encode_input), "decode",
               "set_features", str(features), "encode", "export", str(encoded))
        native_data = bounded_map(encoded, "Native encoded map")
        if native_data != data:
            # Diagnose ordering/encoding differences without exposing map bytes.
            first = next((i for i, (a, b) in enumerate(zip(data, native_data)) if a != b),
                         min(len(data), len(native_data)))
            raise ValueError("Native encoding with negotiated features differs from received map bytes"
                             + f" ({source} {kind} epoch {epoch}; received {len(data)} bytes,"
                             + f" native {len(native_data)} bytes, first difference offset {first})")
        evidence.append({"kind": kind, "epoch": epoch, "bytes": len(data),
                         "sha256": hashlib.sha256(data).hexdigest()})
    result = {"fsid": fsid, "blobs": evidence, "native_feature_encoding_match": True}
    # Preserve the existing OSD manifest/evidence schema when source is omitted.
    if "source" in manifest:
        result["source"] = source
    evidence_path.write_text(json.dumps(result) + "\n", encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: osd_map_oracle.py OUTPUT FIXTURE_ROOT")
    collect(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
