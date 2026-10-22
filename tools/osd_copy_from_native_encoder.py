#!/usr/bin/env python3
"""Development-only fresh COPY_FROM requests from the pinned native SDK.

Reuse the existing official CMake bootstrap. Keep private compiler/loader logs
in the supplied work directory; print only a sanitized, hash-bound receipt.
"""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import platform
import struct

from osd_alloc_hint_native_encoder import COMMIT, IMAGE, build as bootstrap

SHAPES = ("copy-head", "copy2-head", "copy2-truncate", "copy-snapshot", "copy2-snapshot")
PREFIX = "copy-from-native-request-v6-"


def collect(output, fixtures):
    suffixes = (".front.bin", ".data.bin", ".header.bin", ".native-header.json",
                ".decoded-ops.json", ".decoded-source.json")
    names = {PREFIX + shape for shape in SHAPES}
    if set(fixtures) != names or {p.name for p in output.iterdir()} != {
            name + suffix for name in names for suffix in suffixes}:
        raise RuntimeError("native copy artifact set differs")
    for shape in SHAPES:
        name = PREFIX + shape
        fixture = fixtures[name]
        source = json.loads((output / (name + ".decoded-source.json")).read_bytes())
        truncate = shape == "copy2-truncate"
        expected = dict(object_hex=b"src\x01\xc3\xa9".hex(), pool=8, key_hex=b"source-key".hex(),
                        namespace_hex=b"source-ns".hex(), hash=-1,
                        truncate_sequence=9 if truncate else 0,
                        truncate_size=0xfedcba98 if truncate else 0)
        if json.dumps(source, sort_keys=True) != json.dumps(expected, sort_keys=True):
            raise RuntimeError("native decoded source differs: " + shape)
        fixture["decoded_source"] = source
        # These are the raw native builder bytes, including the unused zero
        # truncate tail in flagless COPY_FROM2. Never normalize them away.
        data = bytes.fromhex(fixture["data_hex"])
        snapshot = shape.endswith("snapshot")
        union = struct.pack("<QQBI7x", 6 if snapshot else (1 << 64) - 2,
                            0x0102030405060708 if snapshot else 0,
                            32 if truncate else 0, 8 if snapshot else 0)
        ops = fixture["decoded_operations"]
        disk_ops = json.loads((output / (name + ".decoded-ops.json")).read_bytes())
        if json.dumps(ops, sort_keys=True) != json.dumps(disk_ops, sort_keys=True):
            raise RuntimeError("native operation receipt differs")
        if (len(ops) != 1 or ops[0]["code"] != (0x222d if "copy2" in shape else 0x221a)
                or ops[0]["flags"] != 0 or ops[0]["payload_len"] != len(data)
                or ops[0]["union_hex"] != union.hex() or ops[0]["indata_hex"] != data.hex()
                or ops[0]["outdata_hex"] != ""):
            raise RuntimeError("native decoded operation differs: " + shape)
        if "copy2" in shape and not truncate and data[-12:] != bytes(12):
            raise RuntimeError("native flagless truncate tail differs")
        for part in ("front", "data", "header"):
            raw = (output / (name + "." + part + ".bin")).read_bytes()
            if raw.hex() != fixture[part + "_hex"]:
                raise RuntimeError("native artifact bytes differ")
            fixture[part + "_sha256"] = hashlib.sha256(raw).hexdigest()
        header = bytes.fromhex(fixture["header_hex"])
        if len(header) != 41 or struct.unpack("<QQHHHIHQBHH", header) != (1, 77, 42, 127, 6, 0, 0, 3, 1, 3, 0):
            raise RuntimeError("native copy envelope differs")
    return fixtures


def build(root, wrapper):
    root.mkdir(parents=True, exist_ok=True)
    output = root / "encoded-copy-from"
    if output.exists():
        raise RuntimeError("use a fresh output directory")
    with (root / "copy-build.log").open("w+") as log:
        with contextlib.redirect_stdout(log):
            bootstrap(root, wrapper, build_target="osd_copy_from_native_encoder",
                      output_name=output.name, status="five fresh native COPY_FROM/COPY_FROM2 requests")
        log.seek(0)
        proof = json.loads(log.read().splitlines()[-1])
    if proof["ceph_commit"] != COMMIT or proof["image"] != IMAGE:
        raise RuntimeError("native bootstrap identity differs")
    proof.pop("loader_audit")  # Includes private execution paths; retain hashes.
    digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    proof["bootstrap_driver_sha256"] = proof["build_driver_sha256"]
    proof["build_driver_sha256"] = digest(Path(__file__))
    proof["build_architecture"] = platform.machine()
    proof["source_files_sha256"] = {p: digest(root / ("ceph-" + COMMIT) / p) for p in (
        "src/osdc/Objecter.h", "src/messages/MOSDOp.h", "src/osd/PrimaryLogPG.cc",
        "src/include/rados.h", "src/osd/osd_types.h", "src/osd/osd_types.cc")}
    proof["fixtures"] = collect(output, proof["fixtures"])
    print(json.dumps(proof, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
