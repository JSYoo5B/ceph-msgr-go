#!/usr/bin/env python3
"""Development-only pinned CHECKSUM native encoder/decoder driver.

Each algorithm invokes the shared five-vector builder and merges the proof
object it returns. Builder diagnostics stream to stdout unchanged; they are
never parsed, so their order or content cannot alter the merged proof.
"""
import argparse
import copy
import hashlib
import json
import os
import pathlib
from osd_alloc_hint_native_encoder import build

ALGORITHMS = ("crc32c", "xxhash32", "xxhash64")
SELECTION = "CEPH_CHECKSUM_ALGORITHM"
VARIANT_FIELDS = ("wrapper_sha256", "binary_sha256", "native_library_sha256",
                  "generated_config_sha256", "build_driver_sha256")


def build_algorithms(work, wrapper):
    fixtures, variants, proof = {}, {}, None
    prior = os.environ.get(SELECTION)
    try:
        for algorithm in ALGORITHMS:
            os.environ[SELECTION] = algorithm
            proof = build(work, wrapper, build_target="osd_checksum_native_encoder",
                          output_name="encoded-checksum-" + algorithm,
                          status="five fresh native " + algorithm + " CHECKSUM vectors directly encoded and separately decoded; no live checksum execution proved")
            if not isinstance(proof, dict) or not isinstance(proof.get("fixtures"), dict):
                raise RuntimeError("shared builder returned no structured proof for " + algorithm)
            proof = copy.deepcopy(proof)
            if fixtures.keys() & proof["fixtures"].keys():
                raise RuntimeError("duplicate checksum native vector name")
            fixtures.update(proof["fixtures"])
            variants[algorithm] = {field: proof[field] for field in VARIANT_FIELDS}
    finally:
        if prior is None:
            os.environ.pop(SELECTION, None)
        else:
            os.environ[SELECTION] = prior
    proof["fixtures"] = fixtures
    proof["algorithm_builds"] = variants
    proof["launcher_sha256"] = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    proof["status"] = ("fifteen fresh native CHECKSUM vectors across three independently built and executed algorithm selections; "
                       "native request/reply encoders and decoders verified; logical reply outputs do not prove live checksum calculations")
    print(json.dumps(proof), flush=True)
    return proof


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build_algorithms(args.work.resolve(), args.wrapper.resolve())
