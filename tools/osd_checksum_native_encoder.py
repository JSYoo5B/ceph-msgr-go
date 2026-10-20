#!/usr/bin/env python3
"""Development-only pinned CHECKSUM native encoder/decoder driver.

Each algorithm invokes the unchanged five-vector builder. This preserves the
historical build-driver provenance shared by other retained native vectors.
"""
import argparse
import contextlib
import io
import json
import os
import pathlib
from osd_alloc_hint_native_encoder import build


def build_algorithms(work, wrapper):
    fixtures, variants = {}, {}
    prior = os.environ.get("CEPH_CHECKSUM_ALGORITHM")
    try:
        for algorithm in ("crc32c", "xxhash32", "xxhash64"):
            os.environ["CEPH_CHECKSUM_ALGORITHM"] = algorithm
            captured = io.StringIO()
            try:
                with contextlib.redirect_stdout(captured):
                    build(work, wrapper, build_target="osd_checksum_native_encoder",
                          output_name="encoded-checksum-" + algorithm,
                          status="five fresh native " + algorithm + " CHECKSUM vectors directly encoded and separately decoded; no live checksum execution proved")
            finally:
                print(captured.getvalue(), end="", flush=True)
            proof = json.loads(captured.getvalue().splitlines()[-1])
            if fixtures.keys() & proof["fixtures"].keys():
                raise RuntimeError("duplicate checksum native vector name")
            fixtures.update(proof["fixtures"])
            variants[algorithm] = {
                field: proof[field] for field in
                ("wrapper_sha256", "binary_sha256", "native_library_sha256",
                 "generated_config_sha256", "build_driver_sha256")
            }
        proof["fixtures"] = fixtures
        proof["algorithm_builds"] = variants
        proof["status"] = ("fifteen fresh native CHECKSUM vectors across three independently built and executed algorithm selections; "
                           "native request/reply encoders and decoders verified; logical reply outputs do not prove live checksum calculations")
        print(json.dumps(proof), flush=True)
    finally:
        if prior is None:
            os.environ.pop("CEPH_CHECKSUM_ALGORITHM", None)
        else:
            os.environ["CEPH_CHECKSUM_ALGORITHM"] = prior


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build_algorithms(args.work.resolve(), args.wrapper.resolve())
