#!/usr/bin/env python3
"""Development-only pinned WRITESAME native encoder/decoder driver."""
import argparse
import pathlib
from osd_alloc_hint_native_encoder import build
from build_sparse_read_oracle import build as build_oracle, COMMIT, COMPILER

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve(),
          build_target="osd_writesame_native_encoder", output_name="encoded-writesame",
          status="five fresh native WRITESAME message vectors directly encoded and separately decoded; logical outputs are not live server execution")

    # The disposable encoder container already contains the pinned source,
    # generated configuration and compiler. Build the separate native client
    # while those prerequisites are available; this adds no product dependency.
    work = args.work.resolve()
    build_oracle(source=work / ("ceph-" + COMMIT), builddir=work / "build",
                 wrapper=pathlib.Path("/tmp/osd_writesame_oracle.cc"),
                 output=work / "writesame-oracle",
                 librados=pathlib.Path("/usr/lib64/librados.so.2"), compiler=COMPILER)
