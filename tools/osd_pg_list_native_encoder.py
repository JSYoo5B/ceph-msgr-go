#!/usr/bin/env python3
"""Development-only pinned PGNLS native encoder/decoder driver."""
import argparse
import pathlib
from osd_alloc_hint_native_encoder import build

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve(),
          build_target="osd_pg_list_native_encoder", output_name="encoded-pg-list",
          status="five fresh native PGNLS message vectors; native page and message decode verified independently of Go")
