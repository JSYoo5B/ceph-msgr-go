#!/usr/bin/env python3
"""Development-only current MGR service vectors; no product dependency.

Reuse the pinned MPing builder's official Ceph CMake bootstrap unchanged.
Then compile a separate wrapper against that matching native library. The
bootstrap MPing vectors are not counted as MGR service evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import struct
import subprocess
import sys

from mon_ping_native_encoder import COMMIT, IMAGE, command

HEADER = struct.Struct("<QQHHHIHQBHH")
SHAPES = {"open-empty": (0x700, 3), "open-populated": (0x700, 3),
          "update": (0x70b, 2), "report-omitted": (0x702, 9),
          "report-empty": (0x702, 9), "report-populated": (0x702, 9),
          "configure": (0x701, 4), "close": (0x708, 1)}


def hex_map(values):
    return {key.encode().hex(): value.hex() for key, value in values.items()}


def expected_fields(name):
    identity = dict(service_hex=b"svc".hex(), daemon_hex=b"d1".hex())
    binary = b"\0\xff"
    if name.startswith("open-"):
        populated = name == "open-populated"
        return dict(identity, service_daemon=True, config_bytes=0, defaults_bytes=0,
                    metadata_hex=hex_map({"b": b"2", "a": b"1", "binary": binary}) if populated else {},
                    status_hex=hex_map({"state": b"up"}) if populated else {})
    if name == "update":
        return dict(identity, need_metadata_update=True,
                    metadata_hex=hex_map({"zone": b"b", "binary": binary}), status_hex={})
    if name.startswith("report-"):
        status = task = None
        if name == "report-empty":
            status, task = {}, {}
        elif name == "report-populated":
            status, task = hex_map({"state": b"busy", "binary": binary}), hex_map({"task": b"x"})
        return dict(identity, status_hex=status, task_hex=task,
                    packed_hex="010100000000", empty_auxiliary_fields=True)
    if name == "configure":
        return dict(period=5, threshold=2, empty_extensions=True)
    if name == "close":
        return identity
    raise RuntimeError("unknown native service vector")


def collect(output):
    expected = {name + suffix for name in SHAPES for suffix in
                (".front.bin", ".middle.bin", ".data.bin", ".header.bin", ".decoded.json")}
    if {path.name for path in output.iterdir()} != expected:
        raise RuntimeError("native service artifact set differs")
    fixtures = {}
    for name, (typ, version) in SHAPES.items():
        parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                 for part in ("front", "middle", "data", "header")}
        if (len(parts["header"]) != HEADER.size or
                HEADER.unpack(parts["header"]) != (1, 0, typ, 127, version, 0, 0, 3, 1, 1, 0) or
                parts["middle"] or parts["data"] or not parts["front"]):
            raise RuntimeError("native service envelope differs: " + name)
        fields = json.loads((output / (name + ".decoded.json")).read_bytes())
        # JSON booleans and integers are distinct receipts, despite Python's
        # True == 1. Canonical JSON comparison preserves that distinction.
        if json.dumps(fields, sort_keys=True) != json.dumps(expected_fields(name), sort_keys=True):
            raise RuntimeError("native service decoder fields differ: " + name)
        fixtures[name] = dict(type=typ, version=version, compat=1, decoded=fields)
        for part, raw in parts.items():
            fixtures[name][part + "_hex"] = raw.hex()
            fixtures[name][part + "_sha256"] = hashlib.sha256(raw).hexdigest()
    return fixtures


def build(root, wrapper):
    if not Path("/.dockerenv").is_file():
        raise RuntimeError("native build requires an owned disposable Docker container")
    root.mkdir(parents=True, exist_ok=True)
    tools = Path(__file__).resolve().parent
    # Keep prior producer hashes and receipts valid by leaving the bootstrap
    # implementation unchanged. It verifies source, architecture and loader.
    bootstrap = subprocess.run(
        [sys.executable, str(tools / "mon_ping_native_encoder.py"), "--build-native",
         "--work", str(root), "--wrapper", str(tools / "mon_ping_native_encoder.cc")],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=3600)
    (root / "bootstrap.log").write_text(bootstrap.stdout)
    if bootstrap.returncode:
        raise RuntimeError("pinned bootstrap failed; see bootstrap.log")
    proof = json.loads(bootstrap.stdout.splitlines()[-1])
    if proof["ceph_commit"] != COMMIT or proof["image"] != IMAGE:
        raise RuntimeError("pinned bootstrap identity differs")
    checkout, builddir = root / ("ceph-" + COMMIT), root / "build"
    # Use a stable, owned wrapper path and only append the independent target.
    target = "mgr_service_native_encoder"
    local_wrapper = root / (target + ".cc")
    local_wrapper.write_bytes(wrapper.read_bytes())
    cmake = checkout / "src/CMakeLists.txt"
    if "add_executable(" + target not in cmake.read_text():
        with cmake.open("a") as out:
            out.write('\nadd_executable(' + target + ' "' + str(local_wrapper) + '")\n'
                      'target_link_libraries(' + target + ' PRIVATE ceph-common ${ceph_common_deps})\n'
                      'target_compile_features(' + target + ' PRIVATE cxx_std_20)\n')
    env = dict(os.environ)
    env["PATH"] = "/opt/rh/gcc-toolset-14/root/usr/bin:" + env["PATH"]
    env["LD_LIBRARY_PATH"] = str(builddir / "lib") + ":/opt/rh/gcc-toolset-14/root/usr/lib64"
    command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "mgr-cmake", env)
    command(["cmake", "--build", str(builddir), "--target", target, "-j2"], root, "mgr-compile", env)
    binary = builddir / "bin" / target
    loader = command(["ldd", str(binary)], root, "mgr-loader", env)
    if "libceph-common.so.2 => " + str(builddir / "lib") + "/" not in loader:
        raise RuntimeError("MGR wrapper loaded a foreign native library")
    output = root / "encoded-mgr-service"
    if output.exists():
        raise RuntimeError("use a fresh output directory; stale vectors must not be reused")
    command([str(binary), str(output)], root, "mgr-encode-decode", env)
    result = {key: proof[key] for key in
              ("ceph_commit", "image", "source_archive_url", "source_archive_sha256",
               "pinned_submodules", "header_proof_scope", "offline_transport_inputs")}
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    result.update(build_architecture=platform.machine(),
                  generated_config_sha256=digest(builddir / "include/acconfig.h"),
                  native_library_sha256=digest(builddir / "lib/libceph-common.so.2"),
                  binary_sha256=digest(binary), wrapper_sha256=digest(wrapper),
                  build_driver_sha256=digest(Path(__file__)),
                  bootstrap_sources_sha256={name: digest(tools / name) for name in
                                            ("mon_ping_native_encoder.py", "mon_ping_native_encoder.cc")},
                  source_files_sha256={path: digest(checkout / path) for path in
                                       ("src/messages/MMgrOpen.h", "src/messages/MMgrUpdate.h",
                                        "src/messages/MMgrReport.h", "src/messages/MMgrConfigure.h",
                                        "src/messages/MMgrClose.h", "src/mgr/MgrClient.cc",
                                        "src/mgr/DaemonServer.cc", "src/msg/Message.cc",
                                        "src/msg/async/frames_v2.h")},
                  canonical_message_encode_verified=True, native_payload_decoder_verified=True,
                  matching_native_loader_verified=True, fixtures=collect(output))
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
