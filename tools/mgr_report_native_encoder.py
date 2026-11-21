#!/usr/bin/env python3
"""Development-only MGR-to-MON report vectors; no product dependency.

Reuse the pinned MPing builder's official Ceph CMake bootstrap unchanged,
then compile a separate wrapper (mgr_report_native_encoder.cc) against that
matching native library. The wrapper produces MMonMgrReport messages, the
PGMapDigests their data segment carries and an encoded ServiceMap with
native encoders, checks each with a fresh native decoder and records each
digest's MON statfs projection and native dump. Run it in an owned
disposable container of the pinned image, with a writable tools mount (the
bootstrap touches its wrapper):

    docker run --rm -v <cache>:/work -v <checkout>/tools:/tools <image> \\
        python3 /tools/mgr_report_native_encoder.py --work /work \\
        --wrapper /tools/mgr_report_native_encoder.cc > proof.json

The last stdout line is the proof; internal/msgr/testdata keeps it as
mgr-report-native-provenance.json. The bootstrap MPing vectors are not
counted as MGR report evidence.
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
# Messages: name -> (type, version, compat); each carries a data segment.
MESSAGES = {"report-populated": (0x706, 3, 1), "report-plain": (0x706, 3, 1)}
# Encoded structures: name -> artifact suffix.
STRUCTURES = {"digest-a": "digest", "digest-b": "digest", "digest-empty": "digest",
              "service-map": "servicemap"}
# Cut digests the native decoder refused.
REFUSED = ("digest-truncated", "digest-short-struct")


def collect(output):
    expected = {name + suffix for name in MESSAGES for suffix in
                (".front.bin", ".middle.bin", ".data.bin", ".header.bin", ".decoded.json")}
    expected |= {name + "." + kind + ".bin" for name, kind in STRUCTURES.items()}
    expected |= {name + ".decoded.json" for name in STRUCTURES}
    expected |= {name + suffix for name in REFUSED for suffix in (".digest.bin", ".decoded.json")}
    if {path.name for path in output.iterdir()} != expected:
        raise RuntimeError("native MGR report artifact set differs")
    fixtures = {}
    for name, (typ, version, compat) in MESSAGES.items():
        parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                 for part in ("front", "middle", "data", "header")}
        fields = HEADER.unpack(parts["header"]) if len(parts["header"]) == HEADER.size else None
        if (fields is None or fields[1] != 0 or fields[2:5] != (typ, 127, version) or fields[9] != compat or
                parts["middle"] or not parts["data"] or not parts["front"]):
            raise RuntimeError("native MGR report envelope differs: " + name)
        decoded = json.loads((output / (name + ".decoded.json")).read_bytes())
        fixtures[name] = dict(type=typ, version=version, compat=compat, decoded=decoded)
        for part, raw in parts.items():
            fixtures[name][part + "_hex"] = raw.hex()
            fixtures[name][part + "_sha256"] = hashlib.sha256(raw).hexdigest()
    for name, kind in STRUCTURES.items():
        raw = (output / (name + "." + kind + ".bin")).read_bytes()
        decoded = json.loads((output / (name + ".decoded.json")).read_bytes())
        if not raw or decoded.get("native_refused"):
            raise RuntimeError("native MGR report structure differs: " + name)
        fixtures[name] = dict(kind=kind, decoded=decoded, bytes_hex=raw.hex(), bytes_sha256=hashlib.sha256(raw).hexdigest())
    for name in REFUSED:
        decoded = json.loads((output / (name + ".decoded.json")).read_bytes())
        if decoded != {"native_refused": True}:
            raise RuntimeError("native refusal receipt differs: " + name)
        raw = (output / (name + ".digest.bin")).read_bytes()
        fixtures[name] = dict(kind="digest", decoded=decoded, bytes_hex=raw.hex(), bytes_sha256=hashlib.sha256(raw).hexdigest())
    # The populated report carries digest-a, the plain one the empty digest.
    for report, carried in (("report-populated", "digest-a"), ("report-plain", "digest-empty")):
        if fixtures[report]["data_hex"] != fixtures[carried]["bytes_hex"]:
            raise RuntimeError("native MGR report data segment differs: " + report)
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
    target = "mgr_report_native_encoder"
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
    command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "mgr-report-cmake", env)
    command(["cmake", "--build", str(builddir), "--target", target, "-j2"], root, "mgr-report-compile", env)
    binary = builddir / "bin" / target
    loader = command(["ldd", str(binary)], root, "mgr-report-loader", env)
    if "libceph-common.so.2 => " + str(builddir / "lib") + "/" not in loader:
        raise RuntimeError("MGR report wrapper loaded a foreign native library")
    output = root / "encoded-mgr-report"
    if output.exists():
        raise RuntimeError("use a fresh output directory; stale vectors must not be reused")
    command([str(binary), str(output)], root, "mgr-report-encode-decode", env)
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
                                       ("src/messages/MMonMgrReport.h", "src/messages/PaxosServiceMessage.h",
                                        "src/mon/PGMap.h", "src/mon/PGMap.cc", "src/mon/health_check.h",
                                        "src/mon/mon_types.h", "src/include/health.h", "src/mgr/ServiceMap.h",
                                        "src/mgr/ServiceMap.cc", "src/osd/osd_types.h", "src/osd/osd_types.cc",
                                        "src/include/interval_set.h", "src/msg/Message.cc", "src/msg/async/frames_v2.h")},
                  canonical_message_encode_verified=True, native_payload_decoder_verified=True,
                  matching_native_loader_verified=True, fixtures=collect(output))
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
