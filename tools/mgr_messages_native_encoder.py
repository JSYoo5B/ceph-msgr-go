#!/usr/bin/env python3
"""Development-only MGR daemon message vectors; no product dependency.

Reuse the pinned MPing builder's official Ceph CMake bootstrap unchanged,
then compile a separate wrapper (mgr_messages_native_encoder.cc) against that
matching native library. The wrapper produces the messages a MGR receives
(MMgrOpen, MMgrUpdate, MMgrReport, MMgrClose, MPGStats, MMgrCommand,
MCommand) and sends (MMgrConfigure, MMgrCommandReply, MCommandReply) with
native encoders and checks each with a fresh native decoder; MPGStats is
also dumped by its native formatter. Run it in an owned disposable container
of the pinned image, with a writable tools mount (the bootstrap touches its
wrapper):

    docker run --rm -v <cache>:/work -v <checkout>/tools:/tools <image> \\
        python3 /tools/mgr_messages_native_encoder.py --work /work \\
        --wrapper /tools/mgr_messages_native_encoder.cc > proof.json

The last stdout line is the proof; internal/msgr/testdata keeps it as
mgr-messages-native-provenance.json. The bootstrap MPing vectors are not
counted as MGR message evidence.
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
# name: (type, version, compat, tid, has data)
SHAPES = {"open-osd": (0x700, 3, 1, 0, False), "open-service": (0x700, 3, 1, 0, False),
          "update-metadata": (0x70b, 2, 1, 0, False), "update-plain": (0x70b, 2, 1, 0, False),
          "close": (0x708, 1, 1, 0, False),
          "report-declare": (0x702, 9, 1, 0, False), "report-second": (0x702, 9, 1, 0, False),
          "report-populated": (0x702, 9, 1, 0, False), "report-mds-metric": (0x702, 9, 1, 0, False),
          "pgstats": (87, 2, 1, 0, False), "pgstats-acceptor": (87, 2, 1, 0, False),
          "mgr-command": (0x709, 1, 1, 7, True), "tell-command": (97, 1, 1, 9, True),
          "mgr-command-reply": (0x70a, 1, 1, 7, True), "tell-command-reply": (98, 1, 1, 9, True),
          "configure-plain": (0x701, 4, 1, 0, False), "configure-osd-queries": (0x701, 4, 1, 0, False),
          "configure-mds": (0x701, 4, 1, 0, False), "configure-mds-nopacific": (0x701, 4, 1, 0, False)}
# Hand-cut fronts the native decoder refused; they carry no envelope.
REFUSED = {"report-truncated": 0x702, "pgstats-truncated": 87}


def collect(output):
    expected = {name + suffix for name in SHAPES for suffix in
                (".front.bin", ".middle.bin", ".data.bin", ".header.bin", ".decoded.json")}
    expected |= {name + suffix for name in REFUSED for suffix in (".front.bin", ".decoded.json")}
    if {path.name for path in output.iterdir()} != expected:
        raise RuntimeError("native MGR message artifact set differs")
    fixtures = {}
    for name, (typ, version, compat, tid, data) in SHAPES.items():
        parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                 for part in ("front", "middle", "data", "header")}
        fields = HEADER.unpack(parts["header"]) if len(parts["header"]) == HEADER.size else None
        if (fields is None or fields[1] != tid or fields[2:5] != (typ, 127, version) or fields[9] != compat or
                parts["middle"] or bool(parts["data"]) != data or not parts["front"]):
            raise RuntimeError("native MGR message envelope differs: " + name)
        decoded = json.loads((output / (name + ".decoded.json")).read_bytes())
        fixtures[name] = dict(type=typ, version=version, compat=compat, tid=tid, decoded=decoded)
        for part, raw in parts.items():
            fixtures[name][part + "_hex"] = raw.hex()
            fixtures[name][part + "_sha256"] = hashlib.sha256(raw).hexdigest()
    for name, typ in REFUSED.items():
        decoded = json.loads((output / (name + ".decoded.json")).read_bytes())
        if decoded != {"native_refused": True}:
            raise RuntimeError("native refusal receipt differs: " + name)
        raw = (output / (name + ".front.bin")).read_bytes()
        fixtures[name] = dict(type=typ, decoded=decoded, front_hex=raw.hex(), front_sha256=hashlib.sha256(raw).hexdigest())
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
    target = "mgr_messages_native_encoder"
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
    command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "mgr-messages-cmake", env)
    command(["cmake", "--build", str(builddir), "--target", target, "-j2"], root, "mgr-messages-compile", env)
    binary = builddir / "bin" / target
    loader = command(["ldd", str(binary)], root, "mgr-messages-loader", env)
    if "libceph-common.so.2 => " + str(builddir / "lib") + "/" not in loader:
        raise RuntimeError("MGR messages wrapper loaded a foreign native library")
    output = root / "encoded-mgr-messages"
    if output.exists():
        raise RuntimeError("use a fresh output directory; stale vectors must not be reused")
    command([str(binary), str(output)], root, "mgr-messages-encode-decode", env)
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
                                        "src/messages/MMgrReport.h", "src/messages/MMgrClose.h",
                                        "src/messages/MMgrConfigure.h", "src/messages/MMgrCommand.h",
                                        "src/messages/MMgrCommandReply.h", "src/messages/MCommand.h",
                                        "src/messages/MCommandReply.h", "src/messages/MPGStats.h",
                                        "src/messages/PaxosServiceMessage.h", "src/mgr/MetricTypes.h",
                                        "src/mgr/OSDPerfMetricTypes.h", "src/mgr/DaemonHealthMetric.h",
                                        "src/osd/osd_types.h", "src/osd/osd_types.cc", "src/common/histogram.cc",
                                        "src/msg/Message.cc", "src/msg/async/frames_v2.h")},
                  canonical_message_encode_verified=True, native_payload_decoder_verified=True,
                  matching_native_loader_verified=True, fixtures=collect(output))
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
