#!/usr/bin/env python3
"""Development-only MGR acceptor vectors; no product dependency.

Reuse the pinned MPing builder's official Ceph CMake bootstrap unchanged,
then compile a separate wrapper (mgr_acceptor_native_encoder.cc) against that
matching native library. The wrapper runs the native service side of the
CephX authorizer exchange (cephx_verify_authorizer) against a native
authorizer, records native refusals, plaintext and aes encodings of the
service-side structures, and assembles the acceptor's msgr2.1 control frames
natively. Run it in an owned disposable container of the pinned image, with
a writable tools mount (the bootstrap touches its wrapper):

    docker run --rm -v <cache>:/work -v <checkout>/tools:/tools <image> \\
        python3 /tools/mgr_acceptor_native_encoder.py --work /work \\
        --wrapper /tools/mgr_acceptor_native_encoder.cc > proof.json

The last stdout line is the proof; internal/cephx/testdata keeps it as
mgr-acceptor-native-provenance.json. The bootstrap MPing vectors are not
counted as MGR acceptor evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from mon_ping_native_encoder import COMMIT, IMAGE, command

EXCHANGES = ("exchange-aes256k", "exchange-aes")
REFUSALS = ("service_osd", "unknown_secret", "all_ones_secret", "global_id", "ticket_byte", "authorize_byte")
PLAINTEXTS = ("challenge_plain", "challenge_aes", "reply_v1_plain", "reply_v1_aes", "reply_v2_plain",
              "reply_v2_aes", "authorize_plain", "ticket_info_caps_plain", "ticket_info_allow_all_plain")
FRAMES = ("hello", "auth_reply_more", "auth_done", "auth_bad_method", "ident_missing_features",
          "server_ident", "session_reset", "compression_done")


def collect(output):
    expected = {name + ".json" for name in EXCHANGES} | {"plaintexts.json", "frames.json"}
    if {path.name for path in output.iterdir()} != expected:
        raise RuntimeError("native MGR acceptor artifact set differs")
    fixtures = {}
    for name in EXCHANGES:
        data = json.loads((output / (name + ".json")).read_bytes())
        if sorted(data["refusals"]) != sorted(REFUSALS):
            raise RuntimeError("native refusal set differs: " + name)
        for case, result in data["refusals"].items():
            if result["accepted"] or result["challenged"]:
                raise RuntimeError("native verifier admitted an altered authorizer: " + name + " " + case)
        for mode, length in (("secure", 64), ("crc", 0)):
            if data[mode]["replay_accepted"] or len(bytes.fromhex(data[mode]["connection_secret"])) != length:
                raise RuntimeError("native exchange differs: " + name + " " + mode)
        fixtures[name] = data
    for name, keys in (("plaintexts", PLAINTEXTS), ("frames", FRAMES)):
        data = json.loads((output / (name + ".json")).read_bytes())
        if sorted(data) != sorted(keys):
            raise RuntimeError("native " + name + " set differs")
        fixtures[name] = data
    return fixtures


def build(root, wrapper):
    if not Path("/.dockerenv").is_file():
        raise RuntimeError("native build requires an owned disposable Docker container")
    root.mkdir(parents=True, exist_ok=True)
    tools = Path(__file__).resolve().parent
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
    target = "mgr_acceptor_native_encoder"
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
    command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "mgr-acceptor-cmake", env)
    command(["cmake", "--build", str(builddir), "--target", target, "-j4"], root, "mgr-acceptor-compile", env)
    binary = builddir / "bin" / target
    loader = command(["ldd", str(binary)], root, "mgr-acceptor-loader", env)
    if "libceph-common.so.2 => " + str(builddir / "lib") + "/" not in loader:
        raise RuntimeError("MGR acceptor wrapper loaded a foreign native library")
    output = root / "encoded-mgr-acceptor"
    if output.exists():
        raise RuntimeError("use a fresh output directory; stale vectors must not be reused")
    command([str(binary), str(output)], root, "mgr-acceptor-encode-verify", env)
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
                                       ("src/auth/cephx/CephxProtocol.h", "src/auth/cephx/CephxProtocol.cc",
                                        "src/auth/Auth.h", "src/auth/Auth.cc", "src/common/entity_name.cc",
                                        "src/msg/async/frames_v2.h", "src/msg/async/frames_v2.cc")},
                  native_verifier_checked=True, matching_native_loader_verified=True,
                  fixtures=collect(output))
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
