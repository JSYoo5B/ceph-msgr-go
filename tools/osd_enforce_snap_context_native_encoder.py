#!/usr/bin/env python3
"""Closed native ENFORCE_SNAPC oracle using unchanged shared SDK helpers.

Development only: pinned LGPL-2.1 Ceph encoders/decoders and official generated
configuration. No native product dependency; no fake or decoded producer input.
Existing MPing and IgnoreCache producers, drivers and proofs remain unchanged.
"""
import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import platform
import struct
import sys

import mon_ping_native_encoder as sdk
import osd_ignore_cache_native_encoder as helpers
from osd_ignore_cache_native_encoder import CaptureProof, check_binding, digest, original_sources

TARGET = "osd_enforce_snap_context_native_encoder"
FEATURES = {6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038}
HEADER = struct.Struct("<QQHHHIHQBHH")
CONTEXTS = {"empty": (0, []), "nonempty": (7, [7, 5])}


def collect(output):
    names = {f"enforce-snap-context-native-v{v}-{context}-{mode}"
             for v in FEATURES for context in CONTEXTS for mode in ("normal", "enforce")}
    if {p.name[:-10] for p in output.glob("*.front.bin")} != names:
        raise RuntimeError("native vector set is not exactly 3 versions x 2 contexts x 2 flags")
    fixtures = {}
    for version, features in FEATURES.items():
        if features & (1 << 35):
            raise RuntimeError("unrelated cache-pool capability must be absent")
        for context, (seq, snaps) in CONTEXTS.items():
            for enforce in (False, True):
                name = f"enforce-snap-context-native-v{version}-{context}-{'enforce' if enforce else 'normal'}"
                parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                         for part in ("header", "front", "middle", "data")}
                h, front, data = parts["header"], parts["front"], parts["data"]
                if len(h) != HEADER.size or HEADER.unpack(h) != (1, 77, 42, 127, version, 0, 0, 3, 1, 3, 0) or parts["middle"]:
                    raise RuntimeError("native envelope differs: " + name)
                decoded = json.loads((output / (name + ".decoded.json")).read_text())
                flags = 4 | 0x4000000 | 32 | 0x40 | (0x100000 if enforce else 0)
                expected = dict(features=features, flags=flags, pool=7,
                                seed=0x11223344 if version == 6 else 0x44, shard=-1,
                                raw_hash=0x11223344, client_inc=1, reqid_inc=1, reqid_tid=77,
                                epoch=11, locator_hash=-1, object="enforce-snapc-codec", key="route-key",
                                namespace="tenant", snap=(1 << 64)-2, snap_seq=seq, snaps=snaps, retry_attempt=0,
                                mtime_seconds=1700000000, mtime_nanoseconds=5,
                                otel_valid=False, finish_decode_performed=version >= 8, second_finish_decode_needed=False)
                for field, value in expected.items():
                    if decoded.get(field) != value or type(decoded.get(field)) is not type(value):
                        raise RuntimeError("native decoder identity/snapshot/flags differ: " + name + ":" + field)
                if any(type(v) is not int for v in decoded["snaps"]):
                    raise RuntimeError("native snapshot IDs must be exact integers")
                offset = 8 if version == 6 else 32
                if int.from_bytes(front[offset:offset+4], "little") != flags:
                    raise RuntimeError("native flags disagree with decoder")
                if version >= 8 and (front[63:87] != bytes(24) or int.from_bytes(front[-8:], "little") != features):
                    raise RuntimeError("native trace/features differ")
                if version == 9 and front[87:94] != bytes.fromhex("01010100000000"):
                    raise RuntimeError("native v9 trace is not the invalid empty envelope")
                payload = b"\x00\xffA\n"
                union = struct.pack("<QQ", 0, 4) + bytes(12)
                if decoded.get("operation") != dict(code=0x2202, flags=0, offset=0, length=4, payload_len=4,
                                                   union_hex=union.hex(), indata_hex=payload.hex()) or data != payload:
                    raise RuntimeError("native standalone WRITEFULL differs: " + name)
                fixture = dict(version=version, context=context, enforce_snap_context=enforce, decoded=decoded)
                for part, raw in parts.items():
                    fixture[part + "_hex"] = raw.hex()
                    fixture[part + "_sha256"] = hashlib.sha256(raw).hexdigest()
                fixtures[name] = fixture
            normal = bytes.fromhex(fixtures[f"enforce-snap-context-native-v{version}-{context}-normal"]["front_hex"])
            enforced = bytearray(bytes.fromhex(fixtures[f"enforce-snap-context-native-v{version}-{context}-enforce"]["front_hex"]))
            enforced[offset:offset+4] = normal[offset:offset+4]
            if normal != enforced:
                raise RuntimeError("normal/ENFORCE_SNAPC differ beyond flag0x100000")
    return fixtures


def build(root, wrapper):
    if platform.machine() != "x86_64":
        raise RuntimeError("this retained oracle requires the owned Linux amd64 SDK cache")
    shared = pathlib.Path(sdk.__file__).resolve()
    mping = shared.with_suffix(".cc")
    if (wrapper.parent != shared.parent or pathlib.Path(helpers.__file__).resolve().parent != shared.parent
            or not wrapper.is_file() or not mping.is_file()):
        raise RuntimeError("new and shared native sources must be explicit siblings")
    checkout = root / ("ceph-" + sdk.COMMIT)
    cmake = checkout / "src/CMakeLists.txt"
    check_binding(cmake, "mon_ping_native_encoder", mping)
    check_binding(cmake, TARGET, wrapper)
    capture = CaptureProof(sys.stdout)
    with contextlib.redirect_stdout(capture):
        sdk.build(root, mping)
    base = capture.proof
    if not base or base["build_architecture"] != "x86_64":
        raise RuntimeError("shared SDK build did not provide fresh architecture/source proof")
    sources = original_sources(root, checkout)
    if not check_binding(cmake, TARGET, wrapper):
        with cmake.open("a") as out:
            out.write("\nif(EXISTS " + str(wrapper) + ")\nadd_executable(" + TARGET + " " + str(wrapper) + ")\n"
                      "target_link_libraries(" + TARGET + " PRIVATE ceph-common ${ceph_common_deps})\n"
                      "target_compile_features(" + TARGET + " PRIVATE cxx_std_20)\nendif()\n")
    builddir = root / "build"
    environment = dict(os.environ)
    environment["PATH"] = "/opt/rh/gcc-toolset-14/root/usr/bin:" + environment["PATH"]
    environment["LD_LIBRARY_PATH"] = str(builddir / "lib") + ":/opt/rh/gcc-toolset-14/root/usr/lib64"
    sdk.command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "enforce-snap-context-cmake", environment)
    config, cache = builddir / "include/acconfig.h", builddir / "CMakeCache.txt"
    if b"/* #undef HAVE_JAEGER */" not in config.read_bytes() or b"WITH_JAEGER:BOOL=OFF\n" not in cache.read_bytes():
        raise RuntimeError("official native SDK does not prove WITH_JAEGER=OFF")
    os.utime(wrapper, None)
    sdk.command(["cmake", "--build", str(builddir), "--target", TARGET, "-j4"], root, "enforce-snap-context-compilation", environment)
    units = [item for item in json.loads((builddir / "compile_commands.json").read_text()) if item["file"] == str(wrapper)]
    if len(units) != 1:
        raise RuntimeError("native compile database does not bind the exact wrapper")
    binary, library = builddir / ("bin/" + TARGET), builddir / "lib/libceph-common.so.2"
    for path in (binary, library):
        h = path.read_bytes()[:20]
        if h[:6] != b"\x7fELF\x02\x01" or int.from_bytes(h[18:20], "little") != 62:
            raise RuntimeError("native ELF is not amd64: " + str(path))
    loader = sdk.command(["ldd", str(binary)], root, "enforce-snap-context-loader", environment)
    common = next((line for line in loader.splitlines() if "libceph-common.so" in line), "")
    if "=> " + str(builddir / "lib") + "/" not in common:
        raise RuntimeError("native encoder loads an unrelated ceph-common")
    output = root / "encoded-enforce-snap-context"
    if output.exists():
        raise RuntimeError("preserve earlier native output; use a new output path/cache copy")
    sdk.command([str(binary), str(output)], root, "enforce-snap-context-native-encoding", environment)
    if original_sources(root, checkout) != sources:
        raise RuntimeError("native source changed during build")
    result = dict(base)
    result.update(wrapper_sha256=digest(wrapper), build_driver_sha256=digest(pathlib.Path(__file__)),
                  shared_build_driver_sha256=digest(shared), shared_wrapper_sha256=digest(mping),
                  helper_build_driver_sha256=digest(pathlib.Path(helpers.__file__)),
                  binary_sha256=digest(binary), native_library_sha256=digest(library),
                  compiler_binary_sha256=digest(pathlib.Path("/opt/rh/gcc-toolset-14/root/usr/bin/g++")),
                  generated_config_sha256=digest(config), cmake_cache_sha256=digest(cache),
                  source_files_sha256=sources, loader_audit=loader, fixtures=collect(output),
                  sdk_tracing=dict(cmake_option="WITH_JAEGER=OFF", have_jaeger=False),
                  producer_contract="12 fresh canonical MOSDOp objects: v6/v8/v9 WRITEFULL, head replicated NO_SHARD, ORDERSNAP and empty/nonempty SnapContext, identical default features with only ENFORCE_SNAPC changed",
                  status="native canonical encoder and separate fresh decoder verified; offline envelope projection, not live ProtocolV2 execution")
    result.pop("launcher_sha256", None)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
