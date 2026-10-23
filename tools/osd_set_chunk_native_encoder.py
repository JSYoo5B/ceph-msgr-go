#!/usr/bin/env python3
"""Closed SET_CHUNK request oracle using the unchanged native SDK helper.

Development only: pinned LGPL-2.1 Ceph codecs and official generated config.
This produces request wire evidence for SET_CHUNK (Objecter set_chunk), not
manifest state, chunk references, target existence or server execution.
Existing producers, drivers and proofs are unchanged.
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

TARGET = "osd_set_chunk_native_encoder"
# Default implemented features: a plain replicated pool needs no cache-pool
# capability for SET_CHUNK, so bit 35 is absent.
FEATURES = {6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038}
HEADER = struct.Struct("<QQHHHIHQBHH")
SHAPES = ("chunk", "chunk-located", "nonzero-source", "without-reference")
ONDISK, READ, RETURNVEC = 0x4, 0x10, 0x4000000
SET_CHUNK, WITH_REFERENCE = 0x8228, 0x80
NOSNAP = (1 << 64) - 2
# Independent model of each closed shape: src_offset, src_length, target pool,
# key, namespace, object, tgt_offset and the packed per-operation flag. The
# last two shapes are requests the Go client refuses.
MODEL = {
    "chunk": (0, 8, 9, "", "", "set-chunk-target", 0, WITH_REFERENCE),
    "chunk-located": (0, 4096, 7, "chunk-key", "tenant", "set-chunk-target-2", 65536, WITH_REFERENCE),
    "nonzero-source": (16, 8, 9, "", "", "set-chunk-target", 8, WITH_REFERENCE),
    "without-reference": (0, 8, 9, "", "", "set-chunk-target", 0, 0),
}
# rados set-chunk passes no request flag; Objecter::get_read_flags adds READ.
FLAGS = ONDISK | RETURNVEC | READ


def string(text):
    raw = text.encode()
    return struct.pack("<I", len(raw)) + raw


def locator(pool, key, namespace):
    """object_locator_t ENCODE_START(6, 3): pool, preferred -1, key, nspace, hash -1."""
    body = struct.pack("<qi", pool, -1) + string(key) + string(namespace) + struct.pack("<q", -1)
    return struct.pack("<BBI", 6, 3, len(body)) + body


def indata(shape):
    src_offset, src_length, pool, key, namespace, oid, tgt_offset, _ = MODEL[shape]
    return (struct.pack("<QQ", src_offset, src_length) + locator(pool, key, namespace)
            + string(oid) + struct.pack("<Q", tgt_offset))


def expected_operations(shape):
    data = indata(shape)
    return [dict(code=SET_CHUNK, flags=MODEL[shape][7], offset=0, length=0, payload_len=len(data),
                 union_hex=bytes(28).hex(), indata_hex=data.hex(), outdata_hex="", result=0)]


def expected_set_chunk(shape):
    src_offset, src_length, pool, key, namespace, oid, tgt_offset, _ = MODEL[shape]
    return dict(src_offset=src_offset, src_length=src_length, tgt_pool=pool, tgt_key=key,
                tgt_namespace=namespace, tgt_hash=-1, tgt_oid=oid, tgt_offset=tgt_offset)


def exact(got, want):
    # bool is an int subclass: preserve JSON type as part of the proof contract.
    if type(got) is not type(want):
        return False
    if isinstance(want, dict):
        return got.keys() == want.keys() and all(exact(got[k], v) for k, v in want.items())
    if isinstance(want, list):
        return len(got) == len(want) and all(exact(a, b) for a, b in zip(got, want))
    return got == want


def collect(output):
    names = {f"set-chunk-native-v{v}-{shape}" for v in FEATURES for shape in SHAPES}
    expected_files = {name + suffix for name in names
                      for suffix in (".header.bin", ".front.bin", ".middle.bin", ".data.bin", ".decoded.json")}
    if {p.name for p in output.iterdir()} != expected_files:
        raise RuntimeError("native output must be exactly 12 requests x 5 files; no replies")
    fixtures = {}
    for version, features in FEATURES.items():
        if features & (1 << 35):
            raise RuntimeError("plain-pool SET_CHUNK vectors carry no cache-pool capability")
        for shape in SHAPES:
            name = f"set-chunk-native-v{version}-{shape}"
            for suffix in (".header.bin", ".front.bin", ".middle.bin", ".data.bin", ".decoded.json"):
                path = output / (name + suffix)
                if not path.is_file() or path.stat().st_size > 16384:
                    raise RuntimeError("native evidence segment is missing or unbounded: " + name)
            parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                     for part in ("header", "front", "middle", "data")}
            h, front, data = parts["header"], parts["front"], parts["data"]
            operations = expected_operations(shape)
            input_bytes = bytes.fromhex(operations[0]["indata_hex"])
            if (len(h) != HEADER.size or HEADER.unpack(h) != (1, 77, 42, 127, version, 0, 0, 3, 1, 3, 0)
                    or parts["middle"]):
                raise RuntimeError("native canonical request envelope differs: " + name)
            decoded = json.loads((output / (name + ".decoded.json")).read_text())
            expected = dict(features=features, flags=FLAGS, pool=7,
                            seed=0x11223344 if version == 6 else 0x44, shard=-1,
                            raw_hash=0x11223344, client_inc=1, reqid_inc=1, reqid_tid=77,
                            epoch=11, locator_hash=-1, object="set-chunk-codec", key="route-key",
                            namespace="tenant", snap=NOSNAP, snap_seq=0, snaps=[], retry_attempt=0,
                            mtime_seconds=0, mtime_nanoseconds=0, otel_valid=False,
                            finish_decode_performed=version >= 8, second_finish_decode_needed=False,
                            operations=operations, set_chunk=expected_set_chunk(shape))
            if not exact(decoded, expected):
                raise RuntimeError("fresh native decoded SET_CHUNK identity/operation/input differs: " + name)
            offset = 8 if version == 6 else 32
            if int.from_bytes(front[offset:offset+4], "little") != FLAGS:
                raise RuntimeError("native front flags disagree with decoder")
            if version >= 8 and (front[63:87] != bytes(24) or int.from_bytes(front[-8:], "little") != features):
                raise RuntimeError("native trace/features differ")
            if version == 9 and front[87:94] != bytes.fromhex("01010100000000"):
                raise RuntimeError("native v9 trace must have the invalid empty envelope")
            packed_ops = len(operations).to_bytes(2, "little") + b"".join(
                struct.pack("<HI", op["code"], op["flags"]) + bytes.fromhex(op["union_hex"]) +
                struct.pack("<I", op["payload_len"]) for op in operations)
            if front.count(packed_ops) != 1:
                raise RuntimeError("native wire op count/order/packed union differs: " + name)
            if data != input_bytes:
                raise RuntimeError("native SET_CHUNK data segment differs from the modeled indata: " + name)
            fixture = dict(version=version, shape=shape, decoded=decoded)
            for part, raw in parts.items():
                fixture[part + "_hex"] = raw.hex()
                fixture[part + "_sha256"] = hashlib.sha256(raw).hexdigest()
            fixtures[name] = fixture
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
    sdk.command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "set-chunk-cmake", environment)
    config, cache = builddir / "include/acconfig.h", builddir / "CMakeCache.txt"
    if b"/* #undef HAVE_JAEGER */" not in config.read_bytes() or b"WITH_JAEGER:BOOL=OFF\n" not in cache.read_bytes():
        raise RuntimeError("official native SDK does not prove WITH_JAEGER=OFF")
    os.utime(wrapper, None)
    sdk.command(["cmake", "--build", str(builddir), "--target", TARGET, "-j4"], root, "set-chunk-compilation", environment)
    units = [item for item in json.loads((builddir / "compile_commands.json").read_text()) if item["file"] == str(wrapper)]
    if len(units) != 1:
        raise RuntimeError("native compile database does not bind the exact wrapper")
    binary, library = builddir / ("bin/" + TARGET), builddir / "lib/libceph-common.so.2"
    for path in (binary, library):
        h = path.read_bytes()[:20]
        if h[:6] != b"\x7fELF\x02\x01" or int.from_bytes(h[18:20], "little") != 62:
            raise RuntimeError("native ELF is not amd64: " + str(path))
    loader = sdk.command(["ldd", str(binary)], root, "set-chunk-loader", environment)
    common = next((line for line in loader.splitlines() if "libceph-common.so" in line), "")
    if "=> " + str(builddir / "lib") + "/" not in common:
        raise RuntimeError("native encoder loads an unrelated ceph-common")
    output = root / "encoded-set-chunk"
    if output.exists():
        raise RuntimeError("preserve earlier native output; use a new output path/cache copy")
    sdk.command([str(binary), str(output)], root, "set-chunk-native-encoding", environment)
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
                  producer_contract="12 fresh canonical MOSDOp requests: v6/v8/v9 x 4 closed single SET_CHUNK operations (offset-zero chunk, located target, non-zero source offset, without WITH_REFERENCE) built by Objecter set_chunk with the rados set-chunk request flags, head replicated NO_SHARD, default features, no constructed replies or server execution claim",
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
