#!/usr/bin/env python3
"""Closed native IgnoreCache MOSDOp oracle, using the unchanged MPing SDK builder.

Development only: official pinned LGPL-2.1 Ceph codecs, generated CMake config,
and matching ceph-common. Product builds/runtime do not import this script.
The 24 vectors are fresh native objects, never decoded messages re-encoded.
"""
import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import platform
import re
import struct
import sys
import tarfile

import mon_ping_native_encoder as sdk

TARGET = "osd_ignore_cache_native_encoder"
KINDS = ("stat", "read", "writefull", "delete")
FEATURES = {6: 3026423347916342070 | (1 << 35),
            8: 3386711319181820726 | (1 << 35),
            9: 3387274269135242038 | (1 << 35)}
HEADER = struct.Struct("<QQHHHIHQBHH")
SOURCE_FILES = ("src/messages/MOSDOp.h", "src/osdc/Objecter.h", "src/include/rados.h",
                "src/include/ceph_features.h", "src/msg/Message.h", "src/msg/Message.cc",
                "src/msg/async/frames_v2.h", "src/common/tracer.h")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_binding(cmake, target, wrapper):
    """A cached generated target must bind this exact mounted source, or fail."""
    if not cmake.is_file():
        return False
    text = cmake.read_text()
    lines = re.findall(r"(?m)^\s*add_executable\(" + re.escape(target) + r"\s+([^\n]+?)\)\s*$", text)
    if len(lines) == 1 and lines[0] == str(wrapper):
        block = ("if(EXISTS " + str(wrapper) + ")\nadd_executable(" + target + " " + str(wrapper) + ")\n"
                 "target_link_libraries(" + target + " PRIVATE ceph-common ${ceph_common_deps})\n"
                 "target_compile_features(" + target + " PRIVATE cxx_std_20)\nendif()")
        if block not in text:
            raise RuntimeError("cached wrapper target is not the owned SDK block: " + target)
        return True
    if lines or re.search(r"add_executable\(\s*" + re.escape(target) + r"(?:\s|\))", text):
        raise RuntimeError("cached CMake wrapper binding differs; preserve its original absolute mount: " + target)
    return False


def original_sources(root, checkout):
    result = {}
    with tarfile.open(root / "source.tar.gz", "r:gz") as archive:
        for relative in SOURCE_FILES:
            archived = archive.extractfile("ceph-" + sdk.COMMIT + "/" + relative)
            if archived is None or archived.read() != (checkout / relative).read_bytes():
                raise RuntimeError("native producer source differs from official archive: " + relative)
            result[relative] = digest(checkout / relative)
    return result


class CaptureProof:
    """Forward SDK build progress and retain its final structured provenance."""
    def __init__(self, stream):
        self.stream, self.proof = stream, None

    def write(self, value):
        if value.startswith('{"ceph_commit":'):
            self.proof = json.loads(value)
        return self.stream.write(value)

    def flush(self):
        self.stream.flush()


def collect(output):
    names = {f"ignore-cache-native-v{v}-{kind}-{mode}"
             for v in FEATURES for kind in KINDS for mode in ("normal", "ignore")}
    if {p.name[:-10] for p in output.glob("*.front.bin")} != names:
        raise RuntimeError("native vector set is not exactly 3 versions x 4 operations x 2 flags")
    fixtures = {}
    for version, features in FEATURES.items():
        for kind in KINDS:
            for ignore in (False, True):
                name = f"ignore-cache-native-v{version}-{kind}-{'ignore' if ignore else 'normal'}"
                parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                         for part in ("header", "front", "middle", "data")}
                h, front, data = parts["header"], parts["front"], parts["data"]
                if len(h) != HEADER.size or HEADER.unpack(h) != (1, 77, 42, 127, version, 0, 0, 3, 1, 3, 0) or parts["middle"]:
                    raise RuntimeError("native envelope differs: " + name)
                decoded = json.loads((output / (name + ".decoded.json")).read_text())
                mutation = kind in ("writefull", "delete")
                flags = 4 | 0x4000000 | (32 if mutation else 16) | (0x8000 if ignore else 0)
                expected = dict(features=features, flags=flags, pool=7,
                                seed=0x11223344 if version == 6 else 0x44, shard=-1,
                                raw_hash=0x11223344, client_inc=1, reqid_inc=1, reqid_tid=77,
                                epoch=11, locator_hash=-1, object="cache-codec", key="route-key",
                                namespace="tenant", snap=(1 << 64)-2, snap_seq=0, snaps=[], retry_attempt=0,
                                mtime_seconds=1700000000 if mutation else 0, mtime_nanoseconds=5 if mutation else 0,
                                otel_valid=False, finish_decode_performed=version >= 8, second_finish_decode_needed=False)
                for field, value in expected.items():
                    if decoded.get(field) != value or type(decoded.get(field)) is not type(value):
                        raise RuntimeError("native decoder identity/flags differ: " + name + ":" + field)
                offset = 8 if version == 6 else 32
                if int.from_bytes(front[offset:offset+4], "little") != flags:
                    raise RuntimeError("native flags disagree with decoder")
                if version >= 8 and (front[63:87] != bytes(24) or int.from_bytes(front[-8:], "little") != features):
                    raise RuntimeError("native trace/features differ")
                if version == 9 and front[87:94] != bytes.fromhex("01010100000000"):
                    raise RuntimeError("native v9 trace is not the invalid empty envelope")
                op = decoded.get("operation", {})
                code = {"stat": 0x1202, "read": 0x1201, "writefull": 0x2202, "delete": 0x2205}[kind]
                payload = b"\x00\xffA\n" if kind == "writefull" else b""
                extent = ((2, 4) if kind == "read" else (0, 4) if kind == "writefull" else (0, 0))
                union = struct.pack("<QQ", *extent) + bytes(12)
                if op != dict(code=code, flags=0, offset=extent[0], length=extent[1], payload_len=len(payload),
                              union_hex=union.hex(), indata_hex=payload.hex()) or data != payload:
                    raise RuntimeError("native packed standalone operation differs: " + name)
                fixture = dict(version=version, kind=kind, ignore_cache=ignore, decoded=decoded)
                for part, raw in parts.items():
                    fixture[part + "_hex"] = raw.hex()
                    fixture[part + "_sha256"] = hashlib.sha256(raw).hexdigest()
                fixtures[name] = fixture
            normal = bytes.fromhex(fixtures[f"ignore-cache-native-v{version}-{kind}-normal"]["front_hex"])
            ignored = bytearray(bytes.fromhex(fixtures[f"ignore-cache-native-v{version}-{kind}-ignore"]["front_hex"]))
            ignored[offset:offset+4] = normal[offset:offset+4]
            if normal != ignored:
                raise RuntimeError("normal/IgnoreCache differ beyond the request flag")
    return fixtures


def build(root, wrapper):
    if platform.machine() != "x86_64":
        raise RuntimeError("this retained oracle requires the owned Linux amd64 SDK cache")
    shared = pathlib.Path(sdk.__file__).resolve()
    mping = shared.with_suffix(".cc")
    if wrapper.parent != shared.parent or not wrapper.is_file() or not mping.is_file():
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
    sdk.command(["cmake", "-S", str(checkout), "-B", str(builddir)], root, "ignore-cache-cmake", environment)
    config, cache = builddir / "include/acconfig.h", builddir / "CMakeCache.txt"
    if b"/* #undef HAVE_JAEGER */" not in config.read_bytes() or b"WITH_JAEGER:BOOL=OFF\n" not in cache.read_bytes():
        raise RuntimeError("official native SDK does not prove WITH_JAEGER=OFF")
    os.utime(wrapper, None)
    sdk.command(["cmake", "--build", str(builddir), "--target", TARGET, "-j4"], root, "ignore-cache-compilation", environment)
    units = [item for item in json.loads((builddir / "compile_commands.json").read_text()) if item["file"] == str(wrapper)]
    if len(units) != 1:
        raise RuntimeError("native compile database does not bind the exact wrapper")
    binary, library = builddir / ("bin/" + TARGET), builddir / "lib/libceph-common.so.2"
    for path in (binary, library):
        h = path.read_bytes()[:20]
        if h[:6] != b"\x7fELF\x02\x01" or int.from_bytes(h[18:20], "little") != 62:
            raise RuntimeError("native ELF is not amd64: " + str(path))
    loader = sdk.command(["ldd", str(binary)], root, "ignore-cache-loader", environment)
    common = next((line for line in loader.splitlines() if "libceph-common.so" in line), "")
    if "=> " + str(builddir / "lib") + "/" not in common:
        raise RuntimeError("native encoder loads an unrelated ceph-common")
    output = root / "encoded-ignore-cache"
    if output.exists():
        raise RuntimeError("preserve earlier native output; use a new output path/cache copy")
    sdk.command([str(binary), str(output)], root, "ignore-cache-native-encoding", environment)
    if original_sources(root, checkout) != sources:
        raise RuntimeError("native source changed during build")
    result = dict(base)
    result.update(wrapper_sha256=digest(wrapper), build_driver_sha256=digest(pathlib.Path(__file__)),
                  shared_build_driver_sha256=digest(shared), shared_wrapper_sha256=digest(mping),
                  binary_sha256=digest(binary), native_library_sha256=digest(library),
                  compiler_binary_sha256=digest(pathlib.Path("/opt/rh/gcc-toolset-14/root/usr/bin/g++")),
                  generated_config_sha256=digest(config), cmake_cache_sha256=digest(cache),
                  source_files_sha256=sources, loader_audit=loader, fixtures=collect(output),
                  sdk_tracing=dict(cmake_option="WITH_JAEGER=OFF", have_jaeger=False),
                  producer_contract="24 fresh canonical MOSDOp objects: v6/v8/v9, standalone STAT/READ/WRITEFULL/DELETE, head replicated NO_SHARD, identical bit35 features with only IgnoreCache request flag changed",
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
