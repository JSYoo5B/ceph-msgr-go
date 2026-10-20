#!/usr/bin/env python3
"""Development-only native MOSDOp v9 (empty OTEL only) encoder/decoder builder for an owned disposable container.

The native C++ wrapper includes unchanged LGPL-2.1 Ceph source headers from the
pinned commit; it does not copy the implementation. Generated Ceph configuration
must come from the official CMake build rather than invented ABI definitions.
This script performs no socket capture, interception, or product-module edits.
"""
import argparse
import configparser
import hashlib
import json
import os
import pathlib
import re
import struct
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"


FEATURES = 3387274269135242038
KINDS = ("read", "mutation", "snapshot", "checksum", "pgnls")
EMPTY_OTEL = bytes.fromhex("01010100000000")
HEADER = struct.Struct("<QQHHHIHQBHH")


def off_sdk_proof(config, cache):
    """Inspect official CMake outputs; never define a replacement tracing ABI."""
    raw = config.read_bytes()
    configured = cache.read_bytes()
    if re.search(rb"^\s*#\s*define\s+HAVE_JAEGER(?:\s|$)", raw, re.MULTILINE):
        raise RuntimeError("native v9 oracle requires HAVE_JAEGER undefined")
    if b"/* #undef HAVE_JAEGER */" not in raw or b"WITH_JAEGER:BOOL=OFF\n" not in configured:
        raise RuntimeError("official SDK does not prove WITH_JAEGER=OFF")
    return dict(cmake_option="WITH_JAEGER=OFF", have_jaeger=False,
                generated_config_sha256=hashlib.sha256(raw).hexdigest(),
                cmake_cache_sha256=hashlib.sha256(configured).hexdigest(),
                scope="official OFF SDK; native invalid/empty OTEL only")


def verify_native_fixture(kind, front, data, header, metadata, routing):
    """Cross-check exported native receipts without generating protocol vectors."""
    if kind not in KINDS or len(header)!=HEADER.size:
        raise RuntimeError("outside closed v9 vector set or invalid native header")
    fields=HEADER.unpack(header)
    seq,tid,typ,priority,version,padding,off,ack,flags,compat,reserved=fields
    if (seq,tid,typ,priority,version,padding,off,ack,flags,compat,reserved)!=(1,77,42,127,9,0,0,3,1,3,0):
        raise RuntimeError("native Type42/v9/compat3 frame metadata differs")
    expected=dict(type=typ,version=version,compat=compat,tid=tid,seq=seq,priority=priority,
                  front_len=len(front),middle_len=0,data_len=len(data),data_off=off,
                  flags=flags,ack_seq=ack,reserved=reserved)
    if any(type(value) is not int for value in metadata.values()) or metadata!=expected or len(front)<98 or front[63:87]!=bytes(24) or front[87:94]!=EMPTY_OTEL:
        raise RuntimeError("native envelope lengths or empty legacy/OTEL trace differ")
    numbers=("pool","seed","shard","raw_hash","client_inc","reqid_inc","reqid_tid","features","epoch","locator_hash","flags","snap","snap_seq","mtime_seconds","mtime_nanoseconds")
    if any(type(routing.get(field)) is not int for field in numbers) or any(type(item) is not int for item in routing.get("snaps",[])):
        raise RuntimeError("native routing integers must not be booleans or strings")
    inc=99 if kind=="snapshot" else 1
    hash_value=0x44 if kind=="pgnls" else 0x11223344
    if (routing.get("pool"),routing.get("seed"),routing.get("shard"),routing.get("raw_hash"),
        routing.get("client_inc"),routing.get("reqid_inc"),routing.get("reqid_tid"),routing.get("features"),
        routing.get("epoch"),routing.get("locator_hash"))!=(7,0x44,-1,hash_value,inc,inc,77,FEATURES,11,0x44 if kind=="pgnls" else -1):
        raise RuntimeError("native actual PG/raw hash/incarnation/features/locator differ")
    if routing.get("otel_valid") is not False or routing.get("empty_otel_hex")!=EMPTY_OTEL.hex() or routing.get("legacy_trace_zero") is not True or routing.get("finish_decode_performed") is not True or routing.get("second_finish_decode_needed") is not False:
        raise RuntimeError("native fresh partial/final decode or invalid OTEL proof absent")
    if int.from_bytes(front[94:98],"little")!=inc or int.from_bytes(front[-8:],"little")!=FEATURES:
        raise RuntimeError("native client incarnation or negotiated-feature tail differs")
    if routing.get("object_hex")!=("" if kind=="pgnls" else b"v9-codec".hex()) or routing.get("key_hex")!=("" if kind=="pgnls" else b"route-key".hex()) or routing.get("namespace_hex")!=b"tenant".hex():
        raise RuntimeError("native PGNLS empty oid/explicit hash or object locator differs")
    writes=kind in ("mutation","snapshot")
    flags=4|0x4000000|(32 if writes else 16)|(64 if kind=="snapshot" else 0)|(0x400 if kind=="pgnls" else 0)
    if routing["flags"]!=flags or int.from_bytes(front[32:36],"little")!=flags or int.from_bytes(front[24:28],"little")!=hash_value or int.from_bytes(front[28:32],"little")!=11:
        raise RuntimeError("native flag, actual hash or epoch receipt contradicts front")
    if (routing.get("mtime_seconds"),routing.get("mtime_nanoseconds"))!=((1700000000,123456789) if writes else (0,0)):
        raise RuntimeError("native mtime differs")
    if routing.get("snap")!=(1<<64)-2 or routing.get("snap_seq")!=(7 if kind=="snapshot" else 0) or routing.get("snaps")!=([7,5,2] if kind=="snapshot" else []):
        raise RuntimeError("native head or ordered snapshot context differs")


def collect_native_fixtures(output):
    fronts={p.name[:-len(".front.bin")]:p for p in output.glob("*.front.bin")}
    names={"request-v9-native-"+kind for kind in KINDS}
    if set(fronts)!=names:
        raise RuntimeError("native encoder did not emit exactly the closed five v9 vectors")
    fixtures={}
    for kind in KINDS:
        name="request-v9-native-"+kind
        front=fronts[name].read_bytes()
        data=(output/(name+".data.bin")).read_bytes()
        header=(output/(name+".header.bin")).read_bytes()
        metadata=json.loads((output/(name+".native-header.json")).read_text())
        routing=json.loads((output/(name+".decoded-routing.json")).read_text())
        verify_native_fixture(kind,front,data,header,metadata,routing)
        fixtures[name]=dict(front_hex=front.hex(),data_hex=data.hex(),header_hex=header.hex(),
                            front_bytes=len(front),data_bytes=len(data),header_bytes=len(header),
                            native_header=metadata,decoded_routing=routing,
                            decoded_operations=json.loads((output/(name+".decoded-ops.json")).read_text()))
    return fixtures


def command(argv, cwd, label, env=None):
    (cwd / "stage.txt").write_text(label + "\n")
    print("STEP " + label, flush=True)
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=900)
    (cwd / (label + ".log")).write_text(result.stdout)
    print(result.stdout[-12000:], flush=True)
    if result.returncode:
        raise RuntimeError(label + " exit=" + str(result.returncode))
    return result.stdout


def build(root, wrapper, build_target="osd_request_v9_native_encoder", output_name="encoded-request-v9", status=None):
    if not pathlib.Path("/.dockerenv").is_file():
        raise RuntimeError("native development packages may only be installed in an owned disposable Docker container")
    root.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    packages = ["gcc-toolset-14-gcc-c++", "cmake", "ninja-build", "boost-devel",
                "fmt-devel", "openssl-devel", "libuuid-devel", "libaio-devel",
                "libcurl-devel", "zlib-devel", "gperf", "gperftools-devel",
                "gcc-toolset-14-libatomic-devel", "snappy-devel", "lz4-devel",
                "python3-devel", "libcap-ng-devel", "systemd-devel",
                "libblkid-devel", "keyutils-libs-devel", "libcap-devel", "python3-Cython"]
    command(["dnf", "-y", "--disablerepo=*", "--enablerepo=baseos", "--enablerepo=appstream", "--enablerepo=crb", "--enablerepo=epel", "--setopt=install_weak_deps=False", "--setopt=cachedir=" + str(root / "dnf-cache"), "install", *packages], root, "development-packages")
    compiler = "/opt/rh/gcc-toolset-14/root/usr/bin/g++"
    if not pathlib.Path(compiler).is_file():
        raise RuntimeError("native C++20 compiler was not installed")
    environment["PATH"] = "/opt/rh/gcc-toolset-14/root/usr/bin:" + environment["PATH"]
    environment["LD_LIBRARY_PATH"] = "/opt/rh/gcc-toolset-14/root/usr/lib64"
    archive = root / "source.tar.gz"
    checkout = root / ("ceph-" + COMMIT)
    (root / "stage.txt").write_text("pinned-codeload-source\n")
    print("STEP pinned-codeload-source", flush=True)
    if not archive.is_file():
        with urllib.request.urlopen("https://codeload.github.com/ceph/ceph/tar.gz/" + COMMIT, timeout=60) as response, archive.open("wb") as out:
            shutil.copyfileobj(response, out, 8 << 20)
    if not checkout.is_dir():
        with tarfile.open(archive, "r:gz") as source:
            members = []
            for member in source.getmembers():
                name = pathlib.PurePosixPath(member.name)
                if name.is_absolute() or ".." in name.parts:
                    raise RuntimeError("unsafe pinned source archive path")
                if member.isdir() or member.isfile():
                    members.append(member)
            source.extractall(root, members=members)
    # The public SDK forwards several headers with tracked relative symlinks.
    # Restore only those original include links after regular-file extraction,
    # validating both ends stay inside the immutable source checkout.
    with tarfile.open(archive, "r:gz") as source:
        for member in source.getmembers():
            name = pathlib.PurePosixPath(member.name)
            relative = name.relative_to("ceph-" + COMMIT)
            if not member.issym() or relative.parts[:2] != ("src", "include"):
                continue
            destination = checkout / relative
            target = pathlib.PurePosixPath(member.linkname)
            if target.is_absolute() or ".." in relative.parts:
                raise RuntimeError("unsafe native SDK header link")
            resolved = (destination.parent / member.linkname).resolve()
            if checkout.resolve() not in resolved.parents or not resolved.is_file():
                raise RuntimeError("native SDK header link leaves source checkout")
            if not destination.exists():
                destination.symlink_to(member.linkname)
    # codeload identifies the immutable source by its requested commit; this
    # generated metadata serves Ceph's version-header generator, not proof of
    # a downloaded release tag. Archive hash/URL and gitlink hashes are retained.
    (checkout / "src/.git_version").write_text(COMMIT + "\nv20.2.4\n")
    config = configparser.ConfigParser()
    config.read(checkout / ".gitmodules")
    modules = {}
    for section in config.sections():
        path, url = config[section]["path"], config[section]["url"]
        if url.endswith(".git"):
            url = url[:-4]
        if url.startswith("https://github.com/"):
            modules[path] = url.removeprefix("https://github.com/") if hasattr(url, "removeprefix") else url[len("https://github.com/"):]
    treecache = root / "pinned-git-tree.json"
    if not treecache.is_file():
        request = urllib.request.Request("https://api.github.com/repos/ceph/ceph/git/trees/" + COMMIT + "?recursive=1", headers={"User-Agent": "Ceph-offline-encoder-fixture"})
        with urllib.request.urlopen(request, timeout=60) as response:
            treecache.write_bytes(response.read())
    tree = json.loads(treecache.read_bytes())
    if tree.get("truncated"):
        raise RuntimeError("pinned native source git tree is truncated")
    links = {entry["path"]: entry["sha"] for entry in tree["tree"] if entry["mode"] == "160000"}
    source_modules = {}
    # These libraries appear in native common/codec include paths or eagerly
    # configured CMake subdirectories even when daemon targets are not built.
    needed = ("src/fmt", "src/BLAKE3", "src/rapidjson", "src/xxHash", "src/rocksdb", "src/zstd", "src/isa-l",
              "src/isa-l_crypto", "src/erasure-code/jerasure/jerasure",
              "src/erasure-code/jerasure/gf-complete")
    for path in needed:
        if path not in modules or path not in links:
            continue
        repo, sha = modules[path], links[path]
        tar = root / (sha + ".tar.gz")
        if not tar.is_file():
            with urllib.request.urlopen("https://codeload.github.com/" + repo + "/tar.gz/" + sha, timeout=60) as response, tar.open("wb") as out:
                shutil.copyfileobj(response, out, 8 << 20)
        destination = checkout / path
        destination.mkdir(parents=True, exist_ok=True)
        if not any(destination.iterdir()):
            temporary = root / ("submodule-" + sha)
            temporary.mkdir(exist_ok=True)
            with tarfile.open(tar, "r:gz") as source:
                members = []
                for member in source.getmembers():
                    name = pathlib.PurePosixPath(member.name)
                    if name.is_absolute() or ".." in name.parts:
                        raise RuntimeError("unsafe pinned submodule archive path")
                    if member.isdir() or member.isfile():
                        members.append(member)
                source.extractall(temporary, members=members)
            extracted = next(temporary.iterdir())
            shutil.copytree(extracted, destination, dirs_exist_ok=True)
        source_modules[path] = {"repository": repo, "commit": sha,
                                "archive_sha256": hashlib.sha256(tar.read_bytes()).hexdigest()}
    # Add only our independent test wrapper target to the disposable checkout.
    # All native classes/encoders remain the unchanged pinned implementation.
    if "add_executable(" + build_target not in (checkout / "src/CMakeLists.txt").read_text():
        with (checkout / "src/CMakeLists.txt").open("a") as cmake:
            cmake.write("\nif(EXISTS " + str(wrapper) + ")\n")
            cmake.write("add_executable(" + build_target + " " + str(wrapper) + ")\n")
            cmake.write("target_link_libraries(" + build_target + " PRIVATE ceph-common ${ceph_common_deps})\n")
            cmake.write("target_compile_features(" + build_target + " PRIVATE cxx_std_20)\n")
            cmake.write("endif()\n")

    builddir = root / "build"
    options = ["WITH_TESTS", "WITH_RADOSGW", "WITH_RBD", "WITH_MDS", "WITH_KRBD",
               "WITH_FUSE", "WITH_LTTNG", "WITH_MANPAGE", "WITH_LIBCEPHFS",
               "WITH_CEPHFS_JAVA", "WITH_CEPHFS_PYTHON", "WITH_BLUESTORE",
               "WITH_LIBRADOSSTRIPER", "WITH_JAEGER", "WITH_BLKIN", "WITH_OPENLDAP",
               "WITH_RDMA", "WITH_MGR", "WITH_CEPHFS", "WITH_BABELTRACE",
               "WITH_LIBCEPHSQLITE", "WITH_MGR_DASHBOARD_FRONTEND", "WITH_XFS",
               "WITH_UADK", "WITH_QATLIB", "WITH_QATZIP",
               "WITH_NVMEOF_GATEWAY_MONITOR_CLIENT"]
    configure = ["cmake", "-S", str(checkout), "-B", str(builddir), "-G", "Ninja",
                 "-DCMAKE_CXX_COMPILER=" + compiler,
                 "-DCMAKE_C_COMPILER=/opt/rh/gcc-toolset-14/root/usr/bin/gcc",
                 "-DWITH_SYSTEM_BOOST=OFF", "-DWITH_SYSTEM_FMT=OFF", "-DBOOST_J=2",
                 "-DCMAKE_BUILD_TYPE=Debug", "-DCMAKE_CXX_FLAGS_DEBUG=-O0 -g0",
                 "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"]
    configure.extend("-D" + option + "=OFF" for option in options)
    command(configure, root, "native-cmake-configuration", environment)
    config = builddir / "include/acconfig.h"
    if not config.is_file():
        raise RuntimeError("official Ceph CMake did not generate acconfig.h")
    sdk=off_sdk_proof(config,builddir/"CMakeCache.txt")
    # Container copy can assign an older archive timestamp than an existing
    # cached object. Force only this owned wrapper source to be newer so the
    # claimed wrapper digest always corresponds to an actual fresh compile.
    os.utime(wrapper, None)
    command(["cmake", "--build", str(builddir), "--target", build_target, "-j2"], root, "native-encoder-compilation", environment)
    # Keep each invocation's native segments separate, including first failures.
    output = pathlib.Path(tempfile.mkdtemp(prefix=output_name+"-",dir=root))
    environment["LD_LIBRARY_PATH"] = str(builddir / "lib") + ":/opt/rh/gcc-toolset-14/root/usr/lib64"
    binary = builddir / ("bin/" + build_target)
    loader = command(["ldd", str(binary)], root, "native-loader-audit", environment)
    common = next((line for line in loader.splitlines() if "libceph-common.so" in line), "")
    if "=> " + str(builddir / "lib") + "/" not in common:
        raise RuntimeError("native encoder is not loading its matching freshly compiled ceph-common: " + common)
    command([str(binary), str(output)], root, "native-direct-encoding", environment)
    fixtures=collect_native_fixtures(output)
    selection=json.loads((output/"native-feature-selection.json").read_text())
    if selection!=dict(canonical_message_encode=True,features=FEATURES,
                       without_server_squid_features=FEATURES & ~(1<<49),
                       without_server_squid_version=8,with_server_squid_version=9,
                       have_jaeger=False,empty_otel_hex=EMPTY_OTEL.hex()):
        raise RuntimeError("native canonical feature-selection control proof differs")
    compiler_version=command([compiler,"--version"],root,"native-compiler-version",environment)
    commands=json.loads((builddir/"compile_commands.json").read_text())
    wrapper_commands=[entry for entry in commands if pathlib.Path(entry["file"]).resolve()==wrapper.resolve()]
    if len(wrapper_commands)!=1:
        raise RuntimeError("native wrapper lacks a single real CMake compiler command")
    result = {"ceph_commit": COMMIT, "image": IMAGE,
              "source_archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "source_archive_url": "https://codeload.github.com/ceph/ceph/tar.gz/" + COMMIT,
              "pinned_submodules": source_modules,
              "generated_config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
              "wrapper_sha256": hashlib.sha256(wrapper.read_bytes()).hexdigest(),
              "native_library": "ceph-common built from the same pinned source and official generated configuration as the wrapper",
              "native_payload_encoder_verified": True,
              "native_canonical_message_encode_verified": True,
              "feature_selection_control": selection,
              "sdk_tracing": sdk,
              "native_output_directory":str(output),
              "native_header_version": 9,
              "native_compat_version": 3,
              "compiler": compiler, "compiler_version":compiler_version,
              "compiler_binary_sha256":hashlib.sha256(pathlib.Path(compiler).read_bytes()).hexdigest(),
              "wrapper_compile_command":wrapper_commands[0],
              "tracer_header_sha256":hashlib.sha256((checkout/"src/common/tracer.h").read_bytes()).hexdigest(),
              "mosdop_header_sha256":hashlib.sha256((checkout/"src/messages/MOSDOp.h").read_bytes()).hexdigest(),
              "message_source_sha256":hashlib.sha256((checkout/"src/msg/Message.cc").read_bytes()).hexdigest(),
              "native_message_header_verified": True,
              "native_payload_decoder_verified": True,
              "header_proof_scope": "native Message envelope settings and MessageFrame serialization of an explicit offline header2 projection; not ProtocolV2 live session execution",
              "offline_transport_inputs": {"sequence": 1, "ack_sequence": 3, "priority": "CEPH_MSG_PRIO_DEFAULT", "message_crc_flags": "MSG_CRC_ALL"},
              "build_driver_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "native_library_sha256": hashlib.sha256((builddir / "lib/libceph-common.so.2").read_bytes()).hexdigest(),
              "loader_audit": loader,
              "status": status or "five freshly constructed native MOSDOp v9 (empty OTEL only) messages directly encoded and separately decoded; decoded objects never re-encoded",
              "fixtures": fixtures}
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
