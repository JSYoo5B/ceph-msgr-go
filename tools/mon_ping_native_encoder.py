#!/usr/bin/env python3
"""Independent development-only MPing native producer/decoder builder.

Adapted from this repository's MDS map builder without changing its provenance.
Uses unchanged pinned LGPL-2.1 Ceph headers/library and official CMake config.
The source-shaped request is fresh/empty; MON reply has native string payload
prefilled before canonical Message::encode. This distinction proves request
compat1 versus reply compat0; no header is forged to normalize the reply.
"""
import argparse
import configparser
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import tarfile
import urllib.request

COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"


def command(argv, cwd, label, env=None):
    (cwd / "stage.txt").write_text(label + "\n")
    print("STEP " + label, flush=True)
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=1200)
    (cwd / (label + ".log")).write_text(result.stdout)
    print(result.stdout[-12000:], flush=True)
    if result.returncode:
        raise RuntimeError(label + " exit=" + str(result.returncode))
    return result.stdout


def build(root, wrapper, build_target="mon_ping_native_encoder", output_name="encoded-mon-ping", status=None):
    if not pathlib.Path("/.dockerenv").is_file():
        raise RuntimeError("native development packages may only be installed in an owned disposable Docker container")
    root.mkdir(parents=True, exist_ok=True)
    machine = platform.machine()
    for system in (root / "build/CMakeFiles").glob("*/CMakeSystem.cmake"):
        match = re.search(r'set\(CMAKE_SYSTEM_PROCESSOR "([^"]+)"\)', system.read_text())
        if match and match.group(1) != machine:
            raise RuntimeError("native cache architecture differs: " + match.group(1) + " != " + machine + "; use a fresh cache")
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
    # Container copy can assign an older archive timestamp than an existing
    # cached object. Force only this owned wrapper source to be newer so the
    # claimed wrapper digest always corresponds to an actual fresh compile.
    os.utime(wrapper, None)
    command(["cmake", "--build", str(builddir), "--target", build_target, "-j4"], root, "native-encoder-compilation", environment)
    output = root / output_name
    environment["LD_LIBRARY_PATH"] = str(builddir / "lib") + ":/opt/rh/gcc-toolset-14/root/usr/lib64"
    binary = builddir / ("bin/" + build_target)
    loader = command(["ldd", str(binary)], root, "native-loader-audit", environment)
    common = next((line for line in loader.splitlines() if "libceph-common.so" in line), "")
    if "=> " + str(builddir / "lib") + "/" not in common:
        raise RuntimeError("native encoder is not loading its matching freshly compiled ceph-common: " + common)
    expected_machine = {"x86_64": 62, "aarch64": 183}.get(machine)
    for native in (binary, builddir / "lib/libceph-common.so.2"):
        header = native.read_bytes()[:20]
        if expected_machine is None or header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != expected_machine:
            raise RuntimeError("native ELF architecture differs for " + str(native))
    command([str(binary), str(output)], root, "native-canonical-message-encoding", environment)
    fixtures = {}
    expected = {
        "mon-ping-native-request": (False, 1),
        "mon-ping-native-reply-json": (True, 0),
        "mon-ping-native-reply-empty": (True, 0),
        "mon-ping-native-reply-binary": (True, 0),
    }
    for name, (reply, compat) in expected.items():
        parts = {part: (output / (name + "." + part + ".bin")).read_bytes()
                 for part in ("front", "middle", "data", "header")}
        header = parts["header"]
        metadata = json.loads((output / (name + ".native-header.json")).read_text())
        receipt = json.loads((output / (name + ".decoded-ping.json")).read_text())
        if (len(header) != 41 or metadata["type"] != 2 or metadata["version"] != 1 or
                metadata["compat"] != compat or metadata["tid"] != 0 or
                metadata["seq"] != 1 or metadata["ack_seq"] != 3 or
                metadata["priority"] != 127 or metadata["data_off"] != 0 or
                metadata["reserved"] != 0 or metadata["flags"] != 1 or
                metadata["front_len"] != len(parts["front"]) or metadata["middle_len"] != 0 or
                metadata["data_len"] != 0 or parts["middle"] or parts["data"]):
            raise RuntimeError("native MPing source envelope differs for " + name)
        if (int.from_bytes(header[16:18], "little") != 2 or
                int.from_bytes(header[20:22], "little") != 1 or
                int.from_bytes(header[37:39], "little") != compat or
                int.from_bytes(header[8:16], "little") != 0):
            raise RuntimeError("native MPing header projection differs for " + name)
        if receipt["is_reply"] != reply or receipt["payload_prepopulated"] != reply:
            raise RuntimeError("native MPing producer receipt differs for " + name)
        raw = bytes.fromhex(receipt["raw_hex"])
        if receipt["payload_bytes"] != len(raw):
            raise RuntimeError("native MPing decoder receipt length differs")
        if reply:
            if len(parts["front"]) < 4 or int.from_bytes(parts["front"][:4], "little") != len(raw) or parts["front"][4:] != raw:
                raise RuntimeError("native Ceph string decoder receipt differs for " + name)
        elif parts["front"] or raw:
            raise RuntimeError("native request must remain empty")
        fixtures[name] = {"native_header": metadata, "decoded_ping": receipt}
        for part, value in parts.items():
            fixtures[name][part + "_hex"] = value.hex()
            fixtures[name][part + "_bytes"] = len(value)
            fixtures[name][part + "_sha256"] = hashlib.sha256(value).hexdigest()
    actual_names = {path.name[:-len(".front.bin")] for path in output.glob("*.front.bin")}
    if actual_names != set(expected):
        raise RuntimeError("native MPing vector set differs")
    result = {"ceph_commit": COMMIT, "image": IMAGE,
              "build_architecture": machine,
              "source_archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "source_archive_url": "https://codeload.github.com/ceph/ceph/tar.gz/" + COMMIT,
              "pinned_submodules": source_modules,
              "generated_config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
              "wrapper_sha256": hashlib.sha256(wrapper.read_bytes()).hexdigest(),
              "native_library": "ceph-common built from the same pinned source and official generated configuration as the wrapper",
              "native_payload_encoder_verified": True,
              "canonical_message_encode_verified": True,
              "native_message_header_verified": True,
              "native_payload_decoder_verified": True,
              "header_proof_scope": "native Message envelope settings and MessageFrame serialization of an explicit offline header2 projection; not ProtocolV2 live session execution",
              "offline_transport_inputs": {"sequence": 1, "ack_sequence": 3, "priority": "CEPH_MSG_PRIO_DEFAULT", "message_crc_flags": "MSG_CRC_ALL"},
              "build_driver_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "launcher_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "native_library_sha256": hashlib.sha256((builddir / "lib/libceph-common.so.2").read_bytes()).hexdigest(),
              "loader_audit": loader,
              "producer_contract": "fresh empty request normalizes compat to 1; native string-prefilled MON reply retains compat 0",
              "source_files_sha256": {path: hashlib.sha256((checkout / path).read_bytes()).hexdigest() for path in
                  ("src/messages/MPing.h", "src/msg/Message.h", "src/msg/Message.cc", "src/mon/Monitor.cc", "src/mon/MonClient.h")},
              "status": status or "four fresh native MPing objects, source-shaped payload production then one canonical Message::encode, separate native MPing and Ceph string decoders; no decoded message re-encoded",
              "fixtures": fixtures}
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-native", action="store_true", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--wrapper", type=pathlib.Path, required=True)
    args = parser.parse_args()
    build(args.work.resolve(), args.wrapper.resolve())
