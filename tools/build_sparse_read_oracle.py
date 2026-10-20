#!/usr/bin/env python3
"""Build the opt-in test-only sparse-read native oracle in an owned container.

Requires an existing pinned source checkout, official Ceph-generated build
configuration, GCC toolset 14 and the matching image's installed librados.so.2.
No downloads, package installation, product changes or daemon execution occur.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
COMPILER = "/opt/rh/gcc-toolset-14/root/usr/bin/g++"
IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_run(args, env):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=180, env=env)
    if result.returncode:
        raise RuntimeError("native oracle build/audit failed:\n" + result.stdout[-12000:])
    return result.stdout


def build_environments(librados):
    # The GCC toolset linker itself needs its matching libctf. The finished
    # client's audit must instead resolve only the fixture image's libraries.
    runtime = dict(os.environ)
    runtime["LD_LIBRARY_PATH"] = librados.parent.as_posix() + ":/usr/lib64/ceph"
    compile_env = dict(runtime)
    compile_env["LD_LIBRARY_PATH"] = "/opt/rh/gcc-toolset-14/root/usr/lib64:" + runtime["LD_LIBRARY_PATH"]
    return compile_env, runtime


def build(source, builddir, wrapper, output, librados, compiler):
    if not Path("/.dockerenv").is_file():
        raise RuntimeError("build only in an owned disposable native-development container")
    source, builddir, wrapper, output, librados = [p.resolve() for p in (source, builddir, wrapper, output, librados)]
    generated = builddir / "include/acconfig.h"
    if not generated.is_file() or not (source / "src/include/rados/librados.hpp").is_file():
        raise RuntimeError("requires pinned Ceph source and its official generated build configuration")
    version = source / "src/.git_version"
    if version.is_file():
        if version.read_text().splitlines()[:1] != [COMMIT]:
            raise RuntimeError("source version metadata differs from pinned commit")
    else:
        commit = checked_run(["git", "-C", str(source), "rev-parse", "HEAD"], os.environ).strip()
        if commit != COMMIT:
            raise RuntimeError("source checkout differs from pinned commit")
    if not librados.is_file() or not wrapper.is_file() or not Path(compiler).is_file():
        raise RuntimeError("compiler, wrapper or matching installed librados is unavailable")
    compile_env, runtime_env = build_environments(librados)
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [compiler, "-std=c++20", "-O2", "-g0",
               "-I" + str(source / "src"), "-I" + str(source / "src/include"),
               "-I" + str(builddir / "include"), str(wrapper), str(librados),
               "-Wl,-rpath," + str(librados.parent), "-Wl,-rpath,/usr/lib64/ceph", "-o", str(output)]
    compile_output = checked_run(command, compile_env)
    loader = checked_run(["ldd", str(output)], runtime_env)
    if "not found" in loader:
        raise RuntimeError("native oracle contains unresolved loader dependencies")
    match = re.search(r"^\s*librados\.so\.2\s+=>\s+(\S+)", loader, re.MULTILINE)
    if not match or Path(match.group(1)).resolve() != librados:
        raise RuntimeError("native oracle did not load the selected image librados")
    dependencies = {}
    for name, path in re.findall(r"^\s*(\S+)\s+=>\s+(\S+)\s+\(", loader, re.MULTILINE):
        resolved = Path(path).resolve()
        if resolved.is_file():
            dependencies[name] = dict(path=str(resolved), sha256=sha256(resolved))
    proof = dict(source_commit=COMMIT, image=IMAGE, source_commit_check="checkout metadata or git HEAD",
                 wrapper_sha256=sha256(wrapper), generated_config_sha256=sha256(generated),
                 binary_sha256=sha256(output), librados_path=str(librados), librados_sha256=sha256(librados),
                 compiler=compiler, compiler_version=checked_run([compiler, "--version"], compile_env),
                 compile_ld_library_path=compile_env["LD_LIBRARY_PATH"],
                 runtime_ld_library_path=runtime_env["LD_LIBRARY_PATH"],
                 compile_command=command, compile_output=compile_output, loader_audit=loader,
                 dependencies=dependencies, scope="independent native C++ client oracle only; no daemon request executed by build",
                 product_dependency=False)
    proof_path = output.with_name(output.name + ".proof.json")
    proof_path.write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(dict(binary=str(output), proof=str(proof_path), binary_sha256=proof["binary_sha256"])))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--wrapper", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--librados", type=Path, default=Path("/usr/lib64/librados.so.2"))
    parser.add_argument("--compiler", default=COMPILER)
    args = parser.parse_args()
    build(args.source, args.build, args.wrapper, args.output, args.librados, args.compiler)
