"""Check the CGO-free product graph shared by CI and Windows interoperability."""

import json
import os
from pathlib import Path
import subprocess


PRODUCT_MODULE = "github.com/JSYoo5B/ceph-msgr-go"
APPROVED_MODULES = frozenset({"github.com/klauspost/compress"})
NATIVE_FILES = ("CgoFiles", "CFiles", "CXXFiles", "MFiles", "FFiles",
                "SwigFiles", "SwigCXXFiles", "SysoFiles")


def package_records(output):
    """go list -json emits a stream of objects, rather than a JSON array."""
    decoder = json.JSONDecoder()
    remaining = output.strip()
    while remaining:
        record, end = decoder.raw_decode(remaining)
        if not isinstance(record, dict) or not record.get("ImportPath"):
            raise RuntimeError("invalid product dependency record")
        yield record
        remaining = remaining[end:].lstrip()


def check(execute):
    """Use the caller's toolchain/environment and return checked module evidence."""
    env = json.loads(execute(["go", "env", "-json", "CGO_ENABLED", "GOWORK"],
                             capture=True).stdout)
    if env != {"CGO_ENABLED": "0", "GOWORK": "off"}:
        raise RuntimeError("product dependency check requires CGO_ENABLED=0 and GOWORK=off")
    output = execute(["go", "list", "-mod=readonly", "-deps", "-json", "./..."],
                     capture=True).stdout
    modules = set()
    product_seen = False
    for package in package_records(output):
        name = package["ImportPath"]
        if package.get("Error") or package.get("Incomplete") or package.get("DepsErrors"):
            raise RuntimeError("incomplete product dependency graph: " + name)
        if any(package.get(field) for field in NATIVE_FILES):
            raise RuntimeError("product has native source dependencies: " + name)
        if name == "os/exec":
            raise RuntimeError("product has a subprocess dependency: os/exec")
        if package.get("Standard"):
            continue
        module = package.get("Module", {})
        path = module.get("Path")
        if module.get("Main") and path == PRODUCT_MODULE:
            product_seen = True
            continue
        if path not in APPROVED_MODULES or module.get("Main"):
            raise RuntimeError("product has an unexpected external module: " + str(path))
        if module.get("Replace"):
            raise RuntimeError("product external module is replaced: " + path)
        version = module.get("Version")
        if not version:
            raise RuntimeError("product external module has no version: " + path)
        modules.add((path, version))
    if not product_seen:
        raise RuntimeError("product dependency graph has no product packages")
    return {"CGO_ENABLED": "0", "GOWORK": "off", "standard_library": True,
            "external_modules": [{"path": path, "version": version}
                                 for path, version in sorted(modules)]}


def main():
    project = Path(__file__).resolve().parent.parent
    env = dict(os.environ, GOWORK="off")

    def execute(args, *, capture):
        return subprocess.run(args, cwd=project, env=env, check=True, timeout=300,
                              stdout=subprocess.PIPE, encoding="utf-8")

    print(json.dumps(check(execute), indent=2))


if __name__ == "__main__":
    main()
