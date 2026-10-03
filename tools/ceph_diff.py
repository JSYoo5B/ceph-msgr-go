#!/usr/bin/env python3
"""Read-only upstream comparison for this client's implemented protocol scope.

Uses Python's standard library. Sources stay in memory; reports go to stdout.
Changed files require review and interoperability tests, not automatic porting.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


REFERENCE = "7f793731f1b39eb4f465e960113d2363c311b964"
LIMIT = 4 * 1024 * 1024
GROUPS = {
    "messenger": (
        "src/msg/async/ProtocolV2.cc",
        "src/msg/async/ProtocolV2.h",
        "src/msg/async/frames_v2.h",
        "src/msg/async/frames_v2.cc",
        "src/msg/async/crypto_onwire.h",
        "src/msg/async/crypto_onwire.cc",
        "src/msg/msg_types.h",
        "src/include/msgr.h",
        "src/include/ceph_features.h",
    ),
    "authentication": (
        "src/auth/Auth.h",
        "src/auth/AuthRegistry.h",
        "src/auth/AuthRegistry.cc",
        "src/auth/KeyRing.h",
        "src/auth/KeyRing.cc",
        "src/auth/Crypto.h",
        "src/auth/Crypto.cc",
        "src/auth/cephx/CephxProtocol.h",
        "src/auth/cephx/CephxProtocol.cc",
        "src/auth/cephx/CephxClientHandler.h",
        "src/auth/cephx/CephxClientHandler.cc",
        "src/auth/cephx/CephxServiceHandler.h",
        "src/auth/cephx/CephxServiceHandler.cc",
        "src/auth/cephx/CephxKeyServer.h",
        "src/auth/cephx/CephxKeyServer.cc",
    ),
    "maps-and-recovery": (
        "src/mon/MonMap.h",
        "src/mon/MonMap.cc",
        "src/mon/MonClient.h",
        "src/mon/MonClient.cc",
        "src/mon/MgrMap.h",
        "src/mgr/MgrClient.h",
        "src/mgr/MgrClient.cc",
    ),
    "daemon-policy": (
        "src/common/options/global.yaml.in",
        "src/common/options/mon.yaml.in",
        "src/mon/Monitor.h",
        "src/mon/Monitor.cc",
        "src/mon/AuthMonitor.h",
        "src/mon/AuthMonitor.cc",
        "src/mon/MgrMonitor.h",
        "src/mon/MgrMonitor.cc",
        "src/mon/HealthMonitor.h",
        "src/mon/HealthMonitor.cc",
        "src/mon/LogMonitor.h",
        "src/mon/LogMonitor.cc",
        "src/mon/ConfigMonitor.h",
        "src/mon/ConfigMonitor.cc",
        "src/mon/ConfigMap.h",
        "src/mon/ConfigMap.cc",
        "src/mgr/DaemonServer.h",
        "src/mgr/DaemonServer.cc",
        "src/mgr/MgrStandby.h",
        "src/mgr/MgrStandby.cc",
    ),
    "messages": (
        "src/common/entity_name.h",
        "src/common/entity_name.cc",
        "src/common/LogEntry.h",
        "src/common/LogEntry.cc",
        "src/messages/MAuth.h",
        "src/messages/MAuthReply.h",
        "src/messages/MCommand.h",
        "src/messages/MCommandReply.h",
        "src/messages/MConfig.h",
        "src/messages/MGetConfig.h",
        "src/messages/MLog.h",
        "src/messages/MMonCommand.h",
        "src/messages/MMonCommandAck.h",
        "src/messages/MMgrCommand.h",
        "src/messages/MMgrCommandReply.h",
        "src/messages/MMonMap.h",
        "src/messages/MMonGetMap.h",
        "src/messages/MMgrMap.h",
        "src/messages/MMgrDigest.h",
        "src/messages/MMonSubscribe.h",
        "src/messages/MMonSubscribeAck.h",
    ),
    "command-schemas": (
        "src/common/cmdparse.cc",
        "src/common/cmdparse.h",
        "src/common/options.h",
        "src/mon/MonCommand.h",
        "src/common/admin_socket.cc",
        "src/mon/MonCommands.h",
        "src/mgr/MgrCommands.h",
        "src/pybind/mgr/balancer/module.py",
        "src/pybind/mgr/crash/module.py",
        "src/pybind/mgr/iostat/module.py",
    ),
}


def fetch(url, allow_missing=False):
    request = urllib.request.Request(url, headers={"User-Agent": "ceph-msgr-go-audit"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read(LIMIT + 1)
    except urllib.error.HTTPError as error:
        try:
            if allow_missing and error.code == 404:
                return None
            raise RuntimeError(f"HTTP {error.code}: {url}") from error
        finally:
            error.close()
    if len(body) > LIMIT:
        raise RuntimeError(f"source exceeds {LIMIT} bytes: {url}")
    return body


def resolve(ref):
    if not re.fullmatch(r"[A-Za-z0-9._/-]{1,200}", ref):
        raise ValueError("invalid Ceph ref")
    url = "https://api.github.com/repos/ceph/ceph/commits/" + urllib.parse.quote(ref, safe="")
    metadata = json.loads(fetch(url))
    sha = metadata.get("sha") if isinstance(metadata, dict) else None
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("upstream returned an invalid commit SHA")
    return sha


def source(sha, path):
    return fetch(f"https://raw.githubusercontent.com/ceph/ceph/{sha}/{path}", allow_missing=True)


def describe(body):
    if body is None:
        return None
    return {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body), "lines": len(body.splitlines())}


def compare(area, path, base, head):
    before, after = source(base, path), source(head, path)
    if before is None and after is None:
        raise RuntimeError(f"watch path missing in both commits: {path}")
    status = "unchanged"
    if before is None:
        status = "added"
    elif after is None:
        status = "removed"
    elif before != after:
        status = "modified"
    return {"area": area, "path": path, "status": status,
            "base": describe(before), "head": describe(after),
            "compare_url": f"https://github.com/ceph/ceph/compare/{base}...{head}",
            "base_url": f"https://github.com/ceph/ceph/blob/{base}/{path}",
            "head_url": f"https://github.com/ceph/ceph/blob/{head}/{path}"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("head", help="upstream tag, branch or commit to inspect")
    parser.add_argument("--base", default=REFERENCE, help="comparison base (default: pinned v20.2.4 commit)")
    parser.add_argument("--json", action="store_true", help="include hashes for every watched path")
    args = parser.parse_args()
    started = time.monotonic()
    base, head = resolve(args.base), resolve(args.head)
    paths = [(area, path) for area, paths in GROUPS.items() for path in paths]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(compare, area, path, base, head) for area, path in paths]
        files = [future.result() for future in futures]
    report = {"checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "base": base, "head": head, "elapsed_seconds": round(time.monotonic() - started, 2),
              "files": files}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"base: {base}\nhead: {head}")
        print(f"Watched {len(files)} files in {report['elapsed_seconds']}s; files outside this set are not checked.")
        for area in GROUPS:
            changed = [file for file in files if file["area"] == area and file["status"] != "unchanged"]
            print(f"\n{area}: {len(changed)} changed")
            for file in changed:
                print(f"  {file['status']}: {file['path']}\n    {file['head_url']}")
        print("\nThis comparison does not establish wire or release compatibility.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, KeyError, urllib.error.URLError) as error:
        print(f"Ceph comparison failed: {error}", file=sys.stderr)
        sys.exit(1)
