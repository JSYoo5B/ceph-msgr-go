"""Development-only Windows executables talking to an isolated WSL Ceph fixture."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import traceback

import go_test_annotations


MODULE = "github.com/jsyoo5b/ceph-msgr-go"
SUITES = {
    "api": ("integration", (
        "TestCephContextAdmissionIntegration", "TestCephTellIntegration",
        "TestCephNamedMonitorTellIntegration", "TestCephReadinessIntegration",
        "TestCephSnapshotIntegration", "TestCephLogWatchIntegration",
        "TestCephConfigStreamIntegration", "TestCephDigestIntegration",
        "TestCephHostnameConfigIntegration",
    )),
    "client": ("cephmsgr", ("TestCephIntegration",)),
}


def required_passes(lines, package, tests):
    """Exit zero alone can mean every opt-in test was skipped or unmatched."""
    outcomes = {name: [] for name in tests}
    package_outcomes = []
    for line in lines:
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError) as error:
            raise ValueError("invalid test2json event") from error
        if not isinstance(event, dict) or event.get("Package") != package:
            raise ValueError("unexpected test2json package")
        action, name = event.get("Action"), event.get("Test")
        if action in ("pass", "fail", "skip"):
            if name in outcomes:
                outcomes[name].append(action)
            elif name is None:
                package_outcomes.append(action)
            elif action != "pass":
                raise ValueError("a subtest did not pass")
    if package_outcomes != ["pass"]:
        raise ValueError("suite did not report exactly one package pass")
    missing = [name for name, events in outcomes.items() if events != ["pass"]]
    if missing:
        raise ValueError("required tests missing, skipped or failed: " + ", ".join(missing))
    return list(tests)


def proxy_endpoint(text):
    value = text.strip()
    if not re.fullmatch(r"127\.0\.0\.1:[0-9]{1,5}", value):
        raise ValueError("fixture relay must use a Windows loopback endpoint")
    if not 0 < int(value.rsplit(":", 1)[1]) < 65536:
        raise ValueError("fixture relay port is out of range")
    return value


def native_environment():
    if sys.platform != "win32":
        raise RuntimeError("this verifier must execute on Windows")
    env = os.environ.copy()
    # Do not inherit selectors for special fixtures or unrelated test clusters.
    for name in list(env):
        if name.startswith("CEPH_MSGR_"):
            del env[name]
    env.update(CGO_ENABLED="0", GOTOOLCHAIN="local", GOOS="windows", GOARCH="amd64")
    return env


def command(args, *, cwd, env, timeout=300, capture=False, log=None):
    result = subprocess.run(args, cwd=cwd, env=env, check=False, timeout=timeout,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace")
    if log is not None:
        with log.open("a", encoding="utf-8") as output:
            output.write(json.dumps(args) + "\n" + result.stdout)
    if not capture or result.returncode:
        print(result.stdout, end="", flush=True)
    result.check_returncode()
    return result


def run(distro, diagnostics):
    project = Path(__file__).resolve().parent.parent
    diagnostics = diagnostics.resolve()
    diagnostics.mkdir(parents=True, exist_ok=True)
    (diagnostics / "startup.json").write_text(json.dumps({"platform": sys.platform,
        "distro": distro, "project": str(project)}, indent=2) + "\n", encoding="utf-8")
    env = native_environment()

    def execute(args, *, env=env, timeout=300, capture=False):
        return command(args, cwd=project, env=env, timeout=timeout,
                       capture=capture, log=diagnostics / "bootstrap.log")

    host = json.loads(execute(
        ["go", "env", "-json", "GOHOSTOS", "GOHOSTARCH", "GOOS", "GOARCH", "CGO_ENABLED"],
        capture=True).stdout)
    if host != {"GOHOSTOS": "windows", "GOHOSTARCH": "amd64", "GOOS": "windows",
                "GOARCH": "amd64", "CGO_ENABLED": "0"}:
        raise RuntimeError("expected a native Windows amd64 toolchain with CGO disabled")
    external = execute(["go", "list", "-deps", "-f",
                        "{{if not .Standard}}{{if not .Module.Main}}{{.ImportPath}}{{end}}{{end}}",
                        "./cephmsgr"], capture=True).stdout.strip()
    if external:
        raise RuntimeError("product has non-standard-library dependencies")
    revision = execute(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()
    version = execute(["go", "version"], capture=True).stdout.strip()
    out = Path(tempfile.mkdtemp(prefix="ceph-msgr-windows-", dir=os.getenv("RUNNER_TEMP")))
    wsl = ["wsl.exe", "--distribution", distro, "--user", "root", "--"]
    attempted = False
    report = {"revision": revision, "go": version, "host": host,
              "ceph": "20.2.4", "credential": "aes256k", "service_cipher": "aes256k",
              "connection_mode": "secure", "wire_family": "IPv4", "transport": "WSL loopback relay",
              "product_dependencies": "Go standard library", "suites": {}}
    try:
        for name, (package, _) in SUITES.items():
            execute(["go", "test", "-c", "-o", str(out / (name + ".test.exe")), "./" + package])
        linux = dict(env, GOOS="linux", GOARCH="amd64")
        execute(["go", "build", "-o", str(out / "relay"), "./integration/relay"], env=linux)

        def wsl_path(path):
            return execute(wsl + ["wslpath", "-a", "-u", str(path)],
                           capture=True, timeout=60).stdout.strip()

        fixture = wsl_path(project / "integration/windows-fixture.sh")
        output = wsl_path(out)
        target = wsl_path(diagnostics)

        def fixture_action(action, timeout=60):
            return execute(wsl + ["bash", fixture, action, output, target], timeout=timeout)

        attempted = True
        fixture_action("start", timeout=900)
        report["daemon_version"] = (out / "daemon-version").read_text(encoding="utf-8").strip()
        report["fixture_image"] = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
        proxy = proxy_endpoint((out / "proxy").read_text(encoding="utf-8"))
        env.update(CEPH_MSGR_MONITORS="127.0.0.1:33300,127.0.0.1:33301,127.0.0.1:33302",
                   CEPH_MSGR_KEY_FILE=str(out / "key"), CEPH_MSGR_IDENTITY="client.test",
                   CEPH_MSGR_FSID="80bbab73-69c1-4a0c-a746-4271357750b8",
                   CEPH_MSGR_CONTROL_DIR=str(out), CEPH_MSGR_TEST_PROXY=proxy,
                   CEPH_MSGR_TEST_SERVICE_CIPHER="aes256k", CEPH_MSGR_TEST_MGR_COUNT="2")
        for name, (package, tests) in SUITES.items():
            full_package = MODULE + "/" + package
            print("Executing Windows native " + name + " suite", flush=True)
            log = diagnostics / (name + "-tests.jsonl")
            with log.open("w", encoding="utf-8") as events:
                result = subprocess.run(
                    ["go", "tool", "test2json", "-p", full_package, str(out / (name + ".test.exe")),
                     "-test.v=test2json", "-test.timeout=6m", "-test.run=^(" + "|".join(tests) + ")$"],
                    cwd=project, env=env, stdout=events, stderr=subprocess.STDOUT, timeout=420)
            lines = log.read_text(encoding="utf-8").splitlines()
            for line in lines:
                try:
                    output_event = json.loads(line).get("Output", "")
                except (ValueError, AttributeError):
                    continue
                if output_event:
                    print(output_event, end="", flush=True)
            if result.returncode:
                for annotation in go_test_annotations.annotations(lines, result.returncode):
                    print(annotation, flush=True)
                raise RuntimeError(name + " native suite failed")
            report["suites"][name] = required_passes(lines, full_package, tests)
        fixture_action("diagnostics")
    except BaseException:
        if attempted:
            try:
                fixture_action("diagnostics")
            except Exception as error:
                print("Could not collect fixture diagnostics: " + str(error), file=sys.stderr)
        raise
    finally:
        try:
            if attempted:
                fixture_action("stop")
        finally:
            shutil.rmtree(out)
    report["cleanup_complete"] = True
    (diagnostics / "verification.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Windows native interoperability verified: " + revision, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distro", required=True, help="dedicated WSL2 development distribution")
    parser.add_argument("--diagnostics", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.distro, args.diagnostics)
    except Exception as error:
        args.diagnostics.mkdir(parents=True, exist_ok=True)
        (args.diagnostics / "failure.json").write_text(json.dumps({
            "error_type": type(error).__name__, "error": str(error),
            "traceback": traceback.format_exc(),
        }, indent=2) + "\n", encoding="utf-8")
        message = str(error).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print("::error title=Windows interoperability verification::" + message, flush=True)
        raise
