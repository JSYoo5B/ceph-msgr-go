"""Controller IPC contract for the development-only POSIX/WSL fixture."""

import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parent.parent
FIXTURE = PROJECT / "integration/windows-fixture.sh"
SHELL = shutil.which("sh")
VERSION = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"

# An independent provider implements the published marker contract. It never
# starts Docker or Ceph; all state belongs to one test's temporary directory.
CONTROLLER = r'''
import json
import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
out = Path(args[args.index("-output") + 1])
mode = os.environ.get("MOCK_PROVIDER_MODE", "")
(out / "captured.json").write_text(json.dumps({"args": args, "env": {
    key: value for key, value in os.environ.items() if key.startswith("CEPH_MSGR_")}}))
if mode in ("bootstrap-error", "bootstrap-cleanup-error"):
    (out / "provider.error").write_text("mock bootstrap failed\n")
    if mode == "bootstrap-cleanup-error":
        (out / "provider.cleanup.error").write_text("mock partial bootstrap cleanup failed\n")
    (out / "provider.stopped").touch()
    sys.exit(1)

(out / "daemon-version").write_text(os.environ["MOCK_DAEMON_VERSION"] + "\n")
(out / "proxy").write_text("127.0.0.1:40000\n")
(out / "provider.ready").touch()
count = 0
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    if (out / "provider.diagnostics").exists():
        count += 1
        (out / "diagnostics-count").write_text(str(count))
        (out / "provider.diagnostics").unlink()
        (out / "provider.diagnostics.done").touch()
    if (out / "provider.stop").exists():
        if mode == "cleanup-error":
            # Cleanup failure has its own marker, independently of operation
            # failure. A stopped marker alone cannot establish clean teardown.
            (out / "provider.cleanup.error").write_text("mock cleanup failed\n")
        (out / "provider.stopped").touch()
        sys.exit(1 if mode == "cleanup-error" else 0)
    time.sleep(0.01)
sys.exit(2)
'''


@unittest.skipUnless(os.name == "posix" and SHELL, "fixture shell executes in POSIX/WSL")
class WindowsFixtureContractTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ceph-msgr-windows-provider-contract-")
        self.root = Path(self.directory.name)
        self.out = self.root / "out"
        self.out.mkdir()
        self.diag = self.root / "diagnostics"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        MOCK_DAEMON_VERSION=VERSION, MOCK_DOCKER_CALLS=str(self.root / "docker-calls"),
                        CEPH_MSGR_TEST_MODE_REJECTION="mgr", CEPH_MSGR_TEST_CONNECTION_MODE="crc",
                        CEPH_MSGR_MONITORS="192.0.2.10:3300", CEPH_MSGR_IDENTITY="client.unrelated")
        self.env.pop("MOCK_PROVIDER_MODE", None)
        self.write(self.bin / "id", '#!/bin/sh\nprintf "0\\n"\n')
        self.write(self.bin / "docker", '''#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_DOCKER_CALLS"
case "$1" in
    info|pull) exit 0 ;;
    image) test "$2" = inspect && printf 'amd64\\n' ;;
    *) echo 'Docker lifecycle calls must belong to the controller' >&2; exit 99 ;;
esac
''')
        self.write(self.out / "relay", "#!/bin/sh\nexit 0\n")
        # Use the same Python interpreter as unittest, including paths with
        # spaces. The executable shim is POSIX because the real controller is
        # a Linux binary inside the disposable WSL distribution.
        (self.out / "mock-provider.py").write_text(CONTROLLER, encoding="utf-8")
        self.write(self.out / "fixture-controller", "#!/bin/sh\nexec " + shlex.quote(sys.executable)
                   + " " + shlex.quote(str(self.out / "mock-provider.py")) + ' "$@"\n')

    def write(self, path, text):
        path.write_text(text, encoding="utf-8")
        path.chmod(0o700)

    def action(self, action):
        return subprocess.run([SHELL, str(FIXTURE), action, str(self.out), str(self.diag)],
                              env=self.env, text=True, capture_output=True, timeout=10)

    def tearDown(self):
        try:
            if (self.out / "provider.pid").exists() and not (self.out / "provider.stopped").exists():
                # Ask the owned mock to finish even when an assertion failed.
                (self.out / "provider.stop").touch()
                self.action("stop")
        finally:
            self.directory.cleanup()

    def test_lifecycle_preserves_native_profile_and_supports_repeated_diagnostics(self):
        started = self.action("start")
        self.assertEqual(started.returncode, 0, started.stderr)
        captured = json.loads((self.out / "captured.json").read_text())
        self.assertEqual(captured["args"], ["serve", "-project", str(PROJECT), "-output", str(self.out),
                                            "-diagnostics", str(self.diag)])
        self.assertEqual({key: value for key, value in captured["env"].items() if key != "CEPH_MSGR_TEST_IMAGE"}, {
            "CEPH_MSGR_TEST_KEY_TYPE": "aes256k", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes256k",
            "CEPH_MSGR_TEST_IP_FAMILY": "4", "CEPH_MSGR_TEST_MGR_COUNT": "2",
            "CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS": "1", "CEPH_MSGR_TEST_CONNECTION_MODE": "secure",
            "CEPH_MSGR_TEST_RUNTIME": "host"})
        self.assertIn("@sha256:", captured["env"]["CEPH_MSGR_TEST_IMAGE"])
        for count in (1, 2):
            diagnostics = self.action("diagnostics")
            self.assertEqual(diagnostics.returncode, 0, diagnostics.stderr)
            self.assertEqual((self.out / "diagnostics-count").read_text(), str(count))
        stopped = self.action("stop")
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertTrue((self.out / "provider.stopped").exists())
        calls = (self.root / "docker-calls").read_text().splitlines()
        self.assertFalse(any(line.split()[0] in ("run", "rm", "cp", "logs", "exec", "port") for line in calls))

    def test_wrong_daemon_version_fails_without_losing_cleanup(self):
        self.env["MOCK_DAEMON_VERSION"] = "ceph version 20.2.3"
        started = self.action("start")
        self.assertNotEqual(started.returncode, 0)
        self.assertIn("fixed Tentacle reference", started.stderr)
        stopped = self.action("stop")
        self.assertEqual(stopped.returncode, 0, stopped.stderr)

    def test_partial_bootstrap_error_does_not_mislabel_successful_cleanup(self):
        self.env["MOCK_PROVIDER_MODE"] = "bootstrap-error"
        self.assertNotEqual(self.action("start").returncode, 0)
        stopped = self.action("stop")
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertTrue((self.out / "provider.error").exists())
        self.assertFalse((self.out / "provider.cleanup.error").exists())

    def test_cleanup_error_is_reported_even_with_stopped_marker(self):
        self.env["MOCK_PROVIDER_MODE"] = "cleanup-error"
        self.assertEqual(self.action("start").returncode, 0)
        stopped = self.action("stop")
        self.assertNotEqual(stopped.returncode, 0)
        self.assertIn("cleanup failed", stopped.stderr)
        self.assertTrue((self.out / "provider.stopped").exists())
        self.assertTrue((self.out / "provider.cleanup.error").exists())

    def test_partial_bootstrap_cleanup_failure_is_preserved(self):
        self.env["MOCK_PROVIDER_MODE"] = "bootstrap-cleanup-error"
        self.assertNotEqual(self.action("start").returncode, 0)
        stopped = self.action("stop")
        self.assertNotEqual(stopped.returncode, 0)
        self.assertIn("cleanup failed", stopped.stderr)
        self.assertTrue((self.out / "provider.error").exists())
        self.assertTrue((self.out / "provider.cleanup.error").exists())
        self.assertTrue((self.out / "provider.stopped").exists())

    def test_no_launch_cleanup_is_allowed_despite_operation_error(self):
        (self.out / "provider.error").write_text("mock validation failure\n")
        self.assertEqual(self.action("stop").returncode, 0)
        self.assertNotEqual(self.action("diagnostics").returncode, 0)
        self.assertFalse((self.root / "docker-calls").exists())

    def test_dead_provider_cannot_claim_cleanup_without_confirmation(self):
        finished = subprocess.Popen([SHELL, "-c", "exit 0"])
        finished.wait(timeout=5)
        (self.out / "provider.pid").write_text(str(finished.pid))
        stopped = self.action("stop")
        self.assertNotEqual(stopped.returncode, 0)
        self.assertIn("without confirming cleanup", stopped.stderr)
        self.assertFalse((self.out / "provider.stopped").exists())

    def test_old_diagnostics_ack_cannot_mask_a_stopped_provider(self):
        (self.out / "provider.pid").write_text(str(os.getpid()))
        (self.out / "provider.stopped").touch()
        (self.out / "provider.diagnostics.done").touch()
        diagnostics = self.action("diagnostics")
        self.assertNotEqual(diagnostics.returncode, 0)
        self.assertIn("stopped before collecting diagnostics", diagnostics.stderr)
        self.assertFalse((self.out / "provider.diagnostics.done").exists())


if __name__ == "__main__":
    unittest.main()
