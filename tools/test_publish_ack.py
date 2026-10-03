import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parent.parent / "integration" / "publish-ack.sh"
SHELL = shutil.which("sh")
MOVE = shutil.which("mv")


@unittest.skipUnless(os.name == "posix" and SHELL and MOVE, "ack helper executes in the POSIX fixture")
class PublishAckTest(unittest.TestCase):
    def wait_for(self, path, process):
        deadline = time.monotonic() + 5
        while not path.exists():
            if process.poll() is not None or time.monotonic() >= deadline:
                self.fail("producer did not reach the controlled publication boundary")
            time.sleep(0.005)

    def test_fast_consumer_can_delete_publication_before_producer_returns(self):
        with tempfile.TemporaryDirectory(prefix="ceph ack ") as directory:
            root = Path(directory)
            ack = root / "ack with spaces"
            commands = root / "commands"
            commands.mkdir()
            paused, release, consumed = (root / name for name in ("paused", "release", "consumed"))
            arguments = root / "mv-arguments"
            wrapper = commands / "mv"
            wrapper.write_text("""#!/bin/sh
set -eu
printf '%s\\n' "$@" > "$MV_ARGUMENTS"
: > "$PAUSED"
count=0
while ! test -f "$RELEASE"; do
    count=$((count + 1)); test "$count" -lt 500 || exit 81
    sleep 0.01
done
"$REAL_MOVE" "$@"
count=0
while ! test -f "$CONSUMED"; do
    count=$((count + 1)); test "$count" -lt 500 || exit 82
    sleep 0.01
done
""", encoding="utf-8")
            wrapper.chmod(0o755)
            env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
                       MV_ARGUMENTS=str(arguments), PAUSED=str(paused), RELEASE=str(release),
                       CONSUMED=str(consumed), REAL_MOVE=MOVE)
            producer = subprocess.Popen([SHELL, str(SCRIPT), str(ack)], env=env,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.wait_for(paused, producer)
                paths = [value for value in arguments.read_text(encoding="utf-8").splitlines()
                         if not value.startswith("-")]
                staging = Path(paths[0])
                self.assertEqual(staging.parent, ack.parent)
                self.assertEqual(staging.read_bytes(), b"")
                self.assertFalse(ack.exists(), "partially prepared acknowledgement became visible")
                release.touch()
                self.wait_for(ack, producer)
                self.assertFalse(staging.exists(), "publication did not use the prepared sibling file")
                self.assertIsNone(producer.poll(), "consumer did not run before the producer returned")
                ack.unlink()
                consumed.touch()
                _, error = producer.communicate(timeout=5)
                self.assertEqual(producer.returncode, 0, error.decode("utf-8", errors="replace"))
                self.assertFalse(ack.exists())
                self.assertEqual(list(root.glob("ack with spaces.pending.*")), [])
            finally:
                release.touch()
                consumed.touch()
                if producer.poll() is None:
                    producer.communicate(timeout=6)

    def test_failed_rename_publishes_nothing_and_removes_only_owned_staging(self):
        with tempfile.TemporaryDirectory(prefix="ceph ack ") as directory:
            root = Path(directory)
            ack = root / "ack with spaces"
            unrelated = root / "ack with spaces.pending.unrelated"
            unrelated.write_text("keep", encoding="utf-8")
            commands = root / "commands"
            commands.mkdir()
            wrapper = commands / "mv"
            wrapper.write_text("#!/bin/sh\nexit 17\n", encoding="utf-8")
            wrapper.chmod(0o755)
            env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"])
            result = subprocess.run([SHELL, str(SCRIPT), str(ack)], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            self.assertEqual(result.returncode, 17)
            self.assertFalse(ack.exists())
            self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep")
            self.assertEqual(list(root.glob("ack with spaces.pending.*")), [unrelated])

    def test_creation_failure_does_not_publish_acknowledgement(self):
        with tempfile.TemporaryDirectory(prefix="ceph ack ") as directory:
            root = Path(directory)
            ack = root / "missing parent" / "ack"
            result = subprocess.run([SHELL, str(SCRIPT), str(ack)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(ack.exists())
            self.assertEqual(list(root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
