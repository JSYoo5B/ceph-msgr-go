"""Execute Basic CI test steps with controlled Go outcomes, without Go or Ceph."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
STEPS = {
    "Unit tests without integration tests": "unit-tests.jsonl",
    "Race detector on unit tests without integration tests": "race-tests.jsonl",
    "Unit test fixture tools without a cluster": "fixture-tests.jsonl",
}


def script(name):
    section = (ROOT / ".github/workflows/ci.yml").read_text().split(
        "      - name: " + name + "\n", 1)[1].split("      - ", 1)[0]
    return "\n".join(line[10:] for line in section.split("        run: |\n", 1)[1].splitlines())


@unittest.skipUnless(os.name == "posix", "Basic CI uses POSIX executable shims on Linux")
class BasicDiagnosticsTests(unittest.TestCase):
    def test_go_exit_and_evidence_survive_annotation_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            go = binary / "go"
            go.write_text('#!' + sys.executable + '\n' + '''import json, os, sys
if sys.argv[1] == "list":
    print("example.invalid/product")
    sys.exit(0)
print(json.dumps({"Action": "fail" if os.environ["TEST_EXIT"] != "0" else "pass",
                  "Package": "example.invalid/product"}))
sys.exit(int(os.environ["TEST_EXIT"]))
''')
            go.chmod(0o755)
            python = binary / "python3"
            python.write_text('#!' + sys.executable + '\n' + '''import os, sys
if os.environ["ANNOTATION_FAIL"] == "1":
    sys.exit(23)
os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
''')
            python.chmod(0o755)
            for name, log in STEPS.items():
                for status, parser_failure in ((0, False), (7, False), (7, True)):
                    with self.subTest(step=name, status=status, parser_failure=parser_failure):
                        target = root / log
                        target.unlink(missing_ok=True)
                        env = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"],
                                   RUNNER_TEMP=str(root), TEST_EXIT=str(status),
                                   ANNOTATION_FAIL=str(int(parser_failure)))
                        result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script(name)],
                                                cwd=ROOT, env=env, capture_output=True, text=True)
                        self.assertEqual(result.returncode, status, result.stderr)
                        self.assertIn('"Package": "example.invalid/product"', target.read_text())
                        if status and not parser_failure:
                            self.assertIn("::error", result.stdout)
                        else:
                            self.assertNotIn("::error", result.stdout)


if __name__ == "__main__":
    unittest.main()
