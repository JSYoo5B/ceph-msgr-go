"""Exercise real snapshot collection separately from publisher mocks."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
LINTER = os.environ.get("GOLANGCI_LINT") or shutil.which("golangci-lint")


@unittest.skipUnless(LINTER and shutil.which("go") and shutil.which("git"),
                     "requires the pinned linter, Go and Git")
class CollectionTests(unittest.TestCase):
    def test_real_pair_and_test_only_skip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ, CGO_ENABLED="0", GOWORK="off",
                       GIT_AUTHOR_NAME="Quality test", GIT_AUTHOR_EMAIL="quality@example.invalid",
                       GIT_COMMITTER_NAME="Quality test", GIT_COMMITTER_EMAIL="quality@example.invalid")

            def run(*args):
                result = subprocess.run(args, cwd=root, env=env, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=180)
                self.assertEqual(result.returncode, 0, result.stdout)
                return result.stdout.strip()

            run("git", "init", "-q")
            shutil.copytree(ROOT / "tools/quality", root / "tools/quality")
            (root / "go.mod").write_text("module example.invalid/quality\n\ngo 1.24.0\n")
            (root / "cephmsgr").mkdir()
            source = root / "cephmsgr/owner.go"
            source.write_text("package cephmsgr\n\ntype Owner struct { Value int }\n")
            run("git", "add", ".")
            run("git", "-c", "commit.gpgsign=false", "commit", "-qm", "Baseline")
            base = run("git", "rev-parse", "HEAD")
            source.write_text(source.read_text() + "\nfunc (o *Owner) Read() int { return o.Value }\n")
            run("git", "add", ".")
            run("git", "-c", "commit.gpgsign=false", "commit", "-qm", "Add method")
            head = run("git", "rev-parse", "HEAD")
            output = root / "report"
            run("python3", str(ROOT / ".github/scripts/structural_review.py"),
                "--base", base, "--head", head, "--output", str(output),
                "--golangci-lint", str(LINTER))
            receipt = json.loads((output / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "complete")
            self.assertEqual((receipt["base"], receipt["head"]), (base, head))
            delta = json.loads((output / "structure-delta.json").read_text())
            self.assertEqual(len(delta["changed_owners"]), 1)
            self.assertEqual(delta["changed_owners"][0]["delta"]["exported_methods"], 1)
            self.assertIn("Changed owners", (output / "summary.md").read_text())
            (root / "cephmsgr/owner_test.go").write_text("package cephmsgr\n")
            run("git", "add", "cephmsgr/owner_test.go")
            run("git", "-c", "commit.gpgsign=false", "commit", "-qm", "Test only")
            test_head = run("git", "rev-parse", "HEAD")
            result = run("python3", str(ROOT / ".github/scripts/structural_review.py"),
                         "--classify", "--base", head, "--head", test_head)
            self.assertIn("skipped", result)


if __name__ == "__main__":
    unittest.main()
