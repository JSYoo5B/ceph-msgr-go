import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import fuzz_targets as fuzz


def events(*values):
    return "\n".join(json.dumps(v) for v in values) + "\n"


class FuzzTargetsTests(unittest.TestCase):
    def test_compiled_inventory_detects_added_removed_and_ambiguous_targets(self):
        output = events({"Action": "output", "Package": "example.test/m/internal/a", "Output": "FuzzA\n"},
                        {"Action": "output", "Package": "example.test/m/internal/a", "Output": "ok pkg 0.1s\n"})
        self.assertEqual(fuzz.compiled_events(output, "example.test/m"), [("internal/a", "FuzzA")])
        for bad in ("", output + output, output.replace("example.test/m", "elsewhere/m"), "not json"):
            with self.assertRaises(ValueError):
                fuzz.compiled_events(bad, "example.test/m")
        for expected, actual in (([], [("a", "FuzzA")]), ([("a", "FuzzA")], [])):
            with self.assertRaises(ValueError):
                fuzz.compare(expected, actual)

    def test_manifest_rejects_duplicates_paths_and_empty_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / fuzz.INVENTORY
            path.parent.mkdir(parents=True)
            for targets in ([], [{"package":"../escape", "test":"FuzzA"}],
                            [{"package":"a", "test":"FuzzA"}]*2,
                            [{"package":"a", "test":"FuzzA/sub"}]):
                path.write_text(json.dumps({"version":1, "targets":targets}))
                with self.assertRaises(ValueError):
                    fuzz.inventory(root)

    def test_completion_requires_target_run_pass_and_package_pass(self):
        valid = [{"Action":"run", "Test":"FuzzA"}, {"Action":"pass", "Test":"FuzzA"}, {"Action":"pass"}]
        self.assertTrue(fuzz.completed(events(*valid), "FuzzA"))
        for bad in (valid[1:], valid[:-1], valid + [valid[0]],
                    [{"Action":"skip", "Test":"FuzzA"}, {"Action":"pass"}],
                    valid + [{"Action":"fail"}]):
            self.assertFalse(fuzz.completed(events(*bad), "FuzzA"))
        self.assertFalse(fuzz.completed(events(*valid), "FuzzOther"))

    def test_zero_exit_with_no_matching_test_is_a_failure_with_retained_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            def run(*args, **kwargs):
                kwargs["stdout"].write(events({"Action":"pass"}))
                return mock.Mock(returncode=0)
            with mock.patch.object(fuzz.subprocess, "run", side_effect=run):
                code = fuzz.run_targets(Path(directory), [("internal/a", "FuzzA")], output, "1s")
            self.assertEqual(code, 1)
            report = json.loads((output / "results.json").read_text())
            self.assertEqual(report["results"][0]["outcome"], "failed")
            self.assertTrue((output / report["results"][0]["log"]).exists())

    def test_repository_workflow_uses_inventory_and_bounded_runner(self):
        targets = fuzz.inventory(fuzz.ROOT)
        self.assertIn(("internal/mds", "FuzzDecodeCaps"), targets)
        self.assertIn(("internal/wire", "FuzzDecoder"), targets)
        workflow = (fuzz.ROOT / ".github/workflows/platform-checks.yml").read_text()
        section = workflow.split("  fuzz:\n", 1)[1].split("  windows-ceph:\n", 1)[0]
        self.assertIn("tools/fuzz_targets.py run", section)
        self.assertNotIn("-fuzz '^Fuzz", section)
        self.assertIn("timeout-minutes: 20", section)
        basic = (fuzz.ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("tools/fuzz_targets.py verify", basic)
        self.assertNotIn("tools/fuzz_targets.py run", basic)


if __name__ == "__main__":
    unittest.main()
