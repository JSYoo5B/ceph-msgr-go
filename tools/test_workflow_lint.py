import itertools
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import workflow_lint as lint


class QueueWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.mapping = [
            "  group: " + lint.GROUP,
            "  cancel-in-progress: false",
            "  queue: max",
        ]
        self.prefix = "name: example\non: push\n"
        self.suffix = "jobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n"

    def source(self, entries=None):
        return self.prefix + "concurrency:\n" + "\n".join(entries or self.mapping) + "\n" + self.suffix

    def test_preserves_all_other_text_and_line_numbers(self):
        for entries in itertools.permutations(self.mapping):
            source = self.source(list(entries))
            adapted = lint.command_queue_source(source)
            self.assertEqual(adapted, source.replace("  queue: max\n", "\n"))
            self.assertEqual(adapted.count("\n"), source.count("\n"))

    def test_comments_and_blank_lines(self):
        source = self.source([self.mapping[0], "  # queue rationale", "", *self.mapping[1:]])
        self.assertIn("  # queue rationale", lint.command_queue_source(source))
        for entry in ["  queue: max # inline", "  queue: 'max'", "  queue: &capacity max",
                      "  queue: *capacity", "  <<: *queue", "    queue: max", "\tqueue: max"]:
            with self.subTest(entry=entry), self.assertRaises(ValueError):
                lint.command_queue_source(self.source([*self.mapping[:2], entry]))

    def test_invalid_value_cancel_and_group(self):
        for value in ["single", "100", "true", "MAX", "${{ 'max' }}", "max\n    extra: value"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                lint.command_queue_source(self.source().replace("queue: max", "queue: " + value))
        for value in ["true", "${{ false }}", '"false"', "*cancel"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                lint.command_queue_source(self.source().replace("cancel-in-progress: false", "cancel-in-progress: " + value))
        with self.assertRaises(ValueError):
            lint.command_queue_source(self.source().replace(lint.GROUP, "basic"))
        with self.assertRaises(ValueError):
            lint.command_queue_source(self.source().replace("concurrency:", "concurrency: &shared"))

    def test_duplicate_missing_unknown_keys(self):
        for entry in self.mapping + ["  unexpected: value"]:
            with self.subTest(entry=entry), self.assertRaises(ValueError):
                lint.command_queue_source(self.source(self.mapping + [entry]))
        for index in range(3):
            with self.assertRaises(ValueError):
                lint.command_queue_source(self.source(self.mapping[:index] + self.mapping[index + 1:]))
        with self.assertRaises(ValueError):
            lint.command_queue_source(self.source() + "concurrency:\n  group: extra\n")

    def test_other_workflows_and_other_queue_keys_are_not_adapted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github/workflows"
            workflows.mkdir(parents=True)
            source = self.source() + "unknown-root-key: bad\n"
            other = "concurrency:\n  queue: max\n"
            (workflows / "ci-command.yml").write_text(source)
            (workflows / "baseline.yml").write_text(other)
            with patch.object(lint.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)) as run:
                self.assertEqual(lint.lint(root), 1)
            self.assertEqual(run.call_count, 2)
            inputs = [call.kwargs["input"] for call in run.call_args_list]
            self.assertIn(other, inputs)
            self.assertIn(source.replace("  queue: max\n", "\n"), inputs)
            self.assertTrue(all("-stdin-filename" in call.args[0] for call in run.call_args_list))

    def test_repo_keeps_baseline_cancellation_and_request_status(self):
        root = Path(__file__).resolve().parents[1]
        source = (root / ".github/workflows/ci-command.yml").read_text()
        lint.command_queue_source(source)
        self.assertIn('context="ci-command/run-$GITHUB_RUN_ID/attempt-$GITHUB_RUN_ATTEMPT"', source)
        self.assertEqual(source.count("actions/runs/${{ github.run_id }}/attempts/${{ github.run_attempt }}"), 2)
        self.assertIn("needs.request.outputs.sha", source)
        self.assertIn("cancel-in-progress: true", (root / ".github/workflows/ci.yml").read_text())


if __name__ == "__main__":
    unittest.main()
