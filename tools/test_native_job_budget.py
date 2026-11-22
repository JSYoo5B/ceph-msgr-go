"""The native job's one time budget reaches the fixture runner.

The native workflow bounds each job with timeout-minutes and gives the
fixture runner the job's end, less the reserve of the steps after the run
step, as CEPH_MSGR_JOB_DEADLINE (integration/fixture/job_budget.go). These
checks keep the run step's copy of the budget equal to the job's, the clock
first, and the variable the one the fixture reads.
"""
import re
import subprocess
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "native-scenarios.yml"
FIXTURE_BUDGET = ROOT / "integration" / "fixture" / "job_budget.go"


def job_text():
    text = WORKFLOW.read_text(encoding="utf-8")
    return text[text.index("\n  ceph:\n"):]


def step(text, name):
    start = text.index("      - name: %s\n" % name)
    end = text.find("\n      - ", start + 1)
    return text[start:] if end < 0 else text[start:end]


class NativeJobBudgetTests(unittest.TestCase):
    def test_the_run_step_copies_the_job_timeout_verbatim(self):
        text = job_text()
        job = re.findall(r"(?m)^    timeout-minutes: (.+)$", text)
        self.assertEqual(len(job), 1, job)
        run = step(text, "Real MON, MGR and opt-in OSD protocol verification")
        copy = re.findall(r"(?m)^          JOB_BUDGET_MINUTES: (.+)$", run)
        self.assertEqual(copy, job, "the run step's budget differs from the job's timeout-minutes")
        self.assertIn("JOB_STARTED: ${{ steps.job_clock.outputs.started }}", run)

    def test_the_clock_starts_before_every_other_step(self):
        text = job_text()
        steps = re.findall(r"(?m)^      - (?:name|uses): (.+)$", text)
        self.assertEqual(steps[0], "Start the job budget clock")
        clock = step(text, "Start the job budget clock")
        self.assertIn("id: job_clock", clock)
        self.assertIn('echo "started=$(date +%s)" >> "$GITHUB_OUTPUT"', clock)

    def test_the_deadline_keeps_the_post_run_reserve_and_reaches_the_runner(self):
        run = step(job_text(), "Real MON, MGR and opt-in OSD protocol verification")
        reserve = re.findall(r"(?m)^          POST_RUN_RESERVE=(\d+)$", run)
        self.assertEqual(len(reserve), 1)
        self.assertGreaterEqual(int(reserve[0]), 60)
        export = "export CEPH_MSGR_JOB_DEADLINE=$((JOB_STARTED + JOB_BUDGET_MINUTES * 60 - POST_RUN_RESERVE))"
        self.assertEqual(run.count(export), 1)
        self.assertLess(run.index(export), run.index("integration/run.sh"))
        fixture = FIXTURE_BUDGET.read_text(encoding="utf-8")
        self.assertIn('const jobDeadlineEnv = "CEPH_MSGR_JOB_DEADLINE"', fixture)

    def deadline_block(self):
        """The run step's lines from the reserve through the deadline check."""
        run = step(job_text(), "Real MON, MGR and opt-in OSD protocol verification")
        lines = [line[10:] for line in run.splitlines()]
        start = next(i for i, line in enumerate(lines) if line.startswith("POST_RUN_RESERVE="))
        export = next(i for i, line in enumerate(lines) if line.startswith("export CEPH_MSGR_JOB_DEADLINE="))
        end = next(i for i in range(export, len(lines)) if lines[i] == "fi")
        return "\n".join(lines[start:end + 1])

    def run_block(self, started, minutes):
        script = self.deadline_block() + '\nprintf %s "$CEPH_MSGR_JOB_DEADLINE"'
        return subprocess.run(["bash", "-eo", "pipefail", "-c", script], capture_output=True, text=True,
                              env={"PATH": "/usr/bin:/bin", "JOB_STARTED": started, "JOB_BUDGET_MINUTES": minutes})

    def test_the_deadline_arithmetic_is_the_job_end_less_the_reserve(self):
        reserve = int(re.search(r"POST_RUN_RESERVE=(\d+)", self.deadline_block())[1])
        now = int(time.time())
        for minutes in ("45", "120"):
            with self.subTest(minutes=minutes):
                result = self.run_block(str(now), minutes)
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(int(result.stdout), now + int(minutes) * 60 - reserve)

    def test_a_missing_clock_or_a_spent_budget_stops_before_the_runner(self):
        now = int(time.time())
        malformed = "is not a positive integer"
        for started, minutes, reason in (("", "45", malformed), ("x", "45", malformed), ("0", "45", malformed),
                                         (str(now), "", malformed), (str(now), "0", malformed),
                                         (str(now), "-5", malformed), (str(now), "45x", malformed),
                                         (str(now - 45 * 60), "45", "the job budget is spent")):
            with self.subTest(started=started, minutes=minutes):
                result = self.run_block(started, minutes)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("::error::", result.stdout)
                self.assertIn(reason, result.stdout)


if __name__ == "__main__":
    unittest.main()
