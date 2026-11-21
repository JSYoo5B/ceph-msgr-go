"""The native job's one time budget reaches the fixture runner.

The native workflow bounds each job with timeout-minutes and gives the
fixture runner the job's end, less the reserve of the steps after the run
step, as CEPH_MSGR_JOB_DEADLINE (integration/fixture/job_budget.go). These
checks keep the run step's copy of the budget equal to the job's, the clock
first, and the variable the one the fixture reads.
"""
import re
import subprocess
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

    def test_the_deadline_arithmetic_is_the_job_end_less_the_reserve(self):
        run = step(job_text(), "Real MON, MGR and opt-in OSD protocol verification")
        lines = [line.strip() for line in run.splitlines()
                 if line.strip().startswith(("POST_RUN_RESERVE=", "export CEPH_MSGR_JOB_DEADLINE="))]
        script = "\n".join(lines + ['printf %s "$CEPH_MSGR_JOB_DEADLINE"'])
        for minutes in ("45", "120"):
            with self.subTest(minutes=minutes):
                result = subprocess.run(["bash", "-eo", "pipefail", "-c", script], capture_output=True, text=True,
                                        env={"JOB_STARTED": "1000", "JOB_BUDGET_MINUTES": minutes}, check=True)
                reserve = int(re.search(r"POST_RUN_RESERVE=(\d+)", script)[1])
                self.assertEqual(int(result.stdout), 1000 + int(minutes) * 60 - reserve)


if __name__ == "__main__":
    unittest.main()
