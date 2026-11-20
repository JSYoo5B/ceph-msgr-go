"""Family discovery and the workflow's family decision; no native run."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

import native_evidence as evidence
import scenario_plan

# The families that existed when the workflow listed their selectors itself,
# and that list. A later family never joins this set: the check below only
# shows that the step output reproduces the list for these families.
LISTED = {"capsend", "flock", "soak"}


def listed_expression(matrix):
    """The workflow's former condition, with GitHub's || and == semantics."""
    return (matrix.get("test_run") == "^TestCephMDSCephFSCapSendRecoveryIntegration$"
            or matrix.get("test_run") == "^TestCephMDSCephFSSharedFlockRecoveryIntegration$"
            or (matrix.get("osd_soak") or "") != "")


def planned_jobs():
    output = subprocess.run([sys.executable, str(scenario_plan.ROOT / "tools/scenario_plan.py"), "plan", "--select", "all"],
                            check=True, capture_output=True, text=True).stdout
    return json.loads(output)["include"]


def family_step(matrix):
    """Run the workflow step's command in process; (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, {"MATRIX_JSON": json.dumps(matrix)}), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = evidence.main(["family"])
    return code, out.getvalue(), err.getvalue()


FAMILY = '''
import native_evidence as evidence

KIND = {kind!r}
FILES = {{"tests.json", "runtime.json"}}
PROFILES = [dict(evidence.DEFAULTS, test_run={selector!r})]
CAPABILITIES = ""


def claims(matrix):
    return {claim}


def scenario_id(profile):
    return "example"


def selector(profile):
    return profile["test_run"]


def runtime_facts(raw, ctx):
    pass


def gate(raw_tests):
    return raw_tests


def project(receipts, tests, ctx):
    return {{}}


def validate(payloads, ctx):
    pass
'''


class FamilyDecisionTests(unittest.TestCase):
    def test_step_output_equals_the_former_expression_for_every_planned_job(self):
        jobs = planned_jobs()
        decided = {}
        for job in jobs:
            name = evidence.claimed_family(job)
            self.assertEqual(listed_expression(job), name in LISTED, job["scenario"])
            code, out, err = family_step(job)
            self.assertEqual((code, out, err), (0, "family=%s\n" % (name or ""), ""), job["scenario"])
            if name:
                decided.setdefault(name, []).append(job["scenario"])
        # Every listed family still claims its rows, so the check is not vacuous.
        self.assertLessEqual(LISTED, set(decided))
        self.assertTrue(any(evidence.claimed_family(job) is None for job in jobs))

    def test_every_admitted_profile_is_claimed_by_its_own_family_only(self):
        found = evidence.families()
        self.assertLessEqual(LISTED, set(found))
        for name, module in found.items():
            for profile in module.PROFILES:
                matrix = dict(profile, scenario=module.scenario_id(profile))
                self.assertEqual(evidence.claimed_family(matrix), name)
                self.assertIs(evidence.profile_family(profile), module)

    def test_family_step_fails_closed_on_an_unusable_matrix(self):
        for environment in ({}, {"MATRIX_JSON": "[]"}, {"MATRIX_JSON": "{"}):
            with self.subTest(environment=environment), mock.patch.dict(os.environ, environment, clear=True), \
                    contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(evidence.main(["family"]), 1)
                self.assertEqual(out.getvalue(), "")

    def test_a_matrix_two_families_claim_is_refused(self):
        matrix = {"test_run": "^TestCephMDSCephFSCapSendRecoveryIntegration$", "osd_soak": "4m"}
        with self.assertRaisesRegex(evidence.EvidenceError, "^ambiguous-evidence-family$"):
            evidence.claimed_family(matrix)
        code, out, err = family_step(matrix)
        self.assertEqual((code, out, err), (1, "", "ambiguous-evidence-family\n"))

    def test_workflow_conditions_ask_the_trusted_collector(self):
        text = scenario_plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        tested_checkout = text.index("ref: ${{ inputs.ref }}")
        self.assertLess(text.index('cp .trusted-native/tools/native_evidence_*.py "$RUNNER_TEMP/"'), tested_checkout)
        step = text[text.index("      - name: Ask the trusted collector for the job's evidence family"):tested_checkout]
        self.assertIn("id: evidence_family", step)
        self.assertIn("MATRIX_JSON: ${{ toJSON(matrix) }}", step)
        self.assertIn('python3 "$RUNNER_TEMP/native_evidence.py" family >> "$GITHUB_OUTPUT"', step)
        decision = "steps.evidence_family.outputs.family != ''"
        self.assertIn("if: steps.ceph.outcome == 'success' && %s\n" % decision, text)
        self.assertIn("if: always() && steps.ceph.outcome == 'success' && %s\n" % decision, text)
        self.assertIn("EVIDENCE_PILOT: ${{ %s }}\n" % decision, text)
        self.assertEqual(text.count(decision), 3)
        # No evidence step names a family's selector or matrix field.
        evidence_steps = text[text.index("      - name: Collect reviewed successful-native evidence"):
                              text.index("      - name: Preserve the scenario result")]
        for listed in ("matrix.test_run", "matrix.osd_soak", "Integration$"):
            self.assertNotIn(listed, evidence_steps)


class FamilyDeclarationTests(unittest.TestCase):
    def setUp(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        self.directory = Path(holder.name)
        sys.path.insert(0, str(self.directory))
        self.addCleanup(sys.path.remove, str(self.directory))
        self.names = []
        self.addCleanup(self.forget)

    def forget(self):
        for name in self.names:
            sys.modules.pop(name, None)

    def family(self, name, kind=None, selector="^TestExample$", claim='matrix.get("test_run") == "^TestExample$"', text=None):
        module = "native_evidence_" + name
        self.names.append(module)
        source = text if text is not None else FAMILY.format(kind=kind or "native-%s-success" % name, selector=selector, claim=claim)
        (self.directory / (module + ".py")).write_text(textwrap.dedent(source), encoding="utf-8")

    def problems(self):
        return evidence.declarations(self.directory)[1]

    def test_the_repository_families_have_no_declaration_problem(self):
        found, problems = evidence.declarations(Path(evidence.__file__).parent)
        self.assertEqual(problems, [])
        self.assertLessEqual(LISTED, set(found))

    def test_a_valid_family_is_discovered_by_its_file_name(self):
        self.family("example")
        found = evidence.discover(self.directory)
        self.assertEqual(list(found), ["example"])
        self.assertEqual(found["example"].KIND, "native-example-success")

    def test_an_incomplete_declaration_is_refused(self):
        self.family("partial", text="KIND = 'native-partial-success'\n")
        self.assertEqual(self.problems(), ["native_evidence_partial.py: missing FILES, PROFILES, CAPABILITIES, claims, "
                                           "scenario_id, selector, runtime_facts, gate, project, validate"])
        with self.assertRaisesRegex(evidence.EvidenceError, "^evidence-family-declaration$"):
            evidence.discover(self.directory)

    def test_bad_values_are_refused(self):
        source = FAMILY.format(kind="native-values-success", selector="^TestExample$", claim="True")
        self.family("values", text=source.replace('FILES = {"tests.json", "runtime.json"}', 'FILES = {"tests.json", "manifest.json"}')
                    .replace("PROFILES = [dict(evidence.DEFAULTS, test_run='^TestExample$')]", "PROFILES = [{'test_run': '^TestExample$'}]"))
        self.assertEqual(self.problems(), ["native_evidence_values.py: FILES needs tests.json and runtime.json and never manifest.json",
                                           "native_evidence_values.py: PROFILES needs complete normalized profiles"])

    def test_a_family_name_outside_the_rule_is_refused(self):
        self.family("Upper")
        self.assertIn("native_evidence_Upper.py: a family name is lowercase letters and digits", self.problems())

    def test_a_module_found_elsewhere_is_not_a_family(self):
        # The repository's capsend module is already imported, so this file
        # would not be the module the collector runs.
        self.family("capsend")
        self.assertEqual(self.problems(), ["native_evidence_capsend.py: imported from another location"])

    def test_two_families_admitting_one_profile_are_refused(self):
        self.family("one")
        self.family("two")
        self.assertEqual(self.problems(), [
            "one: an admitted profile is admitted by ['one', 'two'] and claimed by ['one', 'two']",
            "two: an admitted profile is admitted by ['one', 'two'] and claimed by ['one', 'two']"])

    def test_a_family_claiming_another_familys_row_is_refused(self):
        self.family("one")
        self.family("greedy", selector="^TestOther$", claim="True")
        self.assertIn("one: an admitted profile is admitted by ['one'] and claimed by ['greedy', 'one']", self.problems())

    def test_a_family_that_does_not_claim_its_own_row_is_refused(self):
        self.family("silent", claim="False")
        self.assertEqual(self.problems(), ["silent: an admitted profile is admitted by ['silent'] and claimed by []"])

    def test_two_families_with_one_kind_are_refused(self):
        self.family("one", kind="native-same-success")
        self.family("two", kind="native-same-success", selector="^TestOther$", claim='matrix.get("test_run") == "^TestOther$"')
        self.assertEqual(self.problems(), ["two families declare one KIND"])

    def test_an_empty_directory_has_no_family(self):
        self.assertEqual(self.problems(), ["no family module beside the collector"])


if __name__ == "__main__":
    unittest.main()
