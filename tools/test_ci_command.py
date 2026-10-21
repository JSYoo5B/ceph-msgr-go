import os
import tempfile
import unittest
from unittest import mock

import ci_command as cmd

SHA = "0123456789abcdef0123456789abcdef01234567"


class ParseTests(unittest.TestCase):
    def test_valid_commands(self):
        self.assertEqual(cmd.parse_comment("/ci run all"), ["all"])
        self.assertEqual(cmd.parse_comment("Please\n/ci run group:rbd rbd.aes.ip4  group:rbd\nthanks"), ["group:rbd", "rbd.aes.ip4"][:1] + ["rbd.aes.ip4"])
        self.assertEqual(cmd.parse_comment("/ci run a.b-c.ip4\r\n"), ["a.b-c.ip4"])

    def test_non_commands_are_ignored(self):
        for body in ("", "looks good", "see /ci run all", "/cirun all", " /ci run all", "/cinema run all"):
            self.assertIsNone(cmd.parse_comment(body), body)

    def test_fenced_and_quoted_commands_are_ignored(self):
        self.assertIsNone(cmd.parse_comment("```\n/ci run all\n```"))
        self.assertIsNone(cmd.parse_comment("~~~sh\n/ci run all\n~~~"))
        self.assertIsNone(cmd.parse_comment("> /ci run all\nok"))
        self.assertEqual(cmd.parse_comment("```\n/ci run x\n```\n/ci run all"), ["all"])

    def test_malformed_commands_are_refused(self):
        for body in ("/ci", "/ci run", "/ci status", "/ci run all; rm -rf /", "/ci run $(id)", "/ci run `x`",
                     "/ci run a|b", "/ci run UPPER", "/ci run group:", "/ci run all\n/ci run all",
                     "/ci run " + "x" * 161, "/ci run " + " ".join("s%d" % i for i in range(21)),
                     "/ci run á"):
            with self.assertRaises(cmd.CommandError, msg=body):
                cmd.parse_comment(body)

    def test_unterminated_fence_hides_the_rest(self):
        self.assertIsNone(cmd.parse_comment("```\n/ci run all"))

    def test_shorter_fences_and_info_strings_cannot_end_a_quote(self):
        for fence in ("````", "~~~~"):
            body = fence + "\n" + fence[:3] + "\n/ci run all\n" + fence
            self.assertIsNone(cmd.parse_comment(body))
        self.assertIsNone(cmd.parse_comment("```\n```sh\n/ci run all\n```"))
        self.assertEqual(cmd.parse_comment("````\n```\n/ci run quoted\n`````\n/ci run all"), ["all"])


class AuthorizationTests(unittest.TestCase):
    def test_trusted_associations_on_pull_requests_only(self):
        for who in ("OWNER", "MEMBER", "COLLABORATOR"):
            self.assertIsNone(cmd.authorize(who, True))
        for who in ("CONTRIBUTOR", "FIRST_TIMER", "FIRST_TIME_CONTRIBUTOR", "NONE", "", None):
            self.assertIn("only repository owners", cmd.authorize(who, True))
        self.assertIn("only on pull requests", cmd.authorize("OWNER", False))

    def test_pull_request_checks(self):
        self.assertIsNone(cmd.check_pull("o/r", "o/r", SHA, "open"))
        self.assertIn("forks", cmd.check_pull("x/r", "o/r", SHA, "open"))
        self.assertIn("forks", cmd.check_pull("", "o/r", SHA, "open"))
        self.assertIn("not open", cmd.check_pull("o/r", "o/r", SHA, "closed"))
        self.assertIn("full commit", cmd.check_pull("o/r", "o/r", "abc", "open"))
        self.assertIn("full commit", cmd.check_pull("o/r", "o/r", SHA.upper(), "open"))
        self.assertIn("full commit", cmd.check_pull("o/r", "o/r", None, "open"))


class OutputTests(unittest.TestCase):
    def run_main(self, command, **env):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "out")
            with mock.patch.dict(os.environ, dict(env, GITHUB_OUTPUT=path), clear=True):
                self.assertEqual(cmd.main([command]), 0)
            with open(path, encoding="utf-8") as handle:
                return dict(line.rstrip("\n").split("=", 1) for line in handle)

    def test_parse_outcomes(self):
        out = self.run_main("parse", COMMENT_BODY="/ci run all", AUTHOR_ASSOCIATION="OWNER", IS_PULL_REQUEST="true")
        self.assertEqual((out["command"], out["selectors"]), ("accepted", "all"))
        out = self.run_main("parse", COMMENT_BODY="/ci run all", AUTHOR_ASSOCIATION="NONE", IS_PULL_REQUEST="true")
        self.assertEqual(out["command"], "refused")
        out = self.run_main("parse", COMMENT_BODY="/ci run all", AUTHOR_ASSOCIATION="OWNER", IS_PULL_REQUEST="false")
        self.assertEqual(out["command"], "refused")
        out = self.run_main("parse", COMMENT_BODY="hello", AUTHOR_ASSOCIATION="NONE", IS_PULL_REQUEST="true")
        self.assertEqual(out["command"], "none")
        out = self.run_main("parse", COMMENT_BODY="/ci run a b\n", AUTHOR_ASSOCIATION="MEMBER", IS_PULL_REQUEST="true")
        self.assertEqual(out["selectors"], "a b")

    def test_unauthorized_comment_with_bad_syntax_is_refused_without_running(self):
        out = self.run_main("parse", COMMENT_BODY="/ci run $(x)", AUTHOR_ASSOCIATION="NONE", IS_PULL_REQUEST="true")
        self.assertEqual((out["command"], out["selectors"]), ("refused", ""))

    def test_pull_outcomes(self):
        out = self.run_main("pull", HEAD_REPO="o/r", BASE_REPO="o/r", HEAD_SHA=SHA, PR_STATE="open")
        self.assertEqual(out["pull"], "accepted")
        out = self.run_main("pull", HEAD_REPO="f/r", BASE_REPO="o/r", HEAD_SHA=SHA, PR_STATE="open")
        self.assertEqual(out["pull"], "refused")

    def manual(self, **changes):
        env = dict(GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_REF="refs/heads/main",
                   DEFAULT_BRANCH="main", DISPATCH_PR="", DISPATCH_SELECTORS="unit")
        env.update(changes)
        return self.run_main("request", **env)

    def test_manual_selection_targets_main_or_one_positive_pr_number(self):
        out = self.manual(DISPATCH_SELECTORS="group:rbd unit unit")
        self.assertEqual((out["command"], out["target"], out["number"], out["selectors"]),
                         ("accepted", "main", "", "group:rbd unit"))
        out = self.manual(DISPATCH_PR="42")
        self.assertEqual((out["command"], out["target"], out["number"]), ("accepted", "pull", "42"))

    def test_manual_controller_requires_the_default_branch_not_a_pr_ref(self):
        for ref in ("refs/heads/feature", "refs/pull/42/merge", "refs/tags/main", ""):
            out = self.manual(GITHUB_REF=ref, DISPATCH_PR="42")
            self.assertEqual((out["command"], out["number"]), ("refused", ""))
            self.assertIn("default branch", out["reason"])
        self.assertEqual(self.manual(DEFAULT_BRANCH="", GITHUB_REF="refs/heads/")["command"], "refused")

    def test_manual_pr_number_cannot_be_an_api_path_or_shell_fragment(self):
        for number in ("0", "01", "-1", "1.0", "1/commits", "1?state=all", "1\n", " 42", "9" * 11, "$(id)"):
            out = self.manual(DISPATCH_PR=number)
            self.assertEqual((out["command"], out["number"]), ("refused", ""), number)

    def test_manual_selectors_share_comment_grammar_without_multiline_commands(self):
        for selection in ("", " ", "unit\n", "unit\rall", "/ci run unit", "unit; id", "$(id)",
                          "```unit```", "UPPER", "x" * 4097, " ".join("s%d" % i for i in range(21))):
            out = self.manual(DISPATCH_SELECTORS=selection)
            self.assertEqual((out["command"], out["selectors"]), ("refused", ""), selection[:40])
        for selection in ("all", "group:checks", "mon-ping.aes.ip4.crc.host.t-monping"):
            self.assertEqual(self.manual(DISPATCH_SELECTORS=selection)["selectors"], selection)

    def test_normalized_comment_request_keeps_authorization_and_target(self):
        env = dict(GITHUB_EVENT_NAME="issue_comment", COMMENT_BODY="/ci run unit", ISSUE_NUMBER="42",
                   AUTHOR_ASSOCIATION="OWNER", IS_PULL_REQUEST="true")
        out = self.run_main("request", **env)
        self.assertEqual((out["command"], out["target"], out["number"]), ("accepted", "pull", "42"))
        out = self.run_main("request", **dict(env, AUTHOR_ASSOCIATION="NONE"))
        self.assertEqual(out["command"], "refused")
        out = self.run_main("request", **dict(env, IS_PULL_REQUEST="false"))
        self.assertEqual(out["command"], "refused")
        out = self.run_main("request", **dict(env, COMMENT_BODY="looks good"))
        self.assertEqual((out["command"], out["number"]), ("none", ""))
        out = self.run_main("request", **dict(env, ISSUE_NUMBER="42/commits"))
        self.assertEqual((out["command"], out["number"]), ("refused", ""))
        out = self.run_main("request", GITHUB_EVENT_NAME="push")
        self.assertEqual(out["command"], "refused")

    def test_missing_pr_metadata_is_a_refusal_without_a_commit(self):
        out = self.run_main("pull", PULL_LOOKUP_ERROR="pull request metadata is unavailable")
        self.assertEqual(out, {"pull": "refused", "reason": "pull request metadata is unavailable"})

    def test_manual_and_comment_selection_share_the_catalog(self):
        import scenario_plan as plan
        selection = "mon-ping.aes.ip4.crc.host.t-monping"
        manual = self.manual(DISPATCH_PR="42", DISPATCH_SELECTORS=selection)
        comment = self.run_main("request", GITHUB_EVENT_NAME="issue_comment", ISSUE_NUMBER="42",
                                COMMENT_BODY="/ci run " + selection, AUTHOR_ASSOCIATION="OWNER", IS_PULL_REQUEST="true")
        self.assertEqual(manual, comment)
        chosen, _ = plan.select(plan.load_catalog(), manual["selectors"].split())
        matrix = plan.matrix(chosen)
        self.assertEqual(matrix["fixed"], [])
        self.assertEqual(len(matrix["include"]), 1)
        self.assertEqual(matrix["include"][0]["test_run"], "^TestCephMonPingIntegration$")

    def test_manual_dispatch_preserves_the_trusted_workflow_boundary(self):
        from pathlib import Path
        text = (Path(__file__).resolve().parents[1] / ".github/workflows/ci-command.yml").read_text(encoding="utf-8")
        refusal = text.split("  refuse-branch:", 1)[1].split("  request:", 1)[0]
        self.assertNotIn("actions/checkout", refusal)
        self.assertNotIn(": write", refusal)
        self.assertIn("github.ref != format('refs/heads/{0}'", refusal)
        self.assertIn("ref: ${{ github.workflow_sha }}", text)
        self.assertIn("DISPATCH_PR: ${{ inputs.pull_request }}", text)
        self.assertIn("DISPATCH_SELECTORS: ${{ inputs.selectors }}", text)
        self.assertIn('echo "sha=$(git rev-parse HEAD)"', text)
        self.assertIn('if test -n "$NUMBER"; then', text)
        self.assertIn('cat "$RUNNER_TEMP/final.md" >> "$GITHUB_STEP_SUMMARY"', text)

    def test_multiline_output_is_rejected(self):
        with self.assertRaises(ValueError):
            cmd.write_output(reason="a\nb")

    def test_usage_error(self):
        self.assertEqual(cmd.main([]), 2)
        self.assertEqual(cmd.main(["other"]), 2)


if __name__ == "__main__":
    unittest.main()
