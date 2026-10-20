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

    def test_multiline_output_is_rejected(self):
        with self.assertRaises(ValueError):
            cmd.write_output(reason="a\nb")

    def test_usage_error(self):
        self.assertEqual(cmd.main([]), 2)
        self.assertEqual(cmd.main(["other"]), 2)


if __name__ == "__main__":
    unittest.main()
