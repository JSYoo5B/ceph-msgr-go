import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import go_test_annotations as parser


MODULE = "github.com/jsyoo5b/ceph-msgr-go"
PACKAGE = MODULE + "/cephmsgr"


def event(action, test=None, output=None, package=PACKAGE, **fields):
    value = {"Action": action, "Package": package}
    if test is not None:
        value["Test"] = test
    if output is not None:
        value["Output"] = output
    value.update(fields)
    return json.dumps(value) + "\n"


def failed(test="TestFailure", message="assertion", package=PACKAGE):
    return [event("run", test, package=package),
            event("output", test, "    failure_test.go:42: " + message + "\n", package=package),
            event("fail", test, package=package), event("fail", package=package)]


class GoTestAnnotationsTest(unittest.TestCase):
    def test_success_never_emits_annotations(self):
        self.assertEqual(parser.annotations(failed() + ["malformed"], 0), [])

    def test_packages_with_the_same_test_name_keep_their_own_diagnostics(self):
        other = MODULE + "/internal/session"
        log = [event("run", "TestSame"), event("run", "TestSame", package=other),
               event("output", "TestSame", "    client_test.go:3: client assertion\n"),
               event("output", "TestSame", "    session_test.go:7: session assertion\n", package=other),
               event("fail", "TestSame", package=other), event("fail", "TestSame"),
               event("fail", package=other), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 4)
        self.assertIn("session_test.go:7: session assertion", result[1])
        self.assertIn("session assertion", result[1])
        self.assertNotIn("client assertion", result[1])
        self.assertIn("client_test.go:3: client assertion", result[3])
        self.assertNotIn(" file=", "\n".join(result))
        self.assertNotIn("session assertion", result[3])

    def test_parallel_tests_do_not_publish_passing_test_logs(self):
        log = [event("run", "TestFail"), event("pause", "TestFail"),
               event("run", "TestPass"), event("pause", "TestPass"),
               event("cont", "TestFail"), event("cont", "TestPass"),
               event("output", "TestFail", "    fail_test.go:5: missing result\n"),
               event("output", "TestPass", "    pass_test.go:9: PRIVATE_PASS_LOG\n"),
               event("output", "TestFail", "        unexpected EOF\n"),
               event("pass", "TestPass"), event("fail", "TestFail"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 2)
        self.assertIn("missing result%0Aunexpected EOF", result[1])
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_nested_tests_are_reported_without_borrowing_parent_output(self):
        log = [event("run", "TestParent"),
               event("output", "TestParent", "    parent_test.go:1: parent context\n"),
               event("run", "TestParent/MON/timeout"),
               event("output", "TestParent/MON/timeout", "    child_test.go:2: child failure\n"),
               event("fail", "TestParent/MON/timeout"), event("fail", "TestParent"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 4)
        self.assertIn("TestParent/MON/timeout", result[1])
        self.assertIn("child failure", result[1])
        self.assertNotIn("parent context", result[1])
        self.assertIn("parent context", result[3])

    def test_panic_keeps_prior_assertion_but_omits_raw_panic_and_stack(self):
        log = [event("run", "TestPanic"),
               event("output", "TestPanic", "    panic_test.go:12: reached renewal\n"),
               event("output", "TestPanic", "panic: PRIVATE_PANIC_VALUE\n"),
               event("output", "TestPanic", "goroutine 5 [running]:\n"),
               event("output", "TestPanic", "\t/private/PRIVATE_PATH/key.go:99 +0x12\n"),
               event("output", "TestPanic", "        PRIVATE_STACK_TAIL\n"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 3)
        self.assertIn("without a terminal test event", result[0])
        self.assertIn("panic_test.go:12: reached renewal", result[1])
        self.assertIn("reached renewal", result[1])
        self.assertNotIn("PRIVATE_", "\n".join(result))
        self.assertNotIn("OOM", "\n".join(result))

    def test_abrupt_exit_reports_each_unfinished_package_and_test(self):
        other = MODULE + "/internal/cephx"
        log = [event("run", "TestA"), event("pause", "TestA"),
               event("run", "TestB", package=other),
               event("output", "TestB", "    auth_test.go:8: key type checked\n", package=other)]
        result = parser.annotations(log, 137)
        self.assertEqual(len(result), 3)
        self.assertIn(PACKAGE + ": TestA", result[0])
        self.assertIn("exit code 137", result[0])
        self.assertIn(other + ": TestB", result[1])
        self.assertIn("auth_test.go:8: key type checked", result[2])

    def test_completed_failure_does_not_hide_a_later_abrupt_test(self):
        log = failed()[:-1] + [event("run", "TestInterrupted"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 3)
        self.assertIn("TestFailure", result[0])
        self.assertIn("TestInterrupted ended without", result[2])

    def test_package_and_build_failure_do_not_dump_arbitrary_output(self):
        for log in ([event("start"), event("output", output="PRIVATE_COMPILER_OUTPUT\n"), event("fail")],
                    [json.dumps({"Action": "build-output", "ImportPath": PACKAGE,
                                 "Output": "PRIVATE_SOURCE_CODE\n"}) + "\n",
                     json.dumps({"Action": "build-fail", "ImportPath": PACKAGE + " [" + PACKAGE + ".test]"}) + "\n",
                     event("fail")]):
            with self.subTest(log=log):
                result = parser.annotations(log, 1)
                self.assertEqual(len(result), 1)
                self.assertIn("Go package failed: " + PACKAGE, result[0])
                self.assertNotIn("PRIVATE_", result[0])

    def test_passing_and_skipped_assertions_are_discarded(self):
        for action in ("pass", "skip"):
            log = [event("run", "TestDone"),
                   event("output", "TestDone", "    done_test.go:1: PRIVATE_DONE_LOG\n"),
                   event(action, "TestDone"), event("pass")]
            result = parser.annotations(log, 1)
            self.assertEqual(len(result), 1)
            self.assertIn("no failed test or package event", result[0])
            self.assertNotIn("PRIVATE_", result[0])

    def test_unknown_package_has_no_invented_repository_location(self):
        result = parser.annotations(failed(package="example.invalid/elsewhere"), 1)
        self.assertEqual(len(result), 2)
        self.assertNotIn(" file=", "\n".join(result))
        self.assertIn("example.invalid/elsewhere: TestFailure", result[1])
        self.assertIn("assertion", result[1])
        root = parser.annotations(failed(package=MODULE), 1)
        self.assertIn("failure_test.go:42: assertion", root[1])
        self.assertNotIn(" file=", "\n".join(root))
        similar = parser.annotations(failed(package=MODULE + "-other"), 1)
        self.assertNotIn(" file=", "\n".join(similar))

    def test_unsafe_package_or_assertion_paths_do_not_become_file_properties(self):
        for package in ("../PRIVATE_PATH", "/private/PRIVATE_PATH", PACKAGE + "/../PRIVATE_PATH",
                        PACKAGE + ",line=99", PACKAGE + "\nPRIVATE_PATH"):
            result = parser.annotations(failed(package=package), 1)
            self.assertEqual(len(result), 1)
            self.assertNotIn("PRIVATE_", result[0])
        for filename in ("../PRIVATE_PATH/key_test.go", "/private/key_test.go", "C:\\private\\key_test.go"):
            log = [event("run", "TestPath"),
                   event("output", "TestPath", f"    {filename}:1: PRIVATE_PATH_MESSAGE\n"),
                   event("fail", "TestPath"), event("fail")]
            result = parser.annotations(log, 1)
            self.assertEqual(len(result), 1)
            self.assertNotIn("PRIVATE_", result[0])

    def test_fullpath_helper_uses_its_actual_repository_location(self):
        with tempfile.TemporaryDirectory(prefix="go json repo ") as directory:
            helper = Path(directory) / "internal" / "test helper" / "helper.go"
            helper.parent.mkdir(parents=True)
            helper.touch()
            log = [event("run", "TestImportsHelper"),
                   event("output", "TestImportsHelper", f"    {helper}:5: helper assertion\n"),
                   event("fail", "TestImportsHelper"), event("fail")]
            result = parser.annotations(log, 1, directory)
            self.assertEqual(len(result), 2)
            self.assertIn("file=internal/test helper/helper.go,line=5", result[1])
            self.assertNotIn("file=cephmsgr/helper.go", result[1])
            self.assertNotIn(directory, "\n".join(result))
            # Fullpath establishes the source independently of Package.
            unknown = [event("run", "TestUnknown", package="example.invalid/elsewhere"),
                       event("output", "TestUnknown", f"    {helper}:7: helper assertion\n", package="example.invalid/elsewhere"),
                       event("fail", "TestUnknown", package="example.invalid/elsewhere")]
            self.assertIn("file=internal/test helper/helper.go,line=7", parser.annotations(unknown, 1, directory)[1])

    def test_fullpath_outside_repository_and_symlink_escape_are_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"
            outside = Path(directory) / "PRIVATE_OUTSIDE"
            root.mkdir()
            outside.mkdir()
            source = outside / "helper.go"
            source.touch()
            for location in (str(source), str(root / ".." / "PRIVATE_OUTSIDE" / "helper.go")):
                log = [event("run", "TestOutside"),
                       event("output", "TestOutside", f"    {location}:5: PRIVATE_OUTSIDE_ASSERTION\n"),
                       event("output", "TestOutside", "        PRIVATE_CONTINUATION\n"),
                       event("fail", "TestOutside"), event("fail")]
                result = parser.annotations(log, 1, root)
                self.assertEqual(len(result), 1)
                self.assertNotIn("PRIVATE_", "\n".join(result))
            link = root / "escape"
            try:
                link.symlink_to(outside, target_is_directory=True)
            except OSError:
                return  # Windows runners may not permit symlink creation.
            self.assertIsNone(parser.source_location(str(link / "helper.go"), root))

    def test_windows_fullpaths_support_spaces_case_and_containment(self):
        root = r"C:\Users\runner\ceph project"
        for source in (r"C:\Users\runner\ceph project\internal\test helper\helper.go",
                       "c:/users/RUNNER/ceph project/internal/test helper/helper.go"):
            log = [event("run", "TestWindows"),
                   event("output", "TestWindows", f"    {source}:5: windows assertion\n"),
                   event("fail", "TestWindows"), event("fail")]
            result = parser.annotations(log, 1, root)
            self.assertIn("file=internal/test helper/helper.go,line=5", result[1])
            self.assertNotIn("C:", "\n".join(result))
        for source in (r"C:\Users\runner\ceph project-other\PRIVATE\helper.go",
                       r"C:\Users\runner\ceph project\..\PRIVATE\helper.go",
                       r"D:\PRIVATE\helper.go", r"C:PRIVATE\helper.go"):
            self.assertIsNone(parser.source_location(source, root))

    def test_repository_file_properties_escape_commas_colons_and_percent(self):
        root = r"C:\repo"
        source = root + r"\test,dir\helper% name_test.go"
        log = [event("run", "TestProperty"),
               event("output", "TestProperty", f"    {source}:5: assertion\n"),
               event("fail", "TestProperty"), event("fail")]
        result = parser.annotations(log, 1, root)
        self.assertIn("file=test%2Cdir/helper%25 name_test.go,line=5", result[1])
        self.assertEqual(parser.escape_property("dir:file.go"), "dir%3Afile.go")
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "helper,colon:% name_test.go"
            if parser.os.name == "nt":
                return  # A literal colon is not valid in Windows filenames.
            source.touch()
            log = [event("run", "TestUnixProperty"),
                   event("output", "TestUnixProperty", f"    {source}:7: assertion\n"),
                   event("fail", "TestUnixProperty"), event("fail")]
            result = parser.annotations(log, 1, directory)
            self.assertIn("file=helper%2Ccolon%3A%25 name_test.go,line=7", result[1])

    def test_workflow_characters_and_multiline_assertions_are_escaped(self):
        log = [event("run", "TestPercent/50%"),
               event("output", "TestPercent/50%", "    percent_test.go:7: 50%\rnext\n"),
               event("output", "TestPercent/50%", "        ::error::still assertion\n"),
               event("fail", "TestPercent/50%"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertIn("TestPercent/50%25", result[0])
        self.assertIn("50%25%0Dnext%0A::error::still assertion", result[1])
        self.assertEqual(result[1].count("\n"), 0)

    def test_unrelated_stdout_and_json_are_not_assertion_continuations(self):
        for output in ("PRIVATE_STDOUT\n", "        {\"key\":\"PRIVATE_JSON\"}\n"):
            log = [event("run", "TestRaw"),
                   event("output", "TestRaw", "    raw_test.go:1: assertion\n"),
                   event("output", "TestRaw", output),
                   event("output", "TestRaw", "        PRIVATE_AFTER_RAW\n"),
                   event("fail", "TestRaw"), event("fail")]
            result = parser.annotations(log, 1)
            self.assertEqual(len(result), 2)
            self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_ipv6_endpoint_causes_and_following_joined_errors_are_preserved(self):
        for endpoint in ("[::1]:33300", "[fe80::1%3]:33300/7", "[::ffff:192.0.2.1]:3300"):
            log = [event("run", "TestJoined"),
                   event("output", "TestJoined", "    joined_test.go:3: MON rejected\n"),
                   event("output", "TestJoined", "        " + endpoint + ": authentication code -13\n"),
                   event("output", "TestJoined", "        unexpected EOF\n"),
                   event("fail", "TestJoined"), event("fail")]
            result = parser.annotations(log, 1)
            self.assertIn(endpoint.replace("%", "%25") + ": authentication code -13%0Aunexpected EOF", result[1])

    def test_invalid_record_cannot_bridge_a_previous_assertion_continuation(self):
        log = [event("run", "TestInvalid"),
               event("output", "TestInvalid", "    invalid_test.go:3: assertion\n"),
               '{"malformed":\n', event("output", "TestInvalid", "        PRIVATE_AFTER_INVALID\n"),
               event("fail", "TestInvalid"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 2)
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_fragmented_output_is_reassembled_per_test(self):
        log = [event("run", "TestChunks"), event("run", "TestOther"),
               event("output", "TestChunks", "    chunk_"),
               event("output", "TestOther", "    other_test.go:2: PRIVATE_OTHER\n"),
               event("output", "TestChunks", "test.go:3: split"),
               event("output", "TestChunks", " assertion\n        joined"),
               event("output", "TestChunks", " cause\n"), event("pass", "TestOther"),
               event("fail", "TestChunks"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 2)
        self.assertIn("chunk_test.go:3: split assertion", result[1])
        self.assertNotIn(" file=", result[1])
        self.assertIn("split assertion%0Ajoined cause", result[1])
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_interleaved_build_output_does_not_clear_another_test_fragment(self):
        other = MODULE + "/internal/session"
        log = [event("run", "TestChunks"),
               event("output", "TestChunks", "    chunk_test.go:3: split"),
               json.dumps({"Action": "build-output", "ImportPath": other,
                           "Output": "PRIVATE_COMPILER_OUTPUT\n"}) + "\n",
               event("output", "TestChunks", " assertion\n        joined cause\n"),
               event("fail", "TestChunks"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 2)
        self.assertIn("split assertion%0Ajoined cause", result[1])
        self.assertNotIn("PRIVATE_", "\n".join(result))
        self.assertNotIn(other, "\n".join(result))

    def test_long_fragmented_assertion_and_utf8_escaping_are_bounded(self):
        log = [event("run", "TestLong"), event("output", "TestLong", "    long_test.go:3: ")]
        log += [event("output", "TestLong", "☃%" * 500)] * 20
        log += [event("output", "TestLong", "\n        PRIVATE_OMITTED\n"),
                event("fail", "TestLong"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), 2)
        prefix, message = result[1].split("::", 2)[1:]
        self.assertLessEqual(len(message.encode("utf-8")), parser.MESSAGE_LIMIT)
        self.assertTrue(message.endswith("%0A[diagnostic truncated]"))
        self.assertNotIn("%2%0A", message)
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_assertion_count_keeps_bounded_recent_records(self):
        log = [event("run", "TestMany")]
        log += [event("output", "TestMany", f"    many_test.go:1: assertion {number}\n")
                for number in range(parser.DIAGNOSTIC_LIMIT + 2)]
        log += [event("fail", "TestMany"), event("fail")]
        result = parser.annotations(log, 1)
        self.assertEqual(len(result), parser.DIAGNOSTIC_LIMIT + 2)
        self.assertIn("assertion 2", result[1])
        self.assertNotIn("assertion 0", "\n".join(result))
        self.assertIn("earlier assertion diagnostics omitted", result[-1])

    def test_annotation_and_active_test_counts_are_bounded(self):
        result = parser.annotations([line for number in range(parser.ANNOTATION_LIMIT)
                                     for line in failed(test=f"Test{number}")], 1)
        self.assertEqual(len(result), parser.ANNOTATION_LIMIT + 1)
        self.assertIn("additional test diagnostics omitted", result[-1])
        log = [event("run", f"TestActive{number}") for number in range(parser.TEST_LIMIT + 2)]
        result = parser.annotations(log, 137)
        self.assertLessEqual(len(result), parser.ANNOTATION_LIMIT + 1)
        self.assertIn("additional test diagnostics omitted", result[-1])
        log = [event("start", package=f"example.invalid/package{number}")
               for number in range(parser.PACKAGE_LIMIT + 2)]
        result = parser.annotations(log, 1)
        self.assertLessEqual(len(result), parser.PACKAGE_LIMIT + 1)
        self.assertIn("additional test diagnostics omitted", result[-1])

    def test_malformed_events_do_not_crash_or_hide_a_later_valid_failure(self):
        invalid = ["not JSON", "[]", "{", '{"Action":"run","Action":"fail"}',
                   json.dumps({"Action": [], "Package": PACKAGE}),
                   event("run", False), event("output", "TestInvalid", ["PRIVATE_WRONG_TYPE"]),
                   event("output", "TestInvalid", "\ud800"),
                   event("run", "TestInvalid", package=["PRIVATE_WRONG_PACKAGE"]),
                   '{"unknown":' + '[' * 2000 + '"PRIVATE_DEEP"' + ']' * 2000 + '}',
                   '{"unknown":' + '9' * 10000 + '}',
                   '{"Action":"run","Package":NaN}', "x" * (parser.EVENT_LIMIT + 1)]
        result = parser.annotations(invalid + failed(), 1)
        self.assertEqual(len(result), 2)
        self.assertIn("assertion", result[1])
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_oversized_record_tail_is_not_parsed_as_a_new_event(self):
        stream = io.StringIO("x" * (parser.EVENT_LIMIT + 1) + event("fail", "TestTail")
                             + "".join(failed()))
        result = parser.annotations(parser.bounded_lines(stream), 1)
        self.assertEqual(len(result), 2)
        self.assertNotIn("TestTail", "\n".join(result))

    def test_oversized_stdout_breaks_a_previous_assertion_continuation(self):
        content = "".join([event("run", "TestOversized"),
                           event("output", "TestOversized", "    oversize_test.go:3: assertion\n"),
                           event("output", "TestOversized", "x" * (parser.EVENT_LIMIT + 1)),
                           event("output", "TestOversized", "        PRIVATE_AFTER_RAW_STDOUT\n"),
                           event("fail", "TestOversized"), event("fail")])
        result = parser.annotations(parser.bounded_lines(io.StringIO(content)), 1)
        self.assertEqual(len(result), 2)
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_cli_reads_stdin_and_log_files_with_the_supplied_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "actual helper" / "helper_test.go"
            source.parent.mkdir()
            source.touch()
            content = "".join([event("run", "TestFullpath"),
                               event("output", "TestFullpath", f"    {source}:42: assertion\n"),
                               event("fail", "TestFullpath"), event("fail")])
            log = Path(directory) / "go-tests.jsonl"
            log.write_text(content, encoding="utf-8")
            for path in ("-", str(log)):
                captured = io.StringIO()
                with patch("sys.argv", ["go_test_annotations.py", path, "1", "--repo-root", directory]), \
                        patch("sys.stdin", io.StringIO(content)), contextlib.redirect_stdout(captured):
                    parser.main()
                self.assertIn("Go test failed", captured.getvalue())
                self.assertIn("file=actual helper/helper_test.go,line=42", captured.getvalue())
        with patch("sys.argv", ["go_test_annotations.py", "-", "256"]), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.main()


if __name__ == "__main__":
    unittest.main()
