import io
import json
import unittest

import ci_failure_annotations as parser
from ci_failure_annotations import annotations


def crash_metadata(process="ceph-mgr", **fields):
    metadata = {
        "process_name": process,
        "entity_name": "mgr.a" if process == "ceph-mgr" else "mon.b",
        "timestamp": "2026-10-02T05:35:20.123456Z",
        "crash_id": "2026-10-02T05:35:20.123456Z_01234567-89ab-4cde-8123-456789abcdef",
        "ceph_version": "20.2.4",
        "backtrace": [
            "(ceph::ceph_assert_fail(char const*, char const*, int, char const*)+0x12e) [0x1234]",
            "raise()",
        ],
    }
    metadata.update(fields)
    return json.dumps(metadata, separators=(",", ":")) + "\n"


class FailureAnnotationsTest(unittest.TestCase):
    def test_completed_failure_keeps_only_current_test_diagnostics(self):
        log = io.StringIO("=== RUN   TestEarlier\n    old_test.go:1: old result\n"
                          "--- PASS: TestEarlier (1s)\n=== RUN   TestCurrent\n"
                          "    current_test.go:42: server rejected command\n"
                          "--- FAIL: TestCurrent (1s)\nFAIL\n")
        self.assertEqual(annotations(log, 1), [
            "::error::--- FAIL: TestCurrent (1s)",
            "::error file=current_test.go,line=42::server rejected command",
        ])

    def test_abrupt_exit_reports_last_test_and_bounded_progress(self):
        lines = ["=== RUN   TestEarlier\n", "    old_test.go:1: old result\n",
                 "--- PASS: TestEarlier (1s)\n", "=== RUN   TestStress\n"]
        lines.extend(f"    stress_test.go:12: calls={i}\n" for i in range(12))
        result = annotations(lines, 137)
        self.assertEqual(len(result), 11)
        self.assertIn("code 137", result[0])
        self.assertIn("Last started Go test: TestStress", result[0])
        self.assertEqual(result[1], "::error file=stress_test.go,line=12::calls=2")
        self.assertEqual(result[-1], "::error file=stress_test.go,line=12::calls=11")
        self.assertNotIn("old result", "\n".join(result))
        self.assertNotIn("OOM", "\n".join(result))

    def test_bootstrap_failure_does_not_invent_a_running_test(self):
        result = annotations(["docker pull failed\n"], 125)
        self.assertEqual(len(result), 1)
        self.assertIn("code 125", result[0])
        self.assertIn("No Go test start was observed", result[0])

    def test_suite_paths_and_diagnostics_follow_the_current_package(self):
        log = ["Running Ceph test suite: api\n", "=== RUN   TestPublic\n",
               "    commands_test.go:42: public command failed\n", "--- FAIL: TestPublic\n",
               "Running Ceph test suite: client\n", "=== RUN   TestPrivate\n",
               "    integration_pause_test.go:7: internal session failed\n", "--- FAIL: TestPrivate\n"]
        result = annotations(log, 1)
        self.assertEqual(result[1], "::error file=integration/commands_test.go,line=42::public command failed")
        self.assertEqual(result[3], "::error file=cephmsgr/integration_pause_test.go,line=7::internal session failed")
        abrupt = annotations(log[:3] + ["--- PASS: TestPublic\n", "Running Ceph test suite: client\n"], 137)
        self.assertEqual(len(abrupt), 1)
        self.assertIn("No Go test start was observed", abrupt[0])

    def test_success_does_not_emit_failure_annotations(self):
        self.assertEqual(annotations(["=== RUN   TestOK\n--- PASS: TestOK\n"], 0), [])

    def test_diagnostic_workflow_command_characters_are_escaped(self):
        log = ["=== RUN   TestFailure\n", "    file_test.go:7: 50%\rnext\n",
               "--- FAIL: TestFailure\n"]
        self.assertEqual(annotations(log, 1)[1], "::error file=file_test.go,line=7::50%25%0Dnext")

    def test_joined_error_preserves_each_endpoint_cause(self):
        log = ["Running Ceph test suite: api\n", "=== RUN   TestRejection\n",
               "    auth_recovery_test.go:113: authentication rejected\n",
               "ceph messenger authentication: code -13\n",
               "unexpected EOF\n", "    cipher tag 50% mismatch\n",
               "--- FAIL: TestRejection (1s)\n", "FAIL\n",
               "daemon diagnostic after the suite\n"]
        result = annotations(log, 1)
        self.assertEqual(result[1], "::error file=integration/auth_recovery_test.go,line=113::authentication rejected%0Aceph messenger authentication: code -13%0Aunexpected EOF%0A    cipher tag 50%25 mismatch")
        self.assertNotIn("daemon diagnostic", "\n".join(result))

    def test_multiline_diagnostic_is_bounded_and_marks_truncation(self):
        log = ["=== RUN   TestBound\n", "    bound_test.go:7: assertion\n",
               "x" * 20000 + "\n", "omitted continuation\n", "--- FAIL: TestBound\n"]
        result = annotations(log, 1)
        self.assertTrue(result[1].endswith("%0A[diagnostic truncated]"))
        self.assertLess(len(result[1]), 8300)
        self.assertNotIn("omitted continuation", result[1])

    def test_suite_transition_does_not_become_error_continuation(self):
        log = ["Running Ceph test suite: api\n", "=== RUN   TestOld\n",
               "    old_test.go:7: old assertion\n", "old cause\n",
               "Running Ceph test suite: client\n", "=== RUN   TestCurrent\n",
               "    current_test.go:8: current assertion\n", "current cause\n",
               "--- FAIL: TestCurrent\n"]
        result = annotations(log, 1)
        self.assertEqual(result[1], "::error file=cephmsgr/current_test.go,line=8::current assertion%0Acurrent cause")
        self.assertNotIn("old cause", "\n".join(result))

    def test_crash_jsonl_exposes_only_validated_metadata_and_native_frames(self):
        log = ["=== RUN   TestFailover\n", "    fault_test.go:7: standby timeout\n",
               "--- FAIL: TestFailover\n", "FAIL\n", parser.FAILURE_DIAGNOSTICS_MARKER + "\n",
               crash_metadata(assert_file="/private/build/src/common/assert.cc", assert_line=123,
                              assert_msg="PRIVATE_MESSAGE", assert_condition="PRIVATE_CONDITION",
                              utsname_hostname="PRIVATE_HOST", io_error_path="PRIVATE_PATH",
                              unknown={"token": "PRIVATE_TOKEN"})]
        result = annotations(log, 1)
        self.assertEqual(len(result), 3)
        crash = result[-1]
        self.assertTrue(crash.startswith("::error title=Ceph daemon crash metadata::"))
        for value in ("process=ceph-mgr", "entity=mgr.a", "ceph_version=20.2.4",
                      "timestamp=2026-10-02T05:35:20.123456Z", "assertion=assert.cc:123",
                      "ceph::ceph_assert_fail(char const*", "raise()"):
            self.assertIn(value, crash)
        self.assertNotIn("PRIVATE_", "\n".join(result))
        self.assertNotIn("/private/build", crash)
        self.assertNotIn("0x1234", crash)
        self.assertNotIn("caused", crash)

    def test_abrupt_metadata_never_leaks_through_multiline_go_diagnostic(self):
        for metadata in (
                crash_metadata(assert_msg="PRIVATE_SECRET"),
                crash_metadata(process="ceph-osd", assert_msg="PRIVATE_SECRET"),
                '{"process_name":"ceph-mgr","assert_msg":"PRIVATE_SECRET"\n',
                '[{"assert_msg":"PRIVATE_SECRET"}]\n'):
            with self.subTest(metadata=metadata[:40]):
                result = annotations(["=== RUN   TestAbrupt\n",
                                      "    fault_test.go:12: waiting for standby\n",
                                      parser.FAILURE_DIAGNOSTICS_MARKER + "\n",
                                      metadata, "PRIVATE_DAEMON_LOG\n"], 137)
                self.assertNotIn("PRIVATE_", "\n".join(result))
                self.assertIn("::error file=fault_test.go,line=12::waiting for standby", result)

    def test_crash_metadata_is_not_emitted_for_success_or_inferred_from_timeout(self):
        self.assertEqual(annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", crash_metadata()], 0), [])
        result = annotations(["=== RUN   TestFailover\n",
                              "    fault_test.go:7: context deadline exceeded\n",
                              "--- FAIL: TestFailover\n"], 1)
        self.assertEqual(len(result), 2)
        self.assertNotIn("crash", "\n".join(result))

    def test_unmarked_or_later_test_json_does_not_claim_crash_evidence(self):
        result = annotations(["=== RUN   TestAbrupt\n", "    fault_test.go:12: waiting\n",
                              crash_metadata(assert_msg="PRIVATE_SECRET"), "PRIVATE_LOG\n"], 137)
        self.assertEqual(len(result), 2)
        self.assertNotIn("PRIVATE_", "\n".join(result))
        self.assertNotIn("title=Ceph daemon crash metadata", "\n".join(result))
        for transition in ("=== RUN   TestNext\n", "=== CONT  TestNext\n", "=== NAME  TestNext\n",
                           "Running Ceph test suite: client\n"):
            result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", transition,
                                  crash_metadata()], 1)
            self.assertEqual(len(result), 1)
            self.assertNotIn("title=Ceph daemon crash metadata", result[0])

    def test_marked_daemon_text_cannot_become_a_go_diagnostic(self):
        result = annotations(["=== RUN   TestAbrupt\n", "    fault_test.go:12: waiting\n",
                              parser.FAILURE_DIAGNOSTICS_MARKER + "\n", "FAIL\n",
                              "    daemon_test.go:23: PRIVATE_DAEMON_SECRET\n",
                              "PRIVATE_DAEMON_CONTINUATION\n"], 137)
        self.assertEqual(len(result), 2)
        self.assertIn("::error file=fault_test.go,line=12::waiting", result)
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_ipv6_joined_error_is_not_misclassified_as_metadata(self):
        for endpoint in ("[::1]:33300", "[fe80::1%3]:33300", "[::ffff:192.0.2.1]:3300/7"):
            log = ["=== RUN   TestRejection\n", "    fault_test.go:12: authentication rejected\n",
                   endpoint + ": ceph messenger authentication: code -13\n", "unexpected EOF\n",
                   "--- FAIL: TestRejection\n"]
            result = annotations(log, 1)
            self.assertIn(endpoint.replace("%", "%25") + ": ceph messenger authentication: code -13%0Aunexpected EOF", result[1])

    def test_malformed_unknown_and_wrong_type_metadata_is_rejected(self):
        valid = json.loads(crash_metadata())
        bad = ["not JSON", "[]", "{", json.dumps({"process_name": "ceph-mgr"})]
        for field, value in (
                ("process_name", "ceph-osd"), ("process_name", ["ceph-mgr"]),
                ("timestamp", 123), ("timestamp", "2026-02-30T05:35:20.123456Z"),
                ("crash_id", "PRIVATE_SECRET"), ("ceph_version", ["PRIVATE_SECRET"]),
                ("ceph_version", "20.2.4-PRIVATE_SECRET"),
                ("entity_name", "mon.b"), ("entity_name", "mgr.a\nPRIVATE_SECRET"),
                ("backtrace", "PRIVATE_SECRET"), ("backtrace", [{"key": "PRIVATE_SECRET"}])):
            metadata = valid.copy()
            metadata[field] = value
            bad.append(json.dumps(metadata))
        bad.extend([crash_metadata().rstrip()[:-1] + ',"process_name":"ceph-mon"}',
                    crash_metadata().rstrip()[:-1] + ',"unknown":NaN}',
                    crash_metadata().rstrip()[:-1] + ',"unknown":' + '9' * 10000 + '}'])
        for metadata in bad:
            with self.subTest(metadata=metadata[:40]):
                result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", metadata + "\n"], 1)
                self.assertEqual(len(result), 1)
                self.assertIn("No Go test start was observed", result[0])
                self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_oversized_deep_and_invalid_unicode_metadata_does_not_hide_valid_record(self):
        invalid = [crash_metadata(unknown="PRIVATE_" + "x" * parser.CRASH_METADATA_LIMIT),
                   '{"unknown":' + '[' * 100 + '"PRIVATE_SECRET"' + ']' * 100 + '}\n',
                   '{"process_name":"ceph-mgr","unknown":"\ud800"}\n']
        result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n"] + invalid + [crash_metadata("ceph-mon")], 1)
        crashes = [line for line in result if "title=Ceph daemon crash metadata" in line]
        self.assertEqual(len(crashes), 1)
        self.assertIn("process=ceph-mon", crashes[0])
        self.assertNotIn("PRIVATE_", "\n".join(result))

    def test_backtrace_filters_python_runtime_text_and_bounds_native_output(self):
        frames = ["  File \"/private/key.py\", line 1, PRIVATE_PYTHON_SECRET",
                  "(fn(\"PRIVATE_LITERAL\")+0x1) [0x2]",
                  "raise()\n::error::PRIVATE_COMMAND", "PRIVATE_UNRECOGNIZED"]
        frames += ["(ceph::" + "long_type_" * 100 + "()+0x1) [0x2]"]
        frames += ["raise()"] * 20
        result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", crash_metadata(backtrace=frames)], 1)
        crash = next(line for line in result if "title=Ceph daemon crash metadata" in line)
        self.assertNotIn("PRIVATE_", crash)
        self.assertIn("[native backtrace truncated]", crash)
        self.assertIn("[unrecognized backtrace frames omitted]", crash)
        self.assertLessEqual(crash.count("raise()"), parser.CRASH_FRAME_LIMIT)
        self.assertLess(len(crash), parser.CRASH_ANNOTATION_LIMIT + 100)

    def test_crash_record_count_and_duplicates_are_bounded(self):
        repeated = crash_metadata()
        records = [repeated] * 20
        for number in range(1, parser.CRASH_RECORD_LIMIT + 3):
            records.append(crash_metadata(crash_id=f"2026-10-02T05:35:20.123456Z_{number:08x}-89ab-4cde-8123-456789abcdef"))
        result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n"] + records, 1)
        crashes = [line for line in result if "title=Ceph daemon crash metadata" in line]
        self.assertEqual(len(crashes), parser.CRASH_RECORD_LIMIT + 1)
        self.assertEqual(sum("[additional crash metadata omitted]" in line for line in crashes), 1)
        self.assertEqual(sum("01234567-89ab" in line for line in crashes), 1)

    def test_native_operator_percent_is_escaped_and_encoded_output_is_bounded(self):
        frames = ["(ceph::operator%(int)+0x1) [0x2]",
                  "(ceph::Frame::size() const+0x1) [0x2]",
                  "/lib/aarch64-linux-gnu/libc.so.6(+0x123) [0x456]"]
        result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", crash_metadata(backtrace=frames)], 1)
        crash = next(line for line in result if "title=Ceph daemon crash metadata" in line)
        self.assertIn("operator%25(int)", crash)
        self.assertIn("ceph::Frame::size() const", crash)
        self.assertIn("libc.so.6(+0x123)", crash)
        self.assertNotIn("/lib/aarch64", crash)
        self.assertNotIn("0x456", crash)
        self.assertLess(len(crash), parser.CRASH_ANNOTATION_LIMIT + 100)

    def test_encoded_crash_annotation_truncates_without_splitting_escape(self):
        frames = ["(ceph::" + "%" * 500 + "()+0x1) [0x2]"] * parser.CRASH_FRAME_LIMIT
        result = annotations([parser.FAILURE_DIAGNOSTICS_MARKER + "\n", crash_metadata(backtrace=frames)], 1)
        crash = next(line for line in result if "title=Ceph daemon crash metadata" in line)
        prefix = "::error title=Ceph daemon crash metadata::"
        self.assertLessEqual(len(crash), len(prefix) + parser.CRASH_ANNOTATION_LIMIT)
        self.assertTrue(crash.endswith("%0A[crash annotation truncated]"))
        self.assertNotIn("%2%0A", crash)


if __name__ == "__main__":
    unittest.main()
