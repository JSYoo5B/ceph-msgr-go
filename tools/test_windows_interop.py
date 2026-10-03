import json
import os
import unittest
from unittest import mock

import windows_interop as verifier


PACKAGE = "github.com/jsyoo5b/ceph-msgr-go/integration"
REQUIRED = ("TestMonTransport", "TestMgrTransport")


def event(action, test=None, **fields):
    value = {"Action": action, "Package": PACKAGE}
    if test is not None:
        value["Test"] = test
    value.update(fields)
    return json.dumps(value)


def successful_transcript():
    # Deliberately independent of the driver's suite list: these are the
    # externally reported events that its evidence gate must require.
    return [event("start"), event("run", REQUIRED[0]),
            event("output", REQUIRED[0], Output="MON response verified\n"),
            event("pass", REQUIRED[0]), event("run", REQUIRED[1]),
            event("pass", REQUIRED[1]), event("pass")]


class RequiredPassesTest(unittest.TestCase):
    def test_complete_run_accepts_passed_subtests_and_returns_required_tests(self):
        lines = successful_transcript()
        lines[5:5] = [event("run", REQUIRED[1] + "/concurrent"),
                      event("pause", REQUIRED[1] + "/concurrent"),
                      event("cont", REQUIRED[1] + "/concurrent"),
                      event("pass", REQUIRED[1] + "/concurrent")]
        self.assertEqual(verifier.required_passes(lines, PACKAGE, REQUIRED), list(REQUIRED))

    def test_zero_exit_evidence_cannot_substitute_for_executed_tests(self):
        transcripts = {
            "all opt-in tests skipped": [event("skip", name) for name in REQUIRED] + [event("pass")],
            "selector matched nothing": [event("start"), event("pass")],
            "one required test missing": [event("pass", REQUIRED[0]), event("pass")],
            "only subtests passed": [event("pass", name + "/child") for name in REQUIRED] + [event("pass")],
            "no events": [],
        }
        for reason, lines in transcripts.items():
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                verifier.required_passes(lines, PACKAGE, REQUIRED)

    def test_package_success_is_required_once_and_cannot_mask_failure(self):
        passed_tests = [event("pass", name) for name in REQUIRED]
        endings = {
            "missing package outcome": [],
            "package failure": [event("fail")],
            "package skipped": [event("skip")],
            "duplicated package pass": [event("pass"), event("pass")],
            "failure then pass": [event("fail"), event("pass")],
            "pass then failure": [event("pass"), event("fail")],
        }
        for reason, ending in endings.items():
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                verifier.required_passes(passed_tests + ending, PACKAGE, REQUIRED)

    def test_each_required_test_must_have_one_unambiguous_pass(self):
        for actions in (("skip",), ("fail",), ("pass", "pass"),
                        ("skip", "pass"), ("fail", "pass"), ("pass", "fail")):
            lines = [event(action, REQUIRED[0]) for action in actions]
            lines += [event("pass", REQUIRED[1]), event("pass")]
            with self.subTest(actions=actions), self.assertRaises(ValueError):
                verifier.required_passes(lines, PACKAGE, REQUIRED)

    def test_failed_or_skipped_child_is_not_hidden_by_parent_pass(self):
        for action in ("fail", "skip"):
            for name in (REQUIRED[0] + "/child", "TestUnexpected"):
                lines = [event(action, name)] + successful_transcript()
                with self.subTest(action=action, name=name), self.assertRaises(ValueError):
                    verifier.required_passes(lines, PACKAGE, REQUIRED)

    def test_malformed_or_foreign_events_invalidate_otherwise_passing_run(self):
        foreign = json.loads(event("pass", REQUIRED[0]))
        foreign["Package"] = "github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
        missing_package = json.loads(event("output", REQUIRED[0]))
        del missing_package["Package"]
        for invalid in ("", "not JSON", "{", "[]", "null", '"pass"',
                        json.dumps(foreign), json.dumps(missing_package)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                verifier.required_passes([invalid] + successful_transcript(), PACKAGE, REQUIRED)


class ProxyEndpointTest(unittest.TestCase):
    def test_windows_loopback_and_port_boundaries(self):
        for port in (1, 40000, 65535):
            value = "127.0.0.1:" + str(port)
            with self.subTest(port=port):
                self.assertEqual(verifier.proxy_endpoint(" \t" + value + "\r\n"), value)

    def test_rejects_other_hosts_and_invalid_or_injected_ports(self):
        values = (
            "0.0.0.0:40000", "192.0.2.1:40000", "127.0.0.2:40000",
            "localhost:40000", "[::1]:40000", "http://127.0.0.1:40000",
            "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:100000",
            "127.0.0.1:-1", "127.0.0.1:+1", "127.0.0.1:1.5",
            "127.0.0.1:４００００", "127.0.0.1:", "127.0.0.1", "",
            "127.0.0.1:40000/path", "127.0.0.1:40000\n127.0.0.1:33300",
            "127.0.0.1:40000; ignored", "127.0.0.1:40000\x00",
        )
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                verifier.proxy_endpoint(value)


class NativeEnvironmentTest(unittest.TestCase):
    def test_non_windows_host_cannot_claim_native_windows_execution(self):
        for platform in ("darwin", "linux", "cygwin", "msys"):
            with self.subTest(platform=platform), mock.patch.object(verifier.sys, "platform", platform):
                with self.assertRaises(RuntimeError):
                    verifier.native_environment()

    def test_windows_selectors_are_scrubbed_and_native_toolchain_is_forced(self):
        inherited = {
            "PATH": "test-toolchain-path", "RUNNER_TEMP": "test-output-directory",
            "UNRELATED_SETTING": "preserved", "CEPH_MSGR": "not a fixture selector",
            "CEPH_MSGR_TEST_MODE_REJECTION": "mgr", "CEPH_MSGR_MONITORS": "192.0.2.1:3300",
            "CEPH_MSGR_STRESS_DURATION": "1h", "CEPH_MSGR_IDENTITY": "client.unrelated",
            "CGO_ENABLED": "1", "GOTOOLCHAIN": "auto", "GOOS": "linux", "GOARCH": "arm64",
        }
        with mock.patch.object(verifier.sys, "platform", "win32"), mock.patch.dict(os.environ, inherited, clear=True):
            env = verifier.native_environment()
            self.assertFalse(any(name.startswith("CEPH_MSGR_") for name in env))
            self.assertEqual({name: env[name] for name in ("CGO_ENABLED", "GOTOOLCHAIN", "GOOS", "GOARCH")},
                             {"CGO_ENABLED": "0", "GOTOOLCHAIN": "local", "GOOS": "windows", "GOARCH": "amd64"})
            for name in ("PATH", "RUNNER_TEMP", "UNRELATED_SETTING", "CEPH_MSGR"):
                self.assertEqual(env[name], inherited[name])
            self.assertEqual(dict(os.environ), inherited)
            env["UNRELATED_SETTING"] = "changed copy"
            self.assertEqual(os.environ["UNRELATED_SETTING"], "preserved")


if __name__ == "__main__":
    unittest.main()
