import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import windows_interop as verifier
from test_product_dependencies import graph_output, product_graph


PACKAGE = "github.com/JSYoo5B/ceph-msgr-go/integration"
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
        foreign["Package"] = "github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
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


class EvidenceIsolationTest(unittest.TestCase):
    def test_repeated_invocations_cannot_reuse_an_earlier_success_report(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory) / "evidence"
            first = verifier.new_evidence_directory(base)
            (first / "verification.json").write_text('{"cleanup_complete":true}\n', encoding="utf-8")
            second = verifier.new_evidence_directory(base)
            (second / "failure.json").write_text('{"error":"setup failed"}\n', encoding="utf-8")
            self.assertNotEqual(first, second)
            self.assertFalse((second / "verification.json").exists())
            self.assertFalse((first / "failure.json").exists())
            self.assertEqual(json.loads((first / "verification.json").read_text()), {"cleanup_complete": True})


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


class StartupPolicyTest(unittest.TestCase):
    def run_until_build(self, graph):
        commands = []

        class BuildReached(Exception):
            pass

        def execute(args, **kwargs):
            commands.append(args)
            if args[:3] == ["go", "env", "-json"]:
                values = {"GOHOSTOS": "windows", "GOHOSTARCH": "amd64", "GOOS": "windows",
                          "GOARCH": "amd64", "CGO_ENABLED": "0", "GOWORK": "off"}
                output = json.dumps({name: values[name] for name in args[3:]})
            elif args[:2] == ["go", "list"]:
                self.assertEqual(kwargs["env"]["CGO_ENABLED"], "0")
                self.assertEqual(args[-1], "./...")
                output = graph_output(graph)
            elif args == ["git", "rev-parse", "HEAD"]:
                output = "a" * 40
            elif args == ["go", "version"]:
                output = "go version go1.27.0 windows/amd64"
            elif args[:3] == ["go", "test", "-c"]:
                raise BuildReached()
            else:
                self.fail("unexpected startup command: " + str(args))
            return subprocess.CompletedProcess(args, 0, stdout=output)

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(verifier.sys, "platform", "win32"):
                with mock.patch.object(verifier, "command", side_effect=execute):
                    with mock.patch.object(verifier.subprocess, "Popen") as start:
                        with self.assertRaises((BuildReached, RuntimeError)) as raised:
                            verifier.run("test-distro", Path(directory))
                        start.assert_not_called()
        return raised.exception, commands

    def test_approved_graph_reaches_native_test_compilation(self):
        error, commands = self.run_until_build(product_graph())
        self.assertEqual(type(error).__name__, "BuildReached")
        self.assertTrue(any(args[:3] == ["go", "test", "-c"] for args in commands))

    def test_unapproved_module_and_native_sources_stop_before_build_or_wsl(self):
        unknown = product_graph() + [{"ImportPath": "example.com/unapproved/client",
                                     "Module": {"Path": "example.com/unapproved", "Version": "v1.0.0"}}]
        native = product_graph()
        native[-1]["CgoFiles"] = ["native.go"]
        for graph, reason in ((unknown, "unexpected external module"),
                              (native, "native source dependencies")):
            with self.subTest(reason=reason):
                error, commands = self.run_until_build(graph)
                self.assertIn(reason, str(error))
                self.assertFalse(any(args[:3] == ["go", "test", "-c"] for args in commands))


class CommandDiagnosticsTest(unittest.TestCase):
    def test_failed_command_preserves_combined_output_before_raising(self):
        args = ["wsl.exe", "--distribution", "test fixture", "--", "bash", "fixture.sh"]
        combined = "fixture setup started\nWSL could not start the distribution\n"
        result = subprocess.CompletedProcess(args, 23, stdout=combined)
        console = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "bootstrap.log"
            log.write_text("earlier successful step\n", encoding="utf-8")
            with mock.patch.object(verifier.subprocess, "run", return_value=result) as run:
                with contextlib.redirect_stdout(console), self.assertRaises(subprocess.CalledProcessError) as raised:
                    verifier.command(args, cwd=directory, env={"CGO_ENABLED": "0"},
                                     timeout=17, capture=True, log=log)
            self.assertEqual(raised.exception.returncode, 23)
            self.assertEqual(raised.exception.cmd, args)
            self.assertEqual(raised.exception.output, combined)
            self.assertEqual(console.getvalue(), combined)
            recorded = log.read_text(encoding="utf-8")
            prefix, command, output = recorded.split("\n", 2)
            self.assertEqual(prefix, "earlier successful step")
            self.assertEqual(json.loads(command), args)
            self.assertEqual(output, combined)
            run.assert_called_once_with(args, cwd=directory, env={"CGO_ENABLED": "0"},
                                        check=False, timeout=17, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, encoding="utf-8", errors="replace")

    def test_successful_capture_preserves_parseable_stdout_and_quietly_logs(self):
        args = ["go", "env", "-json", "GOHOSTOS", "GOHOSTARCH", "CGO_ENABLED"]
        host = {"GOHOSTOS": "windows", "GOHOSTARCH": "amd64", "CGO_ENABLED": "0"}
        output = json.dumps(host, indent=2) + "\n"
        result = subprocess.CompletedProcess(args, 0, stdout=output)
        console = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "bootstrap.log"
            with mock.patch.object(verifier.subprocess, "run", return_value=result) as run:
                with contextlib.redirect_stdout(console):
                    returned = verifier.command(args, cwd=directory, env={"GOTOOLCHAIN": "local"},
                                                capture=True, log=log)
            self.assertIs(returned, result)
            self.assertEqual(json.loads(returned.stdout), host)
            self.assertEqual(console.getvalue(), "")
            command, recorded = log.read_text(encoding="utf-8").split("\n", 1)
            self.assertEqual(json.loads(command), args)
            self.assertEqual(recorded, output)
            run.assert_called_once_with(args, cwd=directory, env={"GOTOOLCHAIN": "local"},
                                        check=False, timeout=300, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, encoding="utf-8", errors="replace")


class OwnedTestProcessTest(unittest.TestCase):
    def test_success_waits_for_the_owned_parent_without_terminating_processes(self):
        args = ["go", "tool", "test2json", "client.test.exe", "-test.v=test2json"]
        process = mock.Mock(pid=41234)
        process.wait.return_value = 0
        log = io.StringIO()
        env = {"CGO_ENABLED": "0"}
        with mock.patch.object(verifier.sys, "platform", "win32"):
            with mock.patch.object(verifier.subprocess, "Popen", return_value=process) as start:
                with mock.patch.object(verifier.subprocess, "run") as cleanup:
                    result = verifier.run_test_process(args, cwd="test-project", env=env,
                                                       log=log, timeout=123)
        self.assertEqual(result, 0)
        start.assert_called_once_with(args, cwd="test-project", env=env, stdout=log,
                                      stderr=subprocess.STDOUT)
        process.wait.assert_called_once_with(timeout=123)
        cleanup.assert_not_called()

    def test_timeout_terminates_only_the_owned_tree_and_joins_before_reraising(self):
        args = ["go", "tool", "test2json", "client.test.exe"]
        timeout = subprocess.TimeoutExpired(args, 420)
        process = mock.Mock(pid=41234)
        process.wait.side_effect = [timeout, 1]
        taskkill = ["taskkill.exe", "/PID", "41234", "/T", "/F"]
        env = {"CGO_ENABLED": "0"}
        with mock.patch.object(verifier.sys, "platform", "win32"):
            with mock.patch.object(verifier.subprocess, "Popen", return_value=process):
                with mock.patch.object(verifier.subprocess, "run",
                                       return_value=subprocess.CompletedProcess(taskkill, 0, stdout="terminated\n")) as cleanup:
                    with self.assertRaises(subprocess.TimeoutExpired) as raised:
                        verifier.run_test_process(args, cwd="test-project", env=env,
                                                  log=io.StringIO())
        self.assertIs(raised.exception, timeout)
        cleanup.assert_called_once_with(taskkill, cwd="test-project", env=env, check=False,
                                        timeout=30, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        encoding="utf-8", errors="replace")
        self.assertEqual(process.wait.call_args_list, [mock.call(timeout=420), mock.call(timeout=30)])

    def test_unverified_tree_cleanup_never_reports_the_timeout_as_cleanly_stopped(self):
        args = ["go", "tool", "test2json", "client.test.exe"]
        taskkill = ["taskkill.exe", "/PID", "41234", "/T", "/F"]
        cases = (
            ("tree termination refused", subprocess.CompletedProcess(taskkill, 5, stdout="access denied\n"), 1),
            ("tree termination timed out", subprocess.TimeoutExpired(taskkill, 30), 1),
            ("parent did not join", subprocess.CompletedProcess(taskkill, 0, stdout="terminated\n"),
             subprocess.TimeoutExpired(args, 30)),
        )
        for reason, cleanup_result, joined in cases:
            process = mock.Mock(pid=41234)
            process.wait.side_effect = [subprocess.TimeoutExpired(args, 420), joined]
            with self.subTest(reason=reason), mock.patch.object(verifier.sys, "platform", "win32"):
                with mock.patch.object(verifier.subprocess, "Popen", return_value=process):
                    with mock.patch.object(verifier.subprocess, "run") as cleanup:
                        if isinstance(cleanup_result, Exception):
                            cleanup.side_effect = cleanup_result
                        else:
                            cleanup.return_value = cleanup_result
                        with self.assertRaisesRegex(RuntimeError, "could not confirm termination.*41234"):
                            verifier.run_test_process(args, cwd="test-project", env={}, log=io.StringIO())
            self.assertEqual(process.wait.call_args_list, [mock.call(timeout=420), mock.call(timeout=30)])
            self.assertEqual(cleanup.call_args.args[0], taskkill)


class ForegroundProviderReadinessTest(unittest.TestCase):
    VERSION = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.provider = mock.Mock(spec=subprocess.Popen, pid=41234)
        self.provider.poll.return_value = None

    def ready(self, version=VERSION, proxy="127.0.0.1:40001"):
        (self.output / "daemon-version").write_text(version + "\n", encoding="utf-8")
        (self.output / "proxy").write_text(proxy + "\n", encoding="utf-8")
        (self.output / "provider.ready").touch()

    def test_running_check_rejects_every_terminal_exit_including_zero(self):
        verifier.require_provider_running(self.provider)
        for code in (0, 1, -9):
            self.provider.poll.return_value = code
            with self.subTest(code=code), self.assertRaisesRegex(RuntimeError, "exited unexpectedly"):
                verifier.require_provider_running(self.provider)
        self.provider.wait.assert_not_called()

    def test_ready_requires_exact_version_valid_proxy_and_live_process(self):
        self.ready(proxy=" \t127.0.0.1:40001\r")
        self.assertEqual(verifier.wait_for_provider(self.provider, self.output),
                         (self.VERSION, "127.0.0.1:40001"))
        self.provider.poll.assert_called()
        self.provider.wait.assert_not_called()

    def test_pending_provider_can_publish_readiness_after_the_first_poll(self):
        with mock.patch.object(verifier.time, "monotonic", return_value=10):
            with mock.patch.object(verifier.time, "sleep", side_effect=lambda _: self.ready()) as sleep:
                self.assertEqual(verifier.wait_for_provider(self.provider, self.output, timeout=5),
                                 (self.VERSION, "127.0.0.1:40001"))
        sleep.assert_called_once_with(0.1)
        self.provider.wait.assert_not_called()

    def test_dead_process_is_rejected_before_and_after_ready_publication(self):
        for ready in (False, True):
            if ready:
                self.ready()
            self.provider.poll.return_value = 0
            with self.subTest(ready=ready), self.assertRaisesRegex(RuntimeError, "exited unexpectedly"):
                verifier.wait_for_provider(self.provider, self.output)
        self.provider.wait.assert_not_called()

    def test_error_and_stopped_markers_reject_a_still_pending_live_process(self):
        for name in ("provider.error", "provider.stopped"):
            marker = self.output / name
            marker.touch()
            with self.subTest(marker=name), self.assertRaisesRegex(RuntimeError, "failed before readiness"):
                verifier.wait_for_provider(self.provider, self.output)
            marker.unlink()
        self.provider.wait.assert_not_called()

    def test_pending_readiness_timeout_never_joins_or_kills_the_provider(self):
        with mock.patch.object(verifier.time, "monotonic", side_effect=(10, 15)):
            with mock.patch.object(verifier.time, "sleep") as sleep:
                with self.assertRaisesRegex(RuntimeError, "did not become ready"):
                    verifier.wait_for_provider(self.provider, self.output, timeout=5)
        sleep.assert_not_called()
        self.provider.wait.assert_not_called()
        self.provider.kill.assert_not_called()
        self.provider.terminate.assert_not_called()

    def test_ready_rejects_another_release_or_another_commit_of_the_same_release(self):
        versions = (self.VERSION.replace("20.2.4", "20.2.3"),
                    self.VERSION.replace("7f793731f1b39eb4f465e960113d2363c311b964", "0" * 40), "")
        for version in versions:
            self.ready(version=version)
            with self.subTest(version=version), self.assertRaisesRegex(RuntimeError, "fixed Tentacle reference"):
                verifier.wait_for_provider(self.provider, self.output)

    def test_ready_rejects_a_nonloopback_or_out_of_range_proxy(self):
        for proxy in ("192.0.2.1:40001", "127.0.0.1:65536"):
            self.ready(proxy=proxy)
            with self.subTest(proxy=proxy), self.assertRaises(ValueError):
                verifier.wait_for_provider(self.provider, self.output)


class ForegroundProviderCleanupTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.provider = mock.Mock(spec=subprocess.Popen, pid=41234)
        self.provider.wait.return_value = 0

    def test_stop_is_published_before_callback_and_join_precedes_final_confirmation(self):
        order = []

        def stop():
            self.assertTrue((self.output / "provider.stop").exists())
            self.assertFalse((self.output / "provider.pid").exists())
            (self.output / "provider.pid").write_text("41234\n", encoding="utf-8")
            order.append("stop")

        def join(timeout):
            self.assertEqual(order, ["stop"])
            self.assertTrue((self.output / "provider.pid").exists())
            self.assertFalse((self.output / "provider.stopped").exists())
            (self.output / "provider.stopped").touch()
            order.append("join")
            return 0

        self.provider.wait.side_effect = join
        self.assertEqual(verifier.finish_provider(self.provider, self.output, stop, timeout=17), 0)
        self.assertEqual(order, ["stop", "join"])
        self.provider.wait.assert_called_once_with(timeout=17)

    def test_pid_published_during_join_cannot_bypass_cleanup_confirmation(self):
        def join(timeout):
            (self.output / "provider.pid").write_text("41234\n", encoding="utf-8")
            return 1

        self.provider.wait.side_effect = join
        with self.assertRaisesRegex(RuntimeError, "without confirming cleanup"):
            verifier.finish_provider(self.provider, self.output, mock.Mock())
        self.provider.wait.assert_called_once_with(timeout=30)
        self.assertTrue((self.output / "provider.pid").exists())

    def test_startup_error_without_pid_is_joined_even_when_stop_callback_fails(self):
        (self.output / "provider.error").write_text("preflight failed\n", encoding="utf-8")
        self.provider.wait.return_value = 2
        stop = mock.Mock(side_effect=RuntimeError("stop command interrupted"))
        self.assertEqual(verifier.finish_provider(self.provider, self.output, stop), 2)
        stop.assert_called_once_with()
        self.provider.wait.assert_called_once_with(timeout=30)
        self.assertTrue((self.output / "provider.stop").exists())
        self.assertFalse((self.output / "provider.pid").exists())

    def test_operation_error_with_confirmed_cleanup_preserves_the_nonzero_exit(self):
        for name in ("provider.pid", "provider.error", "provider.stopped"):
            (self.output / name).touch()
        self.provider.wait.return_value = 1
        self.assertEqual(verifier.finish_provider(self.provider, self.output, mock.Mock()), 1)
        self.provider.wait.assert_called_once_with(timeout=30)

    def test_callback_error_does_not_override_final_confirmed_cleanup(self):
        (self.output / "provider.pid").touch()
        callback_error = RuntimeError("stop command interrupted")

        def join(timeout):
            (self.output / "provider.stopped").touch()
            return 0

        self.provider.wait.side_effect = join
        self.assertEqual(verifier.finish_provider(
            self.provider, self.output, mock.Mock(side_effect=callback_error)), 0)
        self.provider.wait.assert_called_once_with(timeout=30)

    def test_cleanup_error_rejects_stopped_confirmation_with_or_without_a_pid(self):
        (self.output / "provider.stopped").touch()
        (self.output / "provider.cleanup.error").write_text("cleanup failed\n", encoding="utf-8")
        for has_pid in (False, True):
            if has_pid:
                (self.output / "provider.pid").touch()
            with self.subTest(has_pid=has_pid), self.assertRaisesRegex(RuntimeError, "cleanup failed.*ownership retained"):
                verifier.finish_provider(self.provider, self.output, mock.Mock())
        self.assertEqual(self.provider.wait.call_count, 2)

    def test_dead_provider_without_stopped_marker_never_confirms_cleanup(self):
        (self.output / "provider.pid").touch()
        self.provider.poll.return_value = 1
        self.provider.wait.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "without confirming cleanup"):
            verifier.finish_provider(self.provider, self.output, mock.Mock())
        self.provider.wait.assert_called_once_with(timeout=30)

    def test_join_timeout_retains_ownership_without_killing_the_provider(self):
        (self.output / "provider.pid").write_text("41234\n", encoding="utf-8")
        timeout = subprocess.TimeoutExpired("foreground fixture", 17)
        self.provider.wait.side_effect = timeout
        stop = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, "ownership retained") as raised:
            verifier.finish_provider(self.provider, self.output, stop, timeout=17)
        self.assertIs(raised.exception.__cause__, timeout)
        stop.assert_called_once_with()
        self.provider.wait.assert_called_once_with(timeout=17)
        self.provider.kill.assert_not_called()
        self.provider.terminate.assert_not_called()
        self.provider.send_signal.assert_not_called()
        self.assertEqual((self.output / "provider.pid").read_text(encoding="utf-8"), "41234\n")
        self.assertTrue((self.output / "provider.stop").exists())
        self.assertFalse((self.output / "provider.stopped").exists())


if __name__ == "__main__":
    unittest.main()
