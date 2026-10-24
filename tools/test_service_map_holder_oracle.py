import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import service_map_holder_oracle as oracle

TOKEN = "ab" * 16
DAEMON = "holder-" + TOKEN[:16]


def dump(daemon_entry=None, epoch=7, others=2):
    daemons = {"other-%d" % n: {"gid": 100 + n, "metadata": {"hostname": "unrelated"}} for n in range(others)}
    if daemon_entry is not None:
        daemons[DAEMON] = daemon_entry
    return json.dumps({"epoch": epoch, "services": {"ceph-msgr": {"daemons": dict(daemons, summary="")},
                                                    "rgw": {"daemons": {"x": {"gid": 1}}}}}).encode()


def entry(gid=4242):
    return {"gid": gid, "start_epoch": 3, "metadata": {"ceph_msgr_role": "holder", "ceph_msgr_daemon": DAEMON,
                                                       "hostname": "must-not-persist"}}


class FakeNative:
    """Process-free stand-in: the holder is a flag the test flips."""

    def __init__(self, output, views=None, ready=True, gid=4242, pid=900):
        self.output, self.views, self.gid, self.pid = output, list(views or []), gid, pid
        self.ready, self.process, self.spawned, self.killed, self.exits = ready, False, 0, 0, True
        self.clock, self.survives = 0.0, False
        self.ready_override = None

    def view(self, root, daemon):
        payload = self.views.pop(0) if len(self.views) > 1 else self.views[0]
        return oracle.project(0, payload, daemon)

    def spawn(self, root, output, token):
        self.spawned += 1
        self.process = True
        if self.ready:
            body = {"token": token, "pid": self.pid, "daemon": DAEMON, "service": "ceph-msgr", "gid": self.gid,
                    "registered": True, "lifetime_seconds": oracle.LIFETIME}
            oracle.atomic_write(oracle.state_path(output, token, "ready.json"), self.ready_override or body)
        return self.pid

    def alive(self, pid, token):
        return self.process and pid == self.pid

    def kill(self, pid):
        self.killed += 1
        if not self.survives:
            self.process = False

    def sleep(self, seconds):
        self.clock += seconds
        release = oracle.state_path(self.output, TOKEN, "go.release")
        exited = oracle.state_path(self.output, TOKEN, "exited.json")
        if self.exits and release.exists() and not exited.exists():
            oracle.atomic_write(exited, {"token": TOKEN, "pid": self.pid, "reason": "released", "shutdown_returned": True})
            self.process = False

    def monotonic(self):
        return self.clock


class RequestRejections(unittest.TestCase):
    def test_closed_request_fields(self):
        for raw, token, phase in [("0", TOKEN, "start"), ("-1", TOKEN, "start"), ("1x", TOKEN, "start"),
                                  ("١", TOKEN, "start"), (str(1 << 63), TOKEN, "start"), ("", TOKEN, "start"),
                                  ("1", TOKEN.upper(), "start"), ("1", TOKEN[:-1], "start"), ("1", TOKEN + "0", "start"),
                                  ("1", "../" + TOKEN[3:], "start"), ("1", TOKEN, "restore"), ("1", TOKEN, ""),
                                  ("1", TOKEN, "START"), ("1", TOKEN, "start extra")]:
            with self.subTest(raw=raw, token=token, phase=phase):
                with self.assertRaises(ValueError):
                    oracle.validate(raw, token, phase)
        for phase in oracle.PHASES:
            self.assertEqual(oracle.validate("5", TOKEN, phase), 5)

    def test_state_files_are_closed(self):
        out = pathlib.Path("/out")
        for name in ("../x.json", "a/b.json", "ready.sh", "READY.json", "", ".json", "a" * 30 + ".json"):
            with self.assertRaises(ValueError):
                oracle.state_path(out, TOKEN, name)
        with self.assertRaises(ValueError):
            oracle.state_path(out, "short", "ready.json")

    def test_main_rejects_wrong_argument_shapes(self):
        for argv in (["x"], ["x", "r", "o", "1", TOKEN], ["x", "hold", "r", "o"], ["x", "r", "o", "1", TOKEN, "start", "e"]):
            with patch.object(sys, "argv", argv), self.assertRaises(ValueError):
                oracle.main()

    def test_rejected_request_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp)
            for raw, token, phase in [("0", TOKEN, "start"), ("1", "x", "start"), ("1", TOKEN, "stop")]:
                with self.assertRaises(ValueError):
                    oracle.collect(out, out, raw, token, phase, FakeNative(out, [dump()]))
            self.assertEqual(list(out.iterdir()), [])


class Project(unittest.TestCase):
    def test_keeps_only_the_owned_entry_and_synthetic_metadata(self):
        view = oracle.project(0, dump(entry()), DAEMON)
        self.assertEqual(view["entry"], {"gid": 4242, "start_epoch": 3, "role": "holder", "daemon": DAEMON})
        self.assertEqual((view["epoch"], view["service_daemons"], view["present"]), (7, 3, True))
        self.assertNotIn("must-not-persist", json.dumps(view))
        self.assertNotIn("unrelated", json.dumps(view))

    def test_absent_unreadable_and_malformed(self):
        self.assertFalse(oracle.project(0, dump(), DAEMON)["present"])
        self.assertEqual(oracle.project(-5, b"", DAEMON)["code"], -5)
        self.assertIsNone(oracle.project(0, b"[1]", DAEMON)["code"])
        self.assertIsNone(oracle.project(0, b"{", DAEMON)["code"])
        self.assertIsNone(oracle.project(0, b"x" * ((4 << 20) + 1), DAEMON)["epoch"])
        self.assertIsNone(oracle.project(0, b'{"epoch": true}', DAEMON)["epoch"])
        self.assertIsNone(oracle.project(0, b'{"epoch":1,"services":{"ceph-msgr":[]}}', DAEMON)["service_daemons"])


class Lifecycle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = pathlib.Path(self.tmp.name)

    def run_phase(self, native, phase, raw="1"):
        oracle.collect(self.out, self.out, raw, TOKEN, phase, native)
        return json.loads(oracle.state_path(self.out, TOKEN, phase + ".json").read_text())

    def test_full_cycle_from_the_workers_own_baseline(self):
        native = FakeNative(self.out, [dump(), dump(entry()), dump(), dump()])
        started = self.run_phase(native, "start")
        self.assertEqual((started["state"], started["ready"]["gid"], started["baseline"]["owned_present"]), ("ready", 4242, False))
        present = self.run_phase(native, "observe")
        self.assertTrue(present["view"]["present"])
        self.assertTrue(present["gid_matches_holder"])
        self.assertTrue(present["holder"]["alive"])
        released = self.run_phase(native, "release")
        self.assertEqual((released["state"], released["exited"]["reason"]), ("released", "released"))
        absent = self.run_phase(native, "observe")
        self.assertFalse(absent["view"]["present"])
        self.assertFalse(absent["holder"]["alive"])
        cleanup = self.run_phase(native, "cleanup")
        self.assertEqual((cleanup["killed"], cleanup["restored"], native.killed), (False, True, 0))
        states = [json.loads(line)["state"] for line in (self.out / "service-holder-native.jsonl").read_text().splitlines()]
        self.assertIn("holder-spawn-outcome-unknown", states)
        self.assertIn("release-claimed-outcome-unknown", states)

    def test_start_is_never_replayed(self):
        native = FakeNative(self.out, [dump()])
        self.run_phase(native, "start")
        with self.assertRaisesRegex(ValueError, "never replayed"):
            self.run_phase(native, "start")
        self.assertEqual(native.spawned, 1)

    def test_start_refuses_a_present_or_unreadable_baseline_without_spawning(self):
        for payload in (dump(entry()), b"{", dump(epoch=None)):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                native = FakeNative(out, [payload])
                with self.assertRaises(ValueError):
                    oracle.collect(out, out, "1", TOKEN, "start", native)
                self.assertEqual(native.spawned, 0)
                self.assertFalse(oracle.state_path(out, TOKEN, "baseline.json").exists())
                # The claim marker survives: the failed attempt is not replayed.
                with self.assertRaisesRegex(ValueError, "never replayed"):
                    oracle.collect(out, out, "2", TOKEN, "start", native)

    def test_start_reports_holder_failure_timeout_and_foreign_readiness(self):
        failed = FakeNative(self.out, [dump()], ready=False)
        original = failed.spawn

        def spawn_failure(root, output, token):
            oracle.atomic_write(oracle.state_path(output, token, "failed.json"), {"token": token, "error": "RuntimeError: connect"})
            return original(root, output, token)
        failed.spawn = spawn_failure
        with self.assertRaisesRegex(RuntimeError, "holder failed before readiness"):
            self.run_phase(failed, "start")
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp)
            silent = FakeNative(out, [dump()], ready=False)
            with self.assertRaisesRegex(RuntimeError, "did not become ready"):
                oracle.collect(out, out, "1", TOKEN, "start", silent)
            self.assertGreaterEqual(silent.clock, oracle.READY_WAIT)
        for key, value in (("daemon", "holder-0000000000000000"), ("token", "cd" * 16), ("registered", False), ("service", "rgw")):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                native = FakeNative(out, [dump()])
                native.ready_override = {"token": TOKEN, "pid": 900, "daemon": DAEMON, "service": "ceph-msgr", "gid": 1,
                                         "registered": True, "lifetime_seconds": 120, key: value}
                with self.assertRaises(ValueError):
                    oracle.collect(out, out, "1", TOKEN, "start", native)
        for gid in (0, -1, True, "7"):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                native = FakeNative(out, [dump()], gid=gid)
                with self.assertRaises(ValueError):
                    oracle.collect(out, out, "1", TOKEN, "start", native)

    def test_later_phases_require_the_workers_own_baseline(self):
        native = FakeNative(self.out, [dump()])
        for phase in ("observe", "release"):
            with self.assertRaisesRegex(ValueError, "no matching baseline"):
                self.run_phase(native, phase)
        oracle.atomic_write(oracle.state_path(self.out, TOKEN, "baseline.json"), {"token": "cd" * 16, "baseline": {}})
        with self.assertRaisesRegex(ValueError, "mismatched baseline"):
            self.run_phase(native, "observe")
        self.assertEqual(native.spawned, 0)

    def test_observe_flags_a_foreign_gid(self):
        native = FakeNative(self.out, [dump(), dump(entry(gid=1))], gid=4242)
        self.run_phase(native, "start")
        self.assertFalse(self.run_phase(native, "observe")["gid_matches_holder"])

    def test_release_needs_a_ready_holder(self):
        native = FakeNative(self.out, [dump()], ready=False)
        oracle.atomic_write(oracle.state_path(self.out, TOKEN, "baseline.json"), {"token": TOKEN, "baseline": {"owned_present": False}})
        with self.assertRaisesRegex(ValueError, "never ready"):
            self.run_phase(native, "release")
        self.assertFalse(oracle.state_path(self.out, TOKEN, "go.release").exists())

    def test_release_is_claimed_once(self):
        native = FakeNative(self.out, [dump()])
        self.run_phase(native, "start")
        self.run_phase(native, "release", "2")
        with self.assertRaisesRegex(ValueError, "never replayed"):
            self.run_phase(native, "release", "3")

    def test_release_without_exit_marker_times_out_and_is_not_replayed(self):
        native = FakeNative(self.out, [dump()])
        native.exits = False
        self.run_phase(native, "start")
        with self.assertRaisesRegex(RuntimeError, "did not report exit"):
            self.run_phase(native, "release", "2")
        with self.assertRaisesRegex(ValueError, "never replayed"):
            self.run_phase(native, "release", "3")
        cleanup = self.run_phase(native, "cleanup", "4")
        self.assertEqual((cleanup["killed"], native.killed), (True, 1))

    def test_cleanup_kills_once_and_reports_the_unrestored_view(self):
        native = FakeNative(self.out, [dump(), dump(entry())])
        self.run_phase(native, "start")
        cleanup = self.run_phase(native, "cleanup", "2")
        self.assertEqual((cleanup["killed"], cleanup["restored"]), (True, False))
        again = self.run_phase(native, "cleanup", "3")
        self.assertEqual((again["killed"], native.killed), (False, 1))

    def test_cleanup_reports_a_survivor(self):
        native = FakeNative(self.out, [dump()])
        native.survives = True
        self.run_phase(native, "start")
        with self.assertRaisesRegex(RuntimeError, "survived the kill"):
            self.run_phase(native, "cleanup", "2")
        self.assertEqual(native.killed, 1)

    def test_cleanup_reaches_a_holder_that_never_became_ready(self):
        native = FakeNative(self.out, [dump()], ready=False)
        with self.assertRaises(RuntimeError):
            self.run_phase(native, "start")
        cleanup = self.run_phase(native, "cleanup", "2")
        self.assertEqual((cleanup["killed"], native.killed), (True, 1))

    def test_cleanup_before_start_does_nothing_to_processes(self):
        native = FakeNative(self.out, [dump()])
        cleanup = self.run_phase(native, "cleanup")
        self.assertEqual((cleanup["killed"], cleanup["restored"], cleanup["baseline"], native.killed), (False, True, None, 0))


class FakeLib:
    """Records the native call order of the holder process."""

    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def __getattr__(self, name):
        def call(*args):
            self.calls.append(name)
            if name == "rados_get_instance_id":
                return 4242
            if name == "rados_shutdown":
                return None
            return -13 if name == self.fail else 0
        return call


class Holder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = pathlib.Path(self.tmp.name)

    def test_holds_until_release_then_shuts_down_before_the_exit_marker(self):
        lib, ticks = FakeLib(), []

        def sleep(seconds):
            ticks.append(seconds)
            if len(ticks) == 3:
                oracle.state_path(self.out, TOKEN, "go.release").write_text("release\n")
        oracle.hold(self.out, self.out, TOKEN, lib=lib, sleep=sleep)
        self.assertEqual([c for c in lib.calls if c in ("rados_service_register", "rados_connect", "rados_shutdown")],
                         ["rados_connect", "rados_service_register", "rados_shutdown"])
        ready = json.loads(oracle.state_path(self.out, TOKEN, "ready.json").read_text())
        self.assertEqual((ready["gid"], ready["daemon"], ready["registered"]), (4242, DAEMON, True))
        exited = json.loads(oracle.state_path(self.out, TOKEN, "exited.json").read_text())
        self.assertEqual((exited["reason"], exited["shutdown_returned"]), ("released", True))

    def test_lifetime_cap_expires_without_a_release(self):
        now = [0.0]

        def clock():
            return now[0]

        def sleep(seconds):
            now[0] += 30
        oracle.hold(self.out, self.out, TOKEN, lib=FakeLib(), clock=clock, sleep=sleep)
        exited = json.loads(oracle.state_path(self.out, TOKEN, "exited.json").read_text())
        self.assertEqual(exited["reason"], "expired")
        self.assertGreaterEqual(now[0], oracle.LIFETIME)

    def test_native_failure_is_recorded_and_the_handle_is_released(self):
        for step in ("rados_create2", "rados_connect", "rados_service_register"):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                lib = FakeLib(fail=step)
                with self.assertRaises(RuntimeError):
                    oracle.hold(out, out, TOKEN, lib=lib)
                failed = json.loads(oracle.state_path(out, TOKEN, "failed.json").read_text())
                self.assertIn("native", failed["error"])
                self.assertFalse(oracle.state_path(out, TOKEN, "ready.json").exists())
                self.assertFalse(oracle.state_path(out, TOKEN, "exited.json").exists())
                self.assertEqual("rados_shutdown" in lib.calls, step != "rados_create2")

    def test_diagnostics_mask_paths(self):
        text = oracle.diagnostic(RuntimeError("bad /private/place/file x"))
        self.assertNotIn("/private", text)


if __name__ == "__main__":
    unittest.main()
