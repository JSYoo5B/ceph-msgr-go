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
PINNED_WIDE_SHA256 = "9d72c5a97078ad6ca3ed4c3f19abf97a2ffe13ac2009d99094f983b7437c5ff2"
PINNED_PAD_SHA256 = "bcb31ca7d81a4ea3bce4a8b313d143a6892ac110fff87cb14117db468ac6c2ec"


def dump(daemon_entry=None, epoch=7, others=2):
    daemons = {"other-%d" % n: {"gid": 100 + n, "metadata": {"hostname": "unrelated"}} for n in range(others)}
    if daemon_entry is not None:
        daemons[DAEMON] = daemon_entry
    return json.dumps({"epoch": epoch, "services": {"ceph-msgr": {"daemons": dict(daemons, summary="")},
                                                    "rgw": {"daemons": {"x": {"gid": 1}}}}}).encode()


def bad_dump(service=[]):
    return json.dumps({"epoch": 7, "services": {"ceph-msgr": service}}).encode()


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
        self.modes, self.wrong_size = [], None

    def view(self, root, daemon):
        payload = self.views.pop(0) if len(self.views) > 1 else self.views[0]
        return oracle.project(0, payload, daemon)

    def spawn(self, root, output, token, mode="hold"):
        self.spawned += 1
        self.modes.append(mode)
        self.process = True
        if self.ready:
            registered = oracle.padding(token, self.wrong_size or mode)
            body = {"token": token, "pid": self.pid, "daemon": DAEMON, "service": "ceph-msgr", "gid": self.gid,
                    "registered": True, "lifetime_seconds": oracle.LIFETIME, **oracle.pad_summary(dict(registered))}
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


class Padding(unittest.TestCase):
    def test_padding_is_fixed_by_the_token_and_mode(self):
        self.assertEqual(oracle.padding(TOKEN, "hold"), [])
        # The Go side recomputes this padding independently; both test suites
        # pin the same summaries of the same token.
        for mode, keys, size, width, pinned in (("hold-large", 4096, 360, 4, PINNED_PAD_SHA256),
                                                ("hold-wide", 65537, 8, 5, PINNED_WIDE_SHA256)):
            pairs = oracle.padding(TOKEN, mode)
            self.assertEqual(len(pairs), keys)
            self.assertEqual(pairs, sorted(pairs))
            self.assertEqual(len(set(key for key, _ in pairs)), keys)
            self.assertTrue(all(key.startswith(oracle.PAD_PREFIX) and len(key) == 14 + width and len(value) == size
                                for key, value in pairs))
            self.assertNotEqual(pairs, oracle.padding("cd" * 16, mode))
            self.assertEqual(oracle.pad_summary(dict(pairs)), {
                "pad_keys": keys, "pad_bytes": keys * (14 + width + size), "pad_sha256": pinned})
        for token, mode in (("not-a-token", "hold-large"), (TOKEN, "hold-huge"), (TOKEN, "start-large")):
            with self.assertRaises(ValueError):
                oracle.padding(token, mode)

    def test_summary_counts_only_string_padding_keys(self):
        summary = oracle.pad_summary({"ceph_msgr_pad_0000": "ab", "ceph_msgr_pad_0001": 7, "hostname": "h", 3: "x"})
        self.assertEqual((summary["pad_keys"], summary["pad_bytes"]), (1, 20))
        self.assertNotEqual(summary["pad_sha256"], oracle.pad_summary({"ceph_msgr_pad_0000": "ac"})["pad_sha256"])


class Project(unittest.TestCase):
    def test_keeps_only_the_owned_entry_and_synthetic_metadata(self):
        view = oracle.project(0, dump(entry()), DAEMON)
        self.assertEqual(view["entry"], {"gid": 4242, "start_epoch": 3, "role": "holder", "daemon": DAEMON,
                                         **oracle.pad_summary({})})
        self.assertEqual((view["epoch"], view["service_daemons"], view["present"]), (7, 3, True))
        self.assertNotIn("must-not-persist", json.dumps(view))
        self.assertNotIn("unrelated", json.dumps(view))

    def test_absent_unreadable_and_malformed(self):
        self.assertFalse(oracle.project(0, dump(), DAEMON)["present"])
        self.assertEqual(oracle.project(-5, b"", DAEMON)["code"], -5)
        self.assertIsNone(oracle.project(0, b"[1]", DAEMON)["code"])
        self.assertIsNone(oracle.project(0, b"{", DAEMON)["code"])
        self.assertIsNone(oracle.project(0, b"x" * ((4 << 20) + 1), DAEMON)["epoch"])
        self.assertIsNone(oracle.project(0, b'{"epoch": true, "services": {}}', DAEMON)["code"])

    def test_structurally_invalid_dumps_are_never_absence(self):
        cases = {
            "services not a dict": {"epoch": 7, "services": []},
            "services missing": {"epoch": 7},
            "service not a dict": {"epoch": 7, "services": {"ceph-msgr": []}},
            "service a string": {"epoch": 7, "services": {"ceph-msgr": "x"}},
            "service null": {"epoch": 7, "services": {"ceph-msgr": None}},
            "daemons null": {"epoch": 7, "services": {"ceph-msgr": {"daemons": None}}},
            "daemons wrong type": {"epoch": 7, "services": {"ceph-msgr": {"daemons": []}}},
            "daemons missing": {"epoch": 7, "services": {"ceph-msgr": {}}},
            "entry wrong type": {"epoch": 7, "services": {"ceph-msgr": {"daemons": {DAEMON: "x"}}}},
            "entry null": {"epoch": 7, "services": {"ceph-msgr": {"daemons": {DAEMON: None}}}},
            "epoch missing": {"services": {}},
            "epoch string": {"epoch": "7", "services": {}},
            "epoch bool": {"epoch": True, "services": {}},
            "epoch float": {"epoch": 7.0, "services": {}},
            "epoch negative": {"epoch": -1, "services": {}},
            "epoch too large": {"epoch": 1 << 32, "services": {}},
        }
        for name, document in cases.items():
            with self.subTest(name):
                view = oracle.project(0, json.dumps(document).encode(), DAEMON)
                self.assertIsNone(view["code"])
                self.assertIsNone(view["epoch"])
                self.assertFalse(view["present"])

    def test_missing_owned_keys_in_a_valid_map_are_absence(self):
        for document in ({"epoch": 7, "services": {}}, {"epoch": 7, "services": {"rgw": {}}},
                         {"epoch": 7, "services": {"ceph-msgr": {"daemons": {}}}}):
            view = oracle.project(0, json.dumps(document).encode(), DAEMON)
            self.assertEqual((view["code"], view["epoch"], view["present"]), (0, 7, False))
            self.assertEqual(view["service_daemons"], 0)

    def test_invalid_dump_neither_starts_nor_restores(self):
        for service in ([], None, "x"):
            with self.subTest(service=service):
                with tempfile.TemporaryDirectory() as tmp:
                    out = pathlib.Path(tmp)
                    native = FakeNative(out, [bad_dump(service)])
                    with self.assertRaises(ValueError):
                        oracle.collect(out, out, "1", TOKEN, "start", native)
                    self.assertEqual(native.spawned, 0)
                with tempfile.TemporaryDirectory() as tmp:
                    out = pathlib.Path(tmp)
                    native = FakeNative(out, [dump(), bad_dump(service)])
                    oracle.collect(out, out, "1", TOKEN, "start", native)
                    oracle.collect(out, out, "2", TOKEN, "cleanup", native)
                    receipt = json.loads(oracle.state_path(out, TOKEN, "cleanup.json").read_text())
                    self.assertIsNone(receipt["view"]["code"])
                    self.assertFalse(receipt["restored"])


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

    def test_padded_starts_register_and_observe_the_token_padding(self):
        for phase, mode in (("start-large", "hold-large"), ("start-wide", "hold-wide")):
            with tempfile.TemporaryDirectory() as tmp:
                self.out = pathlib.Path(tmp)
                padded = entry()
                padded["metadata"].update(oracle.padding(TOKEN, mode))
                native = FakeNative(self.out, [dump(), dump(padded), dump(entry())])
                started = self.run_phase(native, phase)
                wanted = oracle.pad_summary(dict(oracle.padding(TOKEN, mode)))
                self.assertEqual(native.modes, [mode])
                self.assertEqual((started["state"], started["phase"], started["ready"]["gid"]), ("ready", phase, 4242))
                self.assertEqual({key: started["ready"][key] for key in wanted}, wanted)
                present = self.run_phase(native, "observe")
                self.assertEqual({key: present["view"]["entry"][key] for key in wanted}, wanted)
                # Neither receipt persists a padding value, only its summary.
                self.assertNotIn(oracle.padding(TOKEN, mode)[0][1], json.dumps([started, present]))
                # A later view without the padding is visibly different.
                self.assertEqual(self.run_phase(native, "observe")["view"]["entry"]["pad_keys"], 0)

    def test_start_requires_the_readiness_of_its_own_registration_size(self):
        for phase, wrong in (("start-large", "hold"), ("start", "hold-large"), ("start-wide", "hold-large"), ("start-large", "hold-wide")):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                native = FakeNative(out, [dump()])
                native.wrong_size = wrong
                with self.assertRaisesRegex(ValueError, "different registration size", msg=wrong):
                    oracle.collect(out, out, "1", TOKEN, phase, native)

    def test_a_token_is_started_once_with_either_step(self):
        for first, second in (("start", "start-large"), ("start-large", "start"), ("start-wide", "start-large"), ("start", "start-wide")):
            with tempfile.TemporaryDirectory() as tmp:
                out = pathlib.Path(tmp)
                native = FakeNative(out, [dump()])
                oracle.collect(out, out, "1", TOKEN, first, native)
                with self.assertRaisesRegex(ValueError, "never replayed"):
                    oracle.collect(out, out, "2", TOKEN, second, native)
                self.assertEqual(native.spawned, 1)

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

        def spawn_failure(root, output, token, mode="hold"):
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

    def test_large_hold_registers_the_padding_as_native_metadata(self):
        registered = []

        class Lib(FakeLib):
            def __getattr__(self, name):
                call = super().__getattr__(name)
                if name != "rados_service_register":
                    return call

                def register(cluster, service, daemon, metadata):
                    registered.append(metadata)
                    return call(cluster, service, daemon, metadata)
                return register

        def sleep(seconds):
            oracle.state_path(self.out, TOKEN, "go.release").write_text("release\n")
        oracle.hold(self.out, self.out, TOKEN, lib=Lib(), sleep=sleep, mode="hold-large")
        fields = registered[0].split(b"\0")
        # key\0value\0 pairs, closed by one further NUL.
        self.assertEqual(fields[-2:], [b"", b""])
        pairs = dict(zip((f.decode() for f in fields[:-2:2]), (f.decode() for f in fields[1:-2:2])))
        self.assertEqual(len(pairs), 4096 + 2)
        self.assertEqual((pairs["ceph_msgr_role"], pairs["ceph_msgr_daemon"]), ("holder", DAEMON))
        ready = json.loads(oracle.state_path(self.out, TOKEN, "ready.json").read_text())
        self.assertEqual(oracle.pad_summary(pairs), {key: ready[key] for key in ("pad_keys", "pad_bytes", "pad_sha256")})
        self.assertEqual(ready["pad_sha256"], PINNED_PAD_SHA256)

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
