import base64
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import mgr_command_outcome_oracle as oracle


def status(mode):
    return json.dumps({"active": True, "mode": mode, "plans": []}, indent=4).encode()


def dump(*entries):
    unrelated = {"section": "global", "name": "mon_allow_pool_delete", "value": "true",
                 "level": "advanced", "can_update_at_runtime": True, "mask": ""}
    return json.dumps([unrelated] + [dict(entry, level="advanced", can_update_at_runtime=True)
                                     for entry in entries]).encode()


def manager(name, gid):
    return json.dumps({"epoch": 9, "active_name": name, "active_gid": gid, "available": True,
                       "active_addrs": {"addrvec": [{"addr": "192.0.2.1:6800"}]}}).encode()


class FakeClient:
    def __init__(self, mode, entries, name="a", gid=4100, effective=None):
        self.mode, self.entries, self.name, self.gid = mode, entries, name, gid
        self.effective = effective if effective is not None else mode
        self.commands = []

    def mon_command(self, command, payload):
        value = json.loads(command)
        self.commands.append(value)
        if value["prefix"] == "mgr dump":
            return 0, manager(self.name, self.gid), ""
        if value["prefix"] == "config get":
            return 0, (self.effective + "\n").encode(), ""
        if value["prefix"] == "config dump":
            return 0, dump(*self.entries), ""
        raise AssertionError("unexpected native command " + value["prefix"])

    def mgr_command(self, command, payload):
        self.commands.append(json.loads(command))
        return 0, status(self.mode), ""


class OwnedManagerOutcomeTest(unittest.TestCase):
    token = "b" * 32

    def test_closed_inputs(self):
        for raw_id, token, phase in (("0", self.token, "initial"),
                                     ("-1", self.token, "initial"),
                                     ("1", "../key", "initial"),
                                     ("1", self.token, "cleanup"),
                                     (str(1 << 63), self.token, "initial")):
            with self.assertRaises(ValueError):
                oracle.validate(raw_id, token, phase)

    def test_views_keep_raw_results_and_only_the_owned_option(self):
        client = FakeClient("upmap", [{"section": "mgr", "name": oracle.OPTION, "value": "none", "mask": ""}],
                            effective="none")
        observed = oracle.views(client)
        self.assertEqual(base64.b64decode(observed["balancer_status"]["data"]), status("upmap"))
        self.assertEqual(base64.b64decode(observed["config_get"]["data"]), b"none\n")
        self.assertEqual(observed["config_entries"]["entries"],
                         [{"section": "mgr", "name": oracle.OPTION, "value": "none", "mask": ""}])
        self.assertNotIn("mon_allow_pool_delete", json.dumps(observed))
        self.assertNotIn("192.0.2.1", json.dumps(observed))
        self.assertEqual((observed["manager_before"]["active_name"], observed["manager_after"]["active_gid"]),
                         ("a", 4100))
        self.assertEqual([command["prefix"] for command in client.commands],
                         ["mgr dump", "balancer status", "config get", "config dump", "mgr dump"])

    def test_effective_value_alone_does_not_establish_an_entry(self):
        absent = oracle.views(FakeClient("upmap", []))
        self.assertEqual(oracle.restorable(absent), {"present": False})
        present = oracle.views(FakeClient("upmap", [{"section": "mgr", "name": oracle.OPTION,
                                                      "value": "upmap", "mask": ""}]))
        self.assertEqual(oracle.restorable(present), {"present": True, "value": "upmap"})
        for entries in ([{"section": "mgr.a", "name": oracle.OPTION, "value": "none", "mask": ""}],
                        [{"section": "mgr", "name": oracle.OPTION, "value": "none", "mask": "host:x"}],
                        [{"section": "mgr", "name": oracle.OPTION, "value": "none", "mask": ""},
                         {"section": "global", "name": oracle.OPTION, "value": "none", "mask": ""}]):
            self.assertIsNone(oracle.restorable(oracle.views(FakeClient("upmap", entries))))

    def run_phases(self, output, baseline_entries, restoration):
        views = [oracle.views(FakeClient("upmap", baseline_entries)),
                 oracle.views(FakeClient("none", [{"section": "mgr", "name": oracle.OPTION, "value": "none", "mask": ""}])),
                 oracle.views(FakeClient("upmap", baseline_entries))]
        with patch.object(oracle, "native_views", side_effect=views) as native, \
                patch.object(oracle, "native_restore", side_effect=restoration) as restore:
            oracle.collect(output, output, "1", self.token, "initial")
            oracle.collect(output, output, "2", self.token, "applied")
            try:
                oracle.collect(output, output, "3", self.token, "restore")
            finally:
                with self.assertRaises(FileExistsError):
                    oracle.collect(output, output, "4", self.token, "restore")
        return native, restore

    def test_absent_baseline_is_removed_once_and_read_freshly(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            native, restore = self.run_phases(output, [], [oracle.result(0, b"", "")])
            self.assertEqual(restore.call_count, 1)
            self.assertEqual(restore.call_args.args[1], {"present": False})
            self.assertEqual(native.call_count, 3)
            receipt = json.loads((output / (oracle.PREFIX + self.token + ".restore.json")).read_text())
            self.assertEqual(receipt["restoration"]["code"], 0)
            self.assertEqual(receipt["views"]["config_entries"]["entries"], [])

    def test_present_baseline_restores_its_original_value(self):
        entry = {"section": "mgr", "name": oracle.OPTION, "value": "upmap", "mask": ""}
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            _, restore = self.run_phases(output, [entry], [oracle.result(-13, b"\xff", "denied")])
            self.assertEqual(restore.call_args.args[1], {"present": True, "value": "upmap"})
            receipt = json.loads((output / (oracle.PREFIX + self.token + ".restore.json")).read_text())
            self.assertEqual((receipt["restoration"]["code"], receipt["restoration"]["data"]), (-13, "/w=="))

    def test_native_restore_commands_are_closed(self):
        client = FakeClient("upmap", [])
        client.mon_command = lambda command, payload: (client.commands.append(json.loads(command)) or (0, b"", ""))
        with patch.object(oracle, "native_handle") as handle:
            handle.return_value.__enter__.return_value = client
            oracle.native_restore(pathlib.Path("."), {"present": False})
            oracle.native_restore(pathlib.Path("."), {"present": True, "value": "none"})
        self.assertEqual(client.commands, [
            {"prefix": "config rm", "who": "mgr", "name": oracle.OPTION},
            {"prefix": "config set", "who": "mgr", "name": oracle.OPTION, "value": "none"}])

    def test_uncertain_restore_cannot_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            with self.assertRaises(TimeoutError):
                self.run_phases(output, [], TimeoutError)
            self.assertTrue((output / (oracle.PREFIX + self.token + ".restore-started.json")).exists())
            self.assertFalse((output / (oracle.PREFIX + self.token + ".restore-mutation.json")).exists())

    def test_unrestorable_baseline_is_never_claimed(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            entries = [{"section": "mgr.a", "name": oracle.OPTION, "value": "none", "mask": ""}]
            with patch.object(oracle, "native_views", return_value=oracle.views(FakeClient("none", entries))), \
                    patch.object(oracle, "native_restore") as restore:
                oracle.collect(output, output, "1", self.token, "initial")
                with self.assertRaises(FileNotFoundError):
                    oracle.collect(output, output, "2", self.token, "restore")
            self.assertEqual(restore.call_count, 0)
            receipt = json.loads((output / (oracle.PREFIX + self.token + ".initial.json")).read_text())
            self.assertIsNone(receipt["baseline"])


if __name__ == "__main__":
    unittest.main()
