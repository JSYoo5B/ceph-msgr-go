import base64
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import mon_tell_outcome_oracle as oracle


def status(name="b", rank=1):
    return json.dumps({"name": name, "rank": rank, "state": "peon",
                       "monmap": {"mons": [{"name": "unrelated", "addr": "192.0.2.9:6789/0"}]}}).encode()


class FakeClient:
    """Process-local option state of one fake daemon, reached by Tell target."""

    def __init__(self, effective="500", override=None, name="b", extra_diff=False):
        self.effective, self.override, self.name = effective, override, name
        self.extra_diff = extra_diff
        self.commands = []

    def mon_command(self, command, payload, target=None):
        value = json.loads(command)
        self.commands.append((target, value))
        prefix = value["prefix"]
        if prefix == "mon_status":
            return 0, status(self.name), ""
        if prefix == "config get":
            return 0, json.dumps({oracle.OPTION: self.effective}).encode(), ""
        if prefix == "config diff get":
            diff = {}
            if self.override is not None:
                diff[oracle.OPTION] = {"default": 500, "override": int(self.override),
                                       "final": int(self.override)}
            if self.extra_diff:
                diff["debug_mon"] = {"default": "1/5", "file": "5/5", "final": "5/5"}
            return 0, json.dumps({"diff": diff}).encode(), ""
        if prefix == "config set":
            self.effective = self.override = value["val"][0]
            return 0, b"", ""
        if prefix == "config unset":
            self.effective, self.override = "500", None
            return 0, b"", ""
        raise AssertionError("unexpected native command " + prefix)


class OwnedMonTellOutcomeTest(unittest.TestCase):
    token = "c" * 32

    def test_closed_inputs(self):
        for raw_id, token, phase, target in (
                ("0", self.token, "initial", "a"), ("-1", self.token, "initial", "a"),
                ("1", "../key", "initial", "a"), ("1", self.token, "cleanup", "a"),
                ("1", self.token, "initial", "mon.a"), ("1", self.token, "initial", "d"),
                ("1", self.token, "initial", ""), (str(1 << 63), self.token, "initial", "a")):
            with self.assertRaises(ValueError):
                oracle.validate(raw_id, token, phase, target)
        self.assertEqual(oracle.validate("7", self.token, "restore", "c"), 7)

    def test_views_use_the_exact_target_and_publish_only_the_owned_option(self):
        client = FakeClient(extra_diff=False)
        observed = oracle.views(client, "b")
        self.assertEqual({target for target, _ in client.commands}, {"b"})
        self.assertEqual([command["prefix"] for _, command in client.commands],
                         ["mon_status", "config get", "config diff get", "mon_status"])
        self.assertEqual(json.loads(base64.b64decode(observed["config_get"]["data"])), {oracle.OPTION: "500"})
        self.assertNotIn("192.0.2.9", json.dumps(observed))
        self.assertEqual((observed["daemon_before"]["name"], observed["daemon_after"]["rank"]), ("b", 1))
        self.assertEqual(oracle.restorable(observed, "b"), {"present": False, "effective": "500"})

    def test_wider_diff_reply_is_refused(self):
        with self.assertRaises(ValueError):
            oracle.views(FakeClient(extra_diff=True), "b")

    def test_override_presence_is_separate_from_the_effective_value(self):
        absent = oracle.views(FakeClient("501"), "b")
        self.assertEqual(oracle.restorable(absent, "b"), {"present": False, "effective": "501"})
        present = oracle.views(FakeClient("501", "501"), "b")
        self.assertEqual(present["config_diff"]["entry"]["override"], "501")
        self.assertEqual(oracle.restorable(present, "b"),
                         {"present": True, "value": "501", "effective": "501"})
        self.assertIsNone(oracle.restorable(oracle.views(FakeClient("700", "501"), "b"), "b"))

    def test_baseline_outside_the_bound_is_not_restorable(self):
        for effective in ("0", "19999", "20000", "20001", "-1", "not-a-number"):
            observed = oracle.views(FakeClient(effective), "b")
            self.assertIsNone(oracle.restorable(observed, "b"), effective)
        for effective in ("1", "19998"):
            self.assertIsNotNone(oracle.restorable(oracle.views(FakeClient(effective), "b"), "b"))

    def test_wrong_daemon_identity_is_not_restorable(self):
        observed = oracle.views(FakeClient(name="a"), "b")
        self.assertIsNone(oracle.restorable(observed, "b"))

    def test_restoration_commands_are_closed(self):
        self.assertEqual(oracle.restoration_command({"present": False}),
                         {"prefix": "config unset", "var": oracle.OPTION, "format": "json"})
        self.assertEqual(oracle.restoration_command({"present": True, "value": "501"}),
                         {"prefix": "config set", "var": oracle.OPTION, "val": ["501"], "format": "json"})
        for baseline in (None, {}, {"present": True}, {"present": True, "value": "20000"},
                         {"present": True, "value": "0"}, {"present": True, "value": "0501"},
                         {"present": True, "value": ["501", "502"]}, {"present": "yes"}):
            with self.assertRaises(ValueError):
                oracle.restoration_command(baseline)

    def test_failure_diagnostic_is_bounded_and_masks_paths(self):
        text = oracle.diagnostic(OSError("cannot read /private/place/keyring now " + "x" * 400))
        self.assertTrue(text.startswith("OSError: cannot read <path> now"))
        self.assertNotIn("/private", text)
        self.assertLessEqual(len(text), 300)
        self.assertEqual(oracle.diagnostic(KeyError()), "KeyError")

    def run_collect(self, output, phase, client, target="b"):
        with patch.object(oracle, "native_handle") as handle:
            handle.return_value.__enter__.return_value = client
            oracle.collect(pathlib.Path("/unused"), output, "9", self.token, phase, target)

    def test_baseline_ownership_restoration_happens_once(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            client = FakeClient("500")
            self.run_collect(output, "initial", client)
            with self.assertRaises(FileExistsError):
                self.run_collect(output, "initial", client)
            client.effective = client.override = "502"
            self.run_collect(output, "applied", client)
            self.run_collect(output, "restore", client)
            self.assertEqual((client.effective, client.override), ("500", None))
            self.assertEqual([c["prefix"] for _, c in client.commands].count("config unset"), 1)
            with self.assertRaises(FileExistsError):
                self.run_collect(output, "restore", client)
            self.assertEqual([c["prefix"] for _, c in client.commands].count("config unset"), 1)
            restored = json.loads((output / (oracle.PREFIX + self.token + ".restore.json")).read_text())
            self.assertEqual(restored["restoration"]["code"], 0)

    def test_other_target_or_token_cannot_use_a_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            client = FakeClient("500")
            self.run_collect(output, "initial", client)
            with self.assertRaises(ValueError):
                self.run_collect(output, "restore", client, target="c")
            self.assertNotIn("config unset", [c["prefix"] for _, c in client.commands])

    def test_out_of_bound_baseline_is_not_claimed_or_restorable(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            client = FakeClient("19999")
            self.run_collect(output, "initial", client)
            self.assertFalse((output / (oracle.PREFIX + self.token + ".baseline.json")).exists())
            with self.assertRaises(FileNotFoundError):
                self.run_collect(output, "restore", client)
            self.assertNotIn("config unset", [c["prefix"] for _, c in client.commands])


if __name__ == "__main__":
    unittest.main()
