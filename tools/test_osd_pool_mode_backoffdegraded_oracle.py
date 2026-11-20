import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_backoffdegraded_oracle as degraded


class FakeRados:
    """The central configuration as `config set`, `config rm` and `config
    dump` change and read it; no daemon is involved."""

    def __init__(self, entries, refuse=()):
        self.entries, self.refuse, self.commands, self.last_status = dict(entries), set(refuse), [], ""

    def mon(self, cluster, command):
        self.commands.append(command["prefix"])
        if command["prefix"] in self.refuse:
            return -1, b""
        key = (command.get("who"), command.get("name"))
        if command["prefix"] == "config set":
            self.entries[key] = command["value"]
        elif command["prefix"] == "config rm":
            self.entries.pop(key, None)
        elif command["prefix"] == "config dump":
            return 0, json.dumps([{"section": s, "name": n, "value": v} for (s, n), v in self.entries.items()]).encode()
        return 0, b""


class BackoffDegraded(unittest.TestCase):
    def run_mode(self, rados, out, action):
        return degraded.mode_backoffdegraded(rados, None, {"action": action}, out)["backoff_degraded"]

    def test_enable_then_restore_without_a_prior_entry(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({("osd", "osd_op_history_size"): "20"})
            self.assertEqual(self.run_mode(rados, out, "enable"), {"action": "enable", "prior": None, "value": "true", "claimed": True})
            with self.assertRaises(RuntimeError):
                self.run_mode(rados, out, "enable")
            self.assertEqual(self.run_mode(rados, out, "restore"),
                             {"action": "restore", "prior": None, "value": None, "claimed": True, "restored": True})
            self.assertEqual(rados.entries, {("osd", "osd_op_history_size"): "20"})
            self.assertFalse((pathlib.Path(out) / degraded.CLAIM).exists())
            self.assertEqual(self.run_mode(rados, out, "restore"), {"action": "restore", "value": None, "claimed": False, "restored": False})

    def test_restore_puts_a_prior_value_back(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({("osd", "osd_backoff_on_degraded"): "false"})
            self.run_mode(rados, out, "enable")
            self.assertEqual(rados.entries[("osd", "osd_backoff_on_degraded")], "true")
            self.assertTrue(self.run_mode(rados, out, "restore")["restored"])
            self.assertEqual(rados.entries[("osd", "osd_backoff_on_degraded")], "false")

    def test_failed_restore_keeps_the_claim(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({})
            self.run_mode(rados, out, "enable")
            rados.refuse.add("config rm")
            self.assertFalse(self.run_mode(rados, out, "restore")["restored"])
            self.assertTrue((pathlib.Path(out) / degraded.CLAIM).exists())

    def test_failed_enable_keeps_the_claim_for_restore(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({}, refuse={"config set"})
            with self.assertRaises(RuntimeError):
                self.run_mode(rados, out, "enable")
            self.assertEqual(degraded.read_claim(out), {"prior": None})

    def test_input_and_entry_shapes(self):
        for request in ({}, {"action": "set"}, {"action": "enable", "value": "true"}):
            with self.assertRaises(ValueError):
                degraded.mode_backoffdegraded(FakeRados({}), None, request, None)
        with self.assertRaises(RuntimeError):
            degraded.entry([{"section": "osd", "name": degraded.OPTION, "value": "true"}] * 2)
        self.assertIsNone(degraded.entry([{"section": "osd.1", "name": degraded.OPTION, "value": "true"},
                                          {"section": "osd", "name": degraded.OPTION, "value": "true", "mask": "host:x"}]))

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["backoffdegraded"], degraded.mode_backoffdegraded)


if __name__ == "__main__":
    unittest.main()
