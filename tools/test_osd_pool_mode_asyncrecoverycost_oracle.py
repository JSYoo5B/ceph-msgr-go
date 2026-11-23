import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_asyncrecoverycost_oracle as cost


class FakeRados:
    """The central configuration of osd_async_recovery_min_cost as `config
    set`, `config rm` and `config dump` change and read it, and OSDs that
    apply it after `lag` reads each; no daemon is involved."""

    def __init__(self, entries=(), osds=(0, 1, 2, 3), lag=0, refuse=()):
        self.entries, self.refuse, self.commands, self.last_status = dict(entries), set(refuse), [], ""
        self.osds, self.lag = list(osds), lag
        self.reads = {osd: 0 for osd in osds}
        self.applied = {osd: self.central() for osd in osds}

    def central(self):
        return int(self.entries.get(("osd", "osd_async_recovery_min_cost"), 100))

    def mon(self, cluster, command):
        self.commands.append((command["prefix"], command.get("name"), command.get("value")))
        if command["prefix"] in self.refuse:
            return -22, b""
        key = (command.get("who"), command.get("name"))
        if command["prefix"] == "config set":
            self.entries[key] = command["value"]
            self.reads = {osd: 0 for osd in self.osds}
        elif command["prefix"] == "config rm":
            self.entries.pop(key, None)
            self.reads = {osd: 0 for osd in self.osds}
        elif command["prefix"] == "config dump":
            return 0, json.dumps([{"section": s, "name": n, "value": v} for (s, n), v in self.entries.items()]).encode()
        return 0, b""

    def dump(self, cluster):
        return {"osds": [{"osd": osd, "up": 1, "in": 1} for osd in self.osds]}

    def osd(self, cluster, osd, command, limit):
        assert command == {"prefix": "config get", "var": "osd_async_recovery_min_cost", "format": "json"}, command
        self.reads[osd] += 1
        if self.reads[osd] > self.lag:
            self.applied[osd] = self.central()
        return 0, json.dumps({"osd_async_recovery_min_cost": str(self.applied[osd])}).encode(), "", None


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class AsyncRecoveryCost(unittest.TestCase):
    def run_mode(self, rados, out, action):
        clock = Clock()
        return cost.mode_asyncrecoverycost(rados, None, {"action": action}, out, clock=clock, sleep=clock.sleep)["async_recovery_cost"]

    def test_lower_then_restore_the_default(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados(lag=2)
            lowered = self.run_mode(rados, out, "lower")
            self.assertTrue(lowered["lowered"], lowered["failures"])
            self.assertEqual(lowered["prior"], {"osd_async_recovery_min_cost": None})
            self.assertEqual(lowered["prior_effective"], {str(osd): {"osd_async_recovery_min_cost": 100} for osd in range(4)})
            self.assertEqual(lowered["effective"], {str(osd): {"osd_async_recovery_min_cost": 0} for osd in range(4)})
            self.assertIn(("config set", "osd_async_recovery_min_cost", "0"), rados.commands)
            self.assertTrue((pathlib.Path(out) / cost.CLAIM).exists())
            with self.assertRaises(RuntimeError):
                self.run_mode(rados, out, "lower")
            restored = self.run_mode(rados, out, "restore")
            self.assertTrue(restored["restored"], restored["failures"])
            self.assertEqual(restored["entries"], {"osd_async_recovery_min_cost": None})
            self.assertEqual(restored["effective"], {str(osd): {"osd_async_recovery_min_cost": 100} for osd in range(4)})
            self.assertIn(("config rm", "osd_async_recovery_min_cost", None), rados.commands)
            self.assertFalse((pathlib.Path(out) / cost.CLAIM).exists())
            self.assertFalse(self.run_mode(rados, out, "restore")["restored"])

    def test_restores_a_prior_entry_and_reports_a_refused_set(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({("osd", "osd_async_recovery_min_cost"): "40"})
            self.assertTrue(self.run_mode(rados, out, "lower")["lowered"])
            restored = self.run_mode(rados, out, "restore")
            self.assertEqual(restored["entries"], {"osd_async_recovery_min_cost": "40"})
            self.assertTrue(restored["restored"])
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados(refuse={"config set"})
            lowered = self.run_mode(rados, out, "lower")
            self.assertFalse(lowered["lowered"])
            self.assertEqual(sum(1 for c in rados.commands if c[0] == "config set"), 1)
            # The claim stays for the restore.
            self.assertTrue((pathlib.Path(out) / cost.CLAIM).exists())

    def test_requests(self):
        for request in ({}, {"action": "set"}, {"action": "lower", "value": 0}):
            with self.assertRaises(ValueError):
                cost.mode_asyncrecoverycost(FakeRados(), None, request, "/nonexistent")


if __name__ == "__main__":
    unittest.main()
