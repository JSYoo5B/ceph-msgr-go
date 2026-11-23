import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_pglogbounds_oracle as bounds

DEFAULTS = {"osd_min_pg_log_entries": 250, "osd_max_pg_log_entries": 10000, "osd_pg_log_trim_min": 100, "osd_pg_log_dups_tracked": 3000}


class FakeRados:
    """The central configuration as `config set`, `config rm` and `config
    dump` change and read it, and OSDs whose effective values follow it after
    `lag` reads each; no daemon is involved."""

    def __init__(self, entries=(), osds=(0, 1, 2, 3), lag=0, refuse=(), down=()):
        self.entries, self.refuse, self.commands, self.last_status = dict(entries), set(refuse), [], ""
        self.osds, self.down, self.lag = list(osds), set(down), lag
        self.reads = {osd: 0 for osd in osds}
        self.applied = {osd: self.central() for osd in osds}

    def central(self):
        values = dict(DEFAULTS)
        for (section, name), value in self.entries.items():
            if section == "osd" and name in values:
                values[name] = int(value)
        return values

    def mon(self, cluster, command):
        self.commands.append(command["prefix"])
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
        return {"osds": [{"osd": osd, "up": 0 if osd in self.down else 1, "in": 1} for osd in self.osds]}

    def osd(self, cluster, osd, command, limit):
        assert command["prefix"] == "config get", command
        name = command["var"]
        if name == bounds.OPTIONS[0]:
            # One read of every option counts as one read of the OSD.
            self.reads[osd] += 1
            if self.reads[osd] > self.lag:
                self.applied[osd] = self.central()
        return 0, json.dumps({name: str(self.applied[osd][name])}).encode(), "", None


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class PGLogBounds(unittest.TestCase):
    def run_mode(self, rados, out, action, clock=None):
        clock = clock or Clock()
        return bounds.mode_pglogbounds(rados, None, {"action": action}, out, clock=clock, sleep=clock.sleep)["pglog_bounds"]

    def test_lower_then_restore_without_prior_entries(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({("osd", "osd_op_history_size"): "20"}, lag=3)
            lowered = self.run_mode(rados, out, "lower")
            self.assertTrue(lowered["lowered"], lowered["failures"])
            self.assertEqual(lowered["prior"], dict.fromkeys(bounds.OPTIONS))
            self.assertEqual(lowered["prior_effective"], {str(osd): DEFAULTS for osd in range(4)})
            self.assertEqual(lowered["effective"], {str(osd): dict(bounds.BOUNDS) for osd in range(4)})
            self.assertEqual(lowered["entries"], {name: str(value) for name, value in bounds.BOUNDS})
            self.assertEqual(rados.commands.count("config set"), len(bounds.BOUNDS))
            with self.assertRaises(RuntimeError):
                self.run_mode(rados, out, "lower")
            restored = self.run_mode(rados, out, "restore")
            self.assertTrue(restored["restored"], restored["failures"])
            self.assertEqual(restored["effective"], {str(osd): DEFAULTS for osd in range(4)})
            self.assertEqual(rados.entries, {("osd", "osd_op_history_size"): "20"})
            self.assertFalse((pathlib.Path(out) / bounds.CLAIM).exists())
            self.assertEqual(self.run_mode(rados, out, "restore"), {"action": "restore", "bounds": dict(bounds.BOUNDS), "claimed": False, "failures": [], "restored": False})

    def test_restore_puts_prior_values_back(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados({("osd", "osd_pg_log_dups_tracked"): "2000"})
            self.assertTrue(self.run_mode(rados, out, "lower")["lowered"])
            self.assertEqual(rados.entries[("osd", "osd_pg_log_dups_tracked")], "12")
            restored = self.run_mode(rados, out, "restore")
            self.assertTrue(restored["restored"], restored["failures"])
            self.assertEqual(rados.entries, {("osd", "osd_pg_log_dups_tracked"): "2000"})
            self.assertEqual(restored["effective"]["0"]["osd_pg_log_dups_tracked"], 2000)

    def test_an_osd_that_never_applies_fails_within_the_bound(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados(lag=10 ** 6)
            clock = Clock()
            lowered = self.run_mode(rados, out, "lower", clock)
            self.assertFalse(lowered["lowered"])
            self.assertTrue(lowered["failures"] and all("shows" in f for f in lowered["failures"]))
            self.assertGreaterEqual(clock.now, bounds.SETTLE_SECONDS)
            self.assertLess(clock.now, bounds.SETTLE_SECONDS + 1)
            # The central entries were set once each; nothing was resent.
            self.assertEqual(rados.commands.count("config set"), len(bounds.BOUNDS))
            self.assertTrue((pathlib.Path(out) / bounds.CLAIM).exists())
            rados.lag = 0
            self.assertTrue(self.run_mode(rados, out, "restore")["restored"])

    def test_refused_set_keeps_the_claim_and_stops(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados(refuse={"config set"})
            lowered = self.run_mode(rados, out, "lower")
            self.assertFalse(lowered["lowered"])
            self.assertEqual(rados.commands.count("config set"), 1)
            self.assertEqual(bounds.read_claim(out)["prior"], dict.fromkeys(bounds.OPTIONS))

    def test_refused_restore_keeps_the_claim(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados()
            self.run_mode(rados, out, "lower")
            rados.refuse.add("config rm")
            restored = self.run_mode(rados, out, "restore")
            self.assertFalse(restored["restored"])
            # Every option is attempted once even after a refusal.
            self.assertEqual(rados.commands.count("config rm"), len(bounds.OPTIONS))
            self.assertTrue((pathlib.Path(out) / bounds.CLAIM).exists())

    def test_restore_confirms_the_central_entries(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados()
            self.run_mode(rados, out, "lower")
            mon = rados.mon
            # A `config rm` that reports success but leaves the entry.
            rados.mon = lambda cluster, command: (0, b"") if command["prefix"] == "config rm" else mon(cluster, command)
            restored = self.run_mode(rados, out, "restore")
            self.assertFalse(restored["restored"])
            self.assertTrue(any("central entries" in f for f in restored["failures"]), restored["failures"])
            self.assertNotIn("effective", restored)
            self.assertTrue((pathlib.Path(out) / bounds.CLAIM).exists())

    def test_restore_waits_for_the_recorded_osds(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados()
            self.run_mode(rados, out, "lower")
            rados.lag = 10 ** 6
            restored = self.run_mode(rados, out, "restore")
            self.assertFalse(restored["restored"])
            self.assertTrue((pathlib.Path(out) / bounds.CLAIM).exists())

    def test_a_down_osd_is_not_recorded_and_too_few_refuse(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados(down={2})
            self.assertEqual(set(self.run_mode(rados, out, "lower")["prior_effective"]), {"0", "1", "3"})
        with tempfile.TemporaryDirectory() as out:
            with self.assertRaises(RuntimeError):
                self.run_mode(FakeRados(down={0, 1, 2, 3}), out, "lower")
            self.assertFalse((pathlib.Path(out) / bounds.CLAIM).exists())

    def test_show_only_reads(self):
        with tempfile.TemporaryDirectory() as out:
            rados = FakeRados()
            shown = self.run_mode(rados, out, "show")
            self.assertEqual(shown["effective"]["3"], DEFAULTS)
            self.assertNotIn("config set", rados.commands)
            self.assertNotIn("config rm", rados.commands)

    def test_input_claim_and_entry_shapes(self):
        for request in ({}, {"action": "set"}, {"action": "lower", "value": "4"}):
            with self.assertRaises(ValueError):
                bounds.mode_pglogbounds(FakeRados(), None, request, None)
        with tempfile.TemporaryDirectory() as out:
            path = pathlib.Path(out) / bounds.CLAIM
            good = {"prior": dict.fromkeys(bounds.OPTIONS), "effective": {"0": DEFAULTS}}
            for bad in ({"prior": {}, "effective": {"0": DEFAULTS}}, dict(good, extra=1), dict(good, effective={}),
                        dict(good, effective={"x": DEFAULTS}), dict(good, effective={"0": dict(DEFAULTS, osd_pg_log_trim_min="1")}),
                        dict(good, prior=dict.fromkeys(bounds.OPTIONS, 4))):
                path.write_text(json.dumps(bad))
                with self.assertRaises(ValueError):
                    bounds.read_claim(out)
            path.write_text(json.dumps(good))
            self.assertEqual(bounds.read_claim(out), good)
        rados = FakeRados()
        rados.entries[("osd", "osd_pg_log_trim_min")] = "1"
        dump = rados.mon(None, {"prefix": "config dump"})[1]
        twice = json.loads(dump) * 2
        rados.mon = lambda cluster, command: (0, json.dumps(twice).encode())
        with self.assertRaises(RuntimeError):
            bounds.entries(rados, None)
        masked = FakeRados()
        masked.mon = lambda cluster, command: (0, json.dumps([{"section": "osd.1", "name": "osd_pg_log_trim_min", "value": "1"},
                                                            {"section": "osd", "name": "osd_pg_log_trim_min", "value": "1", "mask": "host:x"}]).encode())
        self.assertEqual(bounds.entries(masked, None), dict.fromkeys(bounds.OPTIONS))

    def test_bounds_put_a_trimmed_entry_into_the_duplicate_window(self):
        values = dict(bounds.BOUNDS)
        # The target is the clamp of both entry bounds; an entry trimmed past it
        # is younger than the duplicate window (src/osd/PGLog.cc:67-70, 88).
        self.assertEqual(values["osd_min_pg_log_entries"], values["osd_max_pg_log_entries"])
        self.assertLess(values["osd_max_pg_log_entries"] + 4, values["osd_pg_log_dups_tracked"])
        self.assertEqual(values["osd_pg_log_trim_min"], 1)

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["pglogbounds"], bounds.mode_pglogbounds)


if __name__ == "__main__":
    unittest.main()
