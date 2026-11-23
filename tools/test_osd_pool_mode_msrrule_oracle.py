import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_msrrule_oracle as msrrule

ORDER = ("luminous", "mimic", "nautilus", "octopus", "pacific", "quincy", "reef", "squid", "tentacle")


class FakeMon:
    """A scripted MON that applies the pinned guards to an in-memory map."""

    def __init__(self, baseline="luminous", rule_type=5, keep_rule=False, keep_baseline=False):
        self.baseline, self.rule_type, self.keep_rule, self.keep_baseline = baseline, rule_type, keep_rule, keep_baseline
        self.profiles, self.rules, self.commands, self.last_status, self.epoch = set(), ["replicated_rule"], [], "", 20

    def dump(self, cluster):
        return {"epoch": self.epoch, "require_min_compat_client": self.baseline, "pools": []}

    def mon(self, cluster, command):
        self.commands.append(command)
        prefix, self.last_status = command["prefix"], ""
        if prefix == "osd crush rule ls":
            return 0, json.dumps(self.rules).encode()
        if prefix == "osd crush rule dump":
            return 0, json.dumps([{"rule_name": name, "type": self.rule_type if name == msrrule.RULE else 1,
                                   "steps": [{"op": "take"}, {"op": "choosemsr" if name == msrrule.RULE else "choose_firstn"}, {"op": "emit"}]}
                                  for name in self.rules]).encode()
        if prefix == "osd erasure-code-profile get":
            return (0, b"{}") if command["name"] in self.profiles else (-2, b"")
        if prefix == "osd erasure-code-profile set":
            self.profiles.add(command["name"])
            return 0, b""
        if prefix == "osd erasure-code-profile rm":
            self.profiles.discard(command["name"])
            return 0, b""
        if prefix == "osd crush rule create-erasure":
            if ORDER.index(self.baseline) < ORDER.index("squid"):
                self.last_status = "new crush map requires client version squid but require_min_compat_client is " + self.baseline
                return -22, b""
            self.rules.append(command["name"])
            self.epoch += 1
            return 0, b""
        if prefix == "osd crush rule rm":
            if not self.keep_rule:
                self.rules.remove(command["name"])
            return 0, b""
        if prefix == "osd set-require-min-compat-client":
            if msrrule.RULE in self.rules and ORDER.index(command["version"]) < ORDER.index("squid"):
                self.last_status = "osdmap current utilizes features that require squid"
                return -1, b""
            if not command.get("yes_i_really_mean_it") and ORDER.index(command["version"]) > ORDER.index(self.baseline):
                self.last_status = "cannot set require_min_compat_client to squid: 1 connected client(s) look like luminous (missing 0x4000000000)"
                return -1, b""
            if not self.keep_baseline:
                self.baseline = command["version"]
            self.epoch += 1
            return 0, b""
        raise AssertionError("unexpected command %r" % (command,))


class MSRRule(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.out = pathlib.Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)

    def run_mode(self, mon, action):
        return msrrule.mode_msrrule(mon, None, {"action": action}, self.out)["msr_rule"]

    def test_probe_apply_restore(self):
        mon = FakeMon()
        probe = self.run_mode(mon, "probe")
        self.assertEqual((probe["prior"], probe["baseline"], probe["rule_present"]), ("luminous", "luminous", False))
        self.assertEqual(probe["rule_create"]["code"], -22)
        self.assertIn("requires client version squid", probe["rule_create"]["status"])
        self.assertEqual(probe["baseline_change"]["code"], -1)
        self.assertIn("look like", probe["baseline_change"]["status"])
        # Each guarded command went out once, and the change without the confirmation flag had none.
        self.assertEqual(sum(c["prefix"] == "osd crush rule create-erasure" for c in mon.commands), 1)
        plain = [c for c in mon.commands if c["prefix"] == "osd set-require-min-compat-client"]
        self.assertEqual(plain, [{"prefix": "osd set-require-min-compat-client", "version": "squid"}])
        profile = [c for c in mon.commands if c["prefix"] == "osd erasure-code-profile set"]
        self.assertEqual(profile, [{"prefix": "osd erasure-code-profile set", "name": msrrule.PROFILE, "profile": msrrule.PROFILE_SETTINGS}])

        applied = self.run_mode(mon, "apply")
        self.assertEqual((applied["baseline"], applied["rule"]["type"], applied["prior"]), ("squid", 5, "luminous"))
        self.assertIn(msrrule.RULE, mon.rules)

        restored = self.run_mode(mon, "restore")
        self.assertEqual((restored["restored"], restored["owned"], restored["baseline"], restored["rule_present"], restored["profile_present"]),
                         (True, True, "luminous", False, False))
        self.assertEqual((mon.rules, mon.profiles, mon.baseline), (["replicated_rule"], set(), "luminous"))
        self.assertFalse((self.out / msrrule.STATE_FILE).exists())
        # A second restore owns nothing and removes nothing.
        count = len(mon.commands)
        again = self.run_mode(mon, "restore")
        self.assertEqual((again["owned"], again["restored"]), (False, True))
        self.assertFalse(any(c["prefix"] in ("osd crush rule rm", "osd erasure-code-profile rm", "osd set-require-min-compat-client")
                             for c in mon.commands[count:]))

    def test_restore_after_probe_only_removes_the_profile(self):
        mon = FakeMon()
        self.run_mode(mon, "probe")
        restored = self.run_mode(mon, "restore")
        self.assertTrue(restored["restored"])
        self.assertFalse(any(c["prefix"] in ("osd crush rule rm", "osd set-require-min-compat-client") and c.get("yes_i_really_mean_it")
                             for c in mon.commands))
        self.assertFalse(any(c["prefix"] == "osd crush rule rm" for c in mon.commands))

    def test_a_survivor_is_reported_and_the_state_kept(self):
        for fake, field in ((dict(keep_rule=True), "rule_present"), (dict(keep_baseline=True), "baseline")):
            with self.subTest(fake=fake):
                directory = tempfile.TemporaryDirectory()
                self.addCleanup(directory.cleanup)
                self.out = pathlib.Path(directory.name)
                mon = FakeMon()
                self.run_mode(mon, "probe")
                self.run_mode(mon, "apply")
                mon.keep_rule, mon.keep_baseline = fake.get("keep_rule", False), fake.get("keep_baseline", False)
                restored = self.run_mode(mon, "restore")
                self.assertFalse(restored["restored"])
                self.assertIn(restored[field], (True, "squid"))
                self.assertTrue((self.out / msrrule.STATE_FILE).exists())

    def test_a_rule_that_is_not_msr_fails_the_apply(self):
        mon = FakeMon(rule_type=3)
        self.run_mode(mon, "probe")
        with self.assertRaises(RuntimeError):
            self.run_mode(mon, "apply")
        # The rule it did create is recorded, so the restore removes it.
        self.assertTrue(self.run_mode(mon, "restore")["restored"])
        self.assertEqual(mon.rules, ["replicated_rule"])

    def test_refuses_a_baseline_that_already_admits_the_rule_and_leftovers(self):
        with self.assertRaises(RuntimeError):
            self.run_mode(FakeMon(baseline="squid"), "probe")
        mon = FakeMon()
        mon.rules.append(msrrule.RULE)
        with self.assertRaises(RuntimeError):
            self.run_mode(mon, "probe")
        mon = FakeMon()
        mon.profiles.add(msrrule.PROFILE)
        with self.assertRaises(RuntimeError):
            self.run_mode(mon, "probe")
        self.assertFalse(any(c["prefix"] == "osd erasure-code-profile set" for c in mon.commands))

    def test_order_and_requests_are_closed(self):
        with self.assertRaises(RuntimeError):
            self.run_mode(FakeMon(), "apply")
        mon = FakeMon()
        self.run_mode(mon, "probe")
        with self.assertRaises(RuntimeError):
            self.run_mode(mon, "probe")
        for request in ({}, {"action": "remove"}, {"action": "apply", "version": "tentacle"}, "apply"):
            with self.assertRaises(ValueError):
                msrrule.mode_msrrule(FakeMon(), None, request, self.out)

    def test_invalid_state_removes_nothing(self):
        (self.out / msrrule.STATE_FILE).write_text(json.dumps({"prior": "tentacle", "profile": True, "rule": True, "raised": True}))
        mon = FakeMon()
        with self.assertRaises(ValueError):
            self.run_mode(mon, "restore")
        self.assertEqual(mon.commands, [])


if __name__ == "__main__":
    unittest.main()
