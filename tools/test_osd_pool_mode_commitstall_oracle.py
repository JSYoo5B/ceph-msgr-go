import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_commitstall_oracle as stall


class FakeOSD:
    """One OSD's two runtime options behind config get and config set."""

    def __init__(self, crash=0, delay=2, refuse=None, ignore=None, get_reply=None):
        self.values = {stall.CRASH: crash, stall.DELAY_OPTION: delay}
        self.refuse, self.ignore, self.get_reply = refuse, ignore, get_reply
        self.commands = []

    def osd(self, cluster, osd, command, limit):
        self.commands.append((osd, command))
        option = command["var"]
        if command["prefix"] == "config get":
            if self.get_reply is not None:
                return self.get_reply
            return 0, json.dumps({option: str(self.values[option])}).encode(), "", None
        if option == self.refuse:
            return -22, b"", "refused", None
        if option != self.ignore:
            self.values[option] = int(command["val"][0])
        return 0, b'{"success":""}', "", None


class CommitStall(unittest.TestCase):
    def act(self, rados, action="arm", osd=2):
        return stall.mode_commitstall(rados, None, {"osd": osd, "action": action}, None)["commit_stall"]

    def sets(self, rados):
        return [(c["var"], c["val"]) for _, c in rados.commands if c["prefix"] == "config set"]

    def test_arms_the_delay_first_and_once(self):
        rados = FakeOSD()
        armed = self.act(rados)
        self.assertTrue(armed["armed"], armed)
        self.assertEqual((armed["crash"], armed["delay"]), (stall.ODDS, stall.DELAY))
        self.assertEqual(self.sets(rados), [(stall.DELAY_OPTION, [str(stall.DELAY)]), (stall.CRASH, [str(stall.ODDS)])])
        self.assertTrue(all(osd == 2 for osd, _ in rados.commands))
        again = self.act(rados)
        self.assertFalse(again["armed"])
        self.assertTrue(again["failures"])
        self.assertEqual(len(self.sets(rados)), 2)
        shown = self.act(rados, "show")
        self.assertEqual((shown["crash"], shown["delay"], shown["armed"]), (stall.ODDS, stall.DELAY, False))

    def test_refusals_are_reported(self):
        for name, (rados, sets) in {
            "delay refused": (FakeOSD(refuse=stall.DELAY_OPTION), 1),
            "delay ignored": (FakeOSD(ignore=stall.DELAY_OPTION), 1),
            "crash refused": (FakeOSD(refuse=stall.CRASH), 2),
            "crash ignored": (FakeOSD(ignore=stall.CRASH), 2),
            "already armed": (FakeOSD(crash=5), 0),
            "get refused": (FakeOSD(get_reply=(-2, b"", "no such option", None)), 0),
            "get oversized": (FakeOSD(get_reply=(0, b"", "", 9999)), 0),
            "get unparsable": (FakeOSD(get_reply=(0, b"[]", "", None)), 0),
        }.items():
            with self.subTest(name):
                receipt = self.act(rados)
                self.assertFalse(receipt["armed"], receipt)
                self.assertTrue(receipt["failures"])
                self.assertEqual(len(self.sets(rados)), sets)
                if name.startswith("delay"):
                    # The stall never runs with the configured delay.
                    self.assertEqual(rados.values[stall.CRASH], 0)

    def test_request_fields(self):
        for request in ({"osd": 1}, {"osd": 1, "action": "restore"}, {"osd": -1, "action": "arm"}, {"osd": 1, "action": "arm", "value": 3}):
            with self.subTest(request), self.assertRaises(ValueError):
                stall.mode_commitstall(FakeOSD(), None, request, None)


if __name__ == "__main__":
    unittest.main()
