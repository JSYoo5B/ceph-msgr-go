import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_osdstate_oracle as osdstate


def dump(**changes):
    entry = {"osd": 3, "uuid": "u", "up": 1, "in": 1, "weight": 1.0, "primary_affinity": 1.0, "last_clean_begin": 0,
             "last_clean_end": 0, "up_from": 31, "up_thru": 33, "down_at": 28, "lost_at": 0}
    entry.update(changes)
    return {"epoch": 35, "osds": [{"osd": 0, "up": 1, "in": 1, "up_from": 5, "up_thru": 30, "down_at": 0}, entry]}


class FakeRados:
    """One `osd dump` reply; no daemon is involved."""

    def __init__(self, reply, code=0):
        self.reply, self.code, self.commands = reply, code, []

    def mon(self, cluster, command):
        self.commands.append(command)
        return self.code, json.dumps(self.reply).encode()


class OSDState(unittest.TestCase):
    def test_reads_the_requested_entry(self):
        rados = FakeRados(dump())
        state = osdstate.mode_osdstate(rados, None, {"osd": 3}, None)["osd_state"]
        self.assertEqual(state, {"epoch": 35, "osd": 3, "up": True, "in": True, "up_from": 31, "up_thru": 33, "down_at": 28})
        self.assertEqual(rados.commands, [{"prefix": "osd dump", "format": "json"}])
        down = osdstate.osd_entry(dump(up=0, down_at=36), 3)
        self.assertEqual((down["up"], down["down_at"]), (False, 36))

    def test_refuses_malformed_dumps(self):
        for reply in ({"osds": []}, {"epoch": "35", "osds": []}, {"epoch": 35}, [], dump(osd=4),
                      {"epoch": 35, "osds": dump()["osds"] + [dump()["osds"][1]]}):
            with self.assertRaises((ValueError, RuntimeError)):
                osdstate.osd_entry(reply, 3)
        for change in ({"up": True}, {"up": 2}, {"in": None}, {"up_from": "31"}, {"down_at": -1}, {"up_thru": 1.5}, {"up_from": True}):
            with self.assertRaises(ValueError, msg=change):
                osdstate.osd_entry(dump(**change), 3)
        entry = dump()
        del entry["osds"][1]["down_at"]
        with self.assertRaises(ValueError):
            osdstate.osd_entry(entry, 3)

    def test_refuses_inputs_and_a_failed_command(self):
        for request in ({}, {"osd": "3"}, {"osd": -1}, {"osd": True}, {"osd": 3, "slot": 1}):
            with self.assertRaises(ValueError, msg=request):
                osdstate.mode_osdstate(FakeRados(dump()), None, request, None)
        with self.assertRaises(RuntimeError):
            osdstate.mode_osdstate(FakeRados(dump(), code=-1), None, {"osd": 3}, None)


if __name__ == "__main__":
    unittest.main()
