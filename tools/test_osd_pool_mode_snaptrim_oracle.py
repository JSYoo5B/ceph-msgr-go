import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_snaptrim_oracle as snaptrim


class SnapTrimParsing(unittest.TestCase):
    def test_trimq_hex_intervals(self):
        self.assertEqual(snaptrim.trimq("[]"), [])
        self.assertEqual(snaptrim.trimq("[3~1]"), [3])
        self.assertEqual(snaptrim.trimq("[a~2,f~1]"), [10, 11, 15])
        for bad in ("", "[3]", "[3~1 ,5~1]", "[5~1,3~1]", "[3~2,4~1]", "[0~1]", "[3~0]", None, 3):
            with self.assertRaises(ValueError, msg=bad):
                snaptrim.trimq(bad)

    def test_purged_entries(self):
        self.assertEqual(snaptrim.purged([]), [])
        self.assertEqual(snaptrim.purged([{"start": "4", "length": "2"}, {"start": "10", "length": "1"}]), [4, 5, 16])
        for bad in ({}, [{"start": 4, "length": "1"}], [{"start": "4"}], [{"start": "4", "length": "1", "x": 1}]):
            with self.assertRaises(ValueError):
                snaptrim.purged(bad)

    def test_queue_of_one_pool(self):
        dump = {"removed_snaps_queue": [{"pool": 2, "snaps": [{"begin": 9, "length": 1}]},
                                        {"pool": 3, "snaps": [{"begin": 4, "length": 2}]}]}
        self.assertEqual(snaptrim.queue(dump, 3), [4, 5])
        self.assertEqual(snaptrim.queue(dump, 4), [])
        self.assertEqual(snaptrim.queue({}, 3), [])
        with self.assertRaises(ValueError):
            snaptrim.queue({"removed_snaps_queue": [{"pool": 3, "snaps": []}, {"pool": 3, "snaps": []}]}, 3)

    def test_query_state(self):
        document = {"state": "active+clean", "epoch": 31, "snap_trimq": "[5~1]", "snap_trimq_len": 1,
                    "info": {"pgid": "3.0", "purged_snaps": [{"start": "4", "length": "1"}]}}
        self.assertEqual(snaptrim.query_state(document), {"pgid": "3.0", "state": "active+clean", "epoch": 31,
                                                         "snap_trimq": [5], "purged_snaps": [4]})
        document["snap_trimq_len"] = 2
        with self.assertRaises(ValueError):
            snaptrim.query_state(document)
        with self.assertRaises(ValueError):
            snaptrim.query_state({"state": "active", "epoch": 1, "snap_trimq": "[]", "snap_trimq_len": 0})

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["snaptrim"], snaptrim.mode_snaptrim)


if __name__ == "__main__":
    unittest.main()
