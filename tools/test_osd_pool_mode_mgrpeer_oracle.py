import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_mgrpeer_oracle as mgrpeer


class MgrPeerInputs(unittest.TestCase):
    def test_native_dict_is_sorted_and_terminated(self):
        self.assertEqual(mgrpeer.native_dict({"b": "2", "a": "1"}), b"a\x001\x00b\x002\x00\x00")
        self.assertEqual(mgrpeer.native_dict({}), b"\x00")

    def test_close_lines_count_sent_and_received(self):
        log = ("2026-11-21T01:00:00.000+0000 7f 1 -- 10.0.0.1:0/1 --> [v2:10.0.0.2:6800/9,v1:0.0.0.0:0/0] -- mgrclose(ceph-msgr.d) -- 0x1 con 0x2\n"
               "2026-11-21T01:00:00.001+0000 7f 1 -- 10.0.0.1:0/1 <== mgr.4665 v2:10.0.0.2:6800/9 3 ==== mgrclose(ceph-msgr.d) v1 ==== 22+0+0 (crc 0 0 0) 0x3 con 0x2\n"
               "2026-11-21T01:00:00.002+0000 7f 1 -- 10.0.0.1:0/1 --> mgr -- mgrreport(ceph-msgr.d +0-0 packed 6) -- 0x4\n")
        self.assertEqual(mgrpeer.close_lines(log), (1, 1))
        self.assertEqual(mgrpeer.close_lines(log.splitlines()[0]), (1, 0))

    def test_string_maps_are_bounded(self):
        mgrpeer.string_map({"k": "v"}, "m")
        for bad in ([], {"": "v"}, {"k": 1}, {str(i): "v" for i in range(9)}, {"k": "v" * 129}):
            with self.assertRaises(ValueError):
                mgrpeer.string_map(bad, "m")

    def test_request_shape_is_closed_before_any_native_call(self):
        for request in ({}, {"mgr_name": "x", "daemon": "d", "noecho_daemon": "n", "metadata": {}, "status": {}, "commands": [], "extra": 1}):
            with self.assertRaises(ValueError):
                mgrpeer.mode_mgrpeer(None, None, request, None)

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["mgrpeer"], mgrpeer.mode_mgrpeer)


if __name__ == "__main__":
    unittest.main()
