import unittest
from types import SimpleNamespace
import osd_mapext_range_oracle as oracle


class MapExtRangeOracleTests(unittest.TestCase):
    def test_request_snapshot_and_full_range(self):
        self.assertEqual(oracle.request(dict(object="owned", offset=0, length=oracle.MAX_UINT64, snap=123)),
                         (b"owned", 0, oracle.MAX_UINT64, 123))
        for patch in ({"snap": 0}, {"snap": True}, {"snap": oracle.MAX_UINT64-1},
                      {"length": 0}, {"offset": oracle.MAX_UINT64}, {"offset": False}, {"unknown": 1}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                oracle.request(dict(dict(object="owned", offset=0, length=4), **patch))

    def test_native_count_is_distinct_from_go_aggregate(self):
        self.assertEqual(oracle.validate_observation(dict(code=2, extents=[dict(offset=3, length=2), dict(offset=5, length=1)]), 3, 3), 2)
        self.assertEqual(oracle.validate_observation(dict(code=-2, extents=[]), 0, 10), -2)
        self.assertEqual(oracle.validate_observation(dict(code=0, extents=[]), 10, 4), 0)

    def test_native_bounds_and_output_shape(self):
        for result in (dict(code=0, extents=[dict(offset=3, length=1)]),
                       dict(code=-2, extents=[dict(offset=3, length=1)]),
                       dict(code=True, extents=[]), dict(code=1, extents=[dict(offset=2, length=2)]),
                       dict(code=1, extents=[dict(offset=3, length=0)]),
                       dict(code=1, extents=[dict(offset=3, length=5)]),
                       dict(code=2, extents=[dict(offset=3, length=2), dict(offset=4, length=1)]),
                       dict(code=0, extents=[], version=1)):
            with self.subTest(result=result), self.assertRaises(ValueError):
                oracle.validate_observation(result, 3, 4)

    def test_selected_snapshot_resets_on_success_and_failure(self):
        for failure in (False, True):
            selected = []
            native = SimpleNamespace(lib=SimpleNamespace(rados_ioctx_snap_set_read=lambda io, snap: selected.append(snap)),
                                     snapshot=lambda *args: dict(code=0, version=17))
            def execute():
                if failure:
                    raise RuntimeError("native execution failed")
                return {"code": 0, "extents": []}
            if failure:
                with self.assertRaises(RuntimeError):
                    oracle.observe_selected(native, 7, b"owned", 123, execute)
            else:
                before, _, after = oracle.observe_selected(native, 7, b"owned", 123, execute)
                self.assertEqual(before, after)
            self.assertEqual(selected, [123, oracle.writes.SNAP_HEAD])


if __name__ == "__main__":
    unittest.main()
