import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_mgrsample_oracle as mgrsample


class MgrSampleParsing(unittest.TestCase):
    def test_counter_values(self):
        perf = {"osd": {"op_w": 4, "op_w_latency": {"avgcount": 4, "sum": 0.5, "avgtime": 0.125}}}
        self.assertEqual(mgrsample.counter_values(perf, ["osd.op_w", "osd.op_w_latency", "osd.missing", "nosection"]),
                         {"osd.op_w": 4, "osd.op_w_latency": {"avgcount": 4, "sum": 0.5}, "osd.missing": None, "nosection": None})
        with self.assertRaises(ValueError):
            mgrsample.counter_values({"osd": {"x": "text"}}, ["osd.x"])

    def test_pg_stats_keep_only_the_compared_fields(self):
        stats = {"state": "active+clean", "reported_epoch": 9, "reported_seq": 30, "up": [0], "acting": [0],
                 "up_primary": 0, "acting_primary": 0, "extra": 1,
                 "stat_sum": {"num_objects": 4, "num_bytes": 10, "num_write": 4, "num_write_kb": 1, "num_read": 0, "num_rd": 9}}
        got = mgrsample.pg_stats({"info": {"pgid": "3.1", "stats": stats}})
        self.assertEqual(got["pgid"], "3.1")
        self.assertNotIn("extra", got)
        self.assertEqual(set(got["stat_sum"]), set(mgrsample.STAT_SUM))
        with self.assertRaises(KeyError):
            mgrsample.pg_stats({"info": {"pgid": "3.1", "stats": {}}})

    def test_osd_epochs(self):
        self.assertEqual(mgrsample.osd_epochs({"epoch": 7, "osds": [{"osd": 0, "up_from": 5}]}), {"epoch": 7, "up_from": {"0": 5}})

    def test_unknown_input_fields_are_refused_before_any_native_call(self):
        with self.assertRaises(ValueError):
            mgrsample.mode_mgrsample(None, None, {"slot": 1, "object": "o", "extra": 1}, None)
        with self.assertRaises(ValueError):
            mgrsample.mode_mgrsample(None, None, {"slot": 1, "object": "o", "counters": ["x"] * 17}, None)

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["mgrsample"], mgrsample.mode_mgrsample)


if __name__ == "__main__":
    unittest.main()
