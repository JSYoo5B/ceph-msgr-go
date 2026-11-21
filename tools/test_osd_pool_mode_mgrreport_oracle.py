import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_mgrreport_oracle as mgrreport


class MgrReportParsing(unittest.TestCase):
    def test_manager_roles(self):
        dump = {"epoch": 9, "active_gid": 41, "active_name": "a", "available": False, "extra": 1,
                "standbys": [{"gid": 77, "name": "c"}, {"gid": 52, "name": "b"}]}
        self.assertEqual(mgrreport.manager(dump), {"epoch": 9, "active_gid": 41, "active_name": "a",
                                                   "available": False, "standby_gids": [52, 77]})
        self.assertEqual(mgrreport.manager({"epoch": 1, "active_gid": 0, "active_name": "", "available": False})["standby_gids"], [])

    def test_checks_keep_only_the_prefix(self):
        detail = {"status": "HEALTH_WARN", "checks": {
            "GOTEST_A": {"severity": "HEALTH_WARN", "summary": {"message": "a", "count": 3}, "detail": [{"message": "x"}], "muted": False},
            "MGR_DOWN": {"severity": "HEALTH_WARN", "summary": {"message": "m", "count": 0}}}}
        self.assertEqual(mgrreport.checks(detail, "GOTEST_"),
                         {"GOTEST_A": {"severity": "HEALTH_WARN", "summary": "a", "count": 3, "detail": ["x"]}})
        self.assertEqual(mgrreport.checks({"status": "HEALTH_OK"}, "GOTEST_"), {})
        many = {"checks": {"GOTEST_%d" % i: {"severity": "HEALTH_ERR", "summary": {"message": "", "count": 0}} for i in range(17)}}
        with self.assertRaises(ValueError):
            mgrreport.checks(many, "GOTEST_")

    def test_input_is_refused_before_any_native_call(self):
        for request in ({}, {"prefix": "GOTEST_", "extra": 1}, {"prefix": "x"}, {"prefix": "lower_case"}, {"prefix": 7}):
            with self.subTest(request=request), self.assertRaises(ValueError):
                mgrreport.mode_mgrreport(None, None, request, None)

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["mgrreport"], mgrreport.mode_mgrreport)


if __name__ == "__main__":
    unittest.main()
