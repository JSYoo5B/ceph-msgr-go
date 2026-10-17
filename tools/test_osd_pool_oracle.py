import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle as oracle


class PoolOracleTest(unittest.TestCase):
    def test_only_two_owned_pool_names_exist(self):
        self.assertEqual(oracle.pool_name(1), "ceph-msgr-owned-pool-1")
        self.assertEqual(oracle.pool_name(2), "ceph-msgr-owned-pool-2")
        for bad in (0, 3, -1, True, "1", None, 1.0):
            with self.assertRaises(ValueError):
                oracle.pool_name(bad)

    def test_modes_are_closed_and_have_handlers(self):
        self.assertEqual(set(oracle.MODES), set(oracle.HANDLERS))
        with self.assertRaises(SystemExit):
            oracle.main(["x", "delete", "/root", "/out"])
        with self.assertRaises(SystemExit):
            oracle.main(["x", "create", "/root"])

    def test_integer_bounds(self):
        self.assertEqual(oracle.integer(3, 1, 3, "size"), 3)
        for bad in (0, 4, True, "3"):
            with self.assertRaises(ValueError):
                oracle.integer(bad, 1, 3, "size")

    def test_object_identity_rejects_nul_and_empty(self):
        with self.assertRaises(ValueError):
            oracle.object_request({"object": "a\0b"})
        with self.assertRaises(ValueError):
            oracle.object_request({"object": ""})
        self.assertEqual(oracle.object_request({"object": "o", "namespace": "ns"}), (b"o", "ns"))


if __name__ == "__main__":
    unittest.main()
