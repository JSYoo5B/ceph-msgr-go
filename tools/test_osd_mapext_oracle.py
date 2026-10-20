#!/usr/bin/env python3
import unittest
import osd_mapext_oracle as oracle

class MapExtOracleTests(unittest.TestCase):
    def test_pinned_cli_parse_and_intersection(self):
        extents = oracle.parse_map(b"3\t5\n14\t8\n")
        self.assertEqual(extents, [{"offset": 3, "length": 5}, {"offset": 20, "length": 8}])
        self.assertEqual(oracle.intersect(extents, 5, 18), [{"offset": 5, "length": 3}, {"offset": 20, "length": 3}])
        self.assertEqual(oracle.intersect(extents, 8, 12), [])

    def test_bad_cli_output(self):
        for data in (b"3 5\n", b"3\t0\n", b"3\t5\n3\t1\n", b"14\t8\n3\t5\n", b"ffffffffffffffff\t1\n", b"\xff"):
            with self.subTest(data=data), self.assertRaises((ValueError, UnicodeError)):
                oracle.parse_map(data)

    def test_range_input_boundaries(self):
        self.assertEqual(oracle.request({"object":"owned","offset":0,"length":oracle.MAX_UINT64}), (b"owned",0,oracle.MAX_UINT64))
        for raw in ({"object":"owned","offset":0,"length":0}, {"object":"owned","offset":oracle.MAX_UINT64,"length":1}, {"object":"owned","offset":False,"length":1}, {"object":"owned","offset":0,"length":1,"extra":0}):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                oracle.request(raw)

if __name__ == "__main__":
    unittest.main()
