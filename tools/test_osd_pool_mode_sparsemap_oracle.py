import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_sparsemap_oracle as sparsemap


class SparseMapParsing(unittest.TestCase):
    def test_head_map(self):
        # rados.cc prints offset and length in hexadecimal, one per line.
        self.assertEqual(sparsemap.parse(b"0\t4\n2000\t4\n", None),
                         (None, [{"offset": 0, "length": 4}, {"offset": 0x2000, "length": 4}]))
        self.assertEqual(sparsemap.parse(b"", None), (None, []))

    def test_snapshot_selection_line(self):
        out = b"selected snap 7 'compound-snap'\n0\t20000\n"
        self.assertEqual(sparsemap.parse(out, "compound-snap"), (7, [{"offset": 0, "length": 0x20000}]))
        self.assertEqual(sparsemap.parse(b"selected snap 7 'compound-snap'\n", "compound-snap"), (7, []))
        for bad in (b"0\t4\n", b"selected snap 7 'other'\n0\t4\n", b"selected snap x 'compound-snap'\n"):
            with self.assertRaises(ValueError, msg=bad):
                sparsemap.parse(bad, "compound-snap")

    def test_map_lines_keep_the_shared_checks(self):
        for bad in (b"0\t0\n", b"10\t4\n0\t4\n", b"0\t8\n4\t4\n", b"0 4\n", b"selected snap 7 'compound-snap'\n"):
            with self.assertRaises(ValueError, msg=bad):
                sparsemap.parse(bad, None)

    def test_unknown_input_fields_are_refused_before_any_native_call(self):
        with self.assertRaises(ValueError):
            sparsemap.mode_sparsemap(None, None, {"slot": 1, "object": "o", "namespace": "n"}, None)

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["sparsemap"], sparsemap.mode_sparsemap)


if __name__ == "__main__":
    unittest.main()
