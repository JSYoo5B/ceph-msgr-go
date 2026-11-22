import unittest

import osd_sparse_read_compound_native_encoder as head
import osd_sparse_read_compound_replica_native_encoder as encoder


class SparseReadCompoundReplicaNativeEncoderTests(unittest.TestCase):
    def test_each_replica_flag_is_used_at_the_head_and_at_a_snap(self):
        # include/rados.h: CEPH_OSD_FLAG_BALANCE_READS 0x100,
        # CEPH_OSD_FLAG_LOCALIZE_READS 0x2000; CEPH_NOSNAP is (uint64)-2 and a
        # read snap is 1..CEPH_MAXSNAP, (uint64)-3.
        self.assertEqual(set(encoder.LAYOUTS), set(encoder.SHAPES))
        self.assertEqual((encoder.BALANCE_READS, encoder.LOCALIZE_READS), (0x100, 0x2000))
        self.assertEqual((encoder.NOSNAP, encoder.MAXSNAP), ((1 << 64) - 2, (1 << 64) - 3))
        cells = set()
        for shape, (layout, snap, flag) in encoder.LAYOUTS.items():
            self.assertIn(flag, (encoder.BALANCE_READS, encoder.LOCALIZE_READS), shape)
            self.assertTrue(snap == encoder.NOSNAP or 1 <= snap <= encoder.MAXSNAP, shape)
            cells.add((snap == encoder.NOSNAP, flag))
        self.assertEqual(len(cells), 4)
        self.assertIn(encoder.MAXSNAP, [snap for _, snap, _ in encoder.LAYOUTS.values()])

    def test_layouts_are_the_head_producer_layouts(self):
        # The replica producer changes only the request flag and the read
        # snap: each op layout is the head producer's layout of that name.
        for shape, (layout, _, _) in encoder.LAYOUTS.items():
            self.assertIn(layout, head.SHAPES)
            self.assertEqual(encoder.expected_operations(shape), head.expected_operations(layout), shape)

    def test_operations_carry_no_input_or_truncate_fields(self):
        for shape in encoder.SHAPES:
            for op in encoder.expected_operations(shape):
                self.assertIn(op["code"], (0x1205, 0x1201))
                self.assertEqual((op["payload_len"], op["indata_hex"]), (0, ""))
                self.assertEqual(bytes.fromhex(op["union_hex"])[16:], bytes(12))
                self.assertFalse(op["flags"] & ~0x7c, shape)

    def test_features_omit_the_cache_pool_capability(self):
        self.assertEqual(encoder.FEATURES, head.FEATURES)
        for features in encoder.FEATURES.values():
            self.assertFalse(features & (1 << 35))


if __name__ == "__main__":
    unittest.main()
