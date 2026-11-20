import unittest

import osd_sparse_read_compound_native_encoder as head
import osd_sparse_read_compound_snapshot_native_encoder as encoder


class SparseReadCompoundSnapshotNativeEncoderTests(unittest.TestCase):
    def test_every_layout_reads_at_an_allocatable_snap_id(self):
        # include/rados.h: CEPH_NOSNAP is (uint64)-2, CEPH_SNAPDIR (uint64)-1
        # and CEPH_MAXSNAP (uint64)-3; a read snap is 1..CEPH_MAXSNAP.
        self.assertEqual(set(encoder.SNAPS), set(encoder.SHAPES))
        self.assertEqual(encoder.MAXSNAP, (1 << 64) - 3)
        for shape, snap in encoder.SNAPS.items():
            self.assertTrue(1 <= snap <= encoder.MAXSNAP, shape)
        self.assertIn(encoder.MAXSNAP, encoder.SNAPS.values())

    def test_layouts_are_the_head_producer_layouts(self):
        # The snapshot producer changes only the read snap: each layout is
        # the head producer's layout of the same name, op for op.
        for shape in encoder.SHAPES:
            self.assertIn(shape, head.SHAPES)
            self.assertEqual(encoder.expected_operations(shape), head.expected_operations(shape), shape)

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
