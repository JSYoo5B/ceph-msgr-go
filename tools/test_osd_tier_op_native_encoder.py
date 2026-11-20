import unittest

import osd_tier_op_native_encoder as encoder


class TierOpNativeEncoderTests(unittest.TestCase):
    def test_model_opcodes_match_the_pinned_rados_values(self):
        # include/rados.h CEPH_OSD_OP_* values at the pinned Tentacle tag.
        catalog = {"TIER_PROMOTE": 0x2229, "TIER_FLUSH": 0x822b, "TIER_EVICT": 0x822c}
        names = {"promote": "TIER_PROMOTE", "flush": "TIER_FLUSH", "evict": "TIER_EVICT"}
        self.assertEqual(set(encoder.SHAPES), set(encoder.MODEL))
        for shape, (code, _, _) in encoder.MODEL.items():
            self.assertEqual(code, catalog[names[shape]], shape)

    def test_native_caller_flag_layers_stay_distinct(self):
        tier = encoder.IGNORE_CACHE | encoder.IGNORE_OVERLAY
        self.assertEqual(encoder.MODEL["promote"][1], encoder.WRITE)
        for shape in ("flush", "evict"):
            self.assertEqual(encoder.MODEL[shape][1], encoder.READ | tier)
        for shape, (_, flags, write) in encoder.MODEL.items():
            self.assertEqual(bool(flags & encoder.WRITE), write, shape)
            self.assertNotEqual(bool(flags & encoder.READ), write, shape)
            self.assertFalse(flags & encoder.SKIPRWLOCKS, shape)

    def test_features_omit_the_cache_pool_capability(self):
        self.assertEqual(set(encoder.FEATURES), {6, 8, 9})
        for features in encoder.FEATURES.values():
            self.assertFalse(features & (1 << 35))

    def test_operations_carry_no_union_or_input(self):
        for shape in encoder.SHAPES:
            (op,) = encoder.expected_operations(shape)
            self.assertEqual((op["flags"], op["payload_len"], op["indata_hex"]), (0, 0, ""))
            self.assertEqual(bytes.fromhex(op["union_hex"]), bytes(28))


if __name__ == "__main__":
    unittest.main()
