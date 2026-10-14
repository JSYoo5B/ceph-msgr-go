import copy
import unittest

from osd_mutation_oracle import collect_placement


def pair(epoch=19):
    mapped = {"epoch": epoch, "pool": "test", "pool_id": 7, "objname": "binary",
              "acting_primary": 2, "up": [2, 0, 1], "acting": [2, 0, 1],
              "raw_pgid": "7.123abc"}
    dump = {"epoch": epoch,
            "osds": [{"osd": 2, "up": 1, "in": 1,
                      "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.0.2.4:6800", "nonce": 91}]}}],
            "pools": [{"pool": 7, "pool_name": "test", "type": 1, "size": 3, "min_size": 2}]}
    return mapped, dump


class PlacementOracleTests(unittest.TestCase):
    def query(self, responses, *, clock=lambda: 0, timeout=30):
        calls = []
        remaining = iter(responses)

        def command(*args, timeout):
            calls.append((args, timeout))
            return copy.deepcopy(next(remaining))

        result = collect_placement(command, "test", "binary", 7, clock=clock, timeout=timeout)
        return result, calls

    def test_returns_one_matched_epoch_target_and_topology(self):
        result, calls = self.query(pair())
        self.assertEqual(result, {"pool_id": 7, "object": "binary", "osd_id": 2,
                                  "epoch": 19, "hash": 0x123ABC,
                                  "address": "v2:192.0.2.4:6800/91", "up": [2, 0, 1],
                                  "acting": [2, 0, 1], "replicas": 3, "min_size": 2})
        self.assertEqual(calls, [(("osd", "map", "test", "binary"), 10), (("osd", "dump"), 10)])

    def test_mismatched_epochs_requery_both_observations(self):
        old_map, _ = pair(19)
        new_map, new_dump = pair(20)
        old_map["acting_primary"] = 0  # Must not leak into the successful target.
        result, calls = self.query([old_map, new_dump, new_map, new_dump])
        self.assertEqual(result["epoch"], 20)
        self.assertEqual(result["osd_id"], 2)
        self.assertEqual(len(calls), 4)

    def test_mismatch_exhaustion_stops_after_five_read_pairs(self):
        mapped, _ = pair(19)
        _, dump = pair(20)
        calls = []

        def command(*args, timeout):
            calls.append(args)
            return mapped if args[1] == "map" else dump

        with self.assertRaisesRegex(ValueError, "did not converge"):
            collect_placement(command, "test", "binary", 7, clock=lambda: 0)
        self.assertEqual(len(calls), 10)
        self.assertEqual(set(c[1] for c in calls), {"map", "dump"})

    def test_no_matching_primary_or_usable_address_fails_without_retry(self):
        for fault in ("missing primary", "missing address", "duplicate address", "invalid port", "down", "not acting"):
            with self.subTest(fault=fault):
                mapped, dump = pair()
                if fault == "missing primary":
                    dump["osds"] = []
                elif fault == "missing address":
                    dump["osds"][0]["public_addrs"]["addrvec"] = []
                elif fault == "duplicate address":
                    dump["osds"][0]["public_addrs"]["addrvec"] *= 2
                elif fault == "invalid port":
                    dump["osds"][0]["public_addrs"]["addrvec"][0]["addr"] = "192.0.2.4:0"
                elif fault == "down":
                    dump["osds"][0]["up"] = 0
                else:
                    mapped["acting"] = [0, 1]
                with self.assertRaises(ValueError):
                    self.query([mapped, dump])

    def test_required_pool_object_and_replication_identity(self):
        for fault in ("map pool name", "map pool id", "object", "dump pool name", "EC", "min size", "raw hash"):
            with self.subTest(fault=fault):
                mapped, dump = pair()
                if fault == "map pool name":
                    mapped["pool"] = "other"
                elif fault == "map pool id":
                    mapped["pool_id"] = 8
                elif fault == "object":
                    mapped["objname"] = "other"
                elif fault == "dump pool name":
                    dump["pools"][0]["pool_name"] = "other"
                elif fault == "EC":
                    dump["pools"][0]["type"] = 3
                elif fault == "min size":
                    dump["pools"][0]["min_size"] = 4
                else:
                    mapped["raw_pgid"] = "8.123abc"
                with self.assertRaises(ValueError):
                    self.query([mapped, dump])

    def test_each_read_respects_remaining_deadline(self):
        times = iter([0, 0, 4, 4, 8])
        result, calls = self.query(pair(), clock=lambda: next(times), timeout=8)
        self.assertEqual(result["epoch"], 19)
        self.assertEqual([c[1] for c in calls], [8, 4])

    def test_deadline_expiry_stops_before_second_read(self):
        times = iter([0, 0, 2, 3])
        with self.assertRaisesRegex(ValueError, "deadline"):
            self.query(pair(), clock=lambda: next(times), timeout=3)

    def test_subprocess_failure_is_not_retried(self):
        calls = []

        def command(*args, timeout):
            calls.append(args)
            raise RuntimeError("native read failed")

        with self.assertRaisesRegex(RuntimeError, "native read failed"):
            collect_placement(command, "test", "binary", 7, clock=lambda: 0)
        self.assertEqual(len(calls), 1)

    def test_ipv6_public_endpoint_preserves_nonce(self):
        mapped, dump = pair()
        dump["osds"][0]["public_addrs"]["addrvec"][0]["addr"] = "[2001:db8::4]:6801"
        result, _ = self.query([mapped, dump])
        self.assertEqual(result["address"], "v2:[2001:db8::4]:6801/91")


if __name__ == "__main__":
    unittest.main()
