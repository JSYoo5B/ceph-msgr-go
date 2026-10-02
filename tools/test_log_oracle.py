import copy
import json
import unittest

import log_oracle


SENTINEL = "ceph-msgr-log-oracle-" + "a" * 32


def native_entry():
    # Independently specified pinned LogEntry.dump fields. A native CLI
    # source can retain unknown rank (-1), rendered as client.?.
    return {"name": "client.test", "rank": "client.?",
            "addrs": {"addrvec": [{"type": "any", "addr": "[::1]:0", "nonce": 29}]},
            "stamp": "2026-10-02T12:55:01.682887+0000", "seq": 0,
            "channel": "cluster", "priority": "[INF]", "message": SENTINEL}


class NativeLogOracleTests(unittest.TestCase):
    def check(self, entries, sentinel=SENTINEL):
        return log_oracle.select_entry(json.dumps(entries).encode(), sentinel)

    def test_selects_only_unique_native_sentinel(self):
        entry = native_entry()
        other = {"message": "unrelated server log must not be persisted"}
        self.assertEqual(self.check([other, entry]), {"sentinel": SENTINEL, "entry": entry})
        self.assertNotIn(other["message"], json.dumps(self.check([other, entry])))

    def test_numeric_and_unknown_client_ranks(self):
        for rank in ("client.?", "client.0", "client.9223372036854775807"):
            with self.subTest(rank=rank):
                entry = native_entry()
                entry["rank"] = rank
                self.assertEqual(self.check([entry])["entry"]["rank"], rank)

    def test_missing_duplicate_and_wrong_shapes(self):
        entry = native_entry()
        for value in (None, {}, {"tail": [entry]}, [], [entry, entry], [entry] * 65):
            with self.subTest(shape=type(value).__name__):
                with self.assertRaises(ValueError):
                    self.check(value)

    def test_native_fields_reject_invalid_identity_and_types(self):
        for key, value in (("name", "client.other"), ("rank", "mon.1"),
                           ("rank", "client.9223372036854775808"),
                           ("rank", "client.-1"), ("rank", None),
                           ("priority", "[ERR]"), ("channel", "audit"),
                           ("seq", True), ("seq", 1), ("stamp", None),
                           ("stamp", "truncated")):
            with self.subTest(key=key, value=value):
                entry = native_entry()
                entry[key] = value
                with self.assertRaises(ValueError):
                    self.check([entry])

    def test_source_addresses_are_bounded_and_typed(self):
        bad = [None, {}, {"addrvec": []}, {"addrvec": [None]},
               {"addrvec": [native_entry()["addrs"]["addrvec"][0]] * 65}]
        for key, value in (("type", "invalid"), ("addr", None), ("addr", ""),
                           ("addr", "x" * 129), ("nonce", True),
                           ("nonce", -1), ("nonce", 1 << 32)):
            address = copy.deepcopy(native_entry()["addrs"]["addrvec"][0])
            address[key] = value
            bad.append({"addrvec": [address]})
        for addresses in bad:
            entry = native_entry()
            entry["addrs"] = addresses
            with self.assertRaises(ValueError):
                self.check([entry])

    def test_ipv4_v1_source_metadata_remains_raw(self):
        entry = native_entry()
        entry["addrs"] = {"addrvec": [{"type": "v1", "addr": "127.0.0.1:0", "nonce": 0}]}
        self.assertEqual(self.check([entry])["entry"]["addrs"], entry["addrs"])

    def test_malformed_size_and_error_payload_redaction(self):
        for data in (b"{", b"\xff", b"[]" + b" " * log_oracle.LIMIT,
                     b'{"message":"secret-key-sentinel"}'):
            with self.assertRaises(ValueError) as error:
                log_oracle.select_entry(data, SENTINEL)
            self.assertNotIn("secret-key-sentinel", str(error.exception))

    def test_sentinel_requires_generated_uuid_shape(self):
        for sentinel in ("", SENTINEL[:-1], SENTINEL + "x", "secret-key-sentinel"):
            with self.assertRaises(ValueError) as error:
                self.check([native_entry()], sentinel)
            self.assertNotIn("secret-key-sentinel", str(error.exception))


if __name__ == "__main__":
    unittest.main()
