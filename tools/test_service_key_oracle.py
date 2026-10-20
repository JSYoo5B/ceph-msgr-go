import json
import pathlib
import tempfile
import unittest

from service_key_oracle import collect, validate


class ServiceKeyOracleTests(unittest.TestCase):
    def check(self, value, phase="mon-status", mon="b", rank=1):
        return validate(value, phase, mon, rank, 5, 11, "a", 4400)

    def test_requires_local_mon_identity_and_both_epochs(self):
        value = {"name": "b", "rank": 1, "monmap": {"epoch": 5, "auth_epoch": 11}}
        self.assertTrue(self.check(value)[1])
        for field, bad in (("name", "a"), ("rank", 0), ("rank", True)):
            changed = dict(value, **{field: bad})
            self.assertFalse(self.check(changed)[1])
        for field, bad in (("epoch", 4), ("auth_epoch", 10), ("auth_epoch", True)):
            changed = dict(value, monmap=dict(value["monmap"], **{field: bad}))
            self.assertFalse(self.check(changed)[1])

    def test_requires_exact_active_manager_identity(self):
        value = {"available": True, "active_name": "a", "active_gid": 4400}
        self.assertTrue(self.check(value, "mgr-map")[1])
        for field, bad in (("available", 1), ("active_name", "b"), ("active_gid", 4401)):
            self.assertFalse(self.check(dict(value, **{field: bad}), "mgr-map")[1])

    def test_requires_tentacle_version(self):
        self.assertTrue(self.check({"version": "20.2.4", "release": "tentacle"}, "mgr-version")[1])
        for value in ({"version": "19.2.4", "release": "squid"}, {"version": 20, "release": "tentacle"}):
            self.assertFalse(self.check(value, "mgr-version")[1])

    def test_history_preserves_failed_and_successful_public_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, history = root / "input.json", root / "history.jsonl"
            args = (source, history, "42", "mon-status", "b", 1, 5, 11, "a", 4400)
            source.write_text("broken json")
            self.assertFalse(collect(*args))
            source.write_text(json.dumps({"name": "b", "rank": 1,
                                         "monmap": {"epoch": 5, "auth_epoch": 11},
                                         "unrelated": "not copied to public record"}))
            self.assertTrue(collect(*args))
            records = [json.loads(line) for line in history.read_text().splitlines()]
            self.assertEqual([r["accepted"] for r in records], [False, True])
            self.assertEqual(records[1]["auth_epoch"], 11)
            self.assertNotIn("unrelated", records[1])
            self.assertNotIn("unrelated", records[0])

    def test_rejects_oversized_native_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "input.json"
            source.write_bytes(b" " * ((1 << 20) + 1))
            with self.assertRaises(ValueError):
                collect(source, root / "history.jsonl", "42", "mgr-version", "a", 0, 5, 11, "a", 4400)

    def test_failed_command_cannot_satisfy_ready_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, history = root / "input.json", root / "history.jsonl"
            source.write_text(json.dumps({"version": "20.2.4", "release": "tentacle"}))
            self.assertFalse(collect(source, history, "42", "mgr-version", "a", 0, 5, 11, "a", 4400, 124))
            self.assertEqual(json.loads(history.read_text())["command_code"], 124)


if __name__ == "__main__":
    unittest.main()
