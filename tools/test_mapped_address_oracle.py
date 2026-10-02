import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import mapped_address_oracle as oracle


def vector(port, ip="::ffff:127.0.0.1"):
    return {"addrvec": [{"type": "v2", "addr": "[" + ip + "]:" + str(port), "nonce": 0}]}


def metadata(active="a", ip="::ffff:127.0.0.1"):
    return ({"fsid": oracle.FSID},
            {"fsid": oracle.FSID, "min_mon_release": 20,
             "mons": [{"name": name, "public_addrs": vector(port, ip)}
                      for name, port in oracle.MON_PORTS.items()]},
            {"available": True, "active_name": active, "active_gid": 4010,
             "active_addrs": vector(oracle.MGR_PORTS[active], ip),
             "standbys": [{"name": "b" if active == "a" else "a", "gid": 4011}]},
            {"pg_ready": False, "pg_summary": {"num_pgs": 0}})


class MappedAddressOracleTest(unittest.TestCase):
    def test_dotted_and_hex_metadata_keep_both_valid_manager_roles(self):
        for name in ("a", "b"):
            for ip in ("::ffff:127.0.0.1", "::ffff:7f00:1"):
                with self.subTest(name=name, ip=ip):
                    result = oracle.validate(*metadata(name, ip))
                    self.assertEqual(result["mgr_name"], name)
                    self.assertEqual(result["mgr_endpoint"], "[" + ip + "]:" + str(oracle.MGR_PORTS[name]))
                    self.assertEqual(len(result["mon_endpoints"]), 3)

    def test_wrong_cluster_and_release_fail(self):
        for role in (0, 1):
            values = metadata()
            values[role]["fsid"] = "another-cluster"
            with self.subTest(role=role), self.assertRaises(ValueError):
                oracle.validate(*values)
        for release in (None, True, "20", 20.0, 19, 21):
            values = metadata()
            values[1]["min_mon_release"] = release
            with self.subTest(release=release), self.assertRaises(ValueError):
                oracle.validate(*values)

    def test_non_mapped_wrong_role_and_non_numeric_addresses_fail(self):
        for address in ("127.0.0.1:33300", "[::1]:33300", "[::ffff:127.0.0.2]:33300",
                        "[::ffff:127.0.0.1]:36800", "[::ffff:127.0.0.1%3]:33300",
                        "[hostname]:33300", "[::ffff:127.0.0.1]:+33300", None):
            values = metadata()
            values[1]["mons"][0]["public_addrs"]["addrvec"][0]["addr"] = address
            with self.subTest(address=address), self.assertRaises(ValueError):
                oracle.validate(*values)
        values = metadata()
        values[2]["active_addrs"] = vector(36801)
        with self.assertRaises(ValueError):
            oracle.validate(*values)

    def test_monitor_membership_and_address_vector_are_explicit(self):
        for change in (lambda m: m["mons"].pop(),
                       lambda m: m["mons"][1].update(name="a"),
                       lambda m: m["mons"][0].update(name=[]),
                       lambda m: m["mons"][0].update(public_addrs=[]),
                       lambda m: m["mons"][0]["public_addrs"]["addrvec"].append(vector(33300)["addrvec"][0]),
                       lambda m: m["mons"][0]["public_addrs"]["addrvec"][0].update(type="v1")):
            values = metadata()
            change(values[1])
            with self.assertRaises(ValueError):
                oracle.validate(*values)

    def test_integer_fields_reject_boolean_float_string_and_out_of_range(self):
        for gid in (None, True, "4010", 4010.0, 0, -1, 1 << 64):
            values = metadata()
            values[2]["active_gid"] = gid
            with self.subTest(gid=gid), self.assertRaises(ValueError):
                oracle.validate(*values)
        for nonce in (True, "0", -1, 1 << 32):
            values = metadata()
            values[2]["active_addrs"]["addrvec"][0]["nonce"] = nonce
            with self.subTest(nonce=nonce), self.assertRaises(ValueError):
                oracle.validate(*values)

    def test_manager_availability_name_and_standby_are_required(self):
        for change in (lambda m: m.update(available=1), lambda m: m.update(active_name=[]),
                       lambda m: m.update(active_name="c"), lambda m: m.update(standbys=[]),
                       lambda m: m["standbys"][0].update(name="a"),
                       lambda m: m["standbys"][0].update(gid=True),
                       lambda m: m["standbys"][0].update(gid=4010)):
            values = metadata()
            change(values[2])
            with self.assertRaises(ValueError):
                oracle.validate(*values)

    def test_pg_response_must_have_the_native_json_schema(self):
        for pg in ([], None, {}, {"pg_ready": 0, "pg_summary": {"num_pgs": 0}},
                   {"pg_ready": False, "pg_summary": []},
                   {"pg_ready": False, "pg_summary": {"num_pgs": True}}):
            values = list(metadata())
            values[3] = pg
            with self.subTest(pg=pg), self.assertRaises(ValueError):
                oracle.validate(*values)

    def test_all_top_level_values_must_be_objects(self):
        for index in range(4):
            values = list(metadata())
            values[index] = []
            with self.subTest(index=index), self.assertRaises(ValueError):
                oracle.validate(*values)

    def write_metadata(self, out, values):
        for name, value in zip(("status", "mon", "mgr", "pg"), values):
            (out / ("mapped-oracle-" + name + ".json")).write_text(json.dumps(value), encoding="utf-8")

    def test_gate_writes_only_the_validated_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            values = metadata()
            values[2]["unknown_field"] = "PRIVATE_SENTINEL"
            self.write_metadata(out, values)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(oracle.main([directory]), 0)
            result = (out / "mapped-oracle-summary.json").read_text()
            self.assertEqual(json.loads(result), oracle.validate(*values))
            self.assertNotIn("PRIVATE_SENTINEL", output.getvalue() + result)

    def test_invalid_json_missing_file_and_size_limit_fail_without_summary(self):
        for content in (b'{"PRIVATE_SENTINEL":', b'\xff', b' ' * (oracle.MAX_JSON_BYTES + 1), None):
            with self.subTest(content_size=None if content is None else len(content)), tempfile.TemporaryDirectory() as directory:
                out = Path(directory)
                self.write_metadata(out, metadata())
                path = out / "mapped-oracle-status.json"
                if content is None:
                    path.unlink()
                else:
                    path.write_bytes(content)
                error = io.StringIO()
                with contextlib.redirect_stderr(error):
                    self.assertEqual(oracle.main([directory]), 1)
                self.assertFalse((out / "mapped-oracle-summary.json").exists())
                self.assertNotIn("PRIVATE_SENTINEL", error.getvalue())


if __name__ == "__main__":
    unittest.main()
