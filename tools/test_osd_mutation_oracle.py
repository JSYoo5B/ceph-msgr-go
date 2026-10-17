import copy
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import osd_mutation_oracle as oracle
from osd_mutation_oracle import collect_placement


def pair(epoch=19):
    mapped = {"epoch": epoch, "pool": "test", "pool_id": 7, "objname": "binary",
              "acting_primary": 2, "up": [2, 0, 1], "acting": [2, 0, 1],
              "raw_pgid": "7.123abc", "pgid": "7.0"}
    dump = {"epoch": epoch,
            "osds": [{"osd": 2, "up": 1, "in": 1,
                      "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.0.2.4:6800", "nonce": 91}]}}],
            "pools": [{"pool": 7, "pool_name": "test", "type": 1, "size": 3, "min_size": 2}]}
    return mapped, dump


class PlacementOracleTests(unittest.TestCase):
    def query(self, responses, *, clock=lambda: 0, timeout=30, namespace="", object_name="binary"):
        calls = []
        remaining = iter(responses)

        def command(*args, timeout):
            calls.append((args, timeout))
            return copy.deepcopy(next(remaining))

        result = collect_placement(command, "test", object_name, 7, clock=clock,
                                   timeout=timeout, namespace=namespace)
        return result, calls

    def test_returns_one_matched_epoch_target_and_topology(self):
        result, calls = self.query(pair())
        self.assertEqual(result, {"pool_id": 7, "object": "binary", "osd_id": 2, "primary_id": 2, "pg_shard": -1, "pool_type": 1,
                                  "epoch": 19, "hash": 0x123ABC,
                                  "address": "v2:192.0.2.4:6800/91", "up": [2, 0, 1],
                                  "acting": [2, 0, 1], "replicas": 3, "min_size": 2,
                                  "namespace": "", "raw_pgid": "7.123abc", "pgid": "7.0", "pg_seed": 0,
                              "pg_temp": False, "nonprimary_shard_count": 0})
        self.assertEqual(calls, [(("osd", "map", "test", "binary"), 10), (("osd", "dump"), 10)])

    def test_names_a_replica_target_and_still_reports_the_primary(self):
        mapped, dump = pair()
        dump["osds"].append({"osd": 0, "up": 1, "in": 1,
                             "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.0.2.5:6801", "nonce": 92}]}})
        result = collect_placement(lambda *args, timeout: copy.deepcopy(mapped if args[:2] == ("osd", "map") else dump),
                                   "test", "binary", 7, target_osd=0)
        self.assertEqual((result["osd_id"], result["primary_id"], result["address"]), (0, 2, "v2:192.0.2.5:6801/92"))
        for bad in (9, "0", True):
            with self.assertRaises(ValueError):
                collect_placement(lambda *args, timeout: copy.deepcopy(mapped if args[:2] == ("osd", "map") else dump),
                                  "test", "binary", 7, target_osd=bad)

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
        for fault in ("map pool name", "map pool id", "object", "dump pool name", "unknown pool type", "min size", "raw hash"):
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
                elif fault == "unknown pool type":
                    dump["pools"][0]["type"] = 4
                elif fault == "min size":
                    dump["pools"][0]["min_size"] = 4
                else:
                    mapped["raw_pgid"] = "8.123abc"
                with self.assertRaises(ValueError):
                    self.query([mapped, dump])

    def test_erasure_pool_reports_the_target_shard(self):
        mapped, dump = pair()
        dump["pools"][0]["type"] = 3
        dump["osds"].append({"osd": 1, "up": 1, "in": 1,
                             "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.0.2.6:6802", "nonce": 93}]}})
        primary = collect_placement(lambda *args, timeout: copy.deepcopy(mapped if args[:2] == ("osd", "map") else dump), "test", "binary", 7)
        replica = collect_placement(lambda *args, timeout: copy.deepcopy(mapped if args[:2] == ("osd", "map") else dump), "test", "binary", 7, target_osd=1)
        self.assertEqual((primary["pg_shard"], primary["pool_type"]), (0, 3))
        self.assertEqual((replica["osd_id"], replica["pg_shard"]), (1, 2))

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

    def test_namespace_and_object_route_preserves_raw_hash_and_mapped_pg(self):
        mapped, dump = pair()
        mapped.update(objname="tenant-X/another-object", raw_pgid="7.deadbeef", pgid="7.3")
        result, calls = self.query([mapped, dump], namespace="tenant-X", object_name="another-object")
        self.assertEqual(calls[0][0], ("osd", "map", "test", "another-object", "tenant-X"))
        self.assertEqual(result["object"], "another-object")
        self.assertEqual(result["namespace"], "tenant-X")
        self.assertEqual(result["hash"], 0xDEADBEEF)
        self.assertEqual(result["raw_pgid"], "7.deadbeef")
        self.assertEqual(result["pgid"], "7.3")
        self.assertEqual(result["pg_seed"], 3)

    def test_namespace_epoch_requery_keeps_identity_and_uses_new_nonce(self):
        old_map, _ = pair(19)
        current_map, current_dump = pair(20)
        old_map["objname"] = current_map["objname"] = "tenant-X/binary"
        old_map["raw_pgid"] = "7.111"
        current_map["raw_pgid"] = "7.222"
        current_dump["osds"][0]["public_addrs"]["addrvec"][0]["nonce"] = 101
        result, calls = self.query([old_map, current_dump, current_map, current_dump], namespace="tenant-X")
        self.assertEqual([call[0] for call in calls[::2]],
                         [("osd", "map", "test", "binary", "tenant-X")] * 2)
        self.assertEqual(result["hash"], 0x222)
        self.assertEqual(result["epoch"], 20)
        self.assertEqual(result["address"], "v2:192.0.2.4:6800/101")

    def test_namespace_mismatch_or_invalid_mapped_pg_is_not_retried(self):
        for fault in ("bare object", "other namespace", "mapped pool", "mapped overflow", "no mapped PG"):
            mapped, dump = pair()
            mapped["objname"] = "tenant-X/binary"
            if fault == "bare object":
                mapped["objname"] = "binary"
            elif fault == "other namespace":
                mapped["objname"] = "tenant-Y/binary"
            elif fault == "mapped pool":
                mapped["pgid"] = "8.0"
            elif fault == "mapped overflow":
                mapped["pgid"] = "7.100000000"
            else:
                del mapped["pgid"]
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                self.query([mapped, dump], namespace="tenant-X")

    def test_invalid_namespace_or_object_fails_before_native_queries(self):
        calls = []

        def command(*args, timeout):
            calls.append(args)
            raise AssertionError("native query must not run")

        for object_name, namespace in (("", ""), ("binary\0suffix", ""),
                                       ("binary", "tenant\0alias"), ("binary", "x" * 4097)):
            with self.subTest(object_name=object_name, namespace=namespace), self.assertRaises(ValueError):
                collect_placement(command, "test", object_name, 7, namespace=namespace)
        self.assertEqual(calls, [])


class NativeFunction:
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *args):
        return self.callback(*args)


class NativeLibrary:
    def __init__(self):
        self.calls = []
        self.functions = {}

    def __getattr__(self, name):
        if name not in self.functions:
            def function(*args):
                self.calls.append((name, args))
                return -2 if name == "rados_stat2" else 0
            self.functions[name] = NativeFunction(function)
        return self.functions[name]


class NativeMonLibrary(NativeLibrary):
    def __init__(self, responses, *, code=0, diagnostic=b"native status", lengths=None, missing=False):
        super().__init__()
        self.responses = iter(responses)
        self.buffers = []
        self.commands = []
        self.freed = []

        def command(cluster, commands, count, inbuf, inlength, output, outlength, status, statuslength):
            self.calls.append(("rados_mon_command", (cluster,)))
            self.commands.append((commands[0], count, inbuf, inlength))
            data = next(self.responses)
            for ptr, size, value, length in ((output, outlength, data, lengths[0] if lengths else len(data)),
                                            (status, statuslength, diagnostic, lengths[1] if lengths else len(diagnostic))):
                buffer = oracle.C.create_string_buffer(value)
                self.buffers.append(buffer)
                ptr._obj.value = None if missing else oracle.C.addressof(buffer)
                size._obj.value = length
            return code

        def free(pointer):
            self.freed.append(pointer.value)

        self.functions["rados_mon_command"] = NativeFunction(command)
        self.functions["rados_buffer_free"] = NativeFunction(free)


class NativeMonQueryTests(unittest.TestCase):
    def test_direct_utf8_json_and_whole_second_timeout_preserve_native_identity(self):
        library = NativeMonLibrary([b'{"epoch":19}'])
        result = oracle.native_mon_query(library, None, "osd", "map", "test", "object",
                                         "ceph-msgr/네임스페이스", timeout=4.99)
        self.assertEqual(result, {"epoch": 19})
        encoded, count, inbuf, inlength = library.commands[0]
        self.assertIn("네임스페이스".encode(), encoded)
        self.assertEqual(json.loads(encoded), {"prefix": "osd map", "format": "json",
                                               "pool": "test", "object": "object",
                                               "nspace": "ceph-msgr/네임스페이스"})
        self.assertEqual((count, inbuf, inlength), (1, None, 0))
        self.assertEqual(library.calls[0], ("rados_conf_set", (None, b"rados_mon_op_timeout", b"4")))
        self.assertEqual(library.freed, [oracle.C.addressof(buffer) for buffer in library.buffers])

    def test_default_query_json_omits_namespace_and_caps_timeout_at_ten(self):
        library = NativeMonLibrary([b'{}', b'{}'])
        oracle.native_mon_query(library, None, "osd", "map", "test", "binary", timeout=30)
        oracle.native_mon_query(library, None, "osd", "dump", timeout=1)
        self.assertNotIn("nspace", json.loads(library.commands[0][0]))
        self.assertEqual(json.loads(library.commands[1][0]), {"prefix": "osd dump", "format": "json"})
        self.assertEqual([args[2] for name, args in library.calls if name == "rados_conf_set"], [b"10", b"1"])
        self.assertEqual(len(library.freed), 4)

    def test_invalid_query_or_insufficient_deadline_never_enters_native_api(self):
        for arguments, timeout in ((("osd", "dump"), 0.99), (("osd", "dump"), float("inf")),
                                   (("osd", "dump"), float("nan")), (("osd", "dump"), True),
                                   (("osd", "set", "noout"), 10),
                                   (("osd", "map", "test", "bad\0name"), 10),
                                   (("osd", "map", "test", "binary", "bad\0ns"), 10)):
            library = NativeMonLibrary([])
            with self.subTest(arguments=arguments, timeout=timeout), self.assertRaises(ValueError):
                oracle.native_mon_query(library, None, *arguments, timeout=timeout)
            self.assertEqual(library.calls, [])

    def test_timeout_configuration_failure_never_sends_query(self):
        library = NativeMonLibrary([])
        library.functions["rados_conf_set"] = NativeFunction(lambda *args: -22)
        with self.assertRaisesRegex(RuntimeError, "timeout setup failed: -22"):
            oracle.native_mon_query(library, None, "osd", "dump", timeout=10)
        self.assertEqual(library.commands, [])

    def test_negative_result_invalid_json_and_large_receipts_always_free_both_buffers(self):
        cases = (({"code": -13}, b'{}', RuntimeError, "query failed: -13"),
                 ({}, b'not-json', ValueError, "Expecting value"),
                 ({}, b'[]', ValueError, "JSON shape"),
                 ({"lengths": (oracle.MAX_MON_OUTPUT_BYTES + 1, 0)}, b'{}', ValueError, "bounded allocation"),
                 ({"lengths": (2, oracle.MAX_MON_DIAGNOSTIC_BYTES + 1)}, b'{}', ValueError, "bounded allocation"))
        for kwargs, data, error, message in cases:
            library = NativeMonLibrary([data], **kwargs)
            with self.subTest(kwargs=kwargs, data=data), self.assertRaisesRegex(error, message):
                oracle.native_mon_query(library, None, "osd", "dump", timeout=10)
            self.assertEqual(library.freed, [oracle.C.addressof(buffer) for buffer in library.buffers])
            self.assertEqual(len(library.commands), 1)  # Failed reads are never replayed.

    def test_nonzero_length_null_buffer_is_rejected_without_dereference(self):
        library = NativeMonLibrary([b'{}'], missing=True)
        with self.assertRaisesRegex(ValueError, "missing buffer"):
            oracle.native_mon_query(library, None, "osd", "dump", timeout=10)
        self.assertEqual(library.freed, [])


class MutationIdentityTests(unittest.TestCase):
    metadata = {"pool": "test", "pool_id": 7, "object": "original"}

    def test_existing_default_read_identity_and_nil_payload_remain_supported(self):
        self.assertEqual(oracle.validate_mutation_request({"compare": False, "placement": True}, self.metadata),
                         ("original", ""))
        request = {"compare": True, "exists": True, "baseline": None,
                   "seconds": 1700000001, "nanos": 1, "version_delta": -1,
                   "operations": [{"Code": 0x2202, "Offset": 0, "Length": 0, "Data": None, "Flags": 0}]}
        self.assertEqual(oracle.validate_mutation_request(request, self.metadata), ("original", ""))

    def test_request_identity_extent_flags_and_binary_bounds_fail_before_c_load(self):
        requests = [{"object": "bad\0name"}, {"namespace": "bad\0namespace"},
                    {"object": "x" * 4097}, {"placement": 1},
                    {"operations": [{"Code": 0x2202, "Offset": 0, "Length": 1, "Data": None, "Flags": 0}]},
                    {"operations": [{"Code": 0x2205, "Offset": -1, "Length": 0, "Flags": 0}]},
                    {"operations": [{"Code": 0x2205, "Offset": 0, "Length": 0, "Flags": 4}]},
                    {"operations": [{"Code": 0x2205, "Offset": 0, "Length": 0, "Flags": 0}] * 65}]
        with tempfile.TemporaryDirectory() as directory:
            out = pathlib.Path(directory)
            (out / "osd-oracle.json").write_text(json.dumps(self.metadata))
            for request in requests:
                (out / "osd-mutation-input.json").write_text(json.dumps(request))
                with self.subTest(request=request), patch.object(oracle.C, "CDLL") as load:
                    with self.assertRaises(ValueError):
                        oracle.collect(out, out)
                    load.assert_not_called()
            (out / "osd-mutation-input.json").write_bytes(b" " * (oracle.MAX_INPUT_BYTES + 1))
            with patch.object(oracle.C, "CDLL") as load, self.assertRaisesRegex(ValueError, "input bound"):
                oracle.collect(out, out)
            load.assert_not_called()

    def test_native_snapshot_and_mon_mapping_use_same_requested_namespace_object(self):
        mapped, dump = pair()
        namespace = "ceph-msgr/네임스페이스"
        mapped.update(objname=namespace + "/another-object", raw_pgid="7.deadbeef", pgid="7.3")
        library = NativeMonLibrary([json.dumps(mapped).encode(), json.dumps(dump).encode()])

        with tempfile.TemporaryDirectory() as directory:
            out = pathlib.Path(directory)
            (out / "osd-oracle.json").write_text(json.dumps(self.metadata))
            (out / "osd-mutation-input.json").write_text(json.dumps(
                {"object": "another-object", "namespace": namespace, "placement": True}))
            with patch.object(oracle.C, "CDLL", return_value=library), patch("subprocess.check_output") as cli:
                oracle.collect(out, out)
            cli.assert_not_called()
            result = json.loads((out / "osd-mutation-result.json").read_text())
        self.assertEqual(result["actual"]["code"], -2)
        self.assertEqual(result["placement"]["namespace"], namespace)
        self.assertEqual(result["placement"]["object"], "another-object")
        self.assertEqual(result["placement"]["hash"], 0xDEADBEEF)
        namespaces = [args[1] for name, args in library.calls if name == "rados_ioctx_set_namespace"]
        names = [args[1] for name, args in library.calls if name == "rados_stat2"]
        self.assertEqual(namespaces, [namespace.encode()])
        self.assertEqual(names, [b"another-object"])
        self.assertEqual(json.loads(library.commands[0][0]), {"prefix": "osd map", "format": "json",
                         "pool": "test", "object": "another-object", "nspace": namespace})
        self.assertEqual(len(library.commands), 2)
        self.assertEqual(len(library.freed), 4)
        self.assertFalse(any(name in ("rados_remove", "rados_write_op_operate2") for name, _ in library.calls))


class ErasureShardTests(unittest.TestCase):
    def test_plain_pool_uses_first_position(self):
        pool = {"type": 3, "nonprimary_shards": "{}"}
        self.assertEqual(oracle.erasure_shard([4, 2, 2], 2, pool), 1)

    def test_optimized_pool_prefers_primary_eligible_shard(self):
        # osd.2 holds nonprimary shard 1 and parity shard 2; Objecter's
        # primary-first search names shard 2.
        pool = {"type": 3, "nonprimary_shards": "{1}"}
        self.assertEqual(oracle.erasure_shard([4, 2, 2], 2, pool), 2)
        self.assertEqual(oracle.erasure_shard([4, 2, 3], 4, pool), 0)
        # A target found only on a nonprimary shard keeps that position.
        self.assertEqual(oracle.erasure_shard([4, 2, 3], 2, pool), 1)


if __name__ == "__main__":
    unittest.main()
