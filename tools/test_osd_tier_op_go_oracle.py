import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import osd_tier_op_go_oracle as oracle

NS = "_ZN8librados7v14_2_0"
READ = NS + ("5IoCtx7operateERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
             "PNS0_19ObjectReadOperationEPN4ceph6buffer7v15_2_04listEi")


def symbols():
    names = {READ, NS + "5IoCtx7operateE" + oracle.Cxx.STRING + "PNS0_20ObjectWriteOperationEi",
             NS + "5IoCtxC1Ev", NS + "5IoCtxD1Ev", NS + "5IoCtx18from_rados_ioctx_tEPvRS1_",
             NS + "15ObjectOperationC1Ev", NS + "15ObjectOperationD1Ev",
             NS + "19ObjectReadOperation8is_dirtyEPbPi", NS + "20ObjectWriteOperation12tier_promoteEv",
             NS + "19ObjectReadOperation10tier_flushEv", NS + "19ObjectReadOperation10tier_evictEv"}
    return names


class FakeNative:
    calls = []

    def __init__(self, root):
        self.identity = {"name": "client.test", "global_id": 4100}
        self.cxx = mock.Mock(symbols={"tier-flush": "x"}, library="/usr/lib64/librados.so.2")
        self.rados = mock.Mock()
        self.cluster = object()

    def local(self, name, extent=None):
        FakeNative.calls.append(("observe", name, extent))
        return {"code": -2, "version": 0}

    def tier(self, name, operation):
        FakeNative.calls.append((operation, name))
        return {"code": -22, "version": 3, "per_op_result_available": False}

    def close(self):
        pass


class TierOpGoOracleTests(unittest.TestCase):
    def test_native_request_flags_follow_the_rados_tools(self):
        self.assertEqual(oracle.REQUEST_FLAGS, {"tier-promote": 0, "tier-flush": 8 | 32, "tier-evict": 8 | 32})
        self.assertEqual(oracle.WIRE_FLAGS["tier-promote"], 0x20)
        for name in ("tier-flush", "tier-evict"):
            self.assertEqual(oracle.WIRE_FLAGS[name], 0x10 | 0x8000 | 0x20000)
        # Observations never promote: local reads bypass the manifest.
        self.assertEqual(oracle.IGNORE_REDIRECT, 256)

    def test_resolver_binds_every_entry_point_or_refuses(self):
        names = symbols()
        resolved = oracle.Cxx.resolve(names)
        self.assertEqual(set(resolved), {"operate_read", "operate_write", "ioctx_ctor", "ioctx_dtor", "ioctx_from_c",
                                         "operation_ctor", "operation_dtor", "is-dirty", "tier-promote", "tier-flush", "tier-evict"})
        for role in ("tier-promote", "tier-flush", "tier-evict", "is-dirty"):
            with self.assertRaises(ValueError):
                oracle.Cxx.resolve(names - {resolved[role]})

    def test_inputs_are_closed(self):
        good = [{"action": "bind", "source_pool_id": 3, "chunk_pool_id": 4}, {"action": "configure-dedup"},
                {"action": "observe", "object": "tier-chunk-go"}, {"action": "observe", "object": "tier-dedup-native", "extent": [0, 16]},
                {"action": "native", "label": "chunk-evict", "operation": "tier-evict", "object": "tier-chunk-native"}]
        for value in good:
            self.assertEqual(oracle.validate_input(value), value)
        bad = [{"action": "remove", "object": "tier-chunk-go"}, {"action": "bind", "source_pool_id": 3, "chunk_pool_id": 3},
               {"action": "observe", "object": "fixture-object"}, {"action": "observe", "object": "tier-chunk-go", "extent": [4090, 16]},
               {"action": "observe", "object": "tier-chunk-go", "extent": [0, 0]},
               # A native operation never touches the Go object or another opcode.
               {"action": "native", "label": "x", "operation": "tier-evict", "object": "tier-chunk-go"},
               {"action": "native", "label": "x", "operation": "cache-evict", "object": "tier-chunk-native"},
               {"action": "native", "label": "Bad Label", "operation": "tier-evict", "object": "tier-chunk-native"},
               {"action": "native", "label": "x", "operation": "tier-evict", "object": "tier-chunk-native", "flags": 1},
               {"action": "configure-dedup", "pool": 1}]
        for value in bad:
            with self.assertRaises(ValueError, msg=value):
                oracle.validate_input(value)

    def run_call(self, out, value):
        (out / oracle.INPUT).write_text(json.dumps(value))
        with mock.patch.object(oracle, "Native", FakeNative):
            oracle.main(["oracle", str(out), str(out)])
        return json.loads((out / oracle.RESULT).read_text())

    def test_calls_are_bound_once_and_native_labels_are_never_reissued(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "daemon-version").write_text("ceph version 20.2.4 (%s) tentacle (stable)" % oracle.COMMIT)
            FakeNative.calls = []
            with self.assertRaises(ValueError):
                self.run_call(out, {"action": "observe", "object": "tier-absent"})
            self.assertFalse((out / oracle.RECEIPT).exists())
            with mock.patch.object(oracle, "bind", lambda native, request: dict(request, epoch=9)):
                self.run_call(out, {"action": "bind", "source_pool_id": 3, "chunk_pool_id": 4})
                with self.assertRaises(ValueError):
                    self.run_call(out, {"action": "bind", "source_pool_id": 3, "chunk_pool_id": 4})
            result = self.run_call(out, {"action": "native", "label": "plain-evict", "operation": "tier-evict", "object": "tier-plain-native"})
            self.assertEqual(result["result"]["code"], -22)
            self.assertEqual(result["result"]["wire_flags_source_derived"], 0x10 | 0x8000 | 0x20000)
            with self.assertRaises(ValueError):
                self.run_call(out, {"action": "native", "label": "plain-evict", "operation": "tier-evict", "object": "tier-plain-native"})
            self.assertEqual(FakeNative.calls, [("tier-evict", "tier-plain-native")])
            receipt = json.loads((out / oracle.RECEIPT).read_text())
            self.assertEqual([call["state"] for call in receipt["calls"]], ["completed", "completed"])
            # A call left without a result stops every later call.
            receipt["calls"].append({"input": {"action": "observe", "object": "tier-absent"}, "state": "intended"})
            (out / oracle.RECEIPT).write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                self.run_call(out, {"action": "observe", "object": "tier-absent"})
            self.assertEqual(len(FakeNative.calls), 1)

    def test_dedup_configuration_reads_back_the_closed_options(self):
        native = mock.Mock()
        native.rados.last_status = ""
        native.rados.mon.return_value = (0, b"")
        dumps = [{"pool": 3, "pool_name": "ceph-msgr-owned-pool-1", "options": {}},
                 {"pool": 3, "pool_name": "ceph-msgr-owned-pool-1", "options": {"dedup_tier": 4, "fingerprint_algorithm": "sha256",
                                                                               "dedup_chunk_algorithm": "fixed", "dedup_cdc_chunk_size": 16}}]
        with mock.patch.object(oracle, "pool_options", side_effect=dumps):
            result = oracle.configure_dedup(native, {"bound": {"source_pool_id": 3, "chunk_pool_id": 4}})
        self.assertTrue(result["configured"])
        self.assertEqual([step["var"] for step in result["applied"]], ["dedup_tier", "fingerprint_algorithm", "dedup_chunk_algorithm", "dedup_cdc_chunk_size"])
        dumps[1]["options"]["dedup_cdc_chunk_size"] = 32
        with mock.patch.object(oracle, "pool_options", side_effect=dumps):
            self.assertFalse(oracle.configure_dedup(native, {"bound": {"source_pool_id": 3, "chunk_pool_id": 4}})["configured"])
        native.rados.mon.return_value = (-22, b"")
        with mock.patch.object(oracle, "pool_options", side_effect=dumps[:1]):
            refused = oracle.configure_dedup(native, {"bound": {"source_pool_id": 3, "chunk_pool_id": 4}})
        self.assertFalse(refused["configured"])
        self.assertEqual(len(refused["applied"]), 1)
        with self.assertRaises(ValueError):
            oracle.configure_dedup(native, {})


if __name__ == "__main__":
    unittest.main()
