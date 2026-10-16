"""Closed receipt/model tests; these syntheses are never retained native vectors."""
import copy
from pathlib import Path
import tempfile
import unittest

import osd_request_v9_native_encoder as oracle


def model(kind="read"):
    # Deliberately synthetic validator input. It proves only rejection rules;
    # actual native artifacts must later come from compiled pinned C++ objects.
    inc=99 if kind=="snapshot" else 1
    front=bytearray(106)
    flags=4|0x4000000|(32 if kind in ("mutation","snapshot") else 16)|(64 if kind=="snapshot" else 0)|(0x400 if kind=="pgnls" else 0)
    front[24:28]=(0x44 if kind=="pgnls" else 0x11223344).to_bytes(4,"little")
    front[28:32]=(11).to_bytes(4,"little")
    front[32:36]=flags.to_bytes(4,"little")
    front[87:94]=oracle.EMPTY_OTEL
    front[94:98]=inc.to_bytes(4,"little")
    front[-8:]=oracle.FEATURES.to_bytes(8,"little")
    header=oracle.HEADER.pack(1,77,42,127,9,0,0,3,1,3,0)
    metadata=dict(type=42,version=9,compat=3,tid=77,seq=1,priority=127,
                  front_len=len(front),middle_len=0,data_len=0,data_off=0,flags=1,ack_seq=3,reserved=0)
    routing=dict(pool=7,seed=0x44,shard=-1,raw_hash=0x44 if kind=="pgnls" else 0x11223344,
                 client_inc=inc,reqid_inc=inc,reqid_tid=77,features=oracle.FEATURES,epoch=11,flags=flags,
                 locator_hash=0x44 if kind=="pgnls" else -1,
                 object_hex="" if kind=="pgnls" else b"v9-codec".hex(),
                 key_hex="" if kind=="pgnls" else b"route-key".hex(),namespace_hex=b"tenant".hex(),
                 mtime_seconds=1700000000 if kind in ("mutation","snapshot") else 0,
                 mtime_nanoseconds=123456789 if kind in ("mutation","snapshot") else 0,
                 snap=(1<<64)-2,snap_seq=7 if kind=="snapshot" else 0,snaps=[7,5,2] if kind=="snapshot" else [],
                 otel_valid=False,empty_otel_hex=oracle.EMPTY_OTEL.hex(),legacy_trace_zero=True,
                 finish_decode_performed=True,second_finish_decode_needed=False)
    return front,b"",header,metadata,routing


class NativeV9ClosedModelTests(unittest.TestCase):
    def test_five_explicit_shapes_and_header_size(self):
        self.assertEqual(oracle.HEADER.size,41)
        for kind in oracle.KINDS:
            oracle.verify_native_fixture(kind,*model(kind))

    def test_valid_sdk_must_be_off_and_have_jaeger_undefined(self):
        with tempfile.TemporaryDirectory() as directory:
            config,cache=Path(directory)/"acconfig.h",Path(directory)/"CMakeCache.txt"
            config.write_text("/* #undef HAVE_JAEGER */\n");cache.write_text("WITH_JAEGER:BOOL=OFF\n")
            self.assertFalse(oracle.off_sdk_proof(config,cache)["have_jaeger"])
            for declaration in ("#define HAVE_JAEGER 1\n","#define HAVE_JAEGER 0\n",""):
                config.write_text(declaration)
                with self.assertRaises(RuntimeError):oracle.off_sdk_proof(config,cache)
            config.write_text("/* #undef HAVE_JAEGER */\n");cache.write_text("WITH_JAEGER:BOOL=ON\n")
            with self.assertRaises(RuntimeError):oracle.off_sdk_proof(config,cache)

    def test_native_trace_or_header_substitutions_rejected(self):
        for position in (63,86,87,88,89,93,94):
            front,data,header,metadata,routing=model();front[position]^=1
            with self.subTest(position=position),self.assertRaises(RuntimeError):
                oracle.verify_native_fixture("read",front,data,header,metadata,routing)
        for index in (16,20,37):
            front,data,header,metadata,routing=model();header=bytearray(header);header[index]^=1
            with self.subTest(header=index),self.assertRaises(RuntimeError):
                oracle.verify_native_fixture("read",front,data,bytes(header),metadata,routing)

    def test_routing_identity_and_native_decode_proofs_are_required(self):
        for field,value in (("seed",0x11223344),("raw_hash",0x44),("shard",0),("reqid_inc",0),("client_inc",True),
                            ("otel_valid",True),("finish_decode_performed",False),("second_finish_decode_needed",True),
                            ("locator_hash",0),("snap_seq",7),("mtime_seconds",1),("features",oracle.FEATURES ^ (1<<49))):
            values=list(model());values[-1][field]=value
            with self.subTest(field=field),self.assertRaises(RuntimeError):oracle.verify_native_fixture("read",*values)
        values=list(model("pgnls"));values[-1]["object_hex"]=b"unexpected".hex()
        with self.assertRaises(RuntimeError):oracle.verify_native_fixture("pgnls",*values)

    def test_missing_or_extra_artifact_never_counts_as_five_native_vectors(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            with self.assertRaises(RuntimeError):oracle.collect_native_fixtures(out)
            (out/"unexpected.front.bin").write_bytes(b"model")
            with self.assertRaises(RuntimeError):oracle.collect_native_fixtures(out)

if __name__=="__main__":unittest.main()
