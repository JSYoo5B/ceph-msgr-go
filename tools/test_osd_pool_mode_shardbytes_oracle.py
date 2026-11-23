import base64
import copy
import hashlib
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_shardbytes_oracle as shardbytes

NONE = shardbytes.CRUSH_ITEM_NONE


def gf_mul_table(a, b):
    """GF(2^8)/0x11d multiplication through log tables, a second derivation."""
    exp, log, x = [0] * 512, [0] * 256, 1
    for i in range(255):
        exp[i], log[x] = x, i
        x <<= 1
        if x & 0x100:
            x ^= 0x11d
    for i in range(255, 512):
        exp[i] = exp[i - 255]
    return 0 if a == 0 or b == 0 else exp[log[a] + log[b]]


class Field(unittest.TestCase):
    def test_multiplication_is_the_0x11d_field(self):
        self.assertEqual(shardbytes.gf_mul(0x80, 2), 0x1d)
        self.assertEqual(shardbytes.gf_mul(2, 2), 4)
        self.assertEqual(shardbytes.gf_mul(1, 0xa7), 0xa7)
        for a in range(256):
            for b in (0, 1, 2, 3, 0x1d, 0x80, 0xff, a):
                self.assertEqual(shardbytes.gf_mul(a, b), gf_mul_table(a, b), (a, b))
        # Every nonzero element has an inverse.
        for a in range(1, 256):
            self.assertEqual(sum(1 for b in range(1, 256) if shardbytes.gf_mul(a, b) == 1), 1, a)

    def test_parity_rows_are_the_vandermonde_rows(self):
        self.assertEqual(shardbytes.parity_rows(2, 2), [[1, 1], [1, 2]])
        self.assertEqual(shardbytes.parity_rows(3, 1), [[1, 1, 1]])
        self.assertEqual(shardbytes.parity_rows(4, 2), [[1, 1, 1, 1], [1, 2, 4, 8]])
        self.assertEqual(shardbytes.parity_rows(3, 3)[2], [1, 4, 16])


class Chunks(unittest.TestCase):
    def test_shards_hold_their_chunk_of_every_stripe(self):
        # k=2, m=2, stripe_width 4: chunks of 2 bytes, two stripes.
        data = bytes([1, 2, 3, 4, 0x80, 6, 7, 0x80])
        shards = shardbytes.shard_chunks(data, 2, 2, 4)
        self.assertEqual(shards[0], bytes([1, 2, 0x80, 6]))
        self.assertEqual(shards[1], bytes([3, 4, 7, 0x80]))
        self.assertEqual(shards[2], bytes([1 ^ 3, 2 ^ 4, 0x80 ^ 7, 6 ^ 0x80]))
        self.assertEqual(shards[3], bytes([1 ^ 6, 2 ^ 8, 0x80 ^ 14, 6 ^ 0x1d]))
        xor = shardbytes.shard_chunks(bytes(range(6)), 3, 1, 6)
        self.assertEqual(xor, [b"\x00\x01", b"\x02\x03", b"\x04\x05", bytes([0 ^ 2 ^ 4, 1 ^ 3 ^ 5])])

    def test_refuses_what_it_does_not_model(self):
        for data, k, m, width in ((b"x" * 6, 2, 2, 4), (b"", 2, 2, 4), (b"x" * 8, 3, 1, 8), (b"x" * 8, 2, 5, 4), (b"x" * 8, 0, 1, 4)):
            with self.assertRaises(shardbytes.BytesError):
                shardbytes.shard_chunks(data, k, m, width)


LOGICAL = bytes((i * 7 + i // 5) & 0xff for i in range(16))
STALE = bytes(16)


def listing(spg, shard, extra=""):
    ghobject = '{"oid":"ec-bytes","key":"","snapid":-2,"hash":12,"max":0,"pool":4,"namespace":"","shard_id":%d,"max":0}' % shard
    return ('[["%s","%s_head",%s]%s]' % (spg, spg, ghobject, extra)).encode(), '["%s",%s]' % (spg, ghobject)


def capture(up, contents):
    """A fixture capture record and its files for the up vector."""
    before = {"epoch": 30, "pgid": "4.2", "pool_id": 4, "object": "ec-bytes", "namespace": "", "up": up, "acting": up, "primary_id": up[0]}
    before_raw = json.dumps(before).encode()
    files, shards = {}, []
    for index, osd in enumerate(up):
        spg = "4.2s%d" % index
        if osd == NONE:
            shards.append({"index": index, "osd": osd, "spg": spg, "absent": True, "reader_isolated": False, "paused": False, "resumed": False,
                           "paused_ms": 0, "store_archive_bytes": 0, "whoami": "", "unsupported_layout": "", "reader_removed": False,
                           "list_exit_code": None, "list_bytes": 0, "list_stderr": "", "list_file": "", "selected": "",
                           "bytes_exit_code": None, "bytes_stderr": "", "bytes_length": 0, "bytes_file": ""})
            continue
        listed, selected = listing(spg, index)
        files["osd-shard-bytes-s%d-list.json" % index] = listed
        files["osd-shard-bytes-s%d-data.bin" % index] = contents[index]
        shards.append({"index": index, "osd": osd, "spg": spg, "absent": False, "reader_isolated": True, "paused": True, "resumed": True,
                       "paused_ms": 700, "store_archive_bytes": 4096, "whoami": str(osd), "unsupported_layout": "", "reader_removed": True,
                       "list_exit_code": 0, "list_bytes": len(listed), "list_stderr": "", "list_file": "osd-shard-bytes-s%d-list.json" % index,
                       "selected": selected, "bytes_exit_code": 0, "bytes_stderr": "", "bytes_length": len(contents[index]),
                       "bytes_file": "osd-shard-bytes-s%d-data.bin" % index})
    record = {"schema_version": 1, "source_commit": shardbytes.SOURCE_COMMIT, "before_sha256": hashlib.sha256(before_raw).hexdigest(),
              "pgid": "4.2", "pool_id": 4, "object": "ec-bytes", "up": up, "shards": shards, "complete": True}
    return before_raw, before, record, files


def run(up, contents, change=None, profile=None, native=LOGICAL, references=None):
    before_raw, before, record, files = capture(up, contents)
    after = copy.deepcopy(before)
    if change:
        change(after, record, files)

    def read(name, limit):
        if name not in files:
            raise OSError("no such file " + name)
        return files[name]

    descriptor = {"size": len(up), "stripe_width": 8}
    profile = profile or {"plugin": "isa", "k": "2", "m": "2", "technique": "reed_sol_van", "crush-failure-domain": "osd"}
    snapshot = {"code": 0, "version": 5, "data": base64.b64encode(native).decode()}
    return shardbytes.judge(before_raw, after, json.dumps(record).encode(), read, descriptor, profile, snapshot, references or {})


class Judge(unittest.TestCase):
    def test_matches_each_shard_against_the_native_object(self):
        expected = shardbytes.shard_chunks(LOGICAL, 2, 2, 8)
        receipt = run([1, 3, 2, 0], expected)
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual([s["matches"] for s in receipt["shards"]], [["native"]] * 4)
        self.assertEqual([s["shard_id"] for s in receipt["shards"]], [0, 1, 2, 3])
        self.assertEqual((receipt["k"], receipt["m"], receipt["stripe_width"]), (2, 2, 8))
        self.assertEqual(receipt["chunks_used_by_reads"], "not established")
        self.assertEqual(base64.b64decode(receipt["shards"][3]["raw"]), expected[3])

    def test_a_stale_shard_matches_its_reference_only(self):
        expected, stale = shardbytes.shard_chunks(LOGICAL, 2, 2, 8), shardbytes.shard_chunks(STALE, 2, 2, 8)
        contents = [expected[0], stale[1], expected[2], expected[3]]
        receipt = run([1, 3, 2, 0], contents, references={"before": STALE})
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual([s["matches"] for s in receipt["shards"]], [["native"], ["before"], ["native"], ["native"]])
        # A shard equal to neither is reported, not refused.
        receipt = run([1, 3, 2, 0], [expected[0], b"\x01" * 8, expected[2], expected[3]])
        self.assertTrue(receipt["complete"])
        self.assertEqual(receipt["shards"][1]["matches"], [])

    def test_an_empty_position_is_absent(self):
        expected = shardbytes.shard_chunks(LOGICAL, 2, 2, 8)
        receipt = run([1, NONE, 2, 0], [expected[0], None, expected[2], expected[3]])
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertTrue(receipt["shards"][1]["absent"])
        self.assertNotIn("matches", receipt["shards"][1])

    def test_refuses(self):
        expected = shardbytes.shard_chunks(LOGICAL, 2, 2, 8)

        def shard(index, **fields):
            return lambda after, record, files: record["shards"][index].update(fields)

        def two_heads(after, record, files):
            listed, _ = listing("4.2s1", 1)
            files["osd-shard-bytes-s1-list.json"] = listed[:-1] + b"," + listed[1:]
            record["shards"][1]["list_bytes"] = len(files["osd-shard-bytes-s1-list.json"])

        def other_shard(after, record, files):
            listed, selected = listing("4.2s1", 2)
            files["osd-shard-bytes-s1-list.json"], record["shards"][1]["selected"] = listed, selected

        def other_selected(after, record, files):
            record["shards"][1]["selected"] = listing("4.2s1", 1)[1].replace('"hash":12', '"hash":13')

        def generation(after, record, files):
            listed = listing("4.2s1", 1)[0].replace(b'"shard_id":1', b'"generation":4,"shard_id":1')
            files["osd-shard-bytes-s1-list.json"] = listed
            record["shards"][1]["list_bytes"] = len(listed)

        cases = {
            "moved bracket": (lambda after, record, files: after.update(epoch=31), "placement or epoch changed"),
            "other OSD's copy": (shard(2, whoami="3"), "whoami is '3'"),
            "unremoved reader": (shard(0, reader_removed=False), "were not removed"),
            "diagnostics": (shard(3, bytes_stderr="warning"), "wrote diagnostics"),
            "failed list": (shard(3, list_exit_code=1), "offline list exited 1"),
            "a file of another shard": (shard(2, bytes_file="osd-shard-bytes-s3-data.bin"), "as this shard's files"),
            "short file": (shard(2, bytes_length=7), "the files have"),
            "two heads": (two_heads, "2 listed heads"),
            "listed for another shard": (other_shard, "0 listed heads"),
            "a generation": (generation, "0 listed heads"),
            "another identity read": (other_selected, "another identity"),
            "incomplete capture": (lambda after, record, files: record.update(complete=False), "the capture is incomplete"),
            "another bracket": (lambda after, record, files: record.update(before_sha256="0" * 64), "not taken under this first bracket"),
            "absent recorded present": (shard(1, absent=True), "recorded absent"),
        }
        for name, (change, text) in cases.items():
            with self.subTest(name):
                receipt = run([1, 3, 2, 0], expected, change)
                self.assertFalse(receipt["complete"])
                self.assertTrue(any(text in failure for failure in receipt["failures"]), receipt["failures"])
        for name, profile in {"jerasure": {"plugin": "jerasure", "k": "2", "m": "2"}, "cauchy": {"plugin": "isa", "k": "2", "m": "2", "technique": "cauchy"},
                              "mapping": {"plugin": "isa", "k": "2", "m": "2", "mapping": "_DD_"}, "width": {"plugin": "isa", "k": "3", "m": "2"}}.items():
            with self.subTest(name):
                self.assertFalse(run([1, 3, 2, 0], expected, profile=profile)["complete"])
        self.assertFalse(run([1, 3, 2, 0], expected, native=LOGICAL[:12])["complete"])
        # A capture of an empty position fails.
        receipt = run([1, 3, 2, 0], expected, lambda after, record, files: (after.update(up=[1, NONE, 2, 0])))
        self.assertFalse(receipt["complete"])

    def test_admits_only_the_scrub_store_diagnostics(self):
        expected = shardbytes.shard_chunks(LOGICAL, 2, 2, 8)
        scrub = ("Error getting attr on : 4.2s0_head,0#-6:e0000000:::deep_scrub_4.2s0:head#, (61) No data available\n"
                 "Error getting attr on : 4.2s0_head,0#-6:e0000000:::scrub_4.2s0:head#, (61) No data available\n")
        receipt = run([1, 3, 2, 0], expected, lambda after, record, files: record["shards"][0].update(list_stderr=scrub))
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual(len(receipt["shards"][0]["list_scrub_store_diagnostics"]), 2)
        self.assertEqual(receipt["shards"][1]["list_scrub_store_diagnostics"], [])
        for other in (scrub.replace("4.2s0:head", "4.2s1:head"), scrub.replace("#-6:", "#-5:"), scrub.replace("(61)", "(2)"),
                      scrub + scrub, scrub.rstrip("\n"), scrub.replace("deep_scrub", "other"), scrub.replace("0#-6", "1#-6"),
                      "Error getting attr on : 4.2s0_head,0#4:e0000000:::ec-bytes:head#, (61) No data available\n"):
            with self.subTest(other=other):
                receipt = run([1, 3, 2, 0], expected, lambda after, record, files: record["shards"][0].update(list_stderr=other))
                self.assertFalse(receipt["complete"])
        receipt = run([1, 3, 2, 0], expected, lambda after, record, files: record["shards"][0].update(bytes_stderr=scrub))
        self.assertFalse(receipt["complete"])

    def test_references(self):
        self.assertEqual(shardbytes.parse_references([{"label": "before", "data": base64.b64encode(b"x").decode()}]), {"before": b"x"})
        for value in ([{"label": "native", "data": ""}], [{"label": "Before", "data": ""}], [{"label": "a", "data": "", "x": 1}],
                      [{"label": "a", "data": ""}, {"label": "a", "data": ""}], [{"label": str(i), "data": ""} for i in range(4)], {}):
            with self.assertRaises(ValueError):
                shardbytes.parse_references(value)


if __name__ == "__main__":
    unittest.main()
