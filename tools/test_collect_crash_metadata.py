"""Diagnostic selection tests use synthetic archives, not daemon crash proofs."""

import io
import json
import tarfile
import unittest

import collect_crash_metadata as collector


def archive(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as destination:
        for name, data in entries:
            member = tarfile.TarInfo(name)
            if data is None:
                member.type = tarfile.SYMTYPE
                member.linkname = "../../keyring"
                destination.addfile(member)
            else:
                member.size = len(data)
                destination.addfile(member, io.BytesIO(data))
    output.seek(0)
    return output


class CrashMetadataTests(unittest.TestCase):
    def test_preserves_objects_without_extracting_other_members(self):
        first = {"process_name": "ceph-mon", "backtrace": ["frame one", "frame two"]}
        second = {"process_name": "ceph-mgr", "assert_condition": "fixture failure"}
        source = archive([
            ("crash/id-a/meta", json.dumps(first, indent=2).encode()),
            ("crash/id-a/log", b"private log content"),
            ("crash/id-a/core", b"private memory content"),
            ("crash/id-b/meta", json.dumps(second).encode()),
            ("crash/id-c/meta", None),
        ])
        result = io.StringIO()
        self.assertEqual(collector.collect(source, result), 0)
        lines = result.getvalue().splitlines()
        self.assertEqual([json.loads(line) for line in lines], [first, second])
        self.assertNotIn("private", result.getvalue())

    def test_skips_invalid_and_oversized_metadata_but_continues(self):
        source = archive([
            ("crash/a/meta", b"not JSON"),
            ("crash/b/meta", b"[]"),
            ("crash/c/meta", b"\xff"),
            ("crash/d/meta", b" " * (collector.MAX_METADATA_BYTES + 1)),
            ("crash/e/meta", b'{"process_name":"ceph-mon"}'),
        ])
        result = io.StringIO()
        self.assertEqual(collector.collect(source, result), 0)
        self.assertEqual(json.loads(result.getvalue()), {"process_name": "ceph-mon"})

    def test_missing_or_corrupt_archive_fails_without_output(self):
        for data in [b"", b"not an archive"]:
            result = io.StringIO()
            self.assertEqual(collector.collect(io.BytesIO(data), result), 1)
            self.assertEqual(result.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
