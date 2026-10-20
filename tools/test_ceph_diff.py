import hashlib
import io
import unittest
import urllib.error
from unittest.mock import patch

import ceph_diff


class CephDiffTest(unittest.TestCase):
    def test_compare_classifies_bytes_and_preserves_hashes(self):
        for before, after, status in (
            (b"same", b"same", "unchanged"),
            (b"before", b"after", "modified"),
            (None, b"added", "added"),
            (b"removed", None, "removed"),
        ):
            with self.subTest(status=status), patch.object(ceph_diff, "source", side_effect=[before, after]) as source:
                result = ceph_diff.compare("messages", "src/messages/MMonCommand.h", "a" * 40, "b" * 40)
                self.assertEqual(result["status"], status)
                self.assertEqual(source.call_args_list[0].args, ("a" * 40, "src/messages/MMonCommand.h"))
                self.assertEqual(source.call_args_list[1].args, ("b" * 40, "src/messages/MMonCommand.h"))
                for field, body in (("base", before), ("head", after)):
                    if body is None:
                        self.assertIsNone(result[field])
                    else:
                        self.assertEqual(result[field]["sha256"], hashlib.sha256(body).hexdigest())
                        self.assertEqual(result[field]["bytes"], len(body))

    def test_missing_on_both_sides_does_not_establish_unchanged(self):
        with patch.object(ceph_diff, "source", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "missing in both"):
                ceph_diff.compare("messages", "src/messages/MMonCommand.h", "a" * 40, "b" * 40)

    def test_resolve_pins_a_branch_to_its_returned_commit(self):
        with patch.object(ceph_diff, "fetch", return_value=b'{"sha":"' + b"a" * 40 + b'"}') as fetch:
            self.assertEqual(ceph_diff.resolve("wip/branch"), "a" * 40)
            self.assertEqual(fetch.call_args.args, ("https://api.github.com/repos/ceph/ceph/commits/wip%2Fbranch",))

    def test_invalid_refs_never_make_a_network_request(self):
        with patch.object(ceph_diff, "fetch") as fetch:
            for ref in ("", "tentacle\nother", "tag?query=true", "a" * 201):
                with self.subTest(ref=ref), self.assertRaisesRegex(ValueError, "invalid Ceph ref"):
                    ceph_diff.resolve(ref)
            fetch.assert_not_called()

    def test_invalid_upstream_commit_is_rejected(self):
        for metadata in (b'{"sha":"tentacle"}', b'{"sha":null}', b'{"sha":1}', b'{}', b'[]'):
            with self.subTest(metadata=metadata), patch.object(ceph_diff, "fetch", return_value=metadata):
                with self.assertRaisesRegex(ValueError, "invalid commit SHA"):
                    ceph_diff.resolve("tentacle")

    def test_fetch_limits_received_source_bytes(self):
        with patch.object(ceph_diff, "LIMIT", 8), patch("urllib.request.urlopen", return_value=io.BytesIO(b"0123456789")):
            with self.assertRaisesRegex(RuntimeError, "exceeds 8 bytes"):
                ceph_diff.fetch("https://raw.githubusercontent.com/ceph/ceph/commit/src/test")

    def test_optional_missing_source_does_not_hide_access_failures(self):
        url = "https://raw.githubusercontent.com/ceph/ceph/commit/src/test"
        for code in (404, 403, 429, 500):
            body = io.BytesIO(b"upstream error")
            with self.subTest(code=code), patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(url, code, "failure", {}, body)):
                if code == 404:
                    self.assertIsNone(ceph_diff.fetch(url, allow_missing=True))
                else:
                    with self.assertRaisesRegex(RuntimeError, f"HTTP {code}"):
                        ceph_diff.fetch(url, allow_missing=True)
                self.assertTrue(body.closed)
        body = io.BytesIO(b"required source missing")
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(url, 404, "missing", {}, body)):
            with self.assertRaisesRegex(RuntimeError, "HTTP 404"):
                ceph_diff.fetch(url)
            self.assertTrue(body.closed)


if __name__ == "__main__":
    unittest.main()
