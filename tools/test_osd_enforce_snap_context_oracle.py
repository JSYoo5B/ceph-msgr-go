import copy
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import osd_enforce_snap_context_oracle as oracle


class EnforceSnapshotOracleTests(unittest.TestCase):
    def test_closed_two_heads_one_snapshot_input(self):
        valid = {"object": oracle.HEADS[0], "snap_id": 7, "pool_id": 4}
        self.assertEqual(oracle.validate(valid), valid)
        for patch in ({"object": "foreign"}, {"snap_id": 0}, {"snap_id": True}, {"snap_id": oracle.HEAD}, {"pool_id": -1}, {"pool_id": True}, {"extra": "x"}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                oracle.validate({**valid, **patch})

    def test_native_ordinary_write_publishes_intent_then_never_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "write.json"
            identity = {"pool_id": 4, "snap_id": 7, "object": oracle.HEADS[0]}
            calls = []
            def execute():
                calls.append(1)
                entry = json.loads(path.read_bytes())
                self.assertEqual(entry["state"], "intended")
                self.assertEqual(entry["capi_flags"], 512)
                self.assertFalse(entry["wire_enforce_snapc"])
                return {"code": 0, "version": 8}
            first = oracle.ordinary_once(path, identity, execute)
            self.assertEqual(oracle.ordinary_once(path, copy.deepcopy(identity), execute), first)
            self.assertEqual(calls, [1])
            with self.assertRaises(RuntimeError):
                oracle.ordinary_once(path, {**identity, "snap_id": 8}, execute)
            self.assertEqual(calls, [1])

    def test_unknown_and_crashed_native_write_keep_original_identity(self):
        for outcome in (RuntimeError("lost native result"), {"code": -110}, {"code": -85}):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                path, identity, calls = Path(directory) / "write.json", {"snap": 7}, []
                def execute():
                    calls.append(1)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                with self.assertRaises(RuntimeError):
                    oracle.ordinary_once(path, identity, execute)
                first = path.read_bytes()
                with self.assertRaises(RuntimeError):
                    oracle.ordinary_once(path, identity, execute)
                self.assertEqual(path.read_bytes(), first)
                self.assertEqual(calls, [1])
                oracle.publish(path, {"identity": identity, "state": "intended"})
                with self.assertRaises(RuntimeError):
                    oracle.ordinary_once(path, identity, execute)
                self.assertEqual(calls, [1])

    def test_publish_bound_atomic_contents_and_posix_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            oracle.publish(path, {"state": "original"})
            original = path.read_bytes()
            with self.assertRaises(ValueError):
                oracle.publish(path, {"large": "x" * oracle.LIMIT})
            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(path.with_suffix(".tmp").exists())
            if os.name == "posix":
                self.assertEqual(path.stat().st_mode & 0o777, 0o644)

    def test_selected_snapshot_and_head_are_independent_readonly_observations(self):
        selected, destroyed, snapshots = [], [], []
        lib = SimpleNamespace(rados_ioctx_snap_set_read=lambda io, snap: selected.append((io, snap)),
                              rados_ioctx_destroy=lambda io: destroyed.append(io))
        def snapshot(io, name, names):
            snapshots.append((io, name, names, selected[-1][1]))
            return {"code": 0, "version": 8 if selected[-1][1] == oracle.HEAD else 3}
        rados = SimpleNamespace(lib=lib, ioctx=lambda *args: "owned-io", native=SimpleNamespace(snapshot=snapshot))
        raw = b'{"name":"enforce-snap-control", "seq":7,"clones":[]}'
        completed = SimpleNamespace(returncode=0, stdout=raw, stderr=b"")
        root = Path("/fixture")
        with mock.patch.object(oracle.subprocess, "run", return_value=completed) as run:
            result = oracle.observe(rados, "cluster", root, {"object": oracle.HEADS[0], "snap_id": 7})
        self.assertEqual(selected, [("owned-io", oracle.HEAD), ("owned-io", 7)])
        self.assertEqual(destroyed, ["owned-io"])
        self.assertEqual([s[3] for s in snapshots], [oracle.HEAD, 7])
        self.assertEqual(result["head"]["version"], 8)
        self.assertEqual(result["snapshot"]["version"], 3)
        self.assertEqual(oracle.base64.b64decode(result["list_snaps_raw"]), raw)
        self.assertEqual(run.call_args.args[0], ["rados", "--conf", str(root / "ceph.conf"), "--keyring", str(root / "keyring"), "--name", "client.test", "--pool", "ceph-msgr-owned-pool-1", "listsnaps", oracle.HEADS[0], "--format", "json"])

    def test_foreign_or_unbounded_native_listing_is_refused(self):
        for result in (SimpleNamespace(returncode=1, stdout=b"", stderr=b"failed"),
                       SimpleNamespace(returncode=0, stdout=b"x" * (65536 + 1), stderr=b""),
                       SimpleNamespace(returncode=0, stdout=b'{"name":"foreign", "seq":7}', stderr=b""),
                       SimpleNamespace(returncode=0, stdout=b'{"name":"enforce-snap-control", "seq":8}', stderr=b"")):
            with self.subTest(result=result), mock.patch.object(oracle.subprocess, "run", return_value=result):
                rados = SimpleNamespace(ioctx=lambda *args: 1,
                                        lib=SimpleNamespace(rados_ioctx_snap_set_read=lambda *args: None, rados_ioctx_destroy=lambda *args: None),
                                        native=SimpleNamespace(snapshot=lambda *args: {"code": 0}))
                with self.assertRaises((ValueError, RuntimeError)):
                    oracle.observe(rados, None, Path("/fixture"), {"object": oracle.HEADS[0], "snap_id": 7})


if __name__ == "__main__":
    unittest.main()
