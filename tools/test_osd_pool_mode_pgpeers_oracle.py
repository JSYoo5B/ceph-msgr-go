import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_pgpeers_oracle as pgpeers

QUERY = {"state": "active+recovery_wait+degraded", "epoch": 40, "up": [0, 2, 1], "acting": [0, 2, 1],
         "info": {"pgid": "5.3", "last_update": "40'12", "last_complete": "40'12"},
         "peer_info": [{"peer": "1", "last_update": "40'12", "last_complete": "33'7"},
                       {"peer": "2", "last_update": "40'12", "last_complete": "40'12"}]}


class FakeRados:
    """One owned replicated pool 5, one owned EC pool 6, and a pool 7 of the fixture."""

    def __init__(self, reply=(0, json.dumps(QUERY).encode(), "", None)):
        self.reply, self.commands = reply, []

    def dump(self, cluster):
        return {"pools": [{"pool_name": "ceph-msgr-owned-pool-1", "pool": 5, "type": 1},
                          {"pool_name": "ceph-msgr-owned-pool-2", "pool": 6, "type": 3},
                          {"pool_name": "rbd", "pool": 7, "type": 1}]}

    def osd(self, cluster, osd, command, limit):
        self.commands.append((osd, command))
        return self.reply


class PGPeers(unittest.TestCase):
    def test_reports_state_and_every_peer(self):
        rados = FakeRados()
        result = pgpeers.mode_pgpeers(rados, "cluster", {"pgid": "5.3", "osd": 0}, None)["pg_peers"]
        self.assertEqual(result["state"], "active+recovery_wait+degraded")
        self.assertEqual(result["info"], {"last_update": "40'12", "last_complete": "40'12"})
        self.assertEqual(result["peers"], [{"peer": "1", "last_update": "40'12", "last_complete": "33'7"},
                                           {"peer": "2", "last_update": "40'12", "last_complete": "40'12"}])
        self.assertEqual(rados.commands, [(0, {"prefix": "pg", "pgid": "5.3", "cmd": "query", "format": "json"})])

    def test_refuses_pgs_outside_the_owned_replicated_pools(self):
        for request in ({"pgid": "6.1", "osd": 0}, {"pgid": "7.0", "osd": 0}, {"pgid": "9.0", "osd": 0}, {"pgid": "5.g", "osd": 0},
                        {"pgid": "5.3"}, {"pgid": "5.3", "osd": -1}, {"pgid": "5.3", "osd": 0, "object": "o"}):
            rados = FakeRados()
            with self.assertRaises(ValueError, msg=request):
                pgpeers.mode_pgpeers(rados, "cluster", request, None)
            self.assertEqual(rados.commands, [], msg=request)

    def test_a_failed_or_oversized_query_fails(self):
        with self.assertRaises(RuntimeError):
            pgpeers.mode_pgpeers(FakeRados((-2, b"", "no such pg", None)), "cluster", {"pgid": "5.3", "osd": 0}, None)
        with self.assertRaises(ValueError):
            pgpeers.mode_pgpeers(FakeRados((0, b"", "", 5 << 20)), "cluster", {"pgid": "5.3", "osd": 0}, None)

    def test_refuses_another_document(self):
        for change in ({"peer_info": [{"peer": 1, "last_update": "1'1", "last_complete": "1'1"}]},
                       {"peer_info": [{"peer": "1", "last_update": "1'1"}]},
                       {"info": {"last_update": "latest", "last_complete": "1'1"}},
                       {"state": ""}, {"acting": ["0"]}):
            document = dict(QUERY, **change)
            with self.assertRaises(ValueError, msg=change):
                pgpeers.parse(json.dumps(document).encode())
        incomplete = dict(QUERY)
        del incomplete["peer_info"]
        with self.assertRaises(ValueError):
            pgpeers.parse(json.dumps(incomplete).encode())


if __name__ == "__main__":
    unittest.main()
