import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_omapstate_oracle as omapstate


class FakeLib:
    def __init__(self):
        self.destroyed = []

    def rados_ioctx_destroy(self, io):
        self.destroyed.append(io)


class FakeRados:
    """Opens one fake I/O context; no daemon is involved."""

    def __init__(self):
        self.lib, self.opened = FakeLib(), []

    def ioctx(self, cluster, pool, namespace=""):
        self.opened.append((cluster, pool, namespace))
        return "io-%d" % len(self.opened)


class FakeNative:
    def __init__(self, state=None, error=None):
        self.state, self.error, self.calls = state, error, []

    def full_state(self, io, name, keys, cli):
        self.calls.append((io, name, keys, cli))
        if self.error is not None:
            raise self.error
        return self.state


STATE = {"code": 0, "version": 7, "data": "", "attributes": [], "gets": [], "omap": {"aggregate": 0}, "header": ""}


class OmapState(unittest.TestCase):
    def test_reads_one_owned_head(self):
        rados, native = FakeRados(), FakeNative(STATE)
        result = omapstate.mode_omapstate(rados, "cluster", {"slot": 2, "object": "dst"}, None, native=native, cli="cli")
        self.assertEqual(result, {"pool": "ceph-msgr-owned-pool-2", "object": "dst", "omap_state": STATE})
        self.assertEqual(rados.opened, [("cluster", "ceph-msgr-owned-pool-2", "")])
        self.assertEqual(native.calls, [("io-1", b"dst", [], "cli")])
        self.assertEqual(rados.lib.destroyed, ["io-1"])

    def test_releases_the_context_when_the_read_fails(self):
        rados = FakeRados()
        with self.assertRaises(ValueError):
            omapstate.mode_omapstate(rados, "cluster", {"slot": 1, "object": "src"}, None,
                                     native=FakeNative(error=ValueError("native OMAP inventory exceeds one bounded page")), cli="cli")
        self.assertEqual(rados.lib.destroyed, ["io-1"])

    def test_refuses_other_requests(self):
        for request in ({"slot": 1}, {"object": "src"}, {"slot": 3, "object": "src"}, {"slot": 1, "object": "src", "snap": "s"},
                        {"slot": 1, "object": ""}):
            rados = FakeRados()
            with self.assertRaises(ValueError, msg=request):
                omapstate.mode_omapstate(rados, "cluster", request, None, native=FakeNative(STATE), cli="cli")
            self.assertEqual(rados.opened, [])


if __name__ == "__main__":
    unittest.main()
