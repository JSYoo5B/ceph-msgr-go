"""Owned-pool oracle mode commitstall: arm one OSD's BlueStore commit stall.

Test-only; never product code. The pinned source (Ceph 20.2.4 commit
7f793731f1b39eb4f465e960113d2363c311b964) gives the stock control:

- src/blk/kernel/KernelDevice.cc:502-543 (``KernelDevice::flush``): while
  ``bdev_inject_crash`` is nonzero, the first flush with I/O since the last
  one holds ``flush_mutex``, sleeps ``bdev_inject_crash_flush_delay``
  seconds and then ``_exit(1)``s the daemon. Every flush of that device
  waits for the mutex meanwhile.
- src/os/bluestore/BlueStore.cc:15073-15345 (``_kv_sync_thread``) flushes the
  block device, or lets the BlueFS commit flush it, before each batch of
  transactions it commits to the key-value store, and runs the commit
  callbacks only after that commit; so once a flush sleeps no later
  transaction of the OSD is committed and none of its commit callbacks runs.
- KernelDevice.cc:1029-1035 and 1145-1152 also drop a write when
  ``rand() % bdev_inject_crash == 0``; with ODDS that is a chance of about
  one in 2**30 per write, and KernelDevice.cc:778-786 exits from the aio
  thread only after ODDS seconds, so neither plays a part here.
- src/common/options/global.yaml.in:4050-4061 declares both options
  without a startup flag, and ``config set`` (src/common/ceph_context.cc:
  593-620) changes only that daemon's runtime values; a restarted OSD starts
  from its configuration again, where both are unset.

Action arm reads both options of one OSD, refuses unless the stall is
unarmed (``bdev_inject_crash`` is 0), sets the delay to DELAY and reads it
back, then sets ``bdev_inject_crash`` to ODDS and reads it back. Nothing is
retried and nothing is restored: the OSD exits at the latest DELAY seconds
after its next flush, and the test that armed it kills it before that.
Action show only reads. A refused command is reported, not raised.
"""
import json

from osd_pool_oracle import integer, oracle_mode

CRASH = "bdev_inject_crash"
DELAY_OPTION = "bdev_inject_crash_flush_delay"
DELAY = 60
ODDS = 1 << 30
MAX_REPLY = 4 << 10


def effective(rados, cluster, osd, option):
    """(value or None, failure text or None) from one `config get`."""
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "config get", "var": option, "format": "json"}, MAX_REPLY)
    if code != 0 or oversized is not None:
        return None, "native config get %s returned %s (%s): %s" % (option, code, oversized, str(status)[:200])
    try:
        document = json.loads(raw)
        value = int(document[option]) if isinstance(document, dict) and set(document) == {option} else None
    except (ValueError, TypeError):
        value = None
    if value is None or value < 0:
        return None, "native config get has no non-negative %s" % option
    return value, None


def assign(rados, cluster, osd, option, value):
    """The failure text of one `config set`, or None."""
    code, _, status, oversized = rados.osd(cluster, osd, {"prefix": "config set", "var": option, "val": [str(value)], "format": "json"}, MAX_REPLY)
    if code != 0 or oversized is not None:
        return "native config set %s returned %s (%s): %s" % (option, code, oversized, str(status)[:200])
    return None


def set_and_read(rados, cluster, osd, option, value):
    """The readback of one `config set` of option to value, or a failure."""
    failure = assign(rados, cluster, osd, option, value)
    if failure:
        return None, failure
    got, failure = effective(rados, cluster, osd, option)
    if failure is None and got != value:
        failure = "the readback of %s is %d, not %d" % (option, got, value)
    return got, failure


@oracle_mode
def mode_commitstall(rados, cluster, request, out):
    """Arm one OSD's commit stall, or show its two options."""
    if set(request) != {"osd", "action"} or request["action"] not in ("arm", "show"):
        raise ValueError("commitstall takes exactly osd and action arm or show")
    osd = integer(request["osd"], 0, (1 << 31) - 1, "osd")
    receipt = {"osd": osd, "action": request["action"], "delay_limit": DELAY, "odds_limit": ODDS, "failures": [], "armed": False}
    for key, option in (("crash", CRASH), ("delay", DELAY_OPTION)):
        receipt[key], failure = effective(rados, cluster, osd, option)
        if failure:
            receipt["failures"].append(failure)
    if request["action"] == "show" or receipt["failures"]:
        return {"commit_stall": receipt}
    if receipt["crash"] != 0:
        receipt["failures"].append("%s of osd.%d is already %d" % (CRASH, osd, receipt["crash"]))
        return {"commit_stall": receipt}
    # The delay first: the stall must never start with the configured one.
    receipt["delay"], failure = set_and_read(rados, cluster, osd, DELAY_OPTION, DELAY)
    if failure:
        receipt["failures"].append(failure)
        return {"commit_stall": receipt}
    receipt["set"] = True
    receipt["crash"], failure = set_and_read(rados, cluster, osd, CRASH, ODDS)
    if failure:
        receipt["failures"].append(failure)
    receipt["armed"] = not receipt["failures"]
    return {"commit_stall": receipt}
