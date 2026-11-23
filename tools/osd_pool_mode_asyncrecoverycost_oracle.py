"""Owned-pool oracle mode asyncrecoverycost: the OSDs' async recovery cost bound.

Test-only; never product code. In the pinned Ceph 20.2.4 source
(7f793731f1b39eb4f465e960113d2363c311b964) a primary choosing the acting
set of an erasure-coded PG takes an up shard out of it as an async
recovery target when that shard's approximate missing count (its
num_objects_missing plus the versions its last_update is behind the
authoritative one) is above osd_async_recovery_min_cost and the remaining
shards still reach min_size and can recover (src/osd/PeeringState.cc
:2348-2414, choose_async_recovery_ec). The option is read at each call and
may change at runtime (src/common/options/global.yaml.in, default 100), so
a central entry takes effect once each OSD has applied it.

The mode is pglogbounds' lower, restore and show
(osd_pool_mode_pglogbounds_oracle.owned_settings) for this one option,
recorded in osd-async-recovery-cost-claim.json of the fixture output
directory: lower records the osd section's central entry and every up OSD's
effective value, sets the entry to 0 once and waits until every up OSD
shows 0; restore puts the recorded entry back once (`config rm` for none),
confirms it and waits until every recorded OSD shows its recorded value
again. With 0, a shard one version behind is a candidate. No command that
changes the cluster is repeated.
"""
import time

from osd_pool_oracle import oracle_mode

BOUNDS = (("osd_async_recovery_min_cost", 0),)
CLAIM = "osd-async-recovery-cost-claim.json"


@oracle_mode
def mode_asyncrecoverycost(rados, cluster, request, out, clock=time.monotonic, sleep=time.sleep):
    """Lower the osd section's osd_async_recovery_min_cost to 0, or restore it."""
    if set(request) != {"action"} or request["action"] not in ("lower", "restore", "show"):
        raise ValueError("asyncrecoverycost takes only action lower, restore or show")
    # Imported here: a mode module registers exactly its own mode while the
    # oracle loads it (osd_pool_oracle.load_mode_modules).
    from osd_pool_mode_pglogbounds_oracle import owned_settings
    return {"async_recovery_cost": owned_settings(rados, cluster, request["action"], out, BOUNDS, CLAIM, clock, sleep)}
