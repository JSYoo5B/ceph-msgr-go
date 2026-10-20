# Observation: a balanced read at an EC non-primary shard crashes the OSD

Status: observed 2026-10-20, not pursued. This is a Ceph daemon assertion
failure, not a client defect. No product change and no retained test came from
it; the behavior is left unverified and must not be exercised by the suite,
because it kills the disposable OSD and fails the tests that follow.

## What was tried

A probe subtest sent one READ with `BalanceReads` set, routed to an owned
k=2,m=1 EC object's **non-primary shard** OSD (its placement from the native
`place` oracle with that OSD id, `Placement.Primary` left at the acting
primary). The intent was to see whether an EC pool answers a balanced read at
a shard other than the primary.

## What happened

- legacy-v6 (the client sends the raw hash, the OSD maps the shard): the
  request got no reply and ended as an unknown outcome at the 10 s context
  deadline. The OSD did not obviously crash within the run.
- replicated-pg-v8, secure AES256K IPv6 (the client sends the explicit shard
  in spg_t): the connection ended with EOF, and the next test failed restoring
  the fixture because **osd.2 had crashed**:

  ```
  osd/PrimaryLogPG.h: 418: FAILED ceph_assert(m)
  virtual const pg_missing_const_i& PrimaryLogPG::get_shard_missing(pg_shard_t) const
  ```

  Backtrace (Ceph 20.2.4): `ECLegacy::ECCommonL::ReadPipeline::`
  `get_all_avail_shards` → `get_min_avail_to_read_shards` →
  `objects_read_and_reconstruct` → `ECBackendL::objects_read_async` →
  `PrimaryLogPG::OpContext::start_async_reads` → `execute_ctx` → `do_op`.

The EC read pipeline on a replica that is asked to serve the read directly
asserts on a missing shard-missing map. The crashed OSD left the following
`TestCephOSDErasureOptimizedIntegration` unable to reach a clean fixture.

## Why it is not retained

Deliberately crashing the disposable daemon is not a client behavior worth
asserting, and a test that does it corrupts the fixture for the rest of the
run. A caller directs reads to the acting primary; balanced/localized reads to
replicas are verified only on replicated pools
(OSD_REPLICA_READ_VALIDATION.md), which is where that path is supported. EC
replica (shard) reads stay in the not-claimed list of OSD_ERASURE_VALIDATION.md.
