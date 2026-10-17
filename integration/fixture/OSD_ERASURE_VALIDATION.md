# Erasure-coded pool verification

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime, three BlueStore OSDs. Contract:
`internal/osd/ERASURE_CONTRACT.md`. Test: `TestCephOSDErasureIntegration`. The
pool oracle's `create` mode gained `erasure` (profile `ceph-msgr-owned-ec`,
k=2, m=1, `crush-failure-domain=osd`, `allow_ec_overwrites true`, removed on
cleanup), and the placement oracle now reports `pool_type` and `pg_shard` (the
target's position in the acting set for EC pools, -1 otherwise).

## Before the change (probe)

With the EC pool created natively, its rule was `set_chooseleaf_tries 5,
set_choose_tries 100, take default, choose_indep 0 type osd, emit`. A fresh
client's MON admission and a new OSD connection of an existing client were
both refused: "peer requires 0x0000001000000000" (CRUSH_V2).

## After the change

- Admission: a fresh client dials MON and opens an OSD connection while the EC
  rule exists, and replicated-pool I/O works.
- EC object I/O to the PG primary: WRITEFULL of 10000 binary bytes (ONDISK,
  native bytes and version equal), partial READ, partial overwrite, APPEND,
  SETXATTR and GETXATTR, STAT size, TRUNCATE and DELETE all match native
  observation. OMAP SETVALS is a known refusal `-95`.
- Shard addressing (replicated-PG profile): NO_SHARD and a different shard get
  no reply; the five-second wait ends as an unknown outcome.

## Listing, degraded reads and failover (`TestCephOSDErasureFailoverIntegration`)

Six objects are written by Go into the EC pool. Then:

- PG listing of the subject's PG at its primary (with the primary's shard in
  the replicated-PG profile) lists exactly the written objects whose native
  placement is that PG.
- The OSD of a non-primary shard is SIGKILLed; once native placement shows the
  acting set with that position empty (`[2 2147483647 1]`), a Go read through
  the primary returns the exact bytes, as does a native read: the primary
  reconstructed the missing shard.
- The primary's OSD is SIGKILLed; the new primary holds another shard
  (observed shard 0 on osd.2 to shard 1 on osd.0, and osd.0 to osd.1 in the
  other run). A Go read with the new shard returns the bytes; in the
  replicated-PG profile the same read with the old shard gets no answer.
- Each killed OSD is restarted and PGs return to clean before the next step.

## Profiles

Passed: legacy-v6 (object-hash v6 requests, the OSD derives the shard) with
the default secure CephX over IPv4 and with CRC/AES; replicated-pg-v8 with
explicit shards over IPv4 and with secure AES256K over IPv6. The
request-v9 profile's fixture allows only one OSD, so EC is not run there; its
encoder shares the v8 shard path.

Both EC tests passed in legacy-v6 CRC/AES and replicated-pg-v8 (IPv4); the
first also in the profiles listed above.

Not claimed: other EC profiles or plugins, MSR rules, writes while degraded,
recovery or backfill progress, EC snapshots, watch/notify, compound vectors
beyond the cases above, replica (shard) reads, and multiple PG listing pages.
