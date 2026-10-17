# Erasure-coded pools: admission and shard addressing

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/ceph_features.h` CRUSH_V2 bit 36, `osd/OSDMap.cc` get_features,
`crush/CrushWrapper.cc` is_v2_rule, `messages/MOSDOp.h` spg_t; LGPL-2.1,
independently translated).

## Policy

By explicit user decision on 2026-10-17, CRUSH_V2 is advertised on MON and
OSD sessions. Every erasure-code rule uses `choose_indep`/`chooseleaf_indep`
and `set_*_tries` steps, so once the OSDMap holds one, `OSDMap::get_features`
makes MONs and OSDs require CRUSH_V2 from every client. Without it this client
could not connect to a cluster that merely contains an EC pool, even for
MON commands or replicated-pool I/O (observed live: "peer requires
0x0000001000000000"). The bit is admission only: the client computes no CRUSH
placement, applies no OSDMap and claims no other CRUSH generation. CRUSH_MSR
(multi-step retry rules) and the other OSD-side CRUSH bits remain
unadvertised; clusters requiring them are still refused explicitly. Creating
an MSR erasure rule (for example `crush-osds-per-failure-domain=3`) was
observed to need `require_min_compat_client squid` first ("new crush map
requires client version squid"), a cluster-wide client baseline beyond this
admission exception; such clusters are not supported.

## Requests to an EC pool

An EC PG is addressed by the shard the target OSD holds. With the
replicated-PG request profiles (`OSDProfileReplicatedPG*`), set
`OSDPlacement.Erasure` and `Shard` to the primary's position in the PG's acting
set; the caller derives it from its own map like `Primary`. NO_SHARD or another
shard makes the OSD treat the request as misdirected: it never answers, and
the wait ends with the caller's context (observed live). The object-hash
profile sends MOSDOp v6 without a shard and the OSD maps the raw hash to the
primary shard itself (observed live). There is no automatic shard selection.

Object operations travel unchanged; the pool decides what it supports. Live on
k=2, m=1 with `allow_ec_overwrites`: WRITEFULL, partial READ, partial WRITE,
APPEND, SETXATTR/GETXATTR, STAT, TRUNCATE and DELETE behaved as natively; OMAP
writes are refused with `-95` (EOPNOTSUPP). `OSDPGListTarget.Erasure/Shard`
addresses PG listing the same way (replicated-PG profiles; the v6 object-hash
listing has no shard and the OSD derives it). With one shard's OSD down the
primary reconstructs reads; after the primary dies the new primary holds a
different shard and only requests naming it are answered, so the caller must
re-derive the shard from the new map along with the primary. Other opcodes,
snapshots, watch, recovery traffic and EC profiles other than k=2,m=1 are
unverified. Self-managed snapshots and a write during one shard's absence
(later recovered) were verified.

Verification: [OSD_ERASURE_VALIDATION.md](../../integration/fixture/OSD_ERASURE_VALIDATION.md).
