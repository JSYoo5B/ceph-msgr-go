# Go-originated cache-tier operation acceptance

This records the first live run of CACHE_FLUSH, CACHE_TRY_FLUSH, CACHE_EVICT,
CACHE_PIN and CACHE_UNPIN sent by this repository's Go client
(`OSDCacheFlush`, `OSDCacheTryFlush`, `OSDCacheEvict`, `OSDCachePin`,
`OSDCacheUnpin`; see
[CACHE_POOL_CONTRACT.md](../../internal/osd/CACHE_POOL_CONTRACT.md#cache-tier-operations))
against Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. It is one selected profile, not
general cache-tier support.

## Fixture, roles and selection

The test reuses the live cache-pool fixture (`CEPH_MSGR_TEST_OSD_CACHE_POOLS=1`,
see [OSD_IGNORE_CACHE_VALIDATION.md](OSD_IGNORE_CACHE_VALIDATION.md)): two
owned single-replica BlueStore pools, a writeback tier attached without an
overlay, no hit set or snapshots, and scoped identities limited to the
`cache-probe` namespace. The Go client authenticates as the tier read/write
identity with `EnableOSDCachePools` and sends every cache operation itself,
once, to the native placement's primary.

`tools/osd_cache_op_go_oracle.py` is a closed native observer. It never issues
a cache operation (its native wrapper refuses them). In three fixture controls
on the existing `osd-cache-pools` channel (`go-ops prepare`,
`go-ops observe LABEL`, `go-ops finish`) it records placements, seeds base
bytes A and tier bytes B for two heads with IGNORE_CACHE writes, observes both
pools with IGNORE_CACHE STAT+READ and the tier's dirty flag with the native C++
`is_dirty` read, and finally removes all three owned heads. Every observer
call publishes its intent first; an uncertain call refuses every later phase
and nothing is reissued.

The cache-pool profile admits `^TestCephOSDCacheOpGoIntegration$` as a third
exclusive selector. Catalog row
`osd-cache-pools.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcacheopgo`
reuses the already forwarded `osd_cache_pools` input.

## Sequence and expectations

Each Go reply must keep its transaction, GID, object, PG pool and raw hash,
map epoch, raw front and one operation; a negative aggregate must surface as
the same `OSDError` code; the per-op result must be 0 or equal the aggregate;
an `OutcomeUnknownError` stops the test without resending.

1. Evict the dirty head: EBUSY. Native state (bytes, size, version, mtime in
   both pools) and dirty=true are unchanged.
2. Pin; flush, try-flush and evict the pinned head: EPERM each. Bytes, size and
   user version are unchanged (pin may take the request mtime), dirty=true.
   Unpin; evict again: EBUSY, state unchanged.
3. Flush: 0. The base and the tier hold B, and dirty=false.
4. Try-flush a separate dirty head once: accepted results 0, EAGAIN or EBUSY;
   the observer then polls dirty within a bounded budget. No reissue. A 0
   result requires B in both pools, a valid final is_dirty read of false and a
   last poll of false. EAGAIN and EBUSY remain their own recorded outcomes (the
   flush may still be in progress): the tier must still hold B and the base A
   or B. `TestCacheOpGoTryFlushOutcome` rejects contradictory observations,
   such as a 0 result with the base still A or with the head still dirty.
5. Evict the clean, unpinned flushed head: 0. A bypass read finds the tier head
   absent and the base bytes, size, version and mtime equal the flushed base.
6. Absent head: flush, try-flush and evict return 0 or ENOENT; pin and unpin
   return ENOENT; neither pool gains a head.

## Observed run

[CI command run 39878019051](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39878019051)
tested exact commit `9077d5e0beb70a3409149342cc3fa7e204747e30`, which carries
the try-flush outcome gate: secure mode,
aes256k service and client keys, IPv6, container runtime, two MGRs, one
BlueStore OSD, legacy-v6 request profile, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
`TestCephOSDCacheOpGoIntegration` and all six subtests passed in 2.43 s; the try-flush returned 0 and its
postcondition gate held, and
the fixture's finish and pool cleanup completed (the job succeeded and the
test's cleanup assertion requires a finished observer with complete cleanup).
The earlier run 39870831158 at `0bf08b077c100ba4932d0d6f1657479fc7709245` also
passed, but without the try-flush postcondition gate.

| Go request | Transaction | Aggregate | Per-op |
| --- | --- | --- | --- |
| evict dirty | 1 | -16 | -16 |
| pin | 2 | 0 | 0 |
| flush pinned | 3 | -1 | -1 |
| try-flush pinned | 4 | -1 | -1 |
| evict pinned | 5 | -1 | -1 |
| unpin | 6 | 0 | 0 |
| evict dirty after unpin | 7 | -16 | -16 |
| flush | 8 | 0 | 0 |
| try-flush (separate head) | 9 | 0 | 0 |
| evict clean | 10 | 0 | 0 |
| flush absent | 11 | -2 | 0 |
| try-flush absent | 12 | -2 | 0 |
| evict absent | 13 | -2 | 0 |
| pin absent | 14 | -2 | -2 |
| unpin absent | 15 | -2 | -2 |

All replies carried map epoch 27. The values come from the test's bounded log
lines; a successful run uploads no diagnostics, so the observer receipt and
`osd-cache-op-go-results.json` were not retained. The Go results match the
earlier native-only librados checkpoint
([OSD_CACHE_TIER_OPS_NATIVE_VALIDATION.md](OSD_CACHE_TIER_OPS_NATIVE_VALIDATION.md)),
including ENOENT with a zero per-op result for the absent flush/evict family,
which the pinned `do_op` object lookup returns before those handlers.

The lost-reply boundary is synthetic:
`TestOSDCachePoolPublicTransportAndUnknownMutation` drops the connection after
an `OSDCacheFlush` is written and requires `OutcomeUnknownError` with the
original transaction, no redial and no resent request, on all four profiles.

## Not verified

Other security modes, address families, runtimes, request profiles and
stores; multiple OSDs, EC tiers, overlays, hit sets, the tier agent,
snapshots, clones and watchers; a live EAGAIN or EBUSY try-flush; scoped
identity refusals of Go cache operations; raw object_info pin or dirty flags
(pin evidence is behavioral, through EPERM); a live lost reply; and SET_CHUNK.
