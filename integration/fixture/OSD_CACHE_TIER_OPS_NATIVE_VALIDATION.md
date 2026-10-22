# Native cache-tier operation checkpoint

This is the first native-only checkpoint for issue #8: CACHE_FLUSH,
CACHE_TRY_FLUSH, CACHE_EVICT, CACHE_PIN and CACHE_UNPIN issued through
native librados against Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. It adds no Go product opcode,
operation union, feature bit or admission rule, and it makes no Go
cache-tier operation support claim.

## Fixture and selection

The checkpoint reuses the live cache-pool fixture
(`CEPH_MSGR_TEST_OSD_CACHE_POOLS=1`, see
[OSD_IGNORE_CACHE_VALIDATION.md](OSD_IGNORE_CACHE_VALIDATION.md)). That
fixture creates two owned single-replica BlueStore pools, attaches a
writeback tier without an overlay, has no hit set or snapshots, and creates
three scoped identities: tier read/write (`rw`), tier read-only (`ro`) and
base read/write (`base`), each limited to the `cache-probe` namespace.
Seeding, observation and cleanup use the fixture's observer identity.

The cache-pool profile admits either exclusive selector,
`^TestCephOSDIgnoreCacheIntegration$` or
`^TestCephOSDCacheTierOpsIntegration$`. The catalog row
`osd-cache-pools.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcachetierops`
reuses the `osd_cache_pools` opt-in already forwarded by the trusted native
runner, so no new workflow parameter or environment variable is introduced.

`TestCephOSDCacheTierOpsIntegration` sends one `tier-ops` request on the
existing cache-pool control channel. The fixture accepts it at most once,
records the intent, and runs `tools/osd_cache_tier_ops_oracle.py` inside the
fixture container with a four-minute bound. The test then judges the native
receipt `osd-cache-tier-ops-native.json` and, after the fixture's `finish`,
the pool, CRUSH rule, identity and `mon_allow_pool_delete` cleanup gates in
`osd-cache-pools-fixture.json`.

## Native construction

The five operations and `is_dirty` exist only in the public C++ API
(`ObjectReadOperation::cache_flush/cache_try_flush/cache_evict/is_dirty`,
`ObjectWriteOperation::cache_pin/cache_unpin`). The trusted runner has no
Ceph SDK build path, and the existing compiled helpers are opt-in prebuilt
binaries that it does not forward, so a compiled C++ helper cannot run there.
The oracle instead loads the fixture image's
`librados.so.2`, resolves 13 exported C++ entry points from that library's
own dynamic symbol table, and calls them through ctypes with a borrowed
`rados_ioctx_t`. The signed return of `IoCtx::operate` is recorded
unchanged. The resolved symbol names are kept in the receipt. These C++
cache calls expose no per-operation return value; the receipt says so per
step. STAT+READ observations and `is_dirty` keep their per-operation values.

Request flags are librados `OPERATION_*` values and follow the pinned
`rados.cc` CLI:

| Operation | API flags | Source-derived wire flags |
| --- | --- | --- |
| CACHE_FLUSH | IGNORE_CACHE \| IGNORE_OVERLAY (40) | 0x28000 |
| CACHE_TRY_FLUSH | IGNORE_CACHE \| IGNORE_OVERLAY \| SKIPRWLOCKS (56) | 0x38000 |
| CACHE_EVICT | IGNORE_CACHE \| IGNORE_OVERLAY \| SKIPRWLOCKS (56) | 0x38000 |
| CACHE_PIN / CACHE_UNPIN | IGNORE_CACHE (8) | 0x8000 |

The pinned C API `rados_cache_pin` uses no request flag. The oracle selects
IGNORE_CACHE for pin and unpin so an absent tier head cannot be promoted
before the handler. The wire translation column comes from
`translate_flags` in `librados_cxx.cc` and `rados.h:469-471`; wire bytes were
not captured in this checkpoint.

Every call writes an intended step before execution. Results in the
uncertain set (-110, -104, -108, -125, -107) or a native exception stop the
controller, skip owned-head cleanup and leave pool removal to the fixture's
`finish`; nothing is reissued. A completed librados call is not necessarily
one server execution: the native client may resend after a map change.

## Observed result

Run [39800841784](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39800841784)
tested head `7cd8a9291632fab18e76236a3396fe0b9cc64613` with image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
secure mode, AES256K keys and service cipher, IPv6, the container runtime and
one BlueStore OSD. The result gate recorded PASS for
`TestCephOSDCacheTierOpsIntegration` (30.60 s). The receipt had 67 completed
native steps and no stop reason; its SHA-256 was
`5f20d973151fc4f83aa9c3d2604d1b631918650563fb5ae65eb749215d8256de`. Values
below come from the test's bounded log summary. The receipt itself is not
archived, because successful runs upload no diagnostics.

Two objects were seeded with bytes A in the base and B in the tier; one
absent head was the control. `is_dirty` reported dirty with per-op 0 on both
seeded tier heads.

| Step | Identity | Object state | Observed |
| --- | --- | --- | --- |
| evict | rw | dirty, unpinned | -16 |
| evict | ro | dirty, unpinned | -16 |
| evict | base | dirty, unpinned | -1 |
| pin | ro | dirty | -1 |
| pin | base | dirty | -1 |
| pin | rw | dirty | 0 |
| flush (40) | rw | pinned | -1 |
| try-flush (56) | rw | pinned | -1 |
| evict (56) | rw | pinned | -1 |
| flush with SKIPRWLOCKS (56) | rw | pinned | -22 |
| try-flush without SKIPRWLOCKS (40) | rw | pinned | -22 |
| unpin | ro | pinned | -1 |
| unpin | rw | pinned | 0 |
| evict | rw | dirty, unpinned | -16 |
| flush (40) | rw | dirty, unpinned | 0 |
| try-flush (56) | rw | separate dirty head | 0 |
| evict | rw | clean, unpinned | 0 |
| flush, try-flush, evict | rw | absent in both pools | -2 each |
| pin, unpin | rw | absent in both pools | -2 each |

- Refused operations left base bytes, size, version and mtime unchanged and
  tier bytes, size and user version unchanged (tier user version 5, base
  17). Pin and unpin keep the user version; tier mtime is not compared
  across them because a pin takes the request mtime.
- After the blocking flush, both pools held B and `is_dirty` reported clean.
  The base user version changed from 17 to 5, equal to the tier head's
  user version.
- The try-flush head returned 0 and was observed clean on the first of at
  most ten one-second `is_dirty` observations. Both pools then held B (both
  user version 6). It was issued once.
- After clean eviction an IGNORE_CACHE read of the tier returned -2, and the
  base bytes, size, version and mtime equalled the post-flush observation.
  An ordinary tier read by `rw` then returned B, a direct tier read found B
  (user version 5), and the promoted head was clean.
- The absent controls left the head absent in both pools. From the pinned
  source, `PrimaryLogPG::do_op` replies -ENOENT for an absent head of a
  non-write operation before the cache handlers run, and pin/unpin reach
  their own ENOENT branch. The handlers' "absent object succeeds" branch was
  not reached.
- Cleanup removed the three owned heads in both pools with IGNORE_CACHE and
  confirmed each absent. The fixture's `finish` then detached the tier and
  confirmed the owned pools, CRUSH rules and identities absent and the
  central pool-deletion override removed.

An earlier run, [39800081564](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39800081564)
at `ccdee9045a776b340ebd141b090a14c351c58ac5`, also passed the same assertions
(20.40 s). It did not log the summary, so it does not show which accepted
try-flush value occurred.

Pin evidence is behavioral: the -1 results follow pin and stop after unpin.
The raw object_info FLAG_CACHE_PIN and FLAG_DIRTY bits were not decoded;
dirty state comes from native `is_dirty`.

## Not verified

- Any Go product request for these opcodes, their operation unions or their
  admission; native request vectors for a later Go primitive.
- The CRC/AES, IPv4 or host-runtime profiles; only the single profile above
  ran this checkpoint.
- Overlays, hit sets, the background tier agent, snapshots and head clones,
  watchers (evict's watched-head branch), manifests and SET_CHUNK, erasure
  coded pools, multiple OSDs, recovery and fault injection.
- Try-flush -EAGAIN or -EBUSY, a timed-out or otherwise uncertain outcome,
  and the cancellation paths. The controller handles them; they were not
  observed.
- The handler-level absent-object and no-cache-mode branches, and the
  ro identity's ability to flush (not attempted, because it would mutate).
- Wire-level request flags and per-operation results for the cache
  operations, and any durability property beyond librados completion.
