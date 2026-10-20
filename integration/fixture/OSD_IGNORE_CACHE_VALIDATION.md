# Live IgnoreCache acceptance (2026-10-18)

The public pure-Go client passed on two Linux configurations with caller-
supplied routing. Native CLI, librados and placement queries are test-only.

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; exact image digests are in the receipt.
See the [contract](../../internal/osd/CACHE_POOL_CONTRACT.md) and
[independent receipt](OSD_IGNORE_CACHE_LINUX_20261018.json).

## Source and configurations

Both executions used base `source-1` plus
15 selected fixture/test/CI files. The receipt records every selected hash
and the manifest hash. All **703 Go source files** were independently
matched; their sorted path/space/SHA256/newline aggregate is
`194e502d331867954baf55c6d8a3dddb7c045404d49d50f21d4a3750fba413c2`.
This counts source files, not executed tests. Docs were added afterward.

| Linux amd64 configuration | UTC runner window | Go test | Result |
| --- | --- | --- | --- |
| CRC, AES client/service, IPv4 host relay, single BlueStore | 07:32:57.884–07:34:45.812 | 28.78 s | 12/12, exit 0 |
| Secure, AES256K client/service, IPv6 container, single BlueStore | 07:32:59.323–07:34:45.852 | 30.43 s | 12/12, exit 0 |

Both use provider v0.3.0, three MONs, two MGRs and one OSD. Selector: `^TestCephOSDIgnoreCacheIntegration$`, with
`CEPH_MSGR_TEST_OSD_CACHE_POOLS=1`, `CEPH_MSGR_TEST_OSD=1`, profile
`legacy-v6`, store `bluestore`, replicas `1`. Mixed feature/fault profiles
and zero MGRs are refused. Two dedicated CI rows retain existing rows and
selectors.

## Observations

Each fixture owns a replicated base pool and writeback tier, without an
overlay, HASHPSPOOL or snapshots. Sixteen fixed heads separate Go/native
objects for STAT, READ, WRITEFULL and DELETE, normal and IgnoreCache.
Native receipts retain 32 base/tier routes and one foreign-namespace route:
pool ID, raw hash, actual PG, primary, epoch, address and nonce. The product
performs no CRUSH calculation or automatic routing.

With `EnableOSD` and `EnableOSDCachePools` before Dial, eight cases each
send one standalone Go operation and one independent single-op native
counterpart. C API flag `8` becomes request-level `IGNORE_CACHE=0x8000`;
normal operations use `0`. Observer snapshots use bounded STAT+READ with
IgnoreCache.

Normal STAT/READ found base bytes through the tier; IgnoreCache returned
signed ENOENT for an absent tier head. Both WRITEFULL variants stored the
changed binary bytes in the selected tier head. Both DELETE variants
removed its visible head. A subsequent normal native read returned ENOENT
after normal DELETE, but re-promoted original base bytes after IgnoreCache
DELETE. This is **behavior-only whiteout evidence**; raw `FLAG_WHITEOUT`
was not inspected.

User versions were compared only to each object's own baseline/readback,
never for equality across separate Go/native objects. Observed base bytes,
size, version and mtime stayed unchanged, including after DELETE followups
and permission probes. Successful native read/stat per-op results were
zero. Mutation replies retained requested ONDISK completion.

Each run preserved 14 Go responses: eight paired cases, two permitted
scoped reads and four signed EPERM refusals (read-only write, base-only
tier read, foreign namespace, tier-only base read). Full raw reply fronts
and data remain in the hashed results. The auditor independently decoded
the v6 identity/flags/result/version/op prefix and matched exposed metadata
and payload. TIDs were unique per GlobalID; pool/raw hash matched the
caller route; reply epochs were not older than submitted epochs. None was
outcome-unknown.

Four other leaves checked default MON rejection of required bit 35,
private MON/Ping and MGR transport, scoped permissions, and subscription
survival. Scoped MON `version` tell correctly returned EACCES (`-13`);
a separate admin proved the positive tell without widening the scoped
grant. Ping verified MON b/FSID; MGR `pg stat` succeeded. Primary GlobalID
remained 4830 (CRC) and 4812 (secure), with unchanged MON/MGR endpoints,
readiness and an original subscription receiving an independently
published log. Default OSD refusal remains synthetic handshake evidence:
the live default client is refused first by MON.

## Control and cleanup

Each run used 19 closed controls and 172 native C API calls. Every call
saved intent and its exact role/name/GID snapshot: 20 observer GIDs and
eight native RW GIDs, rather than the last process's identity. All calls
completed; a recorded uncertain call cannot be replayed by later controls.
Cleanup removed all heads, detached the tier, removed pools/rules and
scoped clients, and removed the pool-delete override with effective
baseline `false`. Each retained absence audit reported eight owned
containers and its network absent; the offline auditor issued no Docker
queries.

Shared lifecycle regression passed both native-only profiles: each had
66 calls, ten placements, eight containers/network absent. The separate
auditor checked all 132 calls; that probe makes no Go support claim.

## Failures and verification boundaries

Preserved first failures:

- Initial live runs stopped at scoped tell `-13`, before the object cases.
  The expectation now retains the refusal and uses a separate admin for
  the positive control. Product behavior and scoped grants did not change.
- Initial Linux root tests/race/vet passed, then two nested fixture default
  assertions omitted the new zero-valued opt-in. Expectations were updated.
- Windows unit jobs compared `0666` with POSIX `0644`. Exact mode checking
  is now POSIX-only; atomic publication, bounds and content checks remain
  universal. At `source-2`, Windows Go 1.24.0 and 1.27.x unit jobs and Windows
  fixture tools passed. The snapshot was not a full CI pass: other jobs
  remained pending. Cancelled or earlier attempts are not counted as PASS.

Live coverage is limited to legacy-v6 single BlueStore in these two
configurations. The [24-vector native proof](../../internal/osd/testdata/ignore-cache-native-README.md)
separately covers v6/v8/untraced-v9 bytes, not live v8/v9 cache-pool I/O.
EC, snapshots, FULL flags, replica reads, fault injection and additional
durability guarantees remain unverified. No complete final CI pass is
claimed here.
