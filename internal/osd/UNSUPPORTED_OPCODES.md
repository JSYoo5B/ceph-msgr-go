# Unsupported OSD catalog entries

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h`, `osd/PrimaryLogPG.cc`, `osd/osd_types.cc`, `osdc/Objecter.h`,
`librados/`). Classification: 2026-10-21, from a direct read of those pinned
files. This is a source classification, not an interoperability result, and
the entries are not implementation tasks by name alone.

[opcodes-tentacle.csv](opcodes-tentacle.csv) marks 27 of 80 entries
`unsupported`. That count mixes client operations, daemon-only suboperations
and entries without a server handler; it is not a completion percentage.
`unsupported` means this repository emits no such operation. It does not claim
the daemon lacks a handler. Listing an entry here does not enable any feature
bit or imply support; older public APIs are not restored.

Classes:

- **No current server path**: no pinned handler; sending it would be refused.
- **Daemon-only operation / daemon-originated**: not a client primitive.
- **Current server path, deferred**: a handler exists, but this repository
  has no consumer need, or the format is legacy (tmap, non-namespace listing).
- **Current client primitive, dependent**: exposed by librados/tools and
  reserved for the cache-tier or manifest work tracked in the named issue; each
  needs a native prerequisite fixture before any wire work.

| Entry | Class | Pinned-source observation | Follow-up |
| --- | --- | --- | --- |
| MASKTRUNC | No current server path | `osd/osd_types.cc` names it in a mode-classification switch only; `PrimaryLogPG::do_osd_ops` has no case and no Objecter builder exists. | none |
| SETTRUNC | No current server path | No reference outside the `include/rados.h` list. | none |
| SETXATTRS | No current server path | No handler and no builder (see [METADATA_CONTRACT.md](METADATA_CONTRACT.md)). | none |
| RESETXATTRS | No current server path | `Objecter.h` still has a builder, but `PrimaryLogPG::do_osd_ops` has no case. | none |
| PULL | Daemon-only operation | Defined in the SUB opcode group only; no `do_osd_ops` case, builder or other reference. Replication/recovery uses dedicated messages. | none |
| PUSH | Daemon-only operation | As PULL. | none |
| BALANCEREADS | Daemon-only operation | As PULL. | none |
| UNBALANCEREADS | Daemon-only operation | As PULL. | none |
| SCRUB | Daemon-only operation | As PULL. | none |
| SCRUB_RESERVE | Daemon-only operation | As PULL. | none |
| SCRUB_UNRESERVE | Daemon-only operation | As PULL. | none |
| SCRUB_MAP | Daemon-only operation | As PULL. | none |
| COPY_GET | Daemon-originated operation | Handled by `PrimaryLogPG`; its only caller outside `Objecter.h` is `PrimaryLogPG` copy/promote (the OSD's own Objecter). No librados entry point. | #7 (COPY_FROM) classifies the client-visible copy path |
| SYNC_READ | Current server path, deferred primitive | Handled together with READ (same extent semantics), also used internally by manifest reads. No Objecter or librados builder emits it; READ is implemented. | none; no consumer need |
| STARTSYNC | Current server path, deferred primitive | Handler is a no-result nop write. No builder or librados entry point. | none; no consumer need |
| TRIMTRUNC | Current server path, deferred primitive | Handler falls through into TRUNCATE (implemented) with the truncate size as offset. No builder or librados entry point. | none; no consumer need |
| TMAPGET | Current server path, deferred legacy format | Handler returns EOPNOTSUPP on erasure-coded pools. Tmap is the pre-OMAP object format; OMAP operations are implemented. Older APIs are not restored. | none |
| TMAPPUT | Current server path, deferred legacy format | As TMAPGET. | none |
| TMAPUP | Current server path, deferred legacy format | As TMAPGET; `Objecter.h` and `librados` still expose a builder. | none |
| TMAP2OMAP | Current server path, deferred legacy format | Conversion of a tmap object to OMAP; reachable only for tmap-format objects. | none |
| PGLS | Current server path, deferred legacy format | PG listing without namespaces. PGNLS, the namespace-aware form, is implemented. | none |
| PGLS_FILTER | Current server path, deferred legacy format | As PGLS; PGNLS_FILTER is implemented. | none |
| PG_HITSET_LS | Current client primitive, cache-tier dependent | Handled in the PG-op path from the PG's hit-set history; `Objecter.h` and `librados` expose it. Meaningful only with a hit-set configured. | #8 |
| PG_HITSET_GET | Current client primitive, cache-tier dependent | As PG_HITSET_LS; `librados` exposes it. | #8 |
| TIER_PROMOTE | Current client primitive, manifest dependent | Inputless; `ObjectWriteOperation::tier_promote`, `rados tier-promote`. Copies a redirect target or missing chunks back into a manifest object; a plain object is a no-op success. | #8 |
| TIER_FLUSH | Current client primitive, manifest and dedup-pool dependent | Inputless; `ObjectReadOperation::tier_flush`, `rados tier-flush` with IGNORE_CACHE and IGNORE_OVERLAY. Chunks the object into the pool's dedup tier; needs the pool's `fingerprint_algorithm` and `dedup_tier` options. | #8 |
| TIER_EVICT | Current client primitive, manifest dependent | Inputless; `ObjectReadOperation::tier_evict`, `rados tier-evict` with IGNORE_CACHE and IGNORE_OVERLAY. Marks every chunk missing and punches the local data; a non-manifest object is refused. | #8 |

`TestUnsupportedOpcodesAreClassified` fails if this table and the catalog's
`unsupported` entries differ.

CACHE_FLUSH, CACHE_TRY_FLUSH, CACHE_EVICT, CACHE_PIN and CACHE_UNPIN are
implemented as standalone cache-tier operations; see
[CACHE_POOL_CONTRACT.md](CACHE_POOL_CONTRACT.md#cache-tier-operations).
SET_CHUNK is implemented as a bounded standalone manifest operation (offset-zero
source extent with WITH_REFERENCE); see
[MANIFEST_CONTRACT.md](MANIFEST_CONTRACT.md#go-set_chunk-contract).
