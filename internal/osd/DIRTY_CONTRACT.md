# Object dirty flag (ISDIRTY, UNDIRTY)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h` ISDIRTY 0x121d and UNDIRTY 0x221c, `osdc/Objecter.h`
is_dirty/undirty, `osd/PrimaryLogPG.cc` do_osd_ops and make_writeable;
LGPL-2.1, independently translated). [CONTRACT.md](CONTRACT.md) supplies the
shared v6 object identity, per-op result, ONDISK, cancellation and
unknown-outcome rules.

## Wire shape

Both opcodes are **inputless**: the packed 38-byte op envelope carries a zero
extent union and zero payload length, with no data segment — exactly the
UNSET_MANIFEST shape. `Objecter`'s `is_dirty()` and `undirty()` builders add
the op with no indata; `is_dirty()` only wires the reply decoding. They encode
identically through the v6, v8 and v9 request paths (the generic non-union
branch of the op encoder). Each is accepted only as a **standalone, flagless**
head operation here: no EXCL/FAILOK/FADVISE flags, no offset/length/truncate
fields, no snapshot, and no compound vector. Compound ordering, snapshot
interaction and the cache-tier agent paths that also emit these ops are left to
a later contract.

## Server semantics (plain replicated pool)

Both handlers run with **no tier/cache_mode/manifest guard**, so they are
reachable on an ordinary replicated pool.

- **ISDIRTY** (`PrimaryLogPG.cc:6283-6292`): a read. The per-op `result` is
  **always 0** (it never inspects `obs.exists`). The dirty/clean answer is one
  `encode(bool)` byte in the op's **outdata** (reply output), not the result
  code. The client decodes that byte: nonzero means dirty.
- **UNDIRTY** (`PrimaryLogPG.cc:6294-6305`): a write. The per-op `result` is
  **always 0**, with no output. If the object is dirty it sets `ctx->undirty`
  and `ctx->modify`; the flag itself is cleared later in `make_writeable`
  (`PrimaryLogPG.cc:8650-8662`). A clean object is a no-op that still returns 0.

The dirty flag is ordinary object_info state, not cache-tier-only. Any
user-visible write flows through `make_writeable`, which **sets FLAG_DIRTY**
(`:8660`) when the object was clean and no undirty is pending, and **clears it**
(`:8657`) when an UNDIRTY is pending on a dirty object. So a WRITE (native or
through this client) marks the object dirty, a following ISDIRTY reports true,
and a following UNDIRTY clears it.

## Go surface

Public opcodes `OSDIsdirty` and `OSDUndirty`. `OSDOperationResult.Dirty` decodes
the ISDIRTY output bool on a successful, non-redirected read; the raw byte stays
in `Data`. The reply validator allows ISDIRTY exactly one output byte on success
(zero otherwise) and no output for UNDIRTY. A successful UNDIRTY vector reports
ONDISK like any mutation; the existing identity, cancellation and
unknown-outcome rules apply unchanged — a sent UNDIRTY with a lost reply stays
uncertain and is never replayed.

## Not implemented (unreachable on a plain pool)

The cache-tier and dedup-manifest opcodes are deliberately excluded because they
cannot be driven from a client against a plain replicated pool:

- **CACHE_FLUSH (0x821f), CACHE_TRY_FLUSH (0x8221), CACHE_EVICT (0x8220)** guard
  on `cache_mode == CACHEMODE_NONE || has_manifest()` and return **-EINVAL**
  before any work on a non-tier pool (`PrimaryLogPG.cc:6307,6340,6382`).
- **CACHE_PIN (0x2224), CACHE_UNPIN (0x2225)** guard on `!is_tier() ||
  cache_mode == NONE` and return **-EINVAL** on a plain pool (`:7113,:7138`).
- **SET_CHUNK (0x8228)** is a manifest/dedup op (not cache-tier); it carries
  encoded indata and requires the WITH_REFERENCE flag plus dedup setup, failing
  with **-EOPNOTSUPP/-EINVAL/-ERANGE** otherwise (`:7280`). Not a simple probe.

These remain unimplemented client operations. The development fixture can now
prepare an owned cache tier for the bounded IGNORE_CACHE capability; see
[CACHE_POOL_CONTRACT.md](CACHE_POOL_CONTRACT.md) and its separate native
validation records. That setup does not implement or verify flush/evict/pin
messages. Manifest/chunk preparation and its independent oracle remain separate
requirements in #8. Plain-pool refusal is not absence of a daemon handler.

## Verification

- `dirty_test.go`: the inputless 38-byte envelope for both opcodes across the
  shared encoder, the READ/WRITE mode bits, and rejection of flags, data,
  extent fields, versions, xattr variants and compounds before transmission.
- `integration/osd_dirty_test.go` (`TestCephOSDDirtyFlagIntegration`): live on a
  replicated pool — a native seed write leaves the object dirty, ISDIRTY reports
  true with a 1-byte output, UNDIRTY clears it (ONDISK) and ISDIRTY then reports
  false, a Go WRITEFULL re-dirties it, and native observation confirms UNDIRTY
  left the object bytes unchanged.
