# Fresh native cache-tier operation request vectors

[The retained proof](cache-op-native-provenance.json) contains 21 fresh
MOSDOp requests from Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`: seven closed shapes across v6, v8
and untraced v9. Each request carries one operation built by the native
`ObjectOperation` helper (`osdc/Objecter.h` `cache_flush`, `cache_try_flush`,
`cache_evict`, `cache_pin`, `cache_unpin`). None has a union value, payload or
input bytes.

| Shape | Opcode | Request flags beyond ONDISK and RETURNVEC | Mutation mtime |
| --- | --- | --- | --- |
| `flush` | CACHE_FLUSH `0x821f` | READ, IGNORE_CACHE, IGNORE_OVERLAY | none |
| `try-flush` | CACHE_TRY_FLUSH `0x8221` | READ, IGNORE_CACHE, IGNORE_OVERLAY, SKIPRWLOCKS | none |
| `evict` | CACHE_EVICT `0x8220` | READ, IGNORE_CACHE, IGNORE_OVERLAY, SKIPRWLOCKS | none |
| `pin`, `unpin` | CACHE_PIN `0x2224`, CACHE_UNPIN `0x2225` | WRITE | 1700000000.123456789 |
| `pin-ignore-cache`, `unpin-ignore-cache` | as above | WRITE, IGNORE_CACHE | 1700000000.123456789 |

The flags follow the native callers at the pinned commit. The rados CLI
(`tools/rados/rados.cc` `do_cache_flush`, `do_cache_try_flush`,
`do_cache_evict`) submits ObjectReadOperations with
`OPERATION_IGNORE_CACHE | OPERATION_IGNORE_OVERLAY` and adds
`OPERATION_SKIPRWLOCKS` for try-flush and evict. `librados_util.cc`
`translate_flags` maps those to wire flags `0x8000`, `0x20000` and `0x10000`,
and `Objecter::get_read_flags` adds READ. Pin and unpin are
ObjectWriteOperations; `Objecter::prepare_mutate_op` adds WRITE. The CLI has
no pin or unpin command, so both the plain form and the IGNORE_CACHE form are
retained. ONDISK and RETURNVEC are not set by the native read path; they are
the fixed header flags of this repository's encoder and are set explicitly so
that a later Go encoding can be compared byte for byte.

Three flag layers stay distinct. The OSD handler checks the lock mode that
SKIPRWLOCKS selects (`PrimaryLogPG.cc`: try-flush without it and flush with
it return EINVAL). IGNORE_OVERLAY is read by the native Objecter when it
chooses a target (`Objecter.cc` `_calc_target`); the pinned OSD request path
does not consult it. IGNORE_CACHE bypasses the tier's promotion path. These
vectors establish request bytes only: no tier state, dirty or pin flag, lock
mode, flush completion or eviction result.

Each vector uses a fresh native ObjectOperation and MOSDOp. The wrapper checks
that the builder produced exactly one operation without input and with the
expected mode bit, calls canonical `Message::encode` once, and decodes into a
separate fresh object without re-encoding it. The Python driver compares the
decoded request with its own independent model of opcodes, flags and mtimes,
and locates the packed operation exactly once in the front. Request identity,
locator, PG seeds, the modern trace envelopes and the 41-byte offline
Messenger v2 header follow the existing request-vector convention (sequence 1,
TID 77, ACK sequence 3, priority 127). Cache-pool feature bit 35 is a session
capability and is absent from these encoding inputs.

## Generation

The builder reuses the unchanged MPing SDK and IgnoreCache provenance helpers;
hashes bind the new wrapper and driver, the shared wrapper and drivers, the
official archive, pinned submodules, unchanged codec sources, generated
configuration, compiler, library and executable.

The vectors were generated on 2026-10-22 by a disposable GitHub Actions job on
a GitHub-hosted `ubuntu-latest` runner
([run 39848093345](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39848093345),
scratch branch `claude/native-vectors-cache-op`, not merged), with the same
fresh-container procedure as the
[SPARSE_READ/READ compound vectors](sparse-read-compound-native-README.md). Its
first and only attempt ran from 20:21:35 to 20:33:07 UTC, exited zero, and its
container was absent after removal; the proof's `execution` object and
`build_log_sha256` record it. Every retained hexadecimal segment was compared
with the raw files of the run artifact before this proof was committed.

An earlier run on the same branch
([39846819636](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39846819636))
failed before producing a complete set: the wrapper compared the integer
result of `ceph_osd_op_mode_modify` directly with a boolean, so the pin shapes
were refused by the wrapper's own check. The retained wrapper compares the mode
bit as a boolean; that earlier partial output is not used.

## Current Go status

The Go encoder does not implement these opcodes. The Go test checks the
retained segments, flags, mtime presence and packed operation, and requires
`EncodeRequest` to keep refusing every cache opcode until a reviewed admission
stage replaces that check with byte parity:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestCacheOperationsRetain21FreshNativeRequests$' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Native tools
remain development-only; only wire facts and provenance enter the pure-Go
product repository.
