# Fresh native SET_CHUNK request vectors

[The retained proof](set-chunk-native-provenance.json) contains 12 fresh MOSDOp
requests from Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`:
four closed shapes across v6, v8 and untraced v9. Each request carries one
SET_CHUNK operation built by the native `ObjectOperation::set_chunk` helper
(`osdc/Objecter.h`): a zero packed union, the per-operation flag argument, and
indata u64 src_offset, u64 src_length, `object_locator_t` of the target, the
`object_t` target name and u64 tgt_offset.

| Shape | src_offset, src_length | Target locator (pool, key, namespace), object, tgt_offset | Per-op flags | Go client |
| --- | --- | --- | --- | --- |
| `chunk` | 0, 8 | (9, "", ""), `set-chunk-target`, 0 | WITH_REFERENCE `0x80` | encoded, byte-identical |
| `chunk-located` | 0, 4096 | (7, `chunk-key`, `tenant`), `set-chunk-target-2`, 65536 | WITH_REFERENCE `0x80` | encoded, byte-identical |
| `nonzero-source` | 16, 8 | (9, "", ""), `set-chunk-target`, 8 | WITH_REFERENCE `0x80` | refused (`ErrOperation`) |
| `without-reference` | 0, 8 | (9, "", ""), `set-chunk-target`, 0 | none | refused (`ErrOperation`) |

Every request carries READ beside this repository's fixed ONDISK and
RETURNVEC header flags, no mtime, and the default implemented features. The
flags follow the native caller at the pinned commit: `rados set-chunk`
(`tools/rados/rados.cc`) submits an `ObjectReadOperation` through
`IoCtx::operate` with no request flag and passes
`CEPH_OSD_OP_FLAG_WITH_REFERENCE` as the operation flag, and
`Objecter::get_read_flags` adds READ. ONDISK and RETURNVEC are not set by the
native read path; they are set explicitly so that the Go encoding can be
compared byte for byte. Unlike the cache-tier operation vectors, these carry no
OSD_CACHEPOOL bit (35): the OSD checks it only for pools in a tier relation,
and SET_CHUNK refuses a tier source pool.

The `nonzero-source` and `without-reference` shapes are retained as codec facts
for requests the Go client refuses before transmission: a non-zero source
offset because of the failed-increment hazard in
[MANIFEST_CONTRACT.md](../MANIFEST_CONTRACT.md#go-set_chunk-contract), and a
missing WITH_REFERENCE because native callers always send it (the OSD refuses
its absence with EOPNOTSUPP). These vectors establish request bytes only: no
manifest entry, target reference, target existence or server result.

Each vector uses a fresh native ObjectOperation and MOSDOp. The wrapper checks
that the builder produced exactly one CACHE-mode SET_CHUNK operation with the
expected per-operation flag and no payload length before encoding, calls
canonical `Message::encode` once, and decodes into a separate fresh object
without re-encoding it. It then decodes the received indata with the native
decoders in the order the pinned `PrimaryLogPG` handler reads it, rejects
trailing bytes, and emits the decoded fields. The Python driver compares the
decoded request with its own independent model of the flags, mtime, packed
operation and the byte-level indata layout, and requires the data segment to
equal that modeled indata. Request identity, locator, PG seeds, the modern
trace envelopes and the 41-byte offline Messenger v2 header follow the existing
request-vector convention (sequence 1, TID 77, ACK sequence 3, priority 127).

## Generation

The builder reuses the unchanged MPing SDK and IgnoreCache provenance helpers;
hashes bind the new wrapper and driver, the shared wrapper and drivers, the
official archive, pinned submodules, unchanged codec sources, generated
configuration, compiler, library and executable.

The vectors were generated on 2026-10-23 by a disposable GitHub Actions job on
a GitHub-hosted `ubuntu-latest` runner
([run 39900953507](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39900953507),
scratch branch `claude/native-vectors-set-chunk`, not merged), with the same
fresh-container procedure as the
[cache-tier operation vectors](cache-op-native-README.md). Its first and only
attempt ran from 04:34:23 to 04:45:09 UTC, exited zero, and its container was
absent after removal; the proof's `execution` object and `build_log_sha256`
record it. Every retained hexadecimal segment and decoded object was compared
with the raw files of the run artifact before this proof was committed, and
the proof's wrapper and driver hashes equal the committed
`tools/osd_set_chunk_native_encoder.{cc,py}`.

## Go comparison

For each admitted vector, the Go request with the matching `SetChunkInput` and
`OpWithReference` equals the native header, front, middle and data; for the two
refused shapes the Go encoder returns `ErrOperation`:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestSetChunk' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Native tools
remain development-only; only wire facts and provenance enter the pure-Go
product repository.
