# Bounded SPARSE_READ

Pinned reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The Go implementation is independently
written from LGPL-2.1 `src/include/rados.h:257`,
`src/osdc/Objecter.h:516-568,618-621`, and
`src/osd/PrimaryLogPG.cc:5931-6024`; no implementation is copied.

`OSDSparseRead` reads a caller-selected nonempty interval on a caller-targeted
replicated-pool object, the head or a read snapshot (`ReadSnap`). Outside the
[bounded compound subset](#bounded-sparse_readread-compounds) the operation is
standalone. Only fadvise cache hints are accepted as operation flags, and
there is no write context. Offset plus
length must not overflow, and length must fit the configured message bound.
At a snapshot the OSD resolves the object context for that snap id and runs
the same handler on the resolved object (`oi.soid`, PrimaryLogPG.cc:5935): the
head when the snap id is above the object's snapset sequence
(PrimaryLogPG.cc:12169), otherwise the first clone at or above the snap id,
or -ENOENT when there is none (12187-12194), as for READ. The resolved
object's size bounds the read and its store allocation map gives the
extents. librbd sends this form for whole-object parent reads at the parent
snapshot (observed in native traces) and, per source, for clone-snapshot
reads. EC requires separate characterization and is unsupported;
compounds are limited to the subset below.
The caller still supplies placement, map epoch and connection identity.

The request uses the ordinary offset/length extent union and no input bytes.
`OSDSparseReadInput.MaxOutputBytes` is a local budget for the complete encoded
reply. Its minimum is eight bytes: a uint32 extent count followed by zero or
more uint64 offset/length pairs, then a uint32 byte count and the concatenated
extent bytes. Each extent must be nonempty, ordered, nonoverlapping and within
the submitted interval. The sum of extent lengths must exactly match the byte
count. The returned operation Length is that byte count, which may be smaller
than the selected logical range. Both the original encoded operation Data and
decoded `SparseRead.Extents/Data` remain available. Holes contribute no bytes;
consumers may reconstruct a dense buffer if their own use requires it.

The pinned replicated OSD clips the selected length at EOF, asks the backing
store for allocated ranges and reads those ranges. EOF returns an encoded empty
map and empty byte buffer and operation Length zero. MemStore reports the
selected logical range including zero-filled holes; BlueStore reports its own
allocation map. Neither is a claim about physical disk extents. A failed
operation retains its original extent union and has no output. The local budget
does not cap server work or truncate output. Over-budget output invalidates only
the affected session and retains received raw metadata and submission identity.

`tools/osd_sparse_read_native_encoder.cc` constructs five native request/reply
vectors and separately decodes the map, buffer and native envelopes. These prove
wire compatibility when regenerated and compared; their replies are constructed
values rather than observations of OSD execution.

`tools/osd_sparse_read_oracle.cc` independently calls public C++
`IoCtx::sparse_read` for the exact same offset/length, after
`IoCtx::snap_set_read` when a snap id is given. Its native return value
is the number of extents, unlike the wire operation result zero on success.
`tools/osd_sparse_read_oracle.py` restricts reads to fixture-owned object names,
checks the pinned daemon identity, bounds input/output and retains independent
C librados observations of bytes, nanosecond mtime, xattrs and user version
before/after. It never mutates, retries, routes, or uses the Go codec.

`integration/osd_sparse_read_test.go` covers dense and sparse ranges, holes,
EOF, past EOF, ENOENT, raw map/byte encoding, exact native range results,
read-only object preservation and local-budget session isolation. It writes
`osd-sparse-read-results.json`. The five fresh native vectors and both selected daemon profiles passed: CRC/AES
host IPv4 with three BlueStore replicas, and secure/AES256K IPv6 CGO=0 container
with one MemStore. Each ran all ten cases, including an exact same-range native
C++ comparison; see [acceptance](../../integration/fixture/OSD_O3_VALIDATION.md). Product builds have no native dependencies.
`integration/osd_snap_sparse_read_test.go` compares sixteen snapshot cases
(a clone with holes, ranges inside and past the clone's EOF, snap ids that
resolve to the head, an object created after the snapshot, the clone of a
deleted head) with the native C++ client on CRC/AES host IPv4 three BlueStore
replicas and secure/AES256K IPv6 container single MemStore; result code,
extents, bytes and user version matched in every case. See
[the snapshot record](../../integration/fixture/OSD_SNAPSHOT_VALIDATION.md).

## Optional independent native client staging

Ordinary product builds, codec tests and routine CI do not build or require the
independent C++ client helper. `TestCephOSDSparseReadIntegration` explicitly skips
unless `CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE=1` is set. An environment without that
helper has not run this particular independent comparison; it is not evidence
of a failure of the raw Go SPARSE_READ implementation.

For a native comparison run, `tools/build_sparse_read_oracle.py` accepts the
pinned Ceph source checkout, its official generated build directory, this
repository's C++ helper, an explicit output path and the matching fixture
image's installed `librados.so.2`. Build only in the owned native-development
container. The script uses GCC toolset 14, links the image library, records the
compiler command, dependency paths/hashes and loader audit, and writes a
`<binary>.proof.json` receipt. Its provenance check uses the checkout's recorded
commit or Git HEAD; it does not independently authenticate a downloaded archive.
This is a native client oracle, never a product build or runtime dependency.

The fixture launcher accepts `CEPH_MSGR_TEST_SPARSE_READ_ORACLE` as the host path
to the binary beside its `<binary>.proof.json`. It verifies the pinned image,
source, wrapper and binary hashes, stages the executable as `osd_sparse_read_oracle`,
retains proof, and injects only the opt-in marker into the test process. The Python observer executes
`out/osd_sparse_read_oracle`; configuration/keyring files remain in the fixture
root. Setting the marker without a valid staged matching binary fails the test
instead of silently falling back to a Go-derived or modeled oracle.

## Bounded SPARSE_READ/READ compounds

The native caller is the librbd object read
(`librbd/io/ObjectRequest.cc:238-246` at the pinned commit). It builds one
read operation per image extent, choosing SPARSE_READ at or above
`sparse_read_threshold_bytes` and READ below it, then applies one fadvise hint
to the last operation (`librbd/io/Utils.cc:28-44`,
`neorados/RADOS.cc:449-452`). The admitted subset follows that shape.

A vector with at least two operations and at least one SPARSE_READ admits
only SPARSE_READ and READ, up to the ordinary 64-operation bound. Each
operation may carry fadvise hints and nothing else. FAILOK, EXCL, unknown
flags, extent truncate fields, input bytes and every other opcode (including
STAT, ASSERT_VER, CMPEXT, xattr/OMAP reads, CHECKSUM, MAPEXT, CALL and all
mutations) are refused before a transaction ID is allocated. The request is
head-only (`ReadSnap` zero or `NoSnap`) and explicitly targets the replicated
primary. Snapshot selection, balanced/localized replica reads, IgnoreCache,
EnforceSnapContext, explicit resubmission, erasure placement and a v8/v9 shard
other than `NO_SHARD` are refused for this subset. Ordering, lock-bypass and
redirect request flags keep their ordinary validators. Standalone SPARSE_READ,
including its snapshot form, is unchanged. CHECKSUM compounds keep their own
subset, so a vector containing both CHECKSUM and SPARSE_READ is refused.

Both pinned handlers read `ctx->new_obs.oi` (`PrimaryLogPG.cc:5835-6024`).
Because no admitted operation stages a write, that object state is the
committed state for every operation in the vector. The staged-versus-committed
divergence that keeps MAPEXT, metadata reads after writes and full-object
digest reads after writes refused cannot arise in this subset. Each operation
is evaluated on the same object context in order and sees the same size,
truncate sequence and data digest.

Each SPARSE_READ keeps its own `MaxOutputBytes`, and each READ is bounded by
its own length. Their sum, and separately the sum of all logical extent
lengths, must fit the local frame limit, so many small output budgets cannot
multiply server read work beyond one frame. The enclosing reply budget adds
every per-operation output bound. Each reply operation consumes exactly its
`payload_len` bytes of reply data, and SPARSE_READ output must still decode
to an in-range, ordered extent map whose byte count matches its returned
length.

Tentacle stops at the first negative operation and keeps the output of earlier
operations (`PrimaryLogPG.cc:8205-8213`). Because FAILOK is refused, a
negative aggregate must equal the first negative per-op result. Earlier
operations validate as successes. A failed SPARSE_READ returns its original
union and no output, because the handler returns before encoding. A failed
READ has a zero length and may keep bytes read before a digest failure, bounded
by its request. Later operations must keep result zero, no output and their
original union. A missing object fails before any operation runs, with
all-zero per-op results and no output. That ambiguity is preserved: no prefix
is attributed and no failed operation is synthesized. Malformed or
contradictory replies keep raw front/data and fail the session through the
existing uncertain-outcome path.

Decoded `SparseRead` results of a compound appear only after the complete
reply validates, and on a negative aggregate only for the completed prefix.
Raw `Data` and `RawData` stay available. The native C++ convenience callback
differs from the raw wire: `CB_ObjectOperation_sparse_read` turns a zero
result with an empty buffer into -EIO (`osdc/Objecter.h:526-548`). A native
comparison must preserve that boundary rather than reading it as a raw rval.

### Evidence for the compound subset

These kinds of evidence are distinct:

- Native request codec: the [15-request fresh native proof](testdata/sparse-read-compound-native-README.md)
  matches complete v6/v8/v9 request segments for five layouts, including
  per-operation fadvise flags. It contains no replies and no execution.
- Unit tests: `TestSparseReadCompound*` in this package cover admitted and
  refused shapes across the three encoders, aggregate budgets, request scope
  and reply prefix/terminal/suffix/ambiguity rules. `TestOSDSparseReadCompound*`
  in `cephmsgr` cover public refusal before admission, prefix decoding on all
  four profiles and session failure on malformed replies, against synthetic
  transcripts rather than native bytes.
- Live daemon: `integration/osd_sparse_read_compound_test.go` compares each
  operation of five Go compounds with independent native readback (the
  pinned `rados mapext` CLI's full allocation map plus C librados stat and
  dense read bytes). It also checks version and unchanged state,
  missing-object ambiguity, local refusal before transmission and
  output-budget session isolation. The native client does not execute the
  compound or a partial SPARSE_READ; per-op extents are the intersection of
  the native full map with each clipped range. [The validation record](../../integration/fixture/OSD_SPARSE_READ_COMPOUND_VALIDATION.md)
  lists the configurations actually run.

Snapshot compounds, replica reads, EC, MAPEXT compounds, compounds with
STAT/ASSERT_VER/CMPEXT/xattr guards, mid-vector daemon errors and faults remain
unverified and refused.
