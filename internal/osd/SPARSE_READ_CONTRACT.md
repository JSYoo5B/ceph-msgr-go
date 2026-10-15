# Bounded SPARSE_READ

Pinned reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The Go implementation is independently
written from LGPL-2.1 `src/include/rados.h:257`,
`src/osdc/Objecter.h:516-568,618-621`, and
`src/osd/PrimaryLogPG.cc:5931-6024`; no implementation is copied.

`OSDSparseRead` reads a caller-selected nonempty interval on a caller-targeted
replicated-pool head object. The operation must be standalone, with no flags,
write context or snapshot selection. Offset plus length must not overflow, and
length must fit the configured message bound. EC, compound sequencing and
snapshot sparse reads require separate characterization and are unsupported.
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
`IoCtx::sparse_read` for the exact same offset/length. Its native return value
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
