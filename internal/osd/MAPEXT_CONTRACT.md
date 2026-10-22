# Bounded MAPEXT

Reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
The implementation is independently written from the LGPL-2.1 sources below;
no upstream implementation is copied into the Go product.

- `src/include/rados.h:252`: `MAPEXT` is read/data opcode `0x1203`.
- `src/osdc/Objecter.h:614-617,3404-3422`: the ordinary extent union carries
  offset/length, zero truncate metadata, and no input payload.
- `src/osd/PrimaryLogPG.cc:6168-6196`: the OSD rejects EC, invokes the store's
  `fiemap` and returns its encoded logical extent map. It does not rewrite the
  requested offset/length to the output length.
- `src/os/memstore/MemStore.cc:335-368`: MemStore reports one logical range
  intersected with object size; this is not evidence that zero-filled ranges
  occupy individual physical extents.
- `src/os/bluestore/BlueStore.cc:13167-13266`: BlueStore derives logical allocated
  ranges from its extent map, clips them to the requested interval and object
  size, and encodes the interval set as an ordinary map.
- `src/tools/rados/rados.cc:2585-2606`: the independent CLI calls C++
  `IoCtx::mapext` with offset zero and length `UINT64_MAX` and prints hexadecimal
  offset/length pairs.

## Product behavior

`OSDMapExt` is a standalone read at the caller's `ReadSnapshot` (zero
selects the head), with no operation flags or write SnapContext. Callers supply
an explicit target, placement hash and map epoch through the existing OSD API.
`Offset/Length` select a nonempty interval whose end does not overflow uint64.
A zero length is refused: the pinned backends differ in how they encode this
case, and it is not an unbounded read request.

`OSDMapExtInput.MaxOutputBytes` is a local budget, at least four bytes for the
extent-count prefix. It has no wire field and does not ask the server to stop,
page, truncate or avoid generating a larger reply. The aggregate reply frame
budget includes this output. Output over budget invalidates the affected OSD
session while retaining submitted identity and received raw reply metadata.

Successful output is an ordered `map<uint64_t,uint64_t>`: a uint32 entry count,
then little-endian uint64 offset/length pairs. The decoder bounds allocation by
the encoded bytes and rejects empty, overflowing, out-of-order, duplicate or
overlapping ranges and trailing bytes. Adjacent ranges remain distinct. Reply
validation requires every range to lie within the submitted interval. Both raw
bytes and ordered `OSDExtent` metadata are exposed; no physical mapping or object
routing is performed. A failed operation must return no extent-map output.

Snapshot MAPEXT resolves the selected clone/head on the daemon, without
local snapshot discovery. Compound MAPEXT remains unsupported pending
separate characterization of committed versus staged store state. The pinned
native clients send MAPEXT alone (`librados/IoCtxImpl.cc:1408-1437` through
`Objecter::mapext`, and the `rados mapext` CLI); neorados has no MAPEXT
builder. The [SPARSE_READ/READ compound subset](SPARSE_READ_CONTRACT.md#bounded-sparse_readread-compounds)
therefore does not admit MAPEXT. EC remains
unsupported. Allocation granularity and physical disk usage are outside this
wire contract.

## Verification assets and evidence scope

`tools/osd_mapext_native_encoder.cc` constructs five fresh native messages:
one MOSDOp request and populated/empty/ENOENT/EACCES MOSDOpReply outputs.
Its map and message decoders separately check the native objects. Encoded bytes
and native header projections are compared by `internal/osd/mapext_test.go`.
These constructed replies establish wire interoperability, not OSD execution.

`tools/osd_mapext_oracle.py` observes only UUID objects in the disposable native
fixture ownership receipt. It invokes the pinned `rados mapext` CLI and retains
native stat/read bytes, mtime and version before/after. Full-range comparisons
(offset zero, length `UINT64_MAX`) execute the identical independent native
operation. For partial/EOF cases it intersects the independently observed full
map with the selected interval and explicitly records
`native_same_range=false`: the CLI did not execute a native partial request.

`integration/osd_mapext_test.go` compares full dense/sparse maps, partial ranges,
EOF, beyond-EOF and missing-object results, raw map bytes, user versions,
read-only state preservation and per-connection output-budget failure. The
fixtures select actual native placement; product code never calculates it.
The test records `osd-mapext-results.json`. Until a particular fixture run and
native encoder result are retained, these files are verification infrastructure,
not a claim that that profile has passed. Adaptation tests and codec tests do
not prove live daemon or physical allocation behavior.

The first combined native fixture run passed all seven map/range/error cases.
Its final budget-isolation assertion failed because the test opened the same
OSD twice on one client: `OpenOSD` deliberately reused the existing handle, so
the budget failure closed the very session used for the subsequent observation.
This was a test setup error, not evidence that an independent session failed.
The test now creates a separate authenticated client for the limited request;
the corrected eight-case suite passed on CRC/AES IPv4 host-relay three-replica
BlueStore and secure/AES256K IPv6 CGO=0 Linux-container single MemStore.
[Exact runs and limits](../../integration/fixture/OSD_READ_METADATA_VALIDATION.md)
retain the initial setup failure separately from the subsequent native evidence.

## Selected-snapshot and exact-range follow-up (2026-10-16)

The [snapshot acceptance](../../integration/fixture/OSD_SNAPSHOT_MAPEXT_VALIDATION.md)
adds a separate opt-in C++ `IoCtx::mapext` oracle for the exact requested
snapshot, offset and length. It preserves the API's extent count separately
from the normalized aggregate result. The C++ convenience call does not
update `get_last_version`; version evidence comes from an independent
selected-snapshot STAT2. The existing CLI/intersection evidence above is
preserved with its original scope. No fresh native combination vectors are
claimed by the focused snapshot field tests.
