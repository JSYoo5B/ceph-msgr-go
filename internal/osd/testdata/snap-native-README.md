# Fresh Tentacle snapshot context, ROLLBACK and LIST_SNAPS codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The public `encode-osd-snap` SDK run compiled and linked the independently
written C++ wrapper against the official generated headers and the matching
libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects directly run
`encode_payload`; different native objects decode every emitted payload.
Decoded objects are never encoder inputs. No Go byte buffers, retained decoded
fronts, server traffic or authentication material supply the native encoding.
The library hash equals the mutation, xattr, OMAP read, OMAP header/write,
OMAP_CMP, OMAPRMKEYRANGE, CALL and SETALLOCHINT proofs; only the new snapshot
wrapper target was compiled and linked.

Eight triples have a 41-byte native MessageFrame header, front and data. The
first four are MOSDOp requests (type 42, compat 3) and the last four are
MOSDOpReply replies (type 43, compat 2):

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-read | 249/0 | READ 0~4 and STAT at clone snap 4; snap_seq 0, no snaps; READ\|ONDISK\|RETURNVEC; zero mtime |
| request-v6-write | 235/2 | WRITEFULL `v2` on the head (NOSNAP); snap_seq 7, snaps [7,4,2]; WRITE\|ORDERSNAP\|ONDISK\|RETURNVEC; mtime 1739999999 s + 5 ns |
| request-v6-rollback | 235/0 | ROLLBACK to snap 4 on the head (NOSNAP); snap_seq 7, snaps [7,4,2]; WRITE\|ONDISK\|RETURNVEC; mtime 1739999999 s + 5 ns |
| request-v6-list | 211/0 | LIST_SNAPS at SNAPDIR; snap_seq 0, no snaps; READ\|ONDISK\|RETURNVEC; zero mtime |
| reply-v6-list | 201/180 | ACK\|ONDISK; the listing below, aggregate and per-op result 0, versions 0'0 and user 20 |
| reply-v6-rollback | 201/0 | ACK\|ONDISK; ROLLBACK snap 4 echoed, no output, aggregate and per-op 0, versions 11'21 and user 21 |
| reply-v6-eoldsnapc | 201/0 | `reply_op_error` shape for request-v6-write: ONDISK only, aggregate -85 (EOLDSNAPC), per-op 0, zero versions |
| reply-v6-read | 243/20 | ACK\|ONDISK; READ output `v1v1` (extent length 4) and STAT size 4/mtime 1739990000 s + 3 ns, per-op 0/0, versions 0'0 and user 3 |

Files use the `snap-native-` prefix. Request incarnation is 1, TID 77, epoch
11, pool 7/`route-key`/`tenant`, raw hash `0x11223344`, object `snap-codec`
and retry attempt 0. Native `ceph_osd_op_mode_read`/`ceph_osd_op_mode_modify`
derive the READ or WRITE bit (READ, STAT and LIST_SNAPS are RD in `rados.h`
250, 251 and 267; WRITEFULL and ROLLBACK are WR in 274 and 290). ONDISK and
RETURNVEC are always requested, and ORDERSNAP (`rados.h` 460, `0x40`) is an
explicit flag of the write vector only, so the request flags are `0x4000014`
(read and list), `0x4000064` (write) and `0x4000024` (rollback). Only writing
vectors carry an mtime.

## Builders and request bytes

The wrapper uses the pinned public builders: `read(off, len, buffer::list*,
int*, Context*)` with null outputs (Objecter.h 497-505, `add_data` 176-181),
`stat(uint64_t*, nullptr, nullptr)` (427-431), `write_full(buffer::list&&)`
(587-589), `rollback(uint64_t)` (1515-1518) and `list_snaps(librados::snap_set_t*,
int*)` with null outputs (1482-1490). None of them adds a per-op flag. The
`rollback` builder calls `add_op(CEPH_OSD_OP_ROLLBACK)` and stores le64
`snap.snapid` (`rados.h` 610-612) at union offset 0, leaving the other 20
union bytes zero and payload_len 0. With every output null, `list_snaps` only
calls `add_op(CEPH_OSD_OP_LIST_SNAPS)`: an all-zero union and no input. The
request-level fields go through the public `set_snapid`, `set_snap_seq` and
`set_snaps` setters in the order `Objecter::_prepare_osd_op` uses
(Objecter.cc 3394-3396). librados requires the read snap CEPH_SNAPDIR before
LIST_SNAPS (librados_cxx.cc 2224-2238), and the OSD refuses LIST_SNAPS at any
other snap and any other opcode at SNAPDIR (PrimaryLogPG.cc 2321-2338).

The native request operations are:

- `0112` `00000000` `0000000000000000` `0400000000000000` `0000000000000000`
  `00000000` `00000000` (READ 0~4, payload_len 0)
- `0212` `00000000` followed by 28 zero union bytes and `00000000` (STAT)
- `0222` `00000000` with extent 0~2 and payload_len `02000000` (WRITEFULL);
  the request data is `7632`
- `0e22` `00000000` `0400000000000000` followed by 20 zero union bytes and
  `00000000` (ROLLBACK snap 4)
- `0a12` `00000000` followed by 28 zero union bytes and `00000000`
  (LIST_SNAPS)

After the ops the v6 front carries le64 hobj.snap, le64 snap_seq and the
snaps vector (MOSDOp.h 330-332), then retry attempt 0, the feature mask and
the default reqid:

- read: `0400000000000000` `0000000000000000` `00000000`
- write and rollback: `feffffffffffffff` `0700000000000000` `03000000`
  `0700000000000000` `0400000000000000` `0200000000000000`
- list: `ffffffffffffffff` `0000000000000000` `00000000`

The independent native decoder confirms each decoded snapid, snap_seq, snaps,
flags, mtime and TID against the encoder inputs.

## Replies

The public MOSDOpReply constructor (MOSDOpReply.h 143-166) keeps the request
mode bits and replaces only the ACK/ONDISK/ONNVRAM completion bits with its
acktype. Native `merge_osd_op_vector_out_data` derives every reply
payload_len and the data order. Each acktype and version follows the pinned
path for that case:

- reply-v6-list: LIST_SNAPS at SNAPDIR resolves to the head object context
  (PrimaryLogPG.cc 2317-2319). The handler encodes `obj_list_snap_response_t`
  with the head last (6571-6659); `complete_read_ctx` sets versions
  `eversion_t()` and the head user version (9209-9210) and adds ACK|ONDISK
  (9220). The decoded flags are `0x4000015` (ACK|ONDISK|READ|RETURNVEC), the
  legacy version is 0'20 and the user version 20. The output is the native
  `obj_list_snap_response_t::encode` (osd_types.h 6852-6900, version 2,
  compat 1) of clones `{2, [2], [(0,3)], 3}`, `{7, [4,7], [(0,2),(4,4)], 9}`
  and the head `{NOSNAP, [], [], 11}` with seq 7; each `clone_info` uses its
  own native encoder (6786-6846, version 1, compat 1). The 180 data bytes are
  `0201ae000000` `03000000`, then `010130000000` `0200000000000000`
  `01000000 0200000000000000` `01000000 0000000000000000 0300000000000000`
  `0300000000000000`, then `010148000000` `0700000000000000`
  `02000000 0400000000000000 0700000000000000`
  `02000000 0000000000000000 0200000000000000 0400000000000000 0400000000000000`
  `0900000000000000`, then `010118000000` `feffffffffffffff` `00000000`
  `00000000` `0b00000000000000`, and finally seq `0700000000000000`. The
  separate native decoder returns the same listing.
- reply-v6-rollback: ROLLBACK (6901-6905, `_rollback_to` from 8373) leaves
  the union unchanged and produces no output. The commit path runs
  `set_reply_versions(at_version, user_at_version)` (4325) and the on-commit
  callback adds ACK|ONDISK (4387). The decoded flags are `0x4000025`
  (ACK|ONDISK|WRITE|RETURNVEC) with versions 11'21 and user 21.
- reply-v6-eoldsnapc: with ORDERSNAP set and the snapc seq below the object
  snapset seq, `execute_ctx` calls `reply_ctx(ctx, -EOLDSNAPC)` (4223-4230,
  4428-4433). The wrapper repeats the public calls of
  `OSDService::reply_op_error(op, err)` (OSD.cc 1630-1649). The acktype is the
  request's own ACK|ONDISK bits, which is ONDISK alone because the request
  carries no ACK. Output is ignored only without RETURNVEC, versions are
  `eversion_t()`/0 and `op_returns` is empty. The constructor copies the
  request ops with input data cleared, so the WRITEFULL echo (extent 0~2,
  payload_len 0) keeps per-op result 0. The decoded flags are `0x4000064`
  (WRITE|ORDERSNAP|ONDISK|RETURNVEC, no ACK), the aggregate result is
  `abffffff` (-85) and every version field is zero. EOLDSNAPC is the
  Ceph-defined 85 from `rados.h` 498, not a host errno.
- reply-v6-read: `do_read` keeps extent length 4 when four bytes are read
  (5835-5929). READ at offset 0 sets `data_off` 0 (6142-6144), which
  `complete_read_ctx` copies into the header (9196). The STAT handler encodes
  object_info size then mtime (6270-6272), so the wrapper builds the STAT
  output with native `encode(uint64_t)` and `encode(utime_t)`: 16 bytes
  `0400000000000000` `f023b667` `03000000`. `complete_read_ctx` sets versions
  `eversion_t()` and the clone user version 3 (9209-9210) and adds ACK|ONDISK
  (9220). The decoded flags are `0x4000015`, the legacy version is 0'3, and
  the data is `76317631` followed by the STAT bytes.

The decoded union bytes are echoed unchanged in every reply: extent 0~4 for
READ, extent 0~2 for WRITEFULL, `0400000000000000` plus 20 zero bytes for
ROLLBACK and 28 zero bytes for STAT and LIST_SNAPS. Every reply ends with the
v6 redirect and 24-byte trace suffix.

Message::encode supplies the native envelope/footer after direct payload
encoding. Native MessageFrame uses explicit offline sequence 1, ACK 3 and
priority 127; these are not live ProtocolV2 session/ACK observations. Replies
contain constructed logical results. They do not establish live snapshot
creation, clone layout or overlap, rollback execution, snap trimming,
EOLDSNAPC detection, listing contents, caps or server limits, and this
directory makes no claim about live server behavior. Exact library, binary,
wrapper and driver hashes and the decoded operations and messages are in
`snap-native-provenance.json`. The owned builder was independently absent
after public termination. Runtime proof and log:
`/tmp/ceph-snap-native-encoder-attempt2-proof.json` and `.json.log`. The
first run produced byte-identical vectors; only source line citations in the
wrapper comments changed before the recorded second run.

```sh
cd integration/fixture
GOWORK=off go run . encode-osd-snap -project ../.. \
  -cache /private/tmp/ceph-native-encoder-cache \
  -output /tmp/ceph-snap-native-encoder-attempt2-proof.json
```

The runner supplies all nine original wrapper sources when sharing the SDK
cache; each new CMake target is guarded by source existence and the selected
driver builds only its own target. Its proof check treats the first four
vectors as requests. Ceph implementations remain unchanged. Native build
libraries are test tooling only. Source headers and library carry LGPL-2.1
notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently written
development wrapper copies no implementation, and no native dependency enters
the Go product.
