# CephFS DELEG_INO decode-only acceptance

Reference: Ceph Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code remains native Go
with CGO disabled. This checkpoint declares CephFS client bit 13 only in the
CephFS profile; the session-only profile keeps it withheld. The declaration
selects a reply form and satisfies a filesystem requirement. It adds no
asynchronous request, inode allocator, filesystem cache or automatic replay:
`Submit` still refuses the ASYNC flag and never presets an inode number.

## What the bit changes

`Server::set_reply_extra_bl` fills the extra blob of every reply that reports
a created inode: CREATE, a CREATE found in the completed-request record, and
the traceless `already completed` reply of any inode-creating request. For a
session that declared DELEG_INO it chooses per reply:

| MDS state | Extra blob |
| --- | --- |
| `mds_allow_async_dirops` true (default) and a delegation is due | `openc_response_t` with one or more (start, length) ranges |
| `mds_allow_async_dirops` true, and the delegation percentage is 0, or the request was queued for replay, or the session already holds at least half of its share | `openc_response_t` with an empty set (18 bytes) |
| `mds_allow_async_dirops` false | the bare created inode (8 bytes) |

The two options differ in when they apply. `mds_allow_async_dirops` is among
the keys the MDS tracks at runtime (`mds/MDSRank.cc:4063`,
`mds/Server.cc:1381-1383`). `mds_client_delegate_inos_pct` is not: `Server`
reads it when it is constructed (`mds/Server.cc:287`) and its branch in
`Server::handle_conf_change` (`mds/Server.cc:1403-1405`) is never reached,
because the name is missing from `MDSRankDispatcher::get_tracked_keys`
(`mds/MDSRank.cc:4050-4145`). A running MDS therefore keeps the percentage it
started with, whatever `config get` reports afterwards; see "Earlier
failures".

The same request can therefore be answered with different extra bytes naming
the same inode. `MDSReply.DecodeCreateExtra` reads all three; the two sizes
cannot be confused because the envelope form is at least 18 bytes.

## Native codec vectors

Both registered `openc_response_t` test instances of the release were
regenerated with the pinned image and a separate decoder invocation dumped
each exported file:

```sh
ceph-dencoder type openc_response_t select_test N encode export vector.bin
ceph-dencoder type openc_response_t import vector.bin decode dump_json
```

| N | Native dump | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| 1 | created inode 0, `[]` | 18 | `15cc20e68e73692f95f371dab7910dd335345a094df8130114300c42222772d3` |
| 2 | created inode 1, `[0x1~0xa]` | 34 | `697295475a3854e8c85f8afa2e3539e5d7d60190cecb644e2d04d832d5e789b3` |

They are committed as `internal/mds/testdata/mds-openc-native-*.extra.hex`
with `mds-openc-native-provenance.json` and bound by
`TestDecodeCreateExtraMatchesFreshNative`, which also compares the decoded
value with the native dump and refuses every proper prefix. The reader
container ran without a network, was removed and its absence checked. This is
codec evidence: no daemon produced these bytes. Truncated pairs, an interval
count above the bound, empty intervals, intervals that do not fit in 64 bits
and unordered or overlapping intervals are refused by
`TestDecodeCreateExtraRejectsMalformedInput` with hand-built inputs.

## Live recipe and independent oracle

Four added leaves of `TestCephMDSCephFSCompletionIntegration` use the closed
[completion fixture](MDS_CEPHFS_COMPLETION_FIXTURE.md). Each file's inode in
the extra must equal the inode of the reply trace and of an independent
native LibCephFS `statx` (mode 0100644) and differ from every inode created
before. After each create the native `session ls` entry of the session is
read for its `delegated_inos`.

`delegated_inode_reply_forms` opens a new client of the owned identity. Its
native session shows the declared `0x1cb7ff` image and no `delegated_inos`,
and an ASYNC create is refused locally before anything is sent. It then
creates three files:

1. `deleg-plain` with `mds_allow_async_dirops false` (admin socket, read
   back, restored to the recorded baseline afterwards): the extra must be
   exactly 8 bytes and the native session must report no delegated inode.
   Committing this create preallocates the session's inodes.
2. `deleg-grant` with the defaults: the extra must be the delegated form with
   a positive interval count, its length must be 18 + 16 bytes per interval,
   the created inode must lie outside every granted range, and the native
   session's `delegated_inos` must equal the decoded ranges pair by pair. A
   reply without a range fails the leaf; a grant is claimed only when observed.
3. `deleg-held` with the defaults: the session now holds its share, so the
   extra must be the 18-byte delegated form with no range while the native
   session still reports exactly the ranges of step 2.

The session releases its caps and closes; the close must be confirmed and
the session absent from native `session ls`.

`delegation_percentage_zero_armed` sets `mds_client_delegate_inos_pct 0` as a
central override of the owned daemon's section just before the suite's last
owned restart (the one of `force_readonly`). `delegation_percentage_zero`
runs in the incarnation that restart brought up, after the existing leaves.
That incarnation (a different GID) must report
`mds_client_delegate_inos_pct 0` and `mds_allow_async_dirops true`. A fresh session repeats the pattern that
produced the grant above, a committed create (`deleg-prime`) followed by
another (`deleg-zero`): both extras must be the 18-byte delegated form with
no range and the native session must hold no delegated inode.
`delegation_percentage_restored` then removes the override and requires no
central entry to remain. The running daemon keeps its percentage until it
stops with the fixture.

The existing leaf `lost_reply_and_completed_replay` covers the completed
forms with the bit declared. After the owned MDS restart, the REPLAY of a
journaled CREATE is answered traceless from the completed-request record and
the retried CREATE as a CREATE with its trace. Both must carry the delegated
form with an empty set (the MDS delegates nothing with a request queued for
replay; this incarnation runs with the default percentage) and the inode
recorded before the restart, which native `readdir`
with `statx` reports under the same names.

`TestCephMDSSessionsIntegration/admit_required_deleg_ino_only_for_cephfs`
adds an owned `required_client_features=deleg_ino` requirement only when no
native session exists. It requires a typed, raw-reason REJECT from the
session-only profile and OPEN from the CephFS profile, compares the native
`0x1cb7ff` feature image, requires that admission alone delegated nothing,
confirms close and restores the requirement to the recorded baseline.

Every other CephFS acceptance that creates a file now reads the created inode
through the same decoder and requires the delegated form
(`cephfsCreatedIno` in `integration/mds_cephfs_create_extra_test.go`); none
of them changes `mds_allow_async_dirops`.
