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

## Evidence boundary

Native execution tested source `688d619f7601d0f29e9299726ae79582bc59196a` on
a Linux amd64 host (Docker 29.9.1, fixture module built with Go 1.26.9) with
image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Each run used the parameters of its catalog row and the CI result gate
(`tools/native_test_results.py snapshot`, `integration/run.sh`, `verify`),
one `go test` process per Docker engine. These are local runs, not CI; the
detailed receipts are not committed.

| Row | Leaves | Test duration | Runner UTC interval | Public result bytes / SHA-256 |
| --- | ---: | ---: | --- | --- |
| `mds-cephfs-completion`, CRC, AES/AES, IPv4 host, one BlueStore OSD | 12 passed | 290.51 s | 2026-11-13 09:27:34 to 09:34:52 | 432523 / `810d5aae84057bc198fc8da357319474b4f00ad9f22370b590b3705ad93047d3` |
| `mds-cephfs-completion`, secure, aes256k/aes256k, IPv6 container, one MemStore OSD | 12 passed | 290.76 s | 2026-11-13 09:34:52 to 09:42:04 | 441545 / `9cb678c20c0000544ade2548d47088f1161acf04b4f81e5c6920f600de75e1ac` |
| `mds-sessions`, CRC profile as above | 14 passed | 206.22 s | 2026-11-13 09:42:04 to 09:47:36 | 96706 / `04dc111a90515551297c72b929995335dd14e4faf60c319cf5bb1586171fb534` |
| `mds-sessions`, secure profile as above | 14 passed | 204.68 s | 2026-11-13 09:47:36 to 09:53:10 | 97854 / `ffe5b936b4062cf3660ceafdbf3a44a8742aa16b945a4cf9954d39714c360ac4` |

The gate reported no problem and no selected test was skipped. Both
completion profiles observed the same values:

| Step | Extra blob | Decoded | Native `delegated_inos` after the reply |
| --- | --- | --- | --- |
| `deleg-plain`, async directory operations off | `eb03000000010000` | bare inode `0x100000003eb` | none |
| `deleg-grant`, defaults | `01011c000000ec03000000010000` `01000000` `ed03000000010000` `f401000000000000` | inode `0x100000003ec`, one interval `0x100000003ed` length 500 | `0x100000003ed` length 500 |
| `deleg-held`, defaults | `01010c000000e10500000001000000000000` | inode `0x100000005e1`, empty set | `0x100000003ed` length 500, unchanged |
| REPLAY of `replayed`, queued for replay | `01010c000000010000000001000000000000` | inode `0x10000000001`, empty set | not read |
| retried CREATE of `lost-file`, queued for replay | `01010c000000f60100000001000000000000` | inode `0x100000001f6`, empty set | not read |
| `deleg-prime`, incarnation started with percentage 0 | `01010c000000f20300000001000000000000` | inode `0x100000003f2`, empty set | none |
| `deleg-zero`, same incarnation | `01010c000000f30300000001000000000000` | inode `0x100000003f3`, empty set | none |

The created inode of `deleg-held` follows the 500 delegated inodes, which the
MDS no longer hands out itself. The central override was applied with value
`0`, the restarted incarnation reported `0`, and after removal the daemon's
`config get` reported the baseline `50` with no central entry left. The
session suite's admission leaf took 14.7 s and 14.6 s. A grant of several
intervals was also observed and decoded: the CREATE of
`TestCephMDSCephFSIntegration/create_reports_created_inode` returned three
ranges of 495, 2 and 3 inodes in both profiles; that test records them and
does not compare them with the native session.

The same source also passed every other row that opens a CephFS-profile
session, in both profiles, through the same gate: `mds-cephfs` (three
top-level tests, 26 leaves), `mds-cephfs-ranks`, `mds-cephfs-ranks-replay`,
`mds-cephfs-sharedflock-recovery`, `mds-cephfs-data`, `mds-cephfs-capsend`,
`mds-cephfs-writeback`, `mds-cephfs-multi-reconnect`,
`mds-cephfs-heldflock-split-reconnect`,
`mds-cephfs-heldflock-split-fragments` and the three `mds-commands` rows: 27
rows in all. After each batch no fixture container remained.

### Negative controls

Each control changed source `93bf55cbae169999120bb83fec135dfcdd9b7f27`
temporarily on the runner and ran the CRC row; none is committed. That
commit is an ancestor of the tested source; the tested source adds a merge of
the main branch, a fix of the test-only capability agent (below) and the
coverage map, and leaves the completion and session suites, their fixture
controls, the other CephFS tests named below and the feature declaration
unchanged.

| Change | Expected failure observed |
| --- | --- |
| The `deleg-zero-on` override sets 50 and the test accepts that readback | `delegation_percentage_zero` fails on `deleg-zero`: the reply carries a grant, so the empty set of the unmodified run is the percentage's effect. `deleg-prime`, a session's first create, is empty either way |
| The fixture reports each native range start one higher | `delegated_inode_reply_forms` fails on `deleg-grant`: decoded start `0x100000003ed` against the misreported `0x100000003ee` |
| Bit 13 withheld again | `admit_required_deleg_ino_only_for_cephfs` fails with the MDS REJECT `missing required features '0x0000000000002000'`; in `mds-cephfs` the create checks fail on bare 8-byte extras |

## Earlier failures and their boundaries

A first version of this recipe set `mds_client_delegate_inos_pct 0` through
the admin socket of the running MDS, as proposed in issue #11. The daemon
read the value back as `0`. When the create under that setting was a
session's first, the reply was empty and the leaf passed; once another
committed create preceded it, the reply carried a 500-inode grant. The empty
reply had been the first-request case (nothing preallocated yet), not the
percentage. The source explains it: the option is read at `Server`
construction and is not a tracked key (see "What the bit changes"). The
recipe now sets the option centrally before an owned restart. No product
defect was involved, and the unmodified runs above are not an explanation of
that failure; the source reading and the first negative control are.

In the same early runs a leaf that failed midway left its session open, and
the next leaf's MDS restart then waited its reconnect window for that
session, failing `lost_reply_and_completed_replay` with a resume deadline.
The delegation leaves now close their session when they fail.

One run of the secure `mds-cephfs-writeback` row on source
`93bf55cbae169999120bb83fec135dfcdd9b7f27` failed in
`snapshot_write_clones_objects` with `max_size 720000 did not happen on
0x100000001f7: issued pAsxLsXsxFsxcrwb max_size 0 size 600000`; the same row
had passed on the preceding revision and its CRC twin passed in the same
batch. The capability agent's records give the cause. After the snapshot the
test reopened the file, and the MDS sent the OPEN reply (cap sequence 4)
followed by its own GRANT of a larger client range (sequence 5). The
test-only agent handled the GRANT from the event queue before the test
applied the reply, then took `max_size 0` from that older reply, asked for a
range the MDS had already granted and waited for a GRANT the MDS does not
repeat. In the passing runs the reply was applied first (request at sequence
4, grant at 5). The path is an OPEN reply and a cap push; it carries no
created-inode extra and is not specific to this declaration. The agent now
keeps the tracked `max_size` when a reply's cap sequence is older than the
tracked one, and `TestCephFSCapAgentHelpers` reproduces the recorded state
(sequence 5, `max_size 0`) without that fix. The later pass of the row on
the tested source is not offered as the explanation.

## Not established

- Asynchronous directory operations, any use or release of a delegated
  inode, and the kernel client's recovery of async requests.
- The purge of a closed session's delegated range. Only the confirmed close
  and the session's absence from `session ls` were observed.
- What a restarted MDS holds for a session that had a grant; delegation
  across ranks or failover.
- A `deleg_ino` requirement combined with another required bit, and the
  deprecated `min_compat_client` setting that maps to bit 13.
- arm64, and CI on the tested source.

## Source and licensing

Wire facts are independently restated from LGPL-2.1 sources at the pinned
tag: `messages/MClientReply.h:304-334` (`openc_response_t`),
`mds/Server.cc:2558-2582` (`set_reply_extra_bl`), `2625-2648`, `2864-2872`
and `4948` (its callers), `287` and `1381-1405` (option handling),
`743-753` (required-feature admission), `mds/MDSRank.cc:4050-4145` (tracked
keys), `mds/SessionMap.h:202-221` and `mds/SessionMap.cc:665-671`
(delegation and its dump) and `client/Client.cc:2018-2034` (the userspace
client's handling). No upstream code is incorporated. Native tools and
libcephfs remain development dependencies.
