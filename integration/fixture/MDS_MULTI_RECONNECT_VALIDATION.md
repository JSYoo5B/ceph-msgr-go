# Bounded CephFS MULTI_RECONNECT acceptance

Reference: Ceph Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code remains native
Go with CGO disabled. This checkpoint declares CephFS client bit 12 only in
the CephFS profile; the session-only profile keeps it withheld. A reconnect-only
fragment budget is added without changing wire layout. The transport adds
no filesystem cache, inode allocator or automatic replay.

## Recipe and independent oracle

`TestCephMDSCephFSMultiReconnectIntegration` uses the closed
[completion fixture](MDS_CEPHFS_COMPLETION_FIXTURE.md) in isolation. It opens
32 real files with legal 225-byte names in the owned directory and retains
their actual capabilities and paths. The recorded reconnect-only `MDSReconnectState.MaxMessageSize` is
8 KiB; bootstrap, incoming frames and other messages retain the client's
1 MiB `MaxFrameSize` bound. The test
encodes the collected cap/realm state to require that one message would
exceed the reconnect message budget; no fake inode, path or lock data inflates state.

After the owned rank-0 restart, the same client GlobalID and shared nonce
restore caller-supplied caps and realms to the new exact MDS incarnation.
`Resubmit` and `PendingCapFlushes` are empty. Debug logs from that incarnation
must contain at least two `Server::handle_client_reconnect` lines attributed
to this GlobalID: every nonfinal line ends in `(more)` and the final one does
not. Logs with dropped lines fail. Eventual OPEN alone is insufficient.
The native handler logs establish receipt of each fragment; the existing
fresh-native reconnect vectors establish the v5/compat4 data encoding.

After OPEN, the replacement must become active and be explicitly adopted.
Native libcephfs listings before and after the restart compare each inode,
empty-file size and name; subsequent Go LOOKUPs must recover every same
inode. No OSD dial, object write or request replay is allowed, and reconnect
selects no pending dirty cap flush. Ordinary unsafe mutations still use
FlushJournal to await safe completion.
Cleanup unlinks all 32 files, removes the directory, releases caps and
requires a confirmed CLOSE and absence from native `session ls`. The fixture
separately restores debug state, owned blocklist entries and credentials,
then removes its filesystem and resources.

`TestCephMDSSessionsIntegration/admit_required_multi_reconnect_only_for_cephfs`
adds an owned `required_client_features=multi_reconnect` requirement only
when no native sessions exist. It requires a typed, raw-reason REJECT from
the session-only profile and OPEN from the CephFS profile, compares the
native `0x1c97ff` feature image, confirms close and restores the requirement.
The existing completion suite separately covers native reconnect denial
with CLOSE; split-fragment interruption is outside this checkpoint.

## Evidence boundary

Native split execution tested source
`6c0ab362a523bdf95a07b9bea7061d44b09bd809` on Linux/amd64 with image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Each profile passed all four selected leaves with no skips: 34 real caps,
one realm, 9,441 bytes before splitting and exactly two native fragments.
Both preserve GlobalID/shared nonce, generation 1 to 2, zero OSD dials,
no request resubmission and no pending cap flush. Both complete native
namespace/cap/close checks and semantic fixture/FS/pool/identity cleanup.

| Native profile | Runner UTC interval | Test duration | Public result bytes / SHA-256 |
| --- | --- | --- | --- |
| CRC, AES/AES, IPv4 host, one BlueStore OSD | 2026-10-23 08:49:20 to 08:51:55 | 43.79 s | 504343 / `f56bbb323d161ed853898b9f9b9dfd6e1746639bd92af6499dd4761a286bb7bc` |
| Secure, aes256k/aes256k, IPv6 container, one MemStore OSD | 2026-10-23 08:53:40 to 08:56:10 | 42.48 s | 504888 / `c4a26bb89d32edcf1cf05d8e890d254c5bf89c15deb7810bfbd7ab3bcdaece8b` |

Native handler order is `(more)` followed by the final handler without that
marker, from the same test GlobalID on the replacement incarnation. OPEN
and the native session image alone are not used as fragment evidence.
The later exchange-wide preflight guards change only invalid-input refusal;
they have separate focused Linux unit/race verification. Exact-head CI and
operator-required admission are separate from these completed split runs.
The PR's selected native checks must pass before accepting the declaration.
Successful detailed IPC receipts are not committed or uploaded by the
current successful-run workflow; the hashes and bounded summary here do not
replace the separate evidence-retention work under issue #1.

Connection loss between fragments, arbitrary gigabytes of state, held-lock
reconnect inside split reconnect state (a single held lock across the ordinary
restart is in [MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md)), dirty metadata ordering, multi-rank split reconnect, delegated
inodes and Unicode name policy remain unverified or withheld. This recipe
establishes no complete filesystem client or remote rollback behavior.

## Earlier failures and their boundaries

Each attempt used a new owned fixture. These observations are separate from
later successful interoperability and do not establish protocol defects:

| Source | Selected configuration and boundary | Outcome |
| --- | --- | --- |
| `a86b9dc1d17b6ff4f7e4a14a86d23082bd20bb17` | CRC/AES IPv4 host, global MaxFrameSize 8 KiB | Dial returned a length-limit error before an MDS session or any file mutation. The failing bootstrap frame was not independently identified. Both semantic cleanup receipts passed; nine owned containers and their network were absent afterward |
| `9504c365a9601af5f089e0ce0b8bf1b7d2fca61d` | Same selected profile, global MaxFrameSize 64 KiB | Same pre-MDS length-limit refusal; no split proof. Both semantic cleanup receipts passed, and nine containers/network were removed. A later pass does not identify the refused frame |
| `c92034656646678315918cf65ccf01a1f9229a3d` | 1 MiB transport, reconnect-only 64 KiB, 256 real files | File creation completed, but native listing exited 0 with stdout_truncated=true at the existing 64 KiB oracle bound. Reconnect was never attempted. Namespace readback was skipped while the Go session remained; semantic cleanup receipts were incomplete. All nine owned containers and the network were nevertheless independently absent afterward |

The same `6c0ab362a523bdf95a07b9bea7061d44b09bd809` CRC session suite
passed the new required-feature admission/refusal leaf and all other
lifecycle leaves, then failed its final dial audit because that audit still
expected 11 clients rather than the 13 now exercised. Semantic session and
filesystem/pool/identity cleanup passed. That run is a failed suite, not a
successful admission-profile acceptance. The audit is corrected to 13;
the secure admission profile was not executed in that batch. Final-SHA CI
checks cover both admission profiles independently.

The final bounded recipe retains the existing oracle output limits: 32 real
files, a reconnect-only 8 KiB budget and the normal 1 MiB transport bound.
This proves splitting with a smaller state, without claiming large-state
capacity or widening unrelated decoder/output limits. The successful public
result file is about 0.5 MiB, below its 8 MiB bound; the fixed oracle and
control receipts remain below the 1 MiB input bound. The event queue is
bounded at 1,024 and overflow is terminal; both selected runs require the
capability agent to report no errors and the fragment log to drop no lines.

A separate synthetic regression first reproduced duplicate inode admission
when the duplicates fell into different fragments. The fix validates the
whole sorted cap map before encoding; count bounds likewise apply across
the entire exchange, before clone/sort/encode. These are unit-tested input
refusals, not additional native recovery evidence.

## Source and licensing

Wire facts are independently restated from LGPL-2.1 sources:
`messages/MClientReconnect.h` (v5, data, counts, more),
`mds/Server.cc:1571-1745` (attributed `(more)` logs, per-fragment cap gathering,
OPEN and gather completion only for the final fragment),
`client/Client.cc:3316-3331` (native split policy) and
`mds/Server.cc:743-753` (required-feature admission). No upstream code is
incorporated. Native tools and libcephfs remain development dependencies.
