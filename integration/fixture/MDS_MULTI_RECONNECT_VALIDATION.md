# Bounded CephFS MULTI_RECONNECT acceptance

Reference: Ceph Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code remains native
Go with CGO disabled. This checkpoint declares CephFS client bit 12 only in
the CephFS profile; the session-only profile keeps it withheld. No additional
wire layout, filesystem cache, inode allocator or automatic replay is added.

## Recipe and independent oracle

`TestCephMDSCephFSMultiReconnectIntegration` uses the closed
[completion fixture](MDS_CEPHFS_COMPLETION_FIXTURE.md) in isolation. It opens
32 real files with legal 225-byte names in the owned directory and retains
their actual capabilities and paths. The client's recorded `MaxFrameSize` is
8 KiB. Authentication and ordinary replies use that same bound. The test
encodes the collected cap/realm state to require that one message would
exceed the frame bound; no fake inode, path or lock data inflates state.

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
inode. No OSD dial, object write, request replay or metadata flush is allowed.
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

Native executions and their source/profile/cleanup receipts are recorded below
when completed. Unit tests and existing vectors do not establish live split
acceptance. Exact-head Basic CI remains separate from local Linux/native
execution. The feature declaration is not ready for acceptance before both
selected native profiles pass.

Connection loss between fragments, arbitrary gigabytes of state, held-lock
reconnect, dirty metadata ordering, multi-rank split reconnect, delegated
inodes and Unicode name policy remain unverified or withheld. This recipe
establishes no complete filesystem client or remote rollback behavior.

## Source and licensing

Wire facts are independently restated from LGPL-2.1 sources:
`messages/MClientReconnect.h` (v5, data, counts, more),
`mds/Server.cc:1571-1745` (attributed `(more)` logs, per-fragment cap gathering,
OPEN and gather completion only for the final fragment),
`client/Client.cc:3316-3331` (native split policy) and
`mds/Server.cc:743-753` (required-feature admission). No upstream code is
incorporated. Native tools and libcephfs remain development dependencies.
