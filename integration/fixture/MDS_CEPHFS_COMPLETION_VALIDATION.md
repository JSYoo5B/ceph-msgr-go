# CephFS request-completion acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI and admin socket, the
native LibCephFS oracle and the owned MDS are development-only. This
acceptance covers the cases [the CephFS
contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) listed as not exercised
live: a lost reply of a delivered request, a REPLAY answered from the
completed-request record, reconnect denial and a missed reconnect window,
FORCE_RO, lease RENEW and a cap snap FLUSHSNAP. The owned recipe is described
in [MDS_CEPHFS_COMPLETION_FIXTURE.md](MDS_CEPHFS_COMPLETION_FIXTURE.md).

## Test shape

`TestCephMDSCephFSCompletionIntegration` runs alone in its profile
(`CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1`, MDS_FRESH=1, `legacy-v6`). Every Go
client authenticates as `client.mds-cephfs-completion-<token>` and dials
through the counted relays of the recovery acceptance (one MDS connection per
client). Sessions send `entity_id`, `hostname`
(`ceph-msgr-go-<token8>-<role>`) and `root=/` with a 2 s RenewInterval and
1024 event slots; requests use caller and owner UID/GID 0. The test-only
capability agent of the other CephFS acceptances consumes every session
event; for this test it also records lease RENEW answers (the native
`handle_lease` refreshes the dentry lease; nothing is sent), FORCE_RO
(`force_session_readonly`; nothing is sent) and the FLUSHSNAP_ACK of a
FLUSHSNAP the test registered (inode and flush TID).

| Leaf | Client | Checked behavior |
| --- | --- | --- |
| `open_completion_session` | main (MDS) | Session Open, OPEN advertising bits 7, 9, 12, 18, 19, 20; native `session ls` open with the declared features, metadata and `<ip>:0/<CephFSNonce>`. The Go client creates the owned directory |
| `lease_renew_answered` | main, holder | The holder creates `lease` and `lease/leased` and becomes the directory's loner. The main client's LOOKUP returns a LeaseStat. With `debug_mds 10/10` on, `SendLease` RENEW (mask 1, the directory inode, first and last `CEPH_NOSNAP`, the LeaseStat sequence, dentry `leased`) is answered by one MClientLease RENEW: same mask, inode, snap range and name, sequence +1, duration 30000 ms (`client_lease_durations[1]`), counted in the push sequence, after the lookup reply; the daemon log shows `handle_client_lease client.<gid> renew on` |
| `lost_reply_and_completed_replay` | main | Below |
| `reconnect_denied` | deny, deny-new | `mds_deny_all_reconnect true` (central, owned section; the running daemon reports it). Restart; the new incarnation in `up:reconnect` reports it too. `ReconnectSession` fails with `ErrMDSSessionReconnectDenied` wrapping `ErrMDSSessionClosed`; the MDS becomes active by itself; the daemon log has the deny line. Then: no native session, the address instance blocklisted, a fresh session on a new connection of the same Client rejected, and a new Client's session opens and closes. The setting is restored |
| `reconnect_window_missed` | window (MDS and OSD) | Native create+write of `fenced`; the client reads `<ino>.00000000` (stat and read) on its OSD connection. `mds_reconnect_timeout 5` (central, read at the next startup). Restart; the new incarnation reports 5. The client waits until `up:active`; the daemon log shows `reconnect gives up on client.<gid>`; the session is gone and its address instance blocklisted. `ReconnectSession` on a new connection then fails with `ErrMDSSessionReconnectDenied`; the same OSD read on the existing OSD connection fails with -108; a separate OSD-only client reads the bytes. The setting is restored |
| `force_readonly` | readonly | The client creates and keeps `ro-file` open read-write. `force_readonly` on the admin socket: one FORCE_RO session event, `Status().ReadOnly` true, write caps revoked and acknowledged. MKDIR `ro-dir` answers -30 (safe), LOOKUP and GETATTR of `ro-file` answer 0. Restart; `ReconnectSession` gets OPEN without read-only; MKDIR `ro-dir` then succeeds |
| `cap_snap_flush` | readonly (reconnected) | The client creates `snap` and `snap/snap-file`, keeps the file open read-write (Fw). Native `mkdir snap/.snap/<name>`: MClientSnap UPDATE of the global realm and SPLIT moving the directory and file into the new realm. The client sends FLUSHSNAP (dirty Fw, follows the old realm sequence, flush TID 1) and receives FLUSHSNAP_ACK with the TID, follows and dirty mask; the daemon log shows `flushsnap follows 1 -> snap 2` and `flushsnap snap 2 client.<gid>`. The snapshot is removed natively |
| `cleanup_unlink_release_close` | readonly | UNLINK and RMDIR of every created name and the directory (safe result 0), native ENOENT, release of every cap, confirmed Close |

### Lost reply and REPLAY from the completed-request record

One restart covers both cases. With the restart armed, `mds_log_pause true`
on the admin socket; the main client creates `replayed` and receives its
unsafe reply (early replies are still on). Then `mds_early_reply false`; the
client submits CREATE `lost-file` and MKDIR `lost-dir` (WANT_DENTRY) and
waits 1.5 s: no reply arrives, and `dump_ops_in_flight` shows all three
requests at `submit entry: journal_and_reply`. With a log mark taken,
`ms_blackhole_client true`, `mds_log_pause false` and `flush journal`
(return code 0): the journal commits the three requests, `session ls` shows
the session's completed-request record with the three TIDs and created
inodes (the one of `replayed` equal to its unsafe reply), `dump_historic_ops`
shows them done with result 0 after `journal_committed`, and the daemon log
shows exactly one `send_msgclient blackhole client_reply(???:<tid> = 0 (0)
Success safe)` per TID. The client still has no reply for `lost-file` and
`lost-dir` and no safe reply for `replayed`.

`ceph mds fail <armed GID>`: the session ends Lost. Once the new incarnation
waits in `up:reconnect`, its admin socket reports `ms_blackhole_client false`,
`mds_early_reply true` and `mds_log_pause false`; `debug_mds 10/10` and
`debug_ms 1/5` are set on it. `ReconnectSession` with the agent's caps and
realms and `Resubmit` of the three calls (in that order) gets OPEN. The
library sends `replayed` with REPLAY and its inode (retry 1) and the other
two with retry 1. The leaf then requires the three answers below, the session
still Open with no error after them and open natively, the MDS active and
adopted, and the native listing of each name exactly once with the inode of
the completed-request record. A session the client lost would instead be
evicted natively before the listing (containment for a regression only; not
reached in the runs below).

## Results

The tested tree is commit `d88abe1ca514070d9ac27b16586ae5b13d325a39` (this
change rebased onto `source-1`, which accepts the MDS's converted reply),
exported with `git archive` to a separate directory; this record (written
after the runs) is the only difference from it. Image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.
Selector `^TestCephMDSCephFSCompletionIntegration$`.

| Configuration | Controller wall time (UTC, 2026-10-15) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 23:17:17 to 23:21:54 (4 min 37 s) | 177.45 s | 8/8 leaves passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 23:21:53 to 23:26:18 (4 min 25 s) | 169.71 s | 8/8 leaves passed |

The shared capability agent gained RENEW, FORCE_RO and FLUSHSNAP_ACK
handling for this test, so the existing CephFS profile
(`^TestCephMDSCephFS(Push|Recovery)?Integration$`) was run from the same
export in the first configuration (23:26:18 to 23:31:06 UTC): push 7/7,
recovery 5/5 and metadata 9/9 leaves passed. Before the rebase, the tree
`530c354` had also passed that profile in both configurations.

Leaf durations (CRC / secure, seconds): open 2.32 / 2.21, lease 13.15 /
13.04, lost reply 49.36 / 47.47, denied 29.32 / 29.09, window 34.82 / 35.24,
read-only 23.77 / 23.47, cap snap 20.05 / 14.86, cleanup 4.64 / 4.32. In both
configurations the fixture cleanup completed: runtime changes restored or
discarded by a restart, both central settings restored with no entry left,
the namespace gone natively, six new blocklist entries (four restarted
incarnations, the denied and the late session) attributed, removed and
verified absent, both identities removed, and the filesystem removed.

### Observed values

Values are identical in both configurations unless two are given
(CRC / secure).

Lease RENEW: LOOKUP of `leased` (op 0x100, result 0, safe, dentry and
target) carried LeaseStat mask 7, duration 30000 ms, sequence 3, while the
client's directory cap was `pAsLsXs`. The RENEW sent was
`client_lease(a=renew seq 3 mask 1 0x100000003e9/leased)` in the daemon log;
the answer `client_lease(a=renew seq 4 mask 1 0x100000003e9/leased)` arrived
after 754 / 262 µs as push 2 (Arrival 10): action 3, mask 1, inode
`0x100000003e9`, first and last `0xfffffffffffffffe`, sequence 4, duration
30000 ms, dentry `leased`.

Lost reply and completed REPLAY (main client GID 4890 / 4911; TIDs 3, 4, 5):

| Call | Before the restart | Completed record (created ino) | Resubmitted | MDS answer, as the Go call received it |
| --- | --- | --- | --- | --- |
| CREATE `replayed` | unsafe reply: op 0x1301, result 0, dentry and target, trace 734 B, extra `0100000000010000` (ino `0x10000000001`), caps `pAsxLsXsxFsxcrwb`; safe reply blackholed | `0x10000000001` | REPLAY, inode `0x10000000001`, retry 1 | queued as a replayed op; `already completed client.<gid>:3`; op 0x1301, result 0, safe, no dentry, no target, no trace, extra `0100000000010000`, MDSMap epoch 0, Arrival 4 |
| CREATE `lost-file` | no reply (blackholed safe reply) | `0x10000000002` | retry 1 | queued as a completed op; handled as an open of the existing file (`dispatch_client_request ... create`, no new inode); op 0x1301, result 0, safe, dentry and target (trace 739 B, snap trace 70 B, target `0x10000000002`, mode 0100644, caps `pAsxLsXsxFcb`), extra `0200000000010000`, MDSMap epoch 12 / 13, Arrival 5 |
| MKDIR `lost-dir` | no reply (blackholed safe reply) | `0x10000000003` | retry 1, WANT_DENTRY | queued as a completed op; `completed request which created new inode 0x10000000003, convert it to lookup request`; dispatched as `lookup #0x10000000000/lost-dir` (`reply to stat on ...`), sent as `client_reply(???:5 = 0 (0) Success)` (no safe/unsafe mark: the op is no longer a write). Received: op 0x100 (LOOKUP), result 0, safe, dentry `lost-dir` and target (trace 734 B, snap trace 70 B, target `0x10000000003`, mode 040755, caps `pAsLsXsFs`), no extra, MDSMap epoch 12 / 13, Arrival 6 |

The lost session ended 395 / 284 ms after `mds fail`; `ReconnectSession` took
5.6 / 2.4 ms; the three answers arrived 1.99 / 1.27 s after it (after
rejoin, in clientreplay), in TID order. After them the reconnected session
was still Open without error, and natively `open` with 7 caps at the same
address. The MDS was active 8.82 / 8.92 s after the fail. The native listing
then showed `lease`, `replayed`, `lost-file` and `lost-dir` once each with
inodes `0x100000003e9`, `0x10000000001`, `0x10000000002`, `0x10000000003`
(the last a directory, mode 040755). The test then released the session's
caps and closed it with a confirmed CLOSE.

Reconnect denied (deny client GID 6318 / 6360): the new incarnation waited
in `up:reconnect`; the reconnect 4.74 / 4.83 s after the fail was answered
CLOSE (`ErrMDSSessionReconnectDenied`, wrapping `ErrMDSSessionClosed`).
Daemon log: `mds_deny_all_reconnect was set to speed up reboot phase,
ignoring reconnect, sending close`, the cluster-log line `denied reconnect
attempt (mds is up:reconnect)`, then at the next tick `reconnect gives up on
client.<gid>`, `evicting unresponsive client ... after waiting 3.01 seconds
during MDS startup` and `reconnect will complete once clients are evicted`.
Active 9.36 / 8.86 s after the fail. The address instance was blocklisted
(expiry one hour). A fresh session of the same Client was rejected: reason
`blocklisted (blacklisted)`, flags 1, `Blocklisted` true, metadata
`error_string`. A new Client opened and closed a session normally.

Missed window (client GID 7059 / 7104): native `fenced` ino
`0x10000000004`, object `10000000004.00000000` in pool 4 (raw PG
`4.17c5e6c5`, primary osd.0), read before the restart: stat size 5000 and
5000 equal bytes (OSDMap epoch 28). The new incarnation reported
`mds_reconnect_timeout 5.000000` and stayed in `up:reconnect` from about
4.2 s to 11.4 / 11.7 s after the fail (`evicting unresponsive client ...
after waiting 8.01 seconds`: the timeout, the last-seen extension and the
5 s tick), then `up:rejoin` and `up:active` at 12.96 / 13.30 s. The session
was evicted and its address instance blocklisted. `ReconnectSession` 20.6 /
21.0 s after the fail got CLOSE (`ignoring msg from not-open session
client_reconnect(2 caps 1 realms )` in the daemon log: the new connection's
session is a fresh closed one). The same OSD read on the existing OSD
connection with epoch 30 returned result -108 and no data; a separate
OSD-only client read the 5000 bytes.

FORCE_RO: the readonly client held `ro-file` (`0x10000000005`) with
`pAsxLsXsxFsxcrwb`. The FORCE_RO session event (op 10, Arrival 6, not counted
as a push) arrived 0.97 / 0.96 s after the control request, followed by a
REVOKE of the file to `pAsLsXsFscr` (revoked `AxXxFxwb`, Arrival 7, push
sequence 0 to 1) acknowledged with an UPDATE. MKDIR `ro-dir`: op 0x1220,
result -30, safe, no trace (`client_reply(???:2 = -30 (30) Read-only file
system safe)`); LOOKUP 0x100 and GETATTR 0x101 of `ro-file`: result 0 with
the file as target. Daemon log `force file system read-only`. After the
restart the reconnected session was not read-only and MKDIR `ro-dir` returned
0 (unsafe, then safe).

Cap snap flush: `snap` `0x10000000007`, `snap-file` `0x10000000008` with cap
ID 1, issued `pAsxLsXsxFsxcrwb` in realm 1 (sequence 1). After the native
snapshot: MClientSnap UPDATE of realm 3 (sequence 2, snapshot 2) and SPLIT of
`0x10000000007` with split inodes `0x10000000007` and `0x10000000008`, trace
realm `0x10000000007` (sequence 2, created 2, parent 1, snapshot 2) then
realm 1. FLUSHSNAP sent: op 9, realm `0x10000000007`, cap ID 1, caps
`pAsxLsXsxFsxcrwb`, dirty Fw (4096), follows 1, size 0, oldest flush TID 1,
flush TID 1. FLUSHSNAP_ACK received after 3.23 / 0.12 s (journaled through
`_do_snap_update`; the wait depends on the journal tick): op 10, header TID
1, inode `0x10000000008`, cap ID 0, caps 0, dirty Fw, follows 1, oldest
flush TID 1, OSD epoch barrier 31. Daemon log: `handle_client_caps on
0x10000000008 tid 1 follows 1 op flushsnap`, `flushsnap follows 1 -> snap 2`,
`flushsnap snap 2 client.<gid> on [inode 0x10000000008 ...]`.

### Resolved product finding

`Server::handle_client_request` converts a retried (not REPLAY) request that
the completed-request record shows created an inode, other than OPEN and
CREATE, into LOOKUP (dentry wanted) or GETATTR during clientreplay, by
rewriting the request's op before `dispatch_client_request`; the reply is
built from that request (`MClientReply(const MClientRequest&)` copies
`head.op`), so a resubmitted MKDIR is answered with op 0x100. The native
client does not compare a reply's op with its request
(`Client::handle_client_reply`). Before `source-1` the session failed the
transport on that reply ("MDS reply operation differs from its request"): on
the tree `530c354` (both configurations, 2026-10-15 22:51 to 23:01 UTC) the
resubmitted MKDIR ended with an unknown outcome, the session was Lost while
the MDS kept it open with its caps, and the test had to evict it before a
native listing. Since `source-1` the call accepts exactly that conversion
(MKNOD, MKDIR or SYMLINK resubmitted without REPLAY; LOOKUP with WANT_DENTRY,
otherwise GETATTR) and reports the MDS's op, as recorded above. Only the
MKDIR and LOOKUP case was exercised live.

### Not verified

A REPLAY answered with `has_completed` for an op other than CREATE, a lost
reply of an OPEN with O_CREAT, the GETATTR conversion (dentry not wanted),
MKNOD and SYMLINK conversions, cap flushes with dirty data (`FLUSH`), cap
snaps of more than one snapshot, reconnect denial for `mds is readonly` or
missing features, standby takeover with these cases, IPv4/IPv6 crossings and
other OSD stores.

## ceph-testcontainers-go v0.3.0 unsafe window and key input

Local runs on 2026-10-16 from clean `git archive` exports, darwin/arm64,
Go 1.27.2, a compatible Docker engine, Ceph image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a4...`, ceph-testcontainers-go
v0.3.0. Configuration (a): aes/aes, CRC, IPv4, runtime `host`, BlueStore.
Configuration (b): aes256k/aes256k, secure, IPv6, runtime `container`,
MemStore. Both use the legacy-v6 single-OSD fixture.

The native-only keys reach their workers as one bounded
`NativeCommand.Input` record (`native_key_input.go`); no native keyring is
written to the host control directory. Every worker run checks
`InputWritten`, `InputDelivered`, `InputEOF` and the worker's byte-count
receipt separately from `Complete()`. The attach path ran from the host
fixture into the client container under both runtimes.

`lost_reply_and_completed_replay` no longer sleeps 1.5 s after the journal
pause. It samples the owned MDS's `dump_ops_in_flight` until the replayed
create (answered unsafe) and the lost create and mkdir (no reply) are all at
`submit entry: journal_and_reply`, then samples again at least 3 s later.
Both samples must come from the armed MDS GID and show the replayed create
after `early_replied`, the lost requests without it, and none
`journal_committed` (the v0.3.0 `TestCephFSUnsafeRequestWindow` receipt).

| Export | Configuration | Result | Wall time |
| --- | --- | --- | --- |
| `36d5734` | (a) | PASS, 8 leaves (test 168.67 s) | 261 s |
| `36d5734` | (b) | PASS, 8 leaves (test 168.60 s) | 259 s |
