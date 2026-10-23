# CephFS metadata requests and MDS pushes

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code stays native Go with
CGO disabled. This stage adds raw CephFS metadata requests (MClientRequest,
MClientReply, MClientRequestForward) and the MDS-pushed capability, lease,
snapshot-realm and quota messages, with the client messages that answer
them. Same-address Messenger resume and explicit reconnect/resubmission APIs
preserve transport and request identity as described below. Path resolution,
inode/capability state, layout, writeback ordering and POSIX policy remain the
consumer's; these wire APIs implement no filesystem cache or reclaim policy.

## Opt-in profile

`Options.EnableCephFS` (requires `EnableMDS`) selects the CephFS profile for
every MDS connection of the client before Dial:

- Messenger features: the fresh-MDS set plus FLOCK (3), DIRLAYOUTHASH (7),
  CREATEPOOLID (26), REPLY_CREATE_INODE (27), EXPORT_PEER (37),
  MDS_INLINE_DATA (40), MDS_QUOTA (47) and FS_FILE_LAYOUT_V2 (58). Each is
  required from the MDS; a server lacking one is refused during the
  handshake instead of falling back to older encodings. The bits select
  MClientCaps v12 (flock, import peer, inline data, OSD epoch barrier,
  oldest flush TID, caller UID/GID, pool namespace, btime, change attribute,
  flags, directory counts, fscrypt fields), quota delivery, inline data and
  pool namespaces, the created inode in create replies and caller pool IDs in
  open arguments.
- CephFS client features: bits 0–9, RECLAIM_CLIENT (10), MULTI_RECONNECT (12), ALTERNATE_NAME
  (15), 32BITS_RETRY_FWD (18), NEW_SNAPREALM_INFO (19) and HAS_OWNER_UIDGID
  (20), `0x1c97ff` in native `session ls`. REPLY_ENCODING selects the
  versioned InodeStat (v8), DirStat (v1) and LeaseStat (v2);
  NEW_SNAPREALM_INFO selects SnapRealmInfoNew traces. RECLAIM_CLIENT changes
  no MDS behavior and only satisfies a `reclaim_client` filesystem
  requirement. MULTI_RECONNECT is declared only by this profile; it satisfies
  the filesystem requirement and uses the existing bounded reconnect codec.
  Lazy wanted caps, delegated inodes, metrics,
  notify-session-state, getvxattr, client-side auth-cap checks, charmap and
  blockdiff stay withheld; see `internal/mds/CLIENT_EXTENSION_DECISIONS.md`.

Without EnableCephFS the command-only and session-only contracts are
unchanged: sessions declare bits 0–8 and 10, and any request, reply or push
fails the transport.

## Lossless Messenger resume

Native CephFS clients use a lossless policy toward the MDS. With
`EnableCephFS` each MDS connection keeps its stateful Messenger identity and,
on a transport fault, redials the same address, re-authenticates with the
current MDS ticket (waiting for renewal if needed) and resumes with
SESSION_RECONNECT instead of failing. Unacknowledged messages are retransmitted
with their original sequences and the MDS continues its gapless stream, so the
CephFS session, pending requests and pushes survive. `MDSConnection.Resumes`
counts successful resumes. A RESET (the MDS no longer holds the Messenger
session, for example after a daemon restart), an identity change, protocol
errors or an exhausted 30 s episode end the connection; the CephFS session is
then Lost and its address unresolved, as before. This automatic transport
resume does not choose a new MDS incarnation or initiate metadata-request
replay. Those require an explicit `ReconnectSession` call and the caller's
`MDSReconnectState.Resubmit` entries described next.

## Reconnect to a new MDS incarnation

When the MDS daemon of a rank restarts or fails over, the new incarnation
has a new address/nonce and loads the session table from its journal; it
waits in its reconnect phase (`mds_reconnect_timeout`) for clients to restore
their capabilities and evicts the rest. Messenger resume to the old address
then ends in RESET and the session becomes Lost. The caller, driven by its
MDSMap watch, closes the old connection, opens a connection to the new
address and calls `MDSConnection.ReconnectSession(ctx, lost, state,
options)` during that phase.

`MDSReconnectState` carries the caller's capabilities (inode, cap ID,
wanted/issued masks, snap realm, short path, first cap-snap follows and an
opaque file-lock blob) and snap realms (inode, sequence, parent). It is sent
as MClientReconnect v5/compat4 in the data segment (`cap_reconnect_t` v2,
`snaprealm_reconnect_t` v1, caps ordered by inode), the current format
native clients use when the MDS advertises MULTI_RECONNECT; that MDS feature
is required. State larger than a frame is split into several messages with
the `more` flag, all realms in the last one. The previous session must be
Lost, belong to the same Client and GlobalID and name the same filesystem
label and rank at a different exact address.

The MDS answers OPEN on success (`Server::handle_client_reconnect`), which
starts a new local session generation (`Status().ReconnectedFrom` names the
Lost one) with push sequence zero and renewals; CLOSE returns
`ErrMDSSessionReconnectDenied` (wrapping `ErrMDSSessionClosed`): the MDS was
not in its reconnect phase, refuses sessions, lacks a required feature or no
longer holds the session. REJECT returns `*MDSSessionRejectedError`. The
library interprets no capability; the MDS decides what it restores.

Nothing is resubmitted automatically. `MDSReconnectState.Resubmit` lists the
Lost session's calls (unknown outcome only; safe-replied, forwarded and
duplicate calls are refused) that the caller explicitly sends again, before
the reconnect message as native clients do (`Client::send_reconnect`,
`resend_unsafe_requests`, `send_request`). Each keeps its original TID and
stamp, raises its retry count and drops its cap releases. A call that had an
unsafe reply carries the REPLAY flag and the target inode decoded from that
reply. During the reconnect phase the MDS queues REPLAY requests and
retries it has already completed for client replay
(`Server::dispatch`); a completed TID is answered from the session's
journaled completed-request record rather than executed again, which is the
duplicate-suppression scope. A request the MDS never received executes as
new. `ReconnectSession` returns the new calls in the order given; they hold
MaxInFlight slots like new requests. `oldest_client_tid` includes ordinary
resubmissions; SETFILELOCK retains the exception described below.


### Explicit pending cap flushes before reconnect

`MDSReconnectState.PendingCapFlushes` is an ordered list of
`MDSReconnectCapFlush{Caps, FlushTID}` supplied by the caller. It admits
pending dirty capability messages before the first MClientReconnect,
after any explicitly selected `Resubmit` requests. This reproduces the
ordering of pinned `Client::send_reconnect` / `early_kick_flushing_caps`
(`Client.cc` 3295–3314 and 5181–5205): an already revoked flushing cap must
be announced during reconnect before the MDS can issue that cap to another
client. `Locker::handle_client_caps` (3375–3393) records reconnecting dirty
caps and defers processing until replay. The library neither selects a
flush nor computes dirty/snapshot policy.

The current native resends are UPDATE (`Client::send_cap`) and FLUSHSNAP
(`send_flush_snap`), so this list accepts those operations only; it does not
accept the separate FLUSH alias. Each entry needs a nonzero inode, nonzero
dirty mask, distinct nonzero original flush TID and a nonzero
`OldestFlushTID` no greater than its own TID. The oldest TID may identify
another outstanding flush outside this list. `Seq`, `IssueSeq` and
`MigrateSeq` must already be zero for the new incarnation, as natively.
The supplied TID, oldest TID, `SnapFollows`, flags and metadata are preserved
without normalization. For a pending cap-snap after an earlier UPDATE, the
caller can retain the native `FLAG_PENDING_CAPSNAP` value (bit 2) in that
UPDATE's `Flags`; the library does not infer it. `MDSCapReconnect.SnapFollows` in the
MClientReconnect capability entry still describes the first pending cap
snap; it does not carry or replace the pending FLUSHSNAP payload.

Every cap frame and the complete pending-flush list, including each
41-byte message header, must fit `MaxFrameSize`. The complete list is
validated before its payloads are encoded/copied and before any resubmitted
request or flush is admitted. The ordinary `SendCaps` encoder also checks
its frame size before copying the variable-length fields. No caller-owned
byte slice is retained in the queued cap frames.

Cancellation before admission is local. After a selected flush enters the
ordered writer, cancellation while waiting for OPEN reports an
`OutcomeUnknownError` and may return the still-opening session; transport
loss before OPEN also reports an unknown outcome while retaining its cause.
An MDS CLOSE/REJECT after admission also retains an unknown cap-flush
outcome, with the original reconnect refusal available through
`errors.Is`/`errors.As` on the wrapped cause: refusing reconnect does not
prove an earlier flush was rolled back. Success only means
the reconnect opened; the caller must still consume and correlate
FLUSH_ACK/FLUSHSNAP_ACK by its original flush TID and wire metadata. The
library does not select cap flushes or repeat OSD writes in a new CephFS
session automatically; ordinary same-address Messenger resume retains its
existing frame-retransmission contract. A subsequent explicit recovery
decision must retain the original flush identity.

Synthetic tests cover request → ordered UPDATE/FLUSHSNAP → reconnect,
whole-list validation before the first frame, aggregate/frame bounds,
payload ownership, exact ACK identity, cancellation and transport loss.
The [Linux recovery extension](../integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md#explicit-pending-head-flush-before-reconnect-2026-10-18)
verified an explicit head UPDATE first sent after Lost, before reconnect:
CRC/AES IPv4 BlueStore and secure/AES256K IPv6 MemStore both returned the
original flush TID/inode/dirty ACK, and native LibCephFS observed size 4096.
The Go recovery client never dialed an OSD. This proves selected sparse
metadata delivery, not retransmission of a previously sent flush or native
early-kick selection policy. The separate dirty cap-snap recovery below
tests first transmission after reconnect. A later explicit resubmission
case below also verifies an old successful admission with no accepted ACK.
Completed-flush deduplication and recovery after an uncertain or refused
head UPDATE send have the separate acceptances under Evidence.

## Shared client address instance (fencing)

An `EnableCephFS` client draws one nonzero random address nonce at Dial
(`State.CephFSNonce`) and uses it in the ClientIdent address of every MDS and
OSD connection (MON/MGR connections keep per-connection nonces). The address
is the server-reported client IP with port 0. When an MDS evicts the
session it blocklists that `ip:0/nonce`; OSDs compare an op's source address
exactly (`OSDMap::is_blocklisted`, `PrimaryLogPG::do_op` returns
`-EBLOCKLISTED`), so the same entry fences this client's OSD data I/O from
that IP, as for a native client. Connections that reach the cluster through
different local IPs are fenced only per IP. Because the MDS registers
stateful connections by this address instance, the profile refuses a second
handle to one MDS address under another filesystem label
(`ErrMDSAddressInUse`). The OSD already keeps one connection per target.
Clearing a blocklist entry or opening a new Client (new nonce) is the
caller's decision.

## Requests

`MDSSession.Submit(ctx, MDSRequest)` queues one MClientRequest (header
v6/compat1, request head v3, struct_len 118) on an open or stale session.
The caller supplies the operation, the packed 56-byte argument union, caller
and owner UID/GID, supplementary GIDs, paths (base inode and relative path),
capability/lease releases, alternate name and fscrypt fields, the MDSMap
epoch it routed with, and retry/forward counts. The legacy u8 retry/forward
fields carry the low bytes of the 32-bit counts, as natively. REPLAY and
ASYNC flags and a preset inode number are refused by fresh `Submit` calls.
REPLAY is set only by the explicit recovery resubmission path described above;
async directory operations remain unsupported. Requests also require the MDS's
OPEN to advertise REPLY_ENCODING, 32BITS_RETRY_FWD, NEW_SNAPREALM_INFO and
HAS_OWNER_UIDGID.

TIDs come from the client-wide MDS transaction counter shared with raw MDS
commands. `oldest_client_tid` is the lowest non-SETFILELOCK TID this session
still tracks, or zero when none remain. Pinned Client::make_request and
unregister_request exclude SETFILELOCK because its in-memory lock state is
not journaled as a completed request (Server::handle_client_request).
GETFILELOCK and ordinary read/write requests still participate. Synthetic wire
captures cover mixed pending requests, forwarding and explicit reconnect;
this correction does not establish native file-lock interoperability.
Each call holds one `MaxInFlight` slot until its safe reply, a forward or
the end of the session; ctx bounds only slot and write admission.

Raw `MDSOpSetFileLock`/`MDSOpGetFileLock` requests already carry caller-built
56-byte filelock arguments and raw reply Extra. Nonblocking whole-file FLOCK
exclusive/shared conflicts and explicit unlocks now have an independent
native LibCephFS comparison in two Linux profiles; see
`integration/fixture/MDS_FILELOCK_VALIDATION.md`. Lock owners, argument schema
and Extra interpretation remain test-only consumer helpers. The bounded
follow-up in `integration/fixture/MDS_FILELOCK_COMPLETION_VALIDATION.md`
covers local Wait expiry followed by a grant on the original call, and an
explicit interrupt whose own safe result 0 is paired with the original
call's safe -EINTR. It does not establish FCNTL, direct native wait-queue
observation, interrupt after a grant, held-lock reconnect or durable lock state.
Wire inode Version can advance for capability max-size negotiation; user
change_attr and timestamps are independently preserved in the live evidence.

`MDSRequestCall.Wait` returns the first reply, `WaitSafe` the safe completion
reply. Read operations and in-memory SETFILELOCK changes answer safe
immediately; that flag does not make lock state journaled or durable. For a
journaled mutation, the safe reply follows journal completion. A forward
resolves the call with `*MDSRequestForwardedError`; resubmission to the caller's
chosen destination requires an explicit `ResubmitForwarded` call. If the session
ends before resolution the call returns an `OutcomeUnknownError`: the request
may have executed. A call that already received an unsafe reply keeps that
reply for `Wait`, but `WaitSafe` reports
the unknown journaled outcome. Neither a forward nor an unknown result
initiates automatic metadata-request replay.

Replies preserve the signed result (Linux numbering), MDSMap epoch, safe and
trace flags and the raw trace, extra and snap blobs. `DecodeTrace`,
`DecodeReaddir` and `DecodeSnapTrace` decode them in the native client order;
`DecodeMDSXattrs` decodes an inode's xattr blob. Opaque optional metadata is
kept as bytes. A call stays tracked until its safe reply or forward; afterwards the MDS
sends nothing more for that TID on the connection. A reply's `Op` is the
MDS's: a retried non-REPLAY MKNOD, MKDIR or SYMLINK that the MDS finds in its
completed-request record during clientreplay is executed as LOOKUP (dentry
wanted) or GETATTR and answered with that op (`Server::handle_client_request`),
which the call accepts. Any other reply whose operation differs from its
request, a second unsafe reply, a forward after a reply and any reply or
forward for an untracked TID (including a raw command TID) fail the
transport.

Decoded structures are raw wire values. `MDSFileLayout.Pool` keeps the
legacy wire field: a directory's all-zero legacy layout decodes as pool 0,
where native `file_layout_t::from_legacy` reports -1. The fscrypt flag is
preserved although the native decoder discards it. Struct versions are
exact (InodeStat v8, DirStat v1, LeaseStat v2, quota v1, optional metadata
v1, SnapRealmInfoNew v1, all compat 1): a newer server encoding is refused
rather than partially skipped, unlike native `DECODE_START`.

## Journal flush and arrival order

`FlushJournal` sends REQUEST_FLUSH_MDLOG (head-only v1, sequence 0) when the
MDS advertises LUMINOUS, as native `Client::flush_mdlog` does, so unsafe
requests become safe without waiting for the MDS journal tick. The MDS sends
no answer; the safe reply stays the completion signal. Every reply, push and
session event of a session carries `Arrival`, one counter in receive order,
so a consumer can order replies against pushes (for example a REVOKE before
or after the reply that granted the capability) without relying on
timestamps. `Client.MDSClientFeatures` reports the bitset that client
declares; `MDSSessionClientFeatures` and `MDSCephFSClientFeatures` report the
two profiles.

## MDS pushes and client answers

Every MClientCaps, MClientLease, MClientSnap and MClientQuota from the MDS
is counted in the session push sequence, exactly as the MDS counts them
(`send_message_client_counted`) and as the native client counts them
(`got_mds_push`). Each is delivered in arrival order as an `MDSSessionEvent`
with the raw Messenger type, header TID (the flush TID for FLUSH_ACK) and
the decoded message; xattrs travel in the MClientCaps middle segment. Pushes
before OPEN, client-side operations sent by the MDS and malformed messages
fail the transport. Queue overflow fails the connection: pushes are never
dropped. While closing, a push raises the push sequence and the library
resends REQUEST_CLOSE with the new sequence, as natively; the MDS drops a
close whose sequence is behind.

The library sends nothing on its own in answer to a push. The caller
acknowledges revokes and flushes and releases capabilities with
`SendCaps` (UPDATE, DROP, FLUSH, FLUSHSNAP, RELEASE, RENEW), `ReleaseCaps`
(MClientCapRelease v2 with the OSD epoch barrier) and `SendLease`
(RELEASE, RENEW). Their ctx governs write admission only. Capability IDs,
sequences, migrate sequences, wanted/issued/dirty masks, snapshot follows
and barriers are the caller's state; the library checks only the operation
direction and frame bounds.

These sends have two outcome classes. An error is returned only before the
message enters the session's ordered writer: an invalid operation, the frame
limit, a cancelled ctx, a closed, rejected or Lost session, or an ended
transport. That message is never transmitted by this session, so a caller
may send the same flush TID later (known unsent). A nil return means
admission only. The frame may already have reached the MDS, may be
retransmitted by same-address Messenger resume, or may be discarded with
the session; whether the MDS applied it stays unknown until a correlated
FLUSH_ACK/FLUSHSNAP_ACK. The library resends neither class on another
session. Synthetic peers verify that a cancelled send is never written
ahead of a later admitted one, and that a send on a Lost session returns the
session's terminal cause, is not replayed before or after reconnect, and
can then be sent explicitly once with its original TID and ACK identity.

## Evidence

Seven fresh native vectors from the pinned source cover three client
requests (lookup with GIDs and stamp, mkdir with a release, alternate name,
fscrypt fields and 32-bit retry/forward whose legacy u8 copies carry the low
byte, rename with both paths), a forward and three replies (a lookup with a
dentry/target trace and a two-realm SnapRealmInfoNew trace, a two-entry
readdir, an unsafe ENOENT). Request fronts and 41-byte headers equal the
native encoder; replies decode field by field to the native client decoders'
results. Trace and readdir bytes cannot come from `CInode::encode_inodestat`
outside an MDS: they are an independent restatement of its REPLY_ENCODING
field order, proven only by the unchanged native decoders consuming them
completely. See `internal/mds/testdata/mds-request-native-provenance.json`.

Ten further fresh native vectors cover the capability family with the
FLOCK/EXPORT_PEER/MDS_INLINE_DATA feature mask: MClientCaps v12 GRANT (snap
trace, xattr middle segment, flock, inline data and every v4–v12 extension),
EXPORT (zero-padded peer), IMPORT (peer after flock), FLUSH_ACK (header TID)
and a client UPDATE, MClientCapRelease v2, lease REVOKE and RELEASE,
MClientSnap SPLIT and MClientQuota. Go encoders reproduce the native client
UPDATE, cap release and lease release front, middle and header bytes; server
messages decode to the native decoders' receipts (the native decoder fills
the cap peer only for EXPORT/IMPORT and applies `from_legacy` to the
layout pool, both noted above). See
`internal/mds/testdata/mds-caps-native-provenance.json`.

Live Tentacle 20.2.4 acceptance (product `source-1`) passed nine leaves on
CRC/AES host IPv4 BlueStore and secure/AES256K container IPv6 MemStore:
declared features `0x1c83ff` in native `session ls`; mkdir, create (8-byte
REPLY_CREATE_INODE extra equal to the target inode), setxattr, readdir and
lookup compared with native LibCephFS; unsafe-then-safe replies for
mutations; a native writer blocked on a REVOKE until the Go UPDATE ack;
release of all six caps to native `num_caps` 0; confirmed close; and
Messenger resume of an idle and of an in-flight session (same native
session and address, not blocklisted). See
[the validation record](../integration/fixture/MDS_CEPHFS_VALIDATION.md).
That record predates RECLAIM_CLIENT; since PR #55 the same check requires
`0x1c87ff`, observed in every `mds-cephfs*` row and in the CephFS-profile
session of the `admit_required_reclaim_client` leaf
([MDS_SESSIONS_VALIDATION.md](../integration/fixture/MDS_SESSIONS_VALIDATION.md)).
A separate live push acceptance (product `source-2`, both profiles, seven
leaves) received and answered a dentry-lease REVOKE (the native rename
completed only after the Go RELEASE), snapshot UPDATE/SPLIT/DESTROY pushes
whose traces decode as SnapRealmInfoNew, MClientQuota with the set
`max_bytes`, RECALL_STATE (MaxCaps equal to `mds_min_caps_per_client`) with
native `num_caps` falling to the target after `ReleaseCaps`, and FLUSHMSG
during `cache drop` whose gather ended on the Go `AcknowledgeFlush`; push
sequences equaled the pushes counted. See
[the push record](../integration/fixture/MDS_CEPHFS_PUSH_VALIDATION.md).
MKDIR replies grant dentry leases that the client's own later RMDIR may have
to release first. `DecodeMDSSnapTrace` decodes push traces.

A two-active-rank plus standby acceptance (product `source-3`, both profiles)
held one session per rank under one GlobalID and address instance, saw a
rank-0 request forwarded to rank 1 (Forward 1, MustResend) and completed on
rank 1, received cap EXPORT on rank 0 and IMPORT on rank 1 (peer cap IDs,
issue and migrate sequences) with FLUSHMSG acknowledged during a pinned
subtree migration, and reattached rank 0 with `ReconnectSession` after a
standby takeover while the rank-1 session stayed open. See
[the ranks record](../integration/fixture/MDS_CEPHFS_RANKS_VALIDATION.md).
`ResubmitForwarded` resends a forwarded call once on the destination rank's
session with its original TID, stamp and the reported forward count, as
native clients do. Its per-rank retry count restarts at zero on the
destination, including when the source request had a nonzero retry count.
Original embedded releases carry source-rank cap IDs/sequences and dentry
lease state, so they are omitted on the destination. Native clients rebuild
them from destination-rank caches (`Client.cc` `send_request`,
`encode_cap_releases` and `encode_inode_release`); this transport owns no
such cache. Callers may explicitly send destination capability/lease
controls in order before resubmitting. The original call snapshot remains
unchanged.

The destination must retain the source's caller-supplied filesystem label
as well as the reported rank. A different label is a known-unsent
`ErrMDSTargetChanged` refusal before selecting the call or acquiring a
slot/TID; the same call remains available on a valid destination. This is
local routing consistency, not authenticated filesystem membership.
Synthetic peers compare the complete binary request with only retry/forward
changed and source releases removed, and verify refusal with an ordered
wire barrier followed by successful correct-label resubmission. These
source-release and wrong-label cases do not constitute separate native
acceptances. The later [Linux forward acceptance](../integration/fixture/MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md#nonzero-source-retry-on-linux-2026-10-18)
starts CREATE/MKDIR with retry 3 and verifies the native parsed retry is
zero on each destination, with the same TID/stamp, two forwards and native
final entries, in both selected configurations.

A forward carries `Arrival` like replies. A three-rank
acceptance with a standby-replay follower (product `source-4`, both profiles)
resubmitted a CREATE and a MKDIR along a two-hop forward chain (rank 0 to 1
to 2, forward counts 1 and 2) with the same TIDs and saw each executed once
on rank 2; requests submitted while rank 0 held a FLUSHMSG for a subtree
export were forwarded after the acknowledgement and executed once on rank 1;
and after `mds fail` of rank 0 the follower took over (up:replay in about
0.85 s, active in about 6 s), `ReconnectSession` resubmitted two unsafe
requests that the new rank 0 answered safe and traceless, while ranks 1 and 2
kept their sessions. A replayed MKDIR's inode is only in its earlier unsafe
reply, and a rank that must allocate an inode answers CREATE safe first
without an unsafe reply. See
[the replay record](../integration/fixture/MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md).
Not exercised live: followers or takeovers of ranks other than 0, a rank
failing mid-forward and migration with dirty file caps.

A completion acceptance (product `source-5`, both profiles, eight leaves
each) restarted the MDS four times. Requests the MDS executed and journaled
while `ms_blackhole_client` dropped their replies were resubmitted with retry
1 after `ReconnectSession`: the CREATE was answered as a CREATE with its
trace and created inode, and the MKDIR was converted to LOOKUP (dentry
`lost-dir`, target equal to the recorded inode), both from the
completed-request record. A create whose unsafe reply had arrived and whose
safe reply was dropped was resubmitted with REPLAY and answered traceless
from the record (`already completed`, extra holding the inode) instead of
being executed. With `mds_deny_all_reconnect` the MDS answered CLOSE
(`ErrMDSSessionReconnectDenied`), then evicted and blocklisted the address
instance, so the same Client was rejected as blocklisted while a new Client
opened normally. After a missed reconnect window CLOSE followed the eviction
and the client's existing OSD connection read -108. FORCE_RO arrived as a
session event and a MKDIR then returned -30 while LOOKUP and GETATTR
succeeded. A dentry lease RENEW was answered with RENEW at the next sequence
and the same duration. After a native snapshot a FLUSHSNAP with dirty Fw got
FLUSHSNAP_ACK with the same TID and follows. See
[the completion record](../integration/fixture/MDS_CEPHFS_COMPLETION_VALIDATION.md).

The [file-data acceptance](../integration/fixture/MDS_CEPHFS_DATA_VALIDATION.md)
separately verifies caller-driven striped OSD writes followed by dirty-metadata
UPDATE/FLUSH_ACK, buffered writeback during capability revocation, and a single
snapshot whose object clones and file bytes match native clients. Its test-only
consumer owns the layout, dirty buffers and data/metadata ordering; the product
transports the OSD requests and cap messages without implementing that policy.
The [dirty-data cap-snap acceptance](../integration/fixture/MDS_CEPHFS_CAPSNAP_VALIDATION.md)
adds two retained dirty generations across two snapshots using existing raw
APIs. In both configurations its A/B writebacks preceded their separate
FLUSHSNAPs, exact inode/follows/TID/dirty ACKs arrived, and Go/native snapshot
and head bytes, sizes, mtimes and object clone associations matched A/B/C.
Local pending ACKs overlapped; concurrent server execution was not established.
The source asserts OSD ONDISK completion; raw reply flags are not archived.
The later [cap-snap recovery acceptance](../integration/fixture/MDS_CEPHFS_CAPSNAP_RECOVERY_VALIDATION.md)
completes A/B object writes, retains their unsent original flush TIDs across
an owned MDS restart, reconnects eight caps and root/directory realms, then
explicitly sends FLUSHSNAP only after ACTIVE/adoption. Both Linux profiles
returned exact ACKs and matching native A/B/C bytes/metadata/clones. The
object-request count stayed unchanged across reconnect and the flushes;
raw ONDISK flags and write hashes are recorded. This verifies the pure
cap-snap deferred path, without replaying data.
The [explicit resubmission acceptance](../integration/fixture/MDS_CEPHFS_CAPSNAP_RESUBMIT_VALIDATION.md)
then admits A to the old journal-paused MDS and proves its owned arrival,
handler and expected-snapshot branch. After Lost and natural event draining,
the exact A ACK remains absent. The new generation explicitly resubmits A's
original TID and first-sends B; both Linux profiles receive exact ACKs and
matching native data/metadata/clones without extra object requests. This
does not itself prove durable old completion or completed-flush deduplication.
The subsequent [completed-flush acceptance](../integration/fixture/MDS_CEPHFS_CAPSNAP_DEDUPE_VALIDATION.md)
releases the old journal only after a read-back client blackhole, requires
a successful native journal barrier and an attributed dropped ACK triple,
then proves the same original TID's completed-flush branch after restart.
Both profiles retain exact ACKs, native A/B/C postconditions and unchanged
object-request counts. All runtime overrides are restored or discarded
only after the owned incarnation's baseline proof.
The [cap-send recovery acceptance](../integration/fixture/MDS_CEPHFS_CAPSEND_RECOVERY_VALIDATION.md)
admits a head UPDATE while the owned MDS drops incoming client messages
after Messenger receipt, proven by the native drop line for its exact tuple
and peer nonce, and first attempts a second UPDATE on the Lost session,
which `SendCaps` refuses with the session's terminal cause. After reconnect
the test consumer resubmits the first with its original TID and first-sends
the second. In both Linux profiles the new incarnation applied both as new
flushes (no completed-flush branch), both FLUSH_ACKs correlated, native
LibCephFS observed the final size, mtime and bytes, and no object request
was repeated. This covers head UPDATE only; a FLUSHSNAP refused by a Lost
session uses the same product path but was not exercised live.
The [writeback-fence acceptance](../integration/fixture/MDS_CEPHFS_WRITEBACK_FENCE_VALIDATION.md)
completes generation A's object writes and accepts its metadata flush, then
interrupts the test consumer's next object write while the single owned OSD
drops client messages after Messenger receipt. The bounded wait ended with
`OutcomeUnknownError`, and the OSD's own receipt and drop lines attributed
exactly one dropped op to the client. The consumer flushed no size beyond
its completed writes. Across an owned MDS restart, a native reader's
capability revoke and an admin-socket eviction, both Linux profiles kept
the accepted size and mtime and A's bytes natively. The evicted address
instance's next object write on its existing OSD connection returned -108,
and a new client instance found neither the uncertain nor the fenced write
in the objects. No object request was repeated. Buffered data written back
during the revoke itself and partially applied ranges remain unverified.

Synthetic peers cover
the profile handshake refusal, feature gates, request correlation, unsafe and
safe replies, forwards, session loss, the data segment, push ordering and
counting, client answers, close resend and protocol violations.

## Limits

MClientReconnect matches three fresh native vectors byte for byte (two caps
inserted out of inode order with a native `_encode_filelocks` blob and two
realms, a `more` message, a realm-only message; see
`internal/mds/testdata/mds-reconnect-native-provenance.json`); ordering,
splitting, OPEN/CLOSE answers and resubmission have synthetic coverage.

Live recovery acceptance (product `source-6`, both profiles above, five
leaves each) restarted the rank-0 MDS right after an unsafe create: resume
to the old address ended in SESSION_RESET and the session went Lost within
about 0.4 s; `ReconnectSession` to the new incarnation (new GID and nonce,
same port) got OPEN in the reconnect phase with ten caps and the root realm;
the replayed create (REPLAY with its inode) completed safe with the same
inode, an unanswered create resent with retry 1 executed once as new, and
native listing showed each file exactly once with the Go session open again.
After a native eviction the blocklist held exactly `<ip>:0/<CephFSNonce>`
and the same OSD read on the client's existing OSD connection returned
-108 (EBLOCKLISTED) while another client read the object. `FlushJournal`
cut the unsafe-to-safe latency from up to 5 s to a few milliseconds. See
[the recovery record](../integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md).
A consumer that installs caps only from reply traces can see a cap GRANT for
a replayed inode before that request's reply; `Arrival` orders them. Standby
takeover and multiple ranks are covered by the ranks acceptance under
Evidence, and completed-record answers, denial and a missed window by the
completion acceptance there. Not exercised live: split reconnect messages,
realm topologies beyond the root/directory data recipe, the GETATTR conversion,
completed-record answers for other operations, a lost reply of an OPEN with
O_CREAT and completed-flush deduplication after an ACK loss; cap-send error
recovery and interrupted OSD writeback have their own acceptances under
Evidence. Ordinary
dirty-metadata UPDATE/FLUSH_ACK and the two-snapshot recovery recipe have
the separate evidence above.

Recovery automatically resumes only the same-address Messenger transport.
Selecting a new MDS incarnation and reconnect state, replaying an unknown
request or resubmitting a forward requires the caller's explicit APIs. These
APIs do not automatically retry file I/O. The library implements no path
walking, inode or dentry cache, capability bookkeeping,
lease expiry, snapshot realm tree, dirty metadata or data writeback, file
locks, delegated inodes or async operations. File data I/O semantics (layout, striping,
flush ordering against capabilities) remain the consumer's.

Withheld CephFS client feature decisions: [CLIENT_EXTENSION_DECISIONS.md](../internal/mds/CLIENT_EXTENSION_DECISIONS.md).
