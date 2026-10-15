# CephFS metadata requests and MDS pushes

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code stays native Go with
CGO disabled. This stage adds raw CephFS metadata requests (MClientRequest,
MClientReply, MClientRequestForward) and the MDS-pushed capability, lease,
snapshot-realm and quota messages, with the client messages that answer
them. It adds no path resolution, inode cache, capability state machine,
dirty-data flush, file I/O, reconnect, replay or reclaim. Those belong to the
consumer layer (cache, POSIX semantics) or to later milestones (reconnect).

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
- CephFS client features: bits 0–9, ALTERNATE_NAME (15), 32BITS_RETRY_FWD
  (18), NEW_SNAPREALM_INFO (19) and HAS_OWNER_UIDGID (20). REPLY_ENCODING
  selects the versioned InodeStat (v8), DirStat (v1) and LeaseStat (v2);
  NEW_SNAPREALM_INFO selects SnapRealmInfoNew traces. Reclaim, lazy wanted
  caps, multi-reconnect, delegated inodes, metrics, notify-session-state,
  getvxattr, client-side auth-cap checks, charmap and blockdiff stay
  withheld.

Without EnableCephFS the command-only and session-only contracts are
unchanged: sessions declare bits 0–8 and any request, reply or push fails
the transport.

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
then Lost and its address unresolved, as before. Resume is neither a CephFS
reconnect to a new MDS incarnation nor a replay of requests: MClientReconnect
after failover and REPLAY of unsafe requests are not implemented.

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
MaxInFlight slots like new requests. `oldest_client_tid` includes them.

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
ASYNC flags and a preset inode number are refused: replay and async
directory operations are not implemented. Requests also require the MDS's
OPEN to advertise REPLY_ENCODING, 32BITS_RETRY_FWD, NEW_SNAPREALM_INFO and
HAS_OWNER_UIDGID.

TIDs come from the client-wide MDS transaction counter shared with raw MDS
commands. `oldest_client_tid` is the lowest TID this session still tracks,
so the MDS may trim completed-request records only for resolved requests.
Each call holds one `MaxInFlight` slot until its safe reply, a forward or
the end of the session; ctx bounds only slot and write admission.

`MDSRequestCall.Wait` returns the first reply, `WaitSafe` the safe
(journaled) reply. Read operations answer safe immediately. A forward
resolves the call with `*MDSRequestForwardedError`; the library never
resends. If the session ends before resolution the call returns an
`OutcomeUnknownError`: the request may have executed. A call that already
received an unsafe reply keeps that reply for `Wait`, but `WaitSafe` reports
the unknown journaled outcome. No request is ever replayed.

Replies preserve the signed result (Linux numbering), MDSMap epoch, safe and
trace flags and the raw trace, extra and snap blobs. `DecodeTrace`,
`DecodeReaddir` and `DecodeSnapTrace` decode them in the native client order;
`DecodeMDSXattrs` decodes an inode's xattr blob. Opaque optional metadata is
kept as bytes. A call stays tracked until its safe reply or forward; afterwards the MDS
sends nothing more for that TID on the connection. A reply whose operation
differs from its request, a second unsafe reply, a forward after a reply and
any reply or forward for an untracked TID (including a raw command TID) fail
the transport.

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

Live Tentacle 20.2.4 acceptance (product `16f77b9`) passed nine leaves on
CRC/AES host IPv4 BlueStore and secure/AES256K container IPv6 MemStore:
declared features `0x1c83ff` in native `session ls`; mkdir, create (8-byte
REPLY_CREATE_INODE extra equal to the target inode), setxattr, readdir and
lookup compared with native LibCephFS; unsafe-then-safe replies for
mutations; a native writer blocked on a REVOKE until the Go UPDATE ack;
release of all six caps to native `num_caps` 0; confirmed close; and
Messenger resume of an idle and of an in-flight session (same native
session and address, not blocklisted). See
[the validation record](../integration/fixture/MDS_CEPHFS_VALIDATION.md).
Lease, FLUSHMSG, RECALL_STATE, snap and quota pushes, flush/dirty caps,
EXPORT/IMPORT, forwards, multiple ranks and failover were not exercised
live. Synthetic peers cover
the profile handshake refusal, feature gates, request correlation, unsafe and
safe replies, forwards, session loss, the data segment, push ordering and
counting, client answers, close resend and protocol violations.

## Limits

MClientReconnect matches three fresh native vectors byte for byte (two caps
inserted out of inode order with a native `_encode_filelocks` blob and two
realms, a `more` message, a realm-only message; see
`internal/mds/testdata/mds-reconnect-native-provenance.json`); ordering,
splitting, OPEN/CLOSE answers and resubmission have synthetic coverage.

Live recovery acceptance (product `494071b`, both profiles above, five
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
a replayed inode before that request's reply; `Arrival` orders them. Not
exercised live: a REPLAY answered from the completed-request record, a lost
reply of a delivered request, reconnect denial or a missed window, standby
takeover, multiple ranks, split reconnect messages and non-root realms. No path walking, inode or dentry cache, capability bookkeeping, lease
expiry, snapshot realm tree, dirty metadata or data writeback, file locks,
reconnect after MDS failover, request replay, delegated inodes or async
operations are implemented. File data I/O semantics (layout, striping,
flush ordering against capabilities) remain the consumer's.
