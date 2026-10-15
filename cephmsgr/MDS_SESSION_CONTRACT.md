# CephFS MDS client session

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code stays native Go with
CGO disabled. This stage adds the CephFS client session (MClientSession,
type 22) on an explicitly targeted `MDSConnection`. It adds no metadata
request, capability, lease, snapshot realm, quota, reconnect, reclaim or file
I/O. Typed filesystem workflows remain in the consumer layer.

## Wire

`internal/mds/session.go` independently translates `messages/MClientSession.h`,
`include/ceph_fs.h`, `mds/mdstypes.{h,cc}`, `mds/MDSAuthCaps.h` and
`mds/cephfs_features.h` (LGPL-2.1; no code copied).

- REQUEST_OPEN is header v7/compat1, priority 127: the 28-byte packed head
  (op 0, seq 0, zero stamp, zero caps/leases), metadata in key order, the
  declared feature bitset, an empty metric spec (v1/compat1 envelope), flags 0,
  no cap auths and oldest client TID `UINT64_MAX`.
- REQUEST_RENEWCAPS, REQUEST_CLOSE and FLUSHMSG_ACK are head-only v1. With
  empty metadata and features the native encoder selects v1, so
  `oldest_client_tid` is never transmitted on these messages.
- Server messages are decoded only as v1 (head) or v7 (metadata, supported
  features, metric spec, flags, cap auths, oldest TID), compat1, no
  middle/data segment and no trailing bytes. Pinned MDS producers emit only
  these two versions; other versions are refused rather than guessed.
  Bitsets keep their raw little-endian `feature_bitset_t` bytes. Metadata,
  cap-auth, GID and bitset sizes are bounded before allocation.

Eight fresh native vectors from the pinned source (three client requests, five
server replies) match the Go encoder byte for byte and the Go decoder field by
field, including the 41-byte v2 message header for explicit offline sequence
inputs. See `internal/mds/testdata/mds-session-native-provenance.json`.

## Declared CephFS client features

REQUEST_OPEN declares exactly bits 0–8: the reserved release bits through
MIMIC. MIMIC selects v7 replies (REJECT `error_string`, MDS supported features
in OPEN), which are implemented. REPLY_ENCODING (9) and every later protocol
feature (reclaim, multi-reconnect, delegated inodes, metrics, notify session
state, 32-bit retry/forward, new snaprealm info, owner UID/GID, client-side
auth-cap checks, charmap, blockdiff) are withheld until their formats exist.
Callers cannot widen the set. A filesystem whose `required_client_features`
or `min_compat_client` requires a withheld bit makes the MDS REJECT the open
with `missing required features`, which is returned explicitly.

`Options.EnableCephFS` selects a separate CephFS profile with a larger,
equally fixed declaration for metadata requests; see `MDS_CEPHFS_CONTRACT.md`.
Without it, Messenger features are unchanged from the fresh MDS connection
(`MDS_FRESH_CONTRACT.md`); CephFS client features and Messenger features are
negotiated and reported separately.

## Public API

`MDSConnection.OpenSession(ctx, MDSSessionOptions)` owns at most one session
per connection (`ErrMDSSessionExists`). Options:

- `Metadata`: copied and sent unchanged. Nothing is added: no hostname, PID,
  version or entity ID is collected. The MDS interprets `root` (a claimed
  mount root checked against MDS path caps) and `uuid` (duplicate refusal).
- `RenewInterval` (required): while open or stale the library sends
  REQUEST_RENEWCAPS on this period, plus once immediately after OPEN and after
  STALE, as the native client does. The library holds no MDSMap; the caller
  chooses an interval below the filesystem's `session_timeout`.
- `MaxEvents` (default 64, at most 4096): the event FIFO bound.

OpenSession queues REQUEST_OPEN, then waits for OPEN. An explicit REJECT
returns `*MDSSessionRejectedError` (raw `error_string`, flags, blocklist flag,
metadata); a server CLOSE returns `ErrMDSSessionClosed`. Both are confirmed
refusals that leave no session. When ctx ends after the request was queued,
OpenSession returns the still-opening `*MDSSession` together with an
`OutcomeUnknownError`: the MDS may open the session later. The caller keeps
waiting with `WaitOpen` or ends it with `Close`.

`MDSSession` exposes `Status()` (state, local session generation, connection
generation, GlobalID, caller target, push sequence, renewal sequence, last
acknowledged renewal, OPEN reply, read-only flag, terminal cause), `Done()`,
`Err()`, `Next(ctx)`, `Renew(ctx)`, `AcknowledgeFlush(ctx, seq)` and
`Close(ctx)`. `MDSClientFeatures()` returns the declared bitset and
`MDSFeatureBitsetHas` tests raw bitsets (including MDSMap required features).

States: Opening → Open ⇄ Stale → Closing → Closed; Rejected; Lost. Closed and
Rejected are confirmed by the MDS. Lost means the transport ended first.

- OPEN publishes supported features, metric flags, flags, metadata and the raw
  cap-auth grants (UID −1 is "any"). Grants are not enforced locally. A later
  OPEN refreshes features and is delivered as an event; a nonzero OPEN
  sequence seeds the push sequence when it is still zero, as natively.
- RENEWCAPS replies are matched to locally sent sequences; a later reply covers
  older outstanding ones (at most 64 are tracked). `LastRenewal.Requested` is
  the local send time from which native clients extend capability TTL by the
  MDSMap session timeout. A reply for a never-sent sequence fails the
  transport. A matching reply to the newest renewal returns Stale to Open.
- STALE, RECALL_STATE (raw max caps/leases), FLUSHMSG (sequence), FORCE_RO and
  a later OPEN are queued as events in arrival order. Renewal replies are
  status, not events. The library never acknowledges a flush itself: the caller
  answers FLUSHMSG with `AcknowledgeFlush` after sending any cap releases it
  owns (none can exist in this stage).
- Event overflow fails the whole MDS connection (`ErrMDSSessionOverflow`):
  correctness events such as FLUSHMSG are never dropped. Already accepted
  events remain readable before the terminal cause.
- Server-only operations sent by the client side, unknown operations,
  renewals/stale/recall/flush before OPEN, malformed messages and any other MDS
  message type (capabilities, leases, snapshots, quotas, replies) fail the
  transport. Messenger ACK/keepalive does not prove session state.

`Close(ctx)` sends REQUEST_CLOSE once with the current push sequence and waits
for CLOSE. It returns nil after a confirmed CLOSE or REJECT; ctx expiry leaves
the session Closing and Close can be called again. The MDS marks the
connection disposable after CLOSE; the handle remains usable for raw commands
until the server or caller ends it, and a new session can be opened on it.

## Session identity and the address guard

The MDS binds a session to the client entity name (`client.<GlobalID>`), not
to a connection. `MDSDaemon::ms_handle_accept` attaches a newly accepted
connection from the same GlobalID to the existing session and moves the
session's outgoing messages there. Because this stage has no reconnect, the
library refuses to create that situation:

- While this Client's session at an exact MDS address (endpoint and nonce) is
  opening, open, stale or closing, `OpenMDS` refuses another handle to that
  address and `OpenSession` refuses a second session there
  (`ErrMDSSessionActive`). Handles accepted before the session opened keep
  their own closed server-side session object and stay usable for commands.
- When a session whose REQUEST_OPEN was queued ends Lost (transport loss,
  connection or client Close, GlobalID change, event overflow), the address is
  recorded as unresolved for the life of the Client and the same refusal
  applies. `Close` on a Lost session returns an error wrapping
  `ErrMDSSessionActive`. A separate Client (new GlobalID) is the explicit way
  to open another session.

An unresolved remote session is not closed by the library. Without renewals
the MDS evicts it after `session_autoclose`; with Ceph's default
`mds_session_blocklist_on_timeout` the session's address is blocklisted.
With the default `mds_defer_session_stale=true` a session holding no caps
and no pending revoke is never marked STALE first; STALE after
`session_timeout` applies to sessions with caps or with deferral disabled. Callers should Close sessions before
closing connections or the Client; `Client.Close` does not perform a graceful
CephFS close.

Each Messenger connection of this client uses its own random address nonce.
An eviction blocklist therefore names the MDS connection's address, not the
client's MON/OSD connections. That is acceptable only because this stage
performs no CephFS data I/O; coupling OSD writes to a CephFS session (M2) must
first bind OSD connections to the blocklisted session identity.

## Server behavior relied on

- `mds/Server.cc:585-860` (handle_client_session): sessionless and fsname
  capability REJECTs, `refuse_client_session`, blocklist REJECT with
  SESSION_BLOCKLISTED, required-feature REJECT, root-claim and duplicate-uuid
  REJECTs, journaled OPEN, RENEWCAPS echo of the request sequence with
  stale→open resumption, REQUEST_CLOSE dropped unless its sequence equals the
  push sequence.
- `mds/Server.cc:900-1030` (_session_logged): OPEN with supported features,
  metric spec and cap auths for MIMIC clients; CLOSE after a journaled close,
  connection marked disposable; killed (evicted) sessions are marked down
  without CLOSE, which this client observes as Lost.
- `mds/Server.cc:1190-1320` (find_idle_sessions): autoclose eviction after
  `session_autoclose`; STALE after `session_timeout` only when the session
  holds caps or is being revoked, unless `mds_defer_session_stale` is false.
- `mds/Server.cc:1778-1805` (update_required_client_features): adding a
  required client feature evicts open sessions that lack it, so a filesystem
  policy change can end a live session (observed as Lost).
- `mds/MDSRank.cc:1523` (send_message_client_counted): the push sequence counts
  capability, lease, snapshot and quota messages, all unsupported here.
- `client/Client.cc:2506-2690,3505,7459`: native open/close/renew sequencing
  used as the behavioral reference; no client code is copied.

## Evidence and limits

Synthetic authenticated peers cover the exact REQUEST_OPEN bytes, immediate
and periodic renewals, explicit renewal, stale resumption, event order,
FORCE_RO, flush acknowledgement, push-sequence close, reject reasons, server
CLOSE before OPEN, canceled open with a retained handle, protocol violations,
overflow, the address guard, unresolved addresses and connection close. They
are policy evidence, not native interoperability.

Live Tentacle 20.2.4 acceptance passed all nine leaves on CRC/AES host IPv4
with one BlueStore OSD and on secure/AES256K container IPv6 with one MemStore
OSD: native `session ls` metadata and feature bits (`0x1ff`), OPEN feature
and metric images equal to the native vector, cap auths derived from the
identity's grant, periodic renewal, STALE and automatic resumption (with
`mds_defer_session_stale=false` and 30 s/120 s timeouts), confirmed close,
the refuse-session, missing-required-feature and foreign-filesystem REJECT
texts, and eviction observed as Lost with the exact address blocklisted and
the address guard engaged. See
[the validation record](../integration/fixture/MDS_SESSIONS_VALIDATION.md).
RECALL_STATE, FLUSHMSG, FORCE_RO, nonzero push sequences, root-claim and
duplicate-uuid rejects, canceled opens, autoclose eviction, failover and
multiple ranks were not exercised live. Multi-rank, standby/failover,
reconnect/reclaim, eviction recovery, metrics, capabilities and data I/O are
not implemented.
