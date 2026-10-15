# Explicit raw MDS commands

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code and transitive dependencies
remain native Go with CGO disabled; no Ceph CLI, librados, go-ceph, installed Ceph
or native library is used at runtime. Native tools are separate development
verification infrastructure. This contract adds daemon-local command transport,
not a CephFS application session or filesystem workflow.

## Opt-in, target and local identity

Set `Options.EnableMDS = true` before Dial. `OpenMDS(ctx, MDSTarget)` uses the
same client's MON-issued MDS service2 ticket and authenticated GlobalID. The
option independently adds service2 to AUTH/MGR; EnableOSD independently adds
service4. EnableMDSMaps remains an independent MON map-encoding/subscription
option. Enabling MDS commands copies no OSD profile or MON admission features
to the MDS and does not broaden MON HASHPSPOOL admission. It does not require
EnableOSD or an OSD ticket.

`MDSTarget` requires explicit nonnegative `FilesystemID` and active `Rank`, plus
a numeric v2 `Address` with an explicit `/nonce` suffix. `/0` is valid; omission
is refused. An optional `v2:` prefix is accepted. IPv6 must be bracketed and
unscoped; unspecified addresses, DNS names, v1 addresses, invalid IDs/nonces and
zero ports are refused before dialing. The wire address retains its exact
family/nonce. The caller obtains and selects the target; there is no filesystem,
name, daemon or rank discovery, fallback or wildcard expansion.

`FilesystemID` is **local routing metadata only**. It is not encoded into the
CephX authorizer, ServerIdent or MCommand and is not an authenticated FSCID
assertion. The cluster FSID in MCommand comes from the authenticated client;
it supplies no filesystem identity. ServerIdent identifies the expected active
MDS **rank**, including rank0; it is not a daemon GID. The advertised address,
nonce, rank and key/transcript proofs are checked. MDS service keys are shared:
this is not cryptographic proof of one individual daemon GID or a filesystem's
permission isolation.

`MaxMDSConnections` defaults to16 and accepts1 through1024. It bounds cached
handles and concurrent setups independently of the shared MaxInFlight. The
cache key is `(FilesystemID,Rank)`. The same key/exact decoded wire address
shares one handle; its original caller target remains immutable. An equivalent
address with/without the v2 prefix can reuse it. A different exact address for
the same key returns `ErrMDSTargetChanged` until the old handle is closed.
A different FSCID is a distinct local cache key even at the same daemon; this
makes no claim that the daemon authenticates those local labels.

`MDSCommandResult` embeds raw `Result{Data,Message,Code}` and a local
`MDSCommandSubmission{Transaction,GlobalID,ConnectionGeneration,Target}`. No
map epoch or wire FSCID is fabricated. The original target and GlobalID remain
in a sent command's error receipt. TIDs increase across every MDS handle of
this Client; connection generations increase across explicitly opened handles.
Neither counter wraps. Exhaustion refuses a new admission without transmission.
A zero transaction means no Session.Call transaction was allocated.

## Fresh setup and raw wire

OpenMDS uses a fixed private client dial/setup helper and
`session.HandshakeFreshMDS`; ordinary MON/MGR/OSD opens keep their existing lossy
policy. The MDS connection is fresh stateful Messenger: flags0, nonzero fresh
client cookie, expected rank and matching address, peer flags0/nonzero server
cookie, secure CephX nonce/transcript verification and the caller-selected
CRC/secure framing mode. Fixed common features and MSG_ADDR2 are negotiated;
no map, CephFS client-feature or replicated-OSD capability is copied. Cookies
are not retained for reconnect/resume. Existing recovery/reset tags are refused.

OpenMDS's context owns setup and a concurrent-setup wait, not a successful
handle's lifetime. Failed setup keeps its slot until connection cleanup is
joined. Client shutdown cancels and joins setup; a waiting caller's cancellation
does not close another caller's setup. `Done()` is the one admitted Session's
terminal channel. A failed handle remains cached and terminal until Close;
OpenMDS never silently replaces it or redials it.

`MDSConnection.Command(ctx, Command)` sends exactly one raw type97 MCommand with
canonical header v1/compat1, priority127 and a Session.Call transaction. Its
front is cluster FSID16, string-vector count1 and the unchanged JSON bytes;
Input occupies the raw data segment. JSON validation requires an object with a
nonempty string prefix, without command construction, schema rewriting or
operational policy. User buffers must stay stable until Command returns; queued
frames own copies before publication to the writer.

The MDS-only path requires type98 MCommandReply v1/compat1, empty middle segment
and configured whole-frame limits before invoking the shared reply-body decoder.
The body preserves signed Linux wire code and raw server text; Data retains
binary output. Negative codes return `*CommandError` alongside that Result.
There is no host errno translation, command-specific schema interpretation or
automatic retry for negative/EAGAIN responses. Invalid header/body replies fail
the session and retain an OutcomeUnknownError for a sent request. Raw data is
preserved when the matched received reply reaches local validation; malformed
Messenger frames can fail earlier without a returned payload.

MCommand and MCommandReply constructors leave compat0. Native `Message::encode`
normalizes that unset compatibility to version1 on its fresh empty-payload
branch. The explicit MDS command path selects canonical1/1, and rejects compat0;
shared Tell codecs for other daemons are unchanged. The source is
`messages/MCommand.h`, `messages/MCommandReply.h` and `msg/Message.cc:237-251`.

## MDS dispatch and authority boundary

Pinned `mds/MDSDaemon.cc:733-780` accepts MCommand on an authenticated but closed
CephFS Session object. It does not require the product to send SESSION_OPEN.
Without a live application session it marks the connection disposable before
command dispatch. Native disposable handling makes it lossy for recovery; it
does not guarantee immediate close after one reply. An established connection
can serve another command if the daemon leaves it open. Without
`EnableCephFS`, any later loss is terminal and is never resumed or replayed by
this client. With `EnableCephFS` the connection is a lossless CephFS client:
transport faults resume the Messenger session (`MDS_CEPHFS_CONTRACT.md`), so a
sent command can still complete; a failed resume keeps the unknown outcome.

The native command gate is `session->auth_caps.allow_all()`. A read-only MDS
capability can authenticate but returns signed `-13` for a command. Importantly,
`MDSAuthCaps::allow_all` checks a grant's allow-all permission and its unrestricted
UID/path; `MDSAuthCaps.h:140-143` does not include fs_name in `is_match_all`.
An fsname clause on such a grant is therefore **not filesystem command
isolation**. Neither capability text nor local FilesystemID should be presented
as that guarantee. Provisioning, capability policy and command selection belong
to the consumer; the product preserves native decisions and raw responses.

## Admission, cancellation, renewal and shutdown

MaxInFlight is shared with other client commands/requests. A command waiting for
a slot respects its operation context and consumes no TID. Closed handles and
already canceled contexts are checked before JSON validation or buffer copying.
Configured bounds are checked before copying input; ordinary sessions enforce
frame bounds as well. Canceling a local wait does not cancel or roll back native
execution. Once transmission starts, cancellation, transport loss, client/handle
Close or an undecodable response preserves OutcomeUnknownError and original
submission identity. Earlier queued/unstarted work remains a known unsent
outcome. No unknown mutation or raw command is ever re-executed automatically.

MDS ticket expiry/renewal participates in the same selected-service schedule as
AUTH/MGR/OSD. `Snapshot().MDSTicket` exposes only Expires/RenewAfter and is zero
without EnableMDS; keys and opaque proofs remain private. Ticket metadata is
published atomically with the client auth snapshot. Renewal with the same
GlobalID neither reconnects nor reauthenticates an established MDS transport.
If the authenticated client GlobalID changes, existing MDS sessions retire
before the new identity is admitted, then fail with ErrMDSIdentityChanged.
Pending sent calls preserve uncertainty and their old identity. The caller must
Close the old handle and explicitly choose whether to open another one.

`MDSConnection.Close` cancels local handle work, joins any setup and transport
cleanup, and releases its cache slot. Client.Close owns every admitted MDS
session and in-progress setup and waits for cleanup. Commands do not discover
replacement targets, follow redirects, resume stateful sessions, open CephFS
sessions or replay messages after MON/MDS failover.

## Evidence and limits

Authenticated synthetic tests cover CRC and secure framing, independent MDS
service selection, rank0/stateful cookies and exact address binding, explicit
nonce0/17, wrong rank/nonce/GlobalID/lossy-policy refusal, signed results and raw
binary outputs, input ownership, sixteen concurrent out-of-order replies,
strict malformed headers/bodies, bounds, MaxInFlight, cached slots/setup waits,
cancellation, handle/client Close, abrupt loss and no reconnect/replay. Separate
MON candidate-install tests exercise selected MDS-ticket renewal and changed
GlobalID retirement; counter exhaustion and zero disabled ticket state are
checked. Synthetic peers are policy/transport evidence, not native daemon
interoperability or a permission-isolation proof.

Actual native command acceptance and final CI are recorded separately. The
initial native fixture is deliberately MDS-only: EnableMDS with the default
ObjectHash OSD profile, without EnableOSD/EnableMDSMaps, controlled legacy CRUSH
and no HASHPSPOOL, one OSD, metadata16PG/data1PG. This does not establish arbitrary
pool/map topology, default optimal CRUSH, EC or broad filesystem deployment
compatibility. CephFS OPEN/CLOSE/RECONNECT, requests/replies, capabilities,
leases, snapshots, file I/O, rank recovery and typed filesystem workflows remain
unsupported.

Ceph source files cited here carry LGPL-2.1 notices. The implementation independently
translates protocol facts; it incorporates no upstream native implementation.
