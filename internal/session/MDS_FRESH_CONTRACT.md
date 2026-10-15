# Fresh MDS Messenger connection foundation

This is an internal connection foundation. Native fresh authentication and
keepalive passed on the two bounded configurations below. It adds no public
MDS connection API or CephFS OPEN/session handling.

`HandshakeFreshMDS` owns the supplied connection and closes it on failure. Its
role is fixed to MDS (2); the caller supplies a nonnegative active rank. Rank 0
is checked exactly. Standbys and standby-replay identities are outside this
profile. The target must include the intended wire address and nonce. The
returned transport proves only completion of this one authentication and
Messenger identification exchange. A setup context stops governing the
connection after success; the owner must close it on failure or shutdown.

The MDS CephX service key is shared by that service. Its proof does not
cryptographically identify one daemon GID. The caller's owned map supplies the
daemon GID and target address; ServerIdent is checked against the active rank,
not that GID. The handshake carries no independent FSCID assertion.

The client uses fresh address/global-sequence randomness, a nonzero random client
cookie and flags 0. No connect sequence or receive sequence is sent in
ClientIdent. ServerIdent must include the target address, identify the exact
rank, satisfy both feature directions, have flags exactly 0 and a nonzero server
cookie. Cookies are not exposed or retained for a subsequent connection. Feature
support is exactly `Features`; required features are exactly MSG_ADDR2 (59).
MON admission, MON MDS-map opt-in and OSD profiles cannot widen these masks.
CRC and secure modes each offer one mode; their CephX authorizer, transcript
authentication and connection-secret requirements are unchanged.

Only fresh ClientIdent → ServerIdent admission is implemented. Reset, Retry,
RetryGlobal, ReconnectOK, SessionReconnect and Wait are terminal unexpected-frame
errors, including Wait during a fresh connection race. The function does not
redial, retain or resume a peer session, reset sequence state, or replay any
message. Established `Session` already fails on unexpected recovery tags and
never reconnects. Messenger ACK/keepalive remains transport metadata and does
not prove remote execution or durability. This stage sends no application
message, MClientSession OPEN, capability renewal, metadata operation or
CephFS feature declaration. Future callers must not treat a fresh connection
as reopening an earlier CephFS session, nor replay uncertain operations.

The original `Handshake` keeps its existing lossy policy for every role and
still refuses a stateful ServerIdent. Its callers and feature policy are
unchanged. No native dependency or public `cephmsgr` MDS connection API is added.

## Source provenance

Reviewed Ceph v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, LGPL-2.1 sources; no C++ implementation
was copied. The Go code translates these wire relationships:

- `src/ceph_mds.cc:189–190`, `src/msg/Policy.h:68–69`: CLIENT policy is
  `stateful_server(0)`, with `lossy=false`.
- `src/msg/async/ProtocolV2.cc:1946–1980`: no server cookie starts a new session
  with connect_seq 0; a stateful ClientIdent gets a nonzero client cookie and
  flags 0. `src/msg/async/frames_v2.h:619–665` defines ident fields.
- `src/msg/async/ProtocolV2.cc:2965–2989`: ServerIdent resets incoming sequence,
  creates a nonzero stateful server cookie and sends flags 0.
- `src/msg/async/frames_v2.h:28–43`: MSG_ADDR2 is required for Messenger address
  frames; the narrow frame encoding assumption is not a promise to implement
  every SERVER_NAUTILUS application protocol.
- `src/mds/MDSRank.cc:2310–2316`: ordinary ranked MDS identity uses rank;
  standby-replay uses daemon GID and is excluded here.
- `src/msg/async/ProtocolV2.cc:2039–2150`: Reset/Retry/RetryGlobal/ReconnectOK
  belong to reconnect; Wait also occurs during initial connection races.
- `src/mds/MDSDaemon.cc:1225–1238`: accepting an authenticated connection creates
  a closed Session; explicit OPEN is required before adding it to SessionMap.

## Verification boundary

Focused Go tests use actual `ServiceAuth` authorizer proofs with synthetic
AES/AES256K service tickets in CRC/secure transcripts. They check role, rank 0,
address, fixed masks, cookies/flags, authorizer nonce and challenge, transcript
signature, truncation, recovery-tag refusal, cancellation and transport lifetime.
They are separate from native interoperability evidence. Actual Tentacle
20.2.4 fresh admission and authenticated keepalive passed on CRC/AES host IPv4
with one BlueStore OSD (2.42s) and secure/AES256K container IPv6 with one
MemStore OSD (2.21s). Both retained exact rank 0/address, one MON and one MDS
dial, actual client/service key types, a matching keepalive ACK, and no opened
native CephFS session while the connection remained live. Each run removed its
owned filesystem, two pools and dedicated identity, and terminated all nine
owned containers. The native server uses the provider's default MDS mode
policy; the client offers and verifies exactly the selected mode. Server
mode exclusivity, authentication renewal/failover, MDS recovery and I/O are
not established. See [actual evidence and first failures](../../integration/fixture/MDS_FRESH_VALIDATION.md).
