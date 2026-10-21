# msgr2.1 frame tag ledger (client side)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`src/msg/async/frames_v2.h`, `ProtocolV2.cc`). Code: tag values in
[frame.go](../msgr/frame.go), handshake in [handshake.go](handshake.go),
established-session dispatch in [session.go](session.go).
`TestFrameTagLedgerCoversEveryTag` inventories source declarations and rejects
missing, duplicate, undeclared or renamed rows; it does not mirror a fixed enum.

This repository implements the **client** side only. "Sent" and "Received"
describe what the Go client does; a received tag that this client does not
expect in that phase is a terminal protocol failure (`ErrFrame`), never
ignored. Unit tests, live interoperability and CI are different evidence; the
Selector column names the unit tests, and live behavior is in the fixture
validation records. [PROTOCOL_EVIDENCE.md](../../cephmsgr/PROTOCOL_EVIDENCE.md#frame-evidence)
joins every tag to a caller boundary, concrete unit selector and retained
receipt or explicit unobserved native boundary. Captured headers, reported
native results and source-path inference are distinct observation kinds.
Nested authentication payloads, caller-visible ticket selection and their
source-attributed profile limits are in the
[CephX exchange ledger](../cephx/EXCHANGE_LEDGER.md).

| Tag | Name | Phase | Go client behavior | Refusal | Selector |
| --- | --- | --- | --- | --- | --- |
| 1 | Hello | Banner exchange | Sent first (own entity type and target address); one Hello received, entity type must equal the requested role | Wrong role, extra segments or trailing bytes | `TestCephCapturedBannerAndHello`, `TestHandshakeValidatesServerAddressIdentity` |
| 2 | AuthRequest | Authentication | Sent once with CephX method ID 2 and one requested connection mode (CRC or secure; never both) | n/a (sent) | `TestHandshakePinsModeAndRequiresSignature` |
| 3 | AuthBadMethod | Authentication | Received; decoded to a typed authentication error carrying the server method and int32 result code | Always terminal; no method downgrade or retry | `TestAuthenticationRejectionPreservesCodeAndValidatesFrame` |
| 4 | AuthReplyMore | Authentication | Received; the CephX state machine answers each challenge | Malformed payload is a protocol failure; at most 8 rounds | `TestCompleteMalformedCephXPayloadIsProtocolFailure` |
| 5 | AuthRequestMore | Authentication | Sent in answer to AuthReplyMore | n/a (sent) | `TestCompleteEncryptedMgrChallengeRetainsDecodeFailure` |
| 6 | AuthDone | Authentication | Received; global ID taken, selected mode must equal the requested mode, ticket/secret finished | Mode mismatch, missing secret in secure mode, or no AuthDone within 8 rounds | `TestCRCHandshakeCannotCompleteWithoutAuthDone`, `TestHandshakeRejectsTamperingAndDowngrade` |
| 7 | AuthSignature | Authentication | Sent and received in both modes; the client signs its received transcript and verifies the peer signature over its sent transcript with the CephX session key before session admission | Mismatch is an authentication failure | `TestHandshakePinsModeAndRequiresSignature`, `TestHandshakeRejectsTamperingAndDowngrade` |
| 21 | CompressionRequest | Post-auth | Sent only when the peer banner supports compression; offers snappy/zstd on CRC connections, declines on secure connections | n/a (sent) | `TestCRCHandshakeAuthenticatesAndRetainsCRCFraming`, `TestFrameCompressionRoundTrip` |
| 22 | CompressionDone | Post-auth | Received; a selected method must have been offered and implemented | Unoffered or unsupported method | `TestHandshakeRejectsUnofferedCompressionSelection` (complete CRC/zlib and secure/zstd selections); `TestCompleteMalformedHandshakePayloadIsProtocolFailure` (parser) |
| 8 | ClientIdent | Identification | Sent with the role's supported/required feature masks, session flags and cookie | n/a (sent) | `TestCRCHandshakeAuthenticatesAndRetainsCRCFraming` |
| 9 | ServerIdent | Identification | Received; target address must be among the server's addresses, daemon ID must match where required, features and flags must be acceptable | Missing target address, wrong ID, unmet required features, unsupported session flags | `TestHandshakeValidatesServerAddressIdentity`, `TestHandshakeRejectsUnsupportedSessionFlags` |
| 10 | IdentMissingFeatures | Identification | Received; `ErrFeatures` plus the peer's full 64-bit missing mask in error text (no structured mask field) | Always terminal; no silent downgrade | `TestHandshakeMissingFeaturesPreservesPeerMask` (complete CRC/secure refusals); `TestCompleteMalformedHandshakePayloadIsProtocolFailure` (parser) |
| 11 | SessionReconnect | Resume | Sent only for an explicit stateful resume (fresh MDS/CephFS), with the next global/connect sequences | n/a (sent) | `TestResumeHandshakeFollowsRetriesAndReset` |
| 12 | SessionReset | Resume | Received; terminal for the stateful session (`ErrSessionReset`), requests whose transmission began retain an unknown outcome | Always terminal | `TestStatefulSessionTerminalResumeOutcomes` |
| 13 | SessionRetry | Resume | Received; the connect sequence is advanced and the reconnect resent, bounded to 8 attempts | Sequence overflow or malformed body | `TestResumeHandshakeFollowsRetriesAndReset` |
| 14 | SessionRetryGlobal | Resume | Received; the global sequence is advanced and resent, bounded to 8 attempts | Sequence overflow or malformed body | `TestResumeHandshakeFollowsRetriesAndReset` |
| 15 | SessionReconnectOK | Resume | Received; resumes the stream and retransmits unacknowledged messages above the peer's sequence | Malformed body | `TestStatefulSessionResumesAndRetransmitsUnacknowledged` |
| 16 | Wait | Resume | Received during resume; refused | Always terminal (no wait/replace policy implemented) | `TestResumeHandshakeFollowsRetriesAndReset` |
| 17 | Message | Established | Sent and received; sequence, ACK sequence and stateful gaps are validated, duplicates are dropped, replies are matched to pending requests by type and transaction | Zero sequence, ACK beyond sent, gap in a stateful stream, wrong reply type | `TestCRCIntegrityPrecedesAbortedStatus`, `TestTruncatedResponseCannotEstablishCommandOutcome` |
| 18 | Keepalive | Established | Sent periodically; a received Keepalive is echoed as KeepaliveAck | Segment not exactly 8 bytes | `TestPeerKeepaliveEchoPreservesSecureCommandSequences` |
| 19 | KeepaliveAck | Established | Received; refreshes liveness (the keepalive timeout is the only liveness policy) | Segment not exactly 8 bytes | `TestKeepaliveAcknowledgmentsKeepAnIdleSessionAlive` |
| 20 | Ack | Established | Sent for received messages and received for sent ones; releases acknowledged messages | Sequence beyond sent, wrong length | `TestStatefulSessionResumesAndRetransmitsUnacknowledged` |

Not in the tag list: the banner exchange (`TestBannerFixtureAndFeatureRejection`),
the `FRAME_EARLY_DATA_COMPRESSED` preamble flag and aborted-frame handling
(`TestAbortedFrameDoesNotBreakStream`, `TestVerifiedAbortedCRCFramePreservesStream`).

Boundaries:

- **Missing wire code:** server-side tags are not implemented; this repository
  has no listening Messenger.
- **Upstream constraint:** compression is declined by pinned daemons toward
  client peers, so live compressed frames are unreachable (see
  [COMPRESSION_PLAN.md](../msgr/COMPRESSION_PLAN.md)).
- **Consumer responsibility:** reconnect policy above an established session.
  A terminal fault preserves whether transmission began; the library does not
  re-execute uncertain mutations.
