# CephX exchange and evidence ledger

Reference: Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964`.
Source audit: 2026-10-21. This joins the nested authentication exchanges to
their caller-visible boundary, implementation, refusal checks and retained
profiles. It does not add protocol support or report a new native run.

Pinned authorities: `src/auth/cephx/CephxClientHandler.cc`,
`CephxServiceHandler.cc`, `CephxProtocol.h`, `src/auth/Auth.h`,
`src/include/msgr.h` and `src/msg/async/ProtocolV2.cc`. These carry upstream
license notices; no implementation was copied. Authentication uses Messenger
AuthRequest/ReplyMore/RequestMore/Done frames, not message types 17/18. See
[the frame ledger](../session/FRAME_TAG_LEDGER.md) for transcript verification,
connection-mode admission and transport publication after CephX parsing.

## Caller and role boundaries

`cephmsgr.Dial` receives a programmatic identity/key and immutable
`Options.ConnectionMode`. Secure is the public default; CRC must be selected.
AUTH (`0x20`) and MGR (`0x10`) tickets are always requested; `EnableOSD` adds
OSD (`0x04`) and `EnableMDS` adds MDS (`0x02`) independently. Internal
`NewClient(..., 0)` means AUTH/MGR, not an empty mask. Unsupported masks and
unrequested service authorizers are refused. A ticket request or parsed ticket
does not establish permission, service availability or a completed operation.

`Snapshot` exposes GlobalID, ticket expiry/RenewAfter and a copied explicit
authentication rejection. It exposes no key, authorizer proof or connection
secret. Credentials/configuration loading and command retry policy belong to
the consumer. An uncertain mutation is never automatically replayed as part
of authentication renewal or session recovery.

## Exchanges and transitions

All selectors below are checked-in unit/synthetic tests unless explicitly
labeled as integration selectors. Names identify executable tests, not proof
that a native profile ran. `auth.go` supplies CephX state; the session's
`MonAuth` and `ServiceAuth` adapters put those payloads in Messenger frames.

| Exchange / transition | Wire version and implementation | Public boundary or explicit refusal | Unit selectors | Evidence |
| --- | --- | --- | --- | --- |
| Identity and service selection | `Client.Initial`, `NewClient`, `Services`: initial marker 10, CLIENT entity type 8, entity ID without `client.` prefix, previous GlobalID | Dial identity/key and EnableOSD/EnableMDS; non-client identities, unsupported keys and incomplete/unknown service masks rejected before proof generation | `TestInitialIdentityFixture`, `TestClientServiceMaskAndValueCopies`, `TestUnsupportedServiceMasksRejectBeforeProducingProof` | U1; N1/N2 establish selected MON/MGR use, N3 selected MDS use |
| Server challenge to AUTH request | `Client.Challenge`, `authRequest`: complete challenge v1, fresh client nonce, request `0x100`, Authenticate v3 including old-ticket v1 and `other_keys` | AES and aes256k proof paths; no mode/key fallback. All requested services travel with AUTH; this client emits no separate `0x200` principal-session-key request | `TestClientServiceMaskAndValueCopies`, `TestUnsupportedServiceMasksRejectBeforeProducingProof`, `TestLegacyAESFixture`, `TestRFC8009Derivation`, `TestRFC8009Encryption` | U1; key primitives and a source-derived request are not native encoder vectors |
| MON ticket reply and atomic publication | `Client.Finish`, `parseReplies`: reply `0x100`, signed int32 status, ticket collection/entries v1; positive seconds/nanoseconds validity, opaque ticket blob and service key | Preserve explicit server result in AuthenticationError. Parse every entry before replacing GlobalID/tickets; invalid later tickets cannot publish a partial update | `TestRejectedAuthReplyPreservesCodeWithoutPublishingState`, `TestTicketRenewalAndAtomicFailure`, `TestInvalidTicketValidityDoesNotPublishAuthentication`, `TestTicketReceiptIsLocalParseIdentityAndAtomicOnFailure` | U1; N1/N2 selected ticket use; a receipt is local parse identity, not server issuance identity |
| MON connection secret and extra service tickets | `Client.Finish`: encrypted connection-secret byte string, then extra tickets encrypted under AUTH session key | CRC permits absent or encrypted-empty secret; secure requires a valid secret of at least 40 bytes. Nonempty short secrets fail in both modes; CRC cannot bypass verification of extra tickets | `TestMONConnectionSecretModeRequirements`, `TestCRCAuthenticationRenewalVerifiesExtraTicketsAtomically` | U1; AES/aes256k x secret-mode cases are synthetic, not a native Cartesian product |
| Select service ticket and build authorizer | `Client.Authorizer`, `Authorizer.Payload(nil)`: header v1, GlobalID, single MGR/OSD/MDS ID, ticket blob v1, encrypted Authorize v2 | Missing/expired/unrequested/combined service authorizers fail with ErrTicket. An existing authorizer retains its ticket snapshot across client renewal | `TestAuthorizerRejectsUnsupportedOrUnrequestedService`, `TestOSDTicketRenewalAndServiceAuthorizer`, `TestMDSOptionalServiceAuthenticationRenewalAndAuthorizer` | U1; N1 selected MGR; N3 selected MDS; OSD historical evidence has the N4 attribution limit |
| Service challenge and challenge response | `Authorizer.Payload(challenge)`: decrypt complete challenge v1; Authorize v2 carries original client nonce and server challenge plus one | Wrong key/integrity, malformed or wrong-version challenge refused; no service-key substitution | `TestAuthorizerChallengeAndReply`, `TestOSDTicketRenewalAndServiceAuthorizer`, `TestMDSOptionalServiceAuthenticationRenewalAndAuthorizer` | U1; acceptance of the authorizer must not be confused with native negative-peer coverage |
| Service reply and nonce verification | `Authorizer.Finish`: encrypted AuthorizeReply v1 or v2, expected client nonce plus one; v2 carries connection secret | v1 is CRC-only; secure requires v2 and at least 40 secret bytes. Wrong nonce, malformed/version or nonempty short secret refused before session admission | `TestAuthorizerConnectionSecretModeRequirements`, `TestAuthorizerChallengeAndReply`, `TestMDSOptionalServiceAuthenticationRenewalAndAuthorizer` | U1; N1/N3 selected positive handshakes; malformed native-peer injection not claimed |
| Renewal, expiry and coordinator wakeup | Existing MON exchange updates immutable snapshots; validity yields RenewAfter before Expires. Native may encrypt renewed ticket blobs under previous service keys | Snapshot timing and AuthRejection; per-operation contexts and Close remain bounded. No automatic replay of an uncertain in-flight mutation | `TestFractionalTicketRenewalBeforeExpiry`, `TestTicketRenewalDeadlineWakeAndClose`, `TestTicketRenewalDeadlineBackoff`, `TestAlreadyDueTicketRenewalDoesNotSpin`, `TestOSDTicketRenewalAndServiceAuthorizer`, `TestMDSOptionalServiceAuthenticationRenewalAndAuthorizer` | U1/U2; N2 is the retained fractional native campaign. Available expiry/revocation selectors below are separate from successful native receipts |

U1 source: [auth.go](auth.go), [key.go](key.go), authentication/connection-secret/
service tests in this directory. U2 source:
[renewal_deadline_test.go](../../cephmsgr/renewal_deadline_test.go) and the
coordinator in `cephmsgr/`. These tests exercise deliberate synthetic replies;
they do not replace an independent native implementation or daemon oracle.
[MDS_SERVICE_CONTRACT.md](MDS_SERVICE_CONTRACT.md) adds the MDS-specific key and
service-mask boundaries. Malformed lengths, plaintext/version errors and
cryptographic failures have separate selectors in `length_error_test.go`,
`plaintext_error_test.go` and `connection_secret_test.go`.

## Retained native profiles and attribution limits

These rows link observations already recorded elsewhere. Their tested source
identity and limitations remain those of the original record; none is a new
pass of this ledger commit. Common reference/image for the rows: Ceph 20.2.4
at the pinned source above and
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

| ID | Source identity and client profile | Actual observation and boundary | Record |
| --- | --- | --- | --- |
| N1 | `dec9084f0a858e51224f16a9178c1218f8dd8e12`: Linux/amd64 host, IPv4, CRC, AES client/service keys; `66084e170a7f5805bd9126ac28e9f08f8c92b25a`: Linux/amd64 container, IPv6, secure, aes256k client/service keys | Both Go-originated service registration tests passed with an independent librados observer and completed cleanup. This entails initial MON/MGR authentication for these two profiles; it does not establish renewal, authorizer rejection or a native encoder comparison | [MGR_SERVICE_VALIDATION.md](../../integration/fixture/MGR_SERVICE_VALIDATION.md) |
| N2 | `e0173c303a25125669069ca98779a6999e31844a` export: Linux/arm64 container on Docker Desktop, IPv4, secure, AES client/service keys, short-ticket fixture | 20/20 runs, 80 renewal cycles after correcting a native fixture expiry/rotating-key artifact. AUTH validity configured 6.5 s, MGR 12 s. Preserve the earlier refusal history; this is not every renewal/failover combination | [VALIDATION.md, fractional-ticket correction](../../integration/fixture/VALIDATION.md#2026-10-16-fixture-hardening-against-recurring-ci-flakes) |
| N3 | Corrected acceptance-file hash `4b1c7ddf57e15193cb71bf54fef368a22ab57ce7d95e21c19e0236267fe4e9c3`; auth source hash `37adecbd532342f78dc2f34066d477e7d591251c6f9864a87617caffafbf5caf`. CRC: Darwin/arm64 host, IPv4, AES/AES; secure: Linux/arm64 container, IPv6, aes256k/aes256k | Fresh MDS authentication and keepalive each passed. CRC reuses earlier acceptance source; only secure was rerun after mode-label correction. The record retains the separate hashes and cleanup receipts. This is neither a common final-SHA run nor CephFS session/recovery acceptance | [MDS_FRESH_VALIDATION.md](../../integration/fixture/MDS_FRESH_VALIDATION.md) |
| N4 | Historical `source-1` working tree, with migration edits; Darwin/arm64 host CRC and Linux/arm64 container secure, aes256k labels | Selected MON/OSD map/read/renewal tests reported passes, but the record does not identify a single committed tested tree. Retain as historical evidence; do not convert it to a current-SHA or complete OSD authentication acceptance claim | [VALIDATION.md, native executions](../../integration/fixture/VALIDATION.md#native-executions) |

Available integration selectors include
`TestCephFractionalTicketRenewalIntegration` (`short_tickets=1`),
`TestCephExpiredTicketRecoveryIntegration` and
`TestCephDiscardedTicketProofIntegration` (`expiry=1`),
`TestCephServiceKeyEpochIntegration` (`auth_epoch=1`),
`TestCephRevokedCredentialRecoveryIntegration` (ordinary profile), and
`TestCephMDSFreshIntegration` (`mds_fresh=1`). Inspect current catalog and fixture
guards before selecting them. Catalog membership is configuration, not a run
receipt; the N2 observation does not prove every selector in this list.

## Evidence boundaries

- [PROTOCOL_EVIDENCE.md](../../cephmsgr/PROTOCOL_EVIDENCE.md) now joins each
  frame, message, implemented-opcode and role-feature row to a public
  boundary/refusal, unit selector and
  retained native receipt or explicit gap. Its reference check does not turn
  historical sources or unjoined profiles into current-tree acceptance. MGR
  service evidence does not widen ordinary MGR transport, OSD or MDS masks.
- Missing independent codec vectors are distinct from implemented wire paths
  and from incomplete native failure/topology/platform profiles. A missing
  receipt does not establish a missing encoder or a product defect.
- The newly committed complete feature-mask and unsupported-compression refusals
  are synthetic peer checks. Pinned daemons declining compression toward
  clients remains an upstream constraint, not a request to enable secure/zlib
  compression. No daemon peer roles or consumer retry/routing policy are added.
- [FEATURE_SEMANTICS.md](../session/FEATURE_SEMANTICS.md) records every
  advertised bit's role obligations and alias/incarnation limits. The protocol
  evidence join now explicitly separates captured, reported, inferred and
  unobserved frame receipts. Broader native oracle/profile/failure acceptance
  remains separate from #4's implementation/evidence accounting.
