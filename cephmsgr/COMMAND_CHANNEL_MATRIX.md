# Raw command channel matrix

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
This source inventory was reconciled on 2026-10-23 for follow-up #63 against
both `integration/` and `cephmsgr/` test bodies. It maps each channel to its
public API, wire message, target rule and actual selectors. An inventory row
does not execute a test or establish native acceptance. Execution receipts,
their source revision and prerequisites are listed separately below.
Command construction, schemas and output interpretation stay in the consumer
layer; the tests build commands only to verify transport.

Integration tests run on the disposable fixture (`CEPH_MSGR_*` environment, see
[integration/fixture/VALIDATION.md](../integration/fixture/VALIDATION.md));
some need opt-ins or profiles stated in the test (`t.Skip` message). A channel
that works does not prove another channel: each row stands alone.

| Channel | Public API | Wire message | Target rule | Contract / validation | Integration tests |
| --- | --- | --- | --- | --- | --- |
| MON command | `Client.MonCommand` | MonCommand (50) / reply (51) | Connected MON session | [SPEC.md](../SPEC.md), [native applied mutation](../integration/fixture/MON_COMMAND_OUTCOME_VALIDATION.md) | `TestCephConfigCommandsIntegration`, `TestCephCommandErrorsAndBinaryOutputIntegration`, `TestCephBinaryBulkInputIntegration`, `TestCephReadOnlyPermissionsIntegration`, `TestCephCommandDescriptionsIntegration`, `TestCephLargeMonitorReplyIntegration`, `TestCephLostMutationReplyIntegration`, `TestCephAppliedMonitorMutationIsNotReplayedIntegration`, `TestCephMonitorWriteStallIntegration` |
| MGR command | `Client.MgrCommand` | MgrCommand (0x709) / reply (0x70a) | Active MGR from MgrMap | [SPEC.md](../SPEC.md), [native applied mutation](../integration/fixture/MGR_COMMAND_OUTCOME_VALIDATION.md) | `TestCephMgrModulesIntegration`, `TestCephCommandDescriptionsIntegration`, `TestCephAppliedManagerMutationIsNotReplayedIntegration`, `TestCephManagerSetupFailoverIntegration`, `TestCephNaturalManagerFailoverIntegration`, `TestCephManagerReceiveStallIntegration`, `TestCephCloseDuringManagerHandshakeIntegration` |
| Current MON/MGR Tell | `Client.MonTell`, `Client.MgrTell` | Tell command (97) / reply (98) | Currently connected MON / active MGR | [SPEC.md](../SPEC.md) | `TestCephTellRecoveryIntegration`, `TestCephTellDescriptionsIntegration` |
| Exact MON Tell | `Client.MonTellTo` | Tell command (97) / reply (98) | Caller names one MON; no fallback | [SPEC.md](../SPEC.md) | `TestCephNamedMonitorTellIntegration` |
| Exact MGR Tell | `Client.MgrTellTo` | Tell command (97) / reply (98) | Caller names the active MGR; standbys refused locally (pinned source constraint, never rerouted) | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md), [validation](../integration/fixture/MGR_NAMED_TELL_VALIDATION.md) | `TestCephNamedManagerTellIntegration` (subtests: exact active, standby refusal, signed unknown command and read-only denial, mixed shared session, ticket renewal keeps the daemon, failover needs a new name, cancel before send and close) |
| OSD command | `OSDConnection.Command` | MCommand (97) / reply (98) on the OSD session | Caller opens the OSD connection (opt-in); no wildcard or routing | [OSD_COMMAND_VALIDATION.md](../integration/fixture/OSD_COMMAND_VALIDATION.md) | `TestCephOSDCommandsIntegration` (subtests: exact commands, read-only refusal, concurrent commands with object reads, paused-OSD cancellation and Close are uncertain and never resent), `TestCephOSDCommandCapsChangeIntegration`, `TestCephOSDCommandCapsRenewalIntegration` |
| PG command | `OSDConnection.PGCommand` | MCommand (97) with `pgid` in the JSON | Caller-selected primary; non-primary and missing PG refused by the OSD and not retried | [OSD_COMMAND_VALIDATION.md](../integration/fixture/OSD_COMMAND_VALIDATION.md) | `TestCephOSDCommandsIntegration` (subtests: PG query on primary, routing refusals are not retried), `TestCephOSDScrubListIntegration` |
| MDS command | `MDSConnection.Command` | MCommand (97) / reply (98) on the MDS session | Explicit filesystem ID, rank and numeric address with nonce (opt-in `EnableMDS`); no discovery | [MDS_COMMAND_CONTRACT.md](MDS_COMMAND_CONTRACT.md) | `TestCephMDSCommandsIntegration` (requires `CEPH_MSGR_TEST_MDS_COMMANDS=1`; subtests: raw native output, signed unknown prefix, read-only denial, concurrent transactions, target cache limits, pre-canceled command not submitted, ticket renewal and new connection, sent cancel/close preserve an unknown submission, closed-handle refusal) |

Cross-channel behavior covered by common tests (not per channel): rejected,
revoked, replaced and renewed credentials (`TestCephRejectedKeyIntegration`,
`TestCephRevokedCredentialRecoveryIntegration`, `TestCephReplacedCredentialIntegration`),
context admission and recovery (`TestCephContextAdmissionIntegration`,
`TestCephContextRecoveryIntegration`).

## Applied-mutation and no-replay evidence

| Channel / selector | Profile and opt-in | Independent observer and outcome | Remaining boundary |
| --- | --- | --- | --- |
| MON / `TestCephAppliedMonitorMutationIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture; actual first receipt uses existing broad `mon-mgr.aes.ip4.crc.host`. Local focused reproduction may set `CEPH_MSGR_TEST_RUN=^TestCephAppliedMonitorMutationIsNotReplayedIntegration$` | Closed Python librados config-key get on an initially absent owned key; raw signed code, binary bytes and status retained for absence, A, distinct B, recovered B and fresh native cleanup absence. Caller remains `OutcomeUnknownError`; CRC test instrumentation binds the original and recovery command TID/sequence to distinct authenticated connections | [Run 39926008773](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39926008773) passed this exact test on `fa71fb23a132297a07209d1f8e6564bdec24c492`; see the validation record. Local complete writes are not server execution counts. Secure-mode frames have no decoded TID receipt; no universal exactly-once claim |
| MGR / `TestCephAppliedManagerMutationIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture; first target is existing broad `mon-mgr.aes.ip4.crc.host`. Local focused reproduction may set `CEPH_MSGR_TEST_RUN=^TestCephAppliedManagerMutationIsNotReplayedIntegration$` | Closed Python librados views per observation: `balancer status` (MGR effective mode), `config get` who=mgr key=mgr/balancer/mode (MON effective value) and `config dump` entries for that option (explicit presence), bracketed by unchanged active MGR name/GID. Baseline captured before A; A while its reply is withheld, distinct B before replacement, B after replacement under a different GID; caller remains `OutcomeUnknownError`. CRC test instrumentation binds the original and recovery MGR command TID/sequence to distinct connections. One native restoration (`config rm` or `config set` of the original entry) followed by fresh views | No exact-head native receipt is recorded in this file; see the validation record and PR. Local complete writes are not server execution counts. Secure-mode frames have no decoded TID receipt; the running MGR's module cache after a native `config rm` is source-inferred, not observed; no universal exactly-once claim |
| Current/exact MON and MGR Tell | Existing Tell selectors above and named-MGR profile prerequisites | Existing native read-only version/catalog comparisons, target changes/refusals, cancellation and Close | No applied mutating command with independent state, original TID and owned native restoration established here; audit a suitable pinned daemon-local operation before adding a case |
| OSD / PG / MDS command | Existing exact-target selectors and opt-ins above | Existing native command output/error comparison and sent cancellation/Close uncertainty | Those scenarios do not establish applied-mutation/reply-loss no-replay; a suitable operation and native owned-state/restoration cell remain #63 work |

The six previously omitted client-package tests were the applied MON/MGR
mutations, MGR setup failover, natural MGR failover, MGR receive stall and MON
write stall. The separate Close-during-MGR-handshake test is also included above.
They demonstrate existing code paths; adding their names is not a new run.
No product command API or automatic replay behavior is changed by this checkpoint.

## Remaining dedicated acceptance

These #63 cells remain separate from the implemented wire paths and historical
records. Absence of independent native acceptance does not establish a defect.

- **Lost reply after remote execution** already has applied MON and MGR
  scenarios. Both now have closed independent native observers. Current/exact
  Tell and PG
  have no independently observed applied-mutation case here. OSD and MDS cover
  cancellation/Close with an unknown outcome, not a reply lost after execution.
- **Native pre/postcondition cross-check and owned cleanup per mutating
  channel** remain for Tell, OSD, PG and MDS command.
- **Cancellation, shutdown and bounded large replies per channel:** large
  replies are tested for the MON channel only; named MGR Tell covers
  cancel-before-send and Close.
- **Unavailable target versus protocol refusal** has no per-channel acceptance
  statement in this audit; the named MGR Tell and PG command subtests exercise
  refusals only.
- **Subscription dependencies of operations** (ServiceMap MON failover, removal
  timing, large maps; map-version requests across a connected MON death) remain
  unverified, as stated in the ServiceMap and map-version contracts.

Boundaries: wildcard expansion, local admin sockets and HTTP module endpoints
are not missing Messenger codecs. The active-only MGR rule has a pinned-source
explanation and is kept explicit instead of rerouting. No channel has a
typed administrative API.

The guard test in `command_channel_matrix_test.go` fails if a test selector named
here no longer exists in either integration suite.
