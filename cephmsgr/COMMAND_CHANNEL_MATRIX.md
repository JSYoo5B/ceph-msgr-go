# Raw command channel matrix

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
First deliverable of the raw command acceptance in #5: it maps each existing
command channel to its public API, message, target rule, contract and the
integration tests that exercise it, and lists what has no dedicated test.
It was built on 2026-10-21 from the code, contracts and test files; it
re-executes nothing, so it is **not** a new interoperability result.
Command construction, schemas and output interpretation stay in the consumer
layer; the tests build commands only to verify transport.

Integration tests run on the disposable fixture (`CEPH_MSGR_*` environment, see
[integration/fixture/VALIDATION.md](../integration/fixture/VALIDATION.md));
some need opt-ins or profiles stated in the test (`t.Skip` message). A channel
that works does not prove another channel: each row stands alone.

| Channel | Public API | Wire message | Target rule | Contract / validation | Integration tests |
| --- | --- | --- | --- | --- | --- |
| MON command | `Client.MonCommand` | MonCommand (50) / reply (51) | Connected MON session | [SPEC.md](../SPEC.md) | `TestCephConfigCommandsIntegration`, `TestCephCommandErrorsAndBinaryOutputIntegration`, `TestCephBinaryBulkInputIntegration`, `TestCephReadOnlyPermissionsIntegration`, `TestCephCommandDescriptionsIntegration`, `TestCephLargeMonitorReplyIntegration`, `TestCephLostMutationReplyIntegration` |
| MGR command | `Client.MgrCommand` | MgrCommand (0x709) / reply (0x70a) | Active MGR from MgrMap | [SPEC.md](../SPEC.md) | `TestCephMgrModulesIntegration`, `TestCephCommandDescriptionsIntegration` |
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

## No dedicated test found

These are the cases #5 requires that this audit did not find a test for. "Not
found" means the tests named above do not state them; it does not mean they
fail.

- **Lost reply after remote execution** is exercised for MON commands only
  (`TestCephLostMutationReplyIntegration`). MGR command, current and exact
  Tell and PG commands have no test that drops the reply of an executed mutation
  and checks the native postcondition without resending. OSD and MDS cover
  cancellation/Close with an unknown outcome, not a reply lost after execution.
- **Native pre/postcondition cross-check per mutating channel** is not stated
  for MGR command, Tell or PG command.
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
here no longer exists in `integration/`.
