# Raw command channel matrix

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
This source inventory was reconciled on 2026-10-23 for follow-up #63 against
both `integration/` and `cephmsgr/` test bodies. It maps each channel to its
public API, wire message, target rule and actual selectors. An inventory row
does not execute a test or establish native acceptance. Execution receipts,
their source revision and prerequisites are listed separately below.
The exact MGR Tell applied-mutation cell was added on 2026-11-10. The same
day the OSD, PG and MDS command rows were reconciled with their test bodies,
and the current MGR Tell cell, a secure mode of both MGR Tell cells and the
OSD, PG and MDS command cells were added.
On 2026-11-14 the OSD, PG and MDS command cells gained a secure mode, the
per-channel statements on cancellation and shutdown after a command was sent
and on an unavailable target versus a protocol refusal were reconciled with
the test bodies, and the uncovered cases that needed a daemon were added.
Command construction, schemas and output interpretation stay in the consumer
layer; the tests build commands only to verify transport.

Integration tests run on the disposable fixture (`CEPH_MSGR_*` environment, see
[integration/fixture/VALIDATION.md](../integration/fixture/VALIDATION.md));
some need opt-ins or profiles stated in the test (`t.Skip` message). A channel
that works does not prove another channel: each row stands alone.

| Channel | Public API | Wire message | Target rule | Contract / validation | Integration tests |
| --- | --- | --- | --- | --- | --- |
| MON command | `Client.MonCommand` | MonCommand (50) / reply (51) | Connected MON session | [SPEC.md](../SPEC.md), [native applied mutation](../integration/fixture/MON_COMMAND_OUTCOME_VALIDATION.md) | `TestCephConfigCommandsIntegration`, `TestCephCommandErrorsAndBinaryOutputIntegration`, `TestCephBinaryBulkInputIntegration`, `TestCephReadOnlyPermissionsIntegration`, `TestCephCommandDescriptionsIntegration`, `TestCephLargeMonitorReplyIntegration`, `TestCephLostMutationReplyIntegration`, `TestCephAppliedMonitorMutationIsNotReplayedIntegration`, `TestCephMonitorWriteStallIntegration`, `TestCephStreamCancellationIntegration` (subtest MON) |
| MGR command | `Client.MgrCommand` | MgrCommand (0x709) / reply (0x70a) | Active MGR from MgrMap | [SPEC.md](../SPEC.md), [native applied mutation](../integration/fixture/MGR_COMMAND_OUTCOME_VALIDATION.md) | `TestCephMgrModulesIntegration`, `TestCephCommandDescriptionsIntegration`, `TestCephAppliedManagerMutationIsNotReplayedIntegration`, `TestCephManagerSetupFailoverIntegration`, `TestCephNaturalManagerFailoverIntegration`, `TestCephManagerReceiveStallIntegration`, `TestCephCloseDuringManagerHandshakeIntegration`, `TestCephStreamCancellationIntegration` (subtest MGR) |
| Current MON/MGR Tell | `Client.MonTell`, `Client.MgrTell` | Tell command (97) / reply (98) | Currently connected MON / active MGR | [SPEC.md](../SPEC.md), [current-MON applied mutation](../integration/fixture/MON_CURRENT_TELL_OUTCOME_VALIDATION.md), [current-MGR applied mutation](../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md) | `TestCephTellIntegration`, `TestCephTellRecoveryIntegration`, `TestCephTellDescriptionsIntegration`, `TestCephAppliedCurrentMonitorTellIsNotReplayedIntegration` (current MON), `TestCephAppliedCurrentManagerTellIsNotReplayedIntegration` (current MGR), `TestCephSentTellCancellationIntegration` (subtests `MonTell_cancel`, `MonTell_close`, `MgrTell_cancel`, `MgrTell_close`; its own exact selector) |
| Exact MON Tell | `Client.MonTellTo` | Tell command (97) / reply (98) | Caller names one MON; no fallback | [SPEC.md](../SPEC.md), [native applied mutation](../integration/fixture/MON_TELL_OUTCOME_VALIDATION.md) | `TestCephNamedMonitorTellIntegration`, `TestCephAppliedMonitorTellIsNotReplayedIntegration`, `TestCephSentTellCancellationIntegration` (subtests `MonTellTo_cancel`, `MonTellTo_close`) |
| Exact MGR Tell | `Client.MgrTellTo` | Tell command (97) / reply (98) | Caller names the active MGR; standbys refused locally (pinned source constraint, never rerouted) | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md), [validation](../integration/fixture/MGR_NAMED_TELL_VALIDATION.md), [native applied mutation](../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md) | `TestCephNamedManagerTellIntegration` (subtests: exact active, standby refusal, signed unknown command and read-only denial, mixed shared session, ticket renewal keeps the daemon, failover needs a new name, cancel before send and close), `TestCephAppliedNamedManagerTellIsNotReplayedIntegration`, `TestCephSentTellCancellationIntegration` (subtests `MgrTellTo_cancel`, `MgrTellTo_close`) |
| OSD command | `OSDConnection.Command` | MCommand (97) / reply (98) on the OSD session | Caller opens the OSD connection (opt-in); no wildcard or routing | [OSD_COMMAND_VALIDATION.md](../integration/fixture/OSD_COMMAND_VALIDATION.md), [native applied mutation](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md) | `TestCephOSDCommandsIntegration` (subtests: exact commands, read-only refusal, concurrent commands with object reads, paused-OSD cancellation and Close are uncertain and never resent, which also refuses both command forms on the closed handle), `TestCephOSDCommandCapsChangeIntegration`, `TestCephOSDCommandCapsRenewalIntegration`, `TestCephOSDCommandCrashIntegration`, `TestCephOSDCommandCrashInFlightIntegration` (an acknowledged runtime `config set`, then a `bench` in flight while the OSD is killed or stopped), `TestCephOSDCommandFrameLimitIntegration`, `TestCephOSDScrubListIntegration` and `TestCephOSDScrubListPagingIntegration` (an acknowledged `injectdataerr` on a replica connection), `TestCephAppliedOSDCommandIsNotReplayedIntegration` |
| PG command | `OSDConnection.PGCommand` | MCommand (97) with `pgid` in the JSON | Caller-selected primary; non-primary and missing PG refused by the OSD and not retried | [OSD_COMMAND_VALIDATION.md](../integration/fixture/OSD_COMMAND_VALIDATION.md), [native applied mutation](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md) | `TestCephOSDCommandsIntegration` (subtests: PG query on primary; routing refusals are not retried, with the local refusal of a PG command naming another primary; the paused-OSD subtest's canceled PG command), `TestCephOSDCommandOutsideActingSetIntegration`, `TestCephAppliedPGCommandIsNotReplayedIntegration` |
| MDS command | `MDSConnection.Command` | MCommand (97) / reply (98) on the MDS session | Explicit filesystem ID, rank and numeric address with nonce (opt-in `EnableMDS`); no discovery | [MDS_COMMAND_CONTRACT.md](MDS_COMMAND_CONTRACT.md), [native applied mutation](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md) | `TestCephMDSCommandsIntegration` (requires `CEPH_MSGR_TEST_MDS_COMMANDS=1`; subtests: raw native output, signed unknown prefix, read-only denial, concurrent transactions, target cache limits, pre-canceled command not submitted, ticket renewal and new connection, sent cancel/close preserve an unknown submission, closed-handle refusal), `TestCephMDSCephFSRanksIntegration` (a read-only `status` to each rank target after a takeover), `TestCephAppliedMDSCommandIsNotReplayedIntegration` (the same opt-in, its own exact selector) |

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
| Current MON Tell / `TestCephAppliedCurrentMonitorTellIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture; selected AES/CRC IPv4 host reproduction alongside the named MON Tell parent | The same closed option oracle observes A with the shared primary reply withheld, independent B, primary-session loss/recovery and fresh exact-target reads of B; one native restoration with fresh baseline/provenance checks | [Current-MON validation recipe](../integration/fixture/MON_CURRENT_TELL_OUTCOME_VALIDATION.md); direct native execution on `b08048114455597695415a4713cecda963e1e75b` passed both required parents (current 0.82 s, named 0.71 s). Saved-primary identity/liveness is checked immediately before the controlled loss; recovery uses another session on the same MON. CRC logical writes across all observed peers remain local transmission evidence, not server execution counts. Other-MON recovery, secure identity, restart durability and other channels are unverified |
| Current MGR Tell / `TestCephAppliedCurrentManagerTellIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture. Required CRC row `mon-mgr.aes.ip4.crc.host.t-appliedcurrentmanagertell`, selector `^TestCephAppliedCurrentManagerTellIsNotReplayedIntegration$`; required secure row `mon-mgr.aes256k.ip4.host.t-suite-e9be0b`, selector `^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$`; also a candidate of the broad ordinary `mon-mgr` rows. No opt-in parameter | The body, native observer, option and bound of the exact-name cell below, submitted through `MgrTell`. The baseline belongs to the natively observed active manager. The observer is bound to the manager whose connection carried A: the fixture role of the dialed endpoint of the written frame (CRC) or of the padded write (secure) must be that manager, and every state is read on that daemon's own admin socket under its bracketing name and GID. The read-only control and the recovery read also go through `MgrTell`; the standby manager is an unchanged bystander. Caller remains `OutcomeUnknownError` without a target-change cause. CRC and secure modes as described for the exact-name cell. One native restoration, then a separate fresh read | Direct native execution only; see the [validation record](../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md). The cell keeps one manager active throughout, so it does not show what `MgrTell` does when the active manager changes while a reply is withheld; that is the [MGR command cell's](../integration/fixture/MGR_COMMAND_OUTCOME_VALIDATION.md) fault, on a different message. The receiving manager is identified by the fixture address of the dialed endpoint, not by a wire field. Other limits as for the exact-name cell |
| Exact MGR Tell / `TestCephAppliedNamedManagerTellIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture. Required CRC row `mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell`, selector `^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$`; required secure row `mon-mgr.aes256k.ip4.host.t-suite-e9be0b`, selector `^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$`; also a candidate of the broad ordinary `mon-mgr` rows. No opt-in parameter | Closed fixture control `observe-mgr-tell` on the named manager's own admin socket (a local Unix socket, independent of the client's Messenger connections): raw `config get` and the `config diff get` entry of the one option `log_max_recent`, each observation bracketed by two fresh native MgrMap reads of the available active name and GID. Baseline effective value and override presence captured before A; candidates A=baseline+1 and B=baseline+2 are refused unless the baseline is 1..19998. CRC: the reply to A's own transaction (type 98, the TID of the actually written type 97 frame) is decoded and held before any byte of it reaches the Go session; a read-only Tell on the same session passes the armed gate first, and A is then read natively once with the caller still pending. Secure: frames are not decoded; every byte received on the connection of the one padded write (at least 4096 bytes to a manager) is withheld from before that write leaves, A is awaited natively and fails on any state outside the change from the baseline to A or on the end of the caller, and the caller is checked to be still pending afterwards. In both modes B is written once through the admin socket, and only the subject connection is closed. The manager stays the available active daemon. Caller remains `OutcomeUnknownError` without a target-change cause; a new read-only invocation on the same client (another connection), an independent client and a fresh native read show B. CRC: exactly one complete local write of A, with its connection, transaction and sequence, across every subject connection. Secure: exactly one padded write across every subject connection, and the recovery read on a later connection to the same manager. The mutation named for the standby is refused locally with no write and the standby's own option is unchanged. One native restoration (`config unset`, or `config set` of the original override), then a separate fresh read of value and provenance | Direct native execution only; see the [validation record](../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md) for the tested commit. Process-local state on one unchanged manager incarnation: manager replacement or restart would clear it and is not this cell. The pre-existing-override restoration branch is covered by daemon-free fixture tests only. Local complete writes are not server execution counts. In secure mode no write, transaction or reply frame is identified: the withheld bytes are not attributed to a reply, and the count is of padded writes. Secure evidence is AES256K IPv4 host only. No universal exactly-once claim |
| MON Tell (exact target) / `TestCephAppliedMonitorTellIsNotReplayedIntegration` | Ordinary disposable three-MON/two-MGR fixture; first target is existing broad `mon-mgr.aes.ip4.crc.host`. Local focused reproduction may set `CEPH_MSGR_TEST_RUN=^TestCephAppliedMonitorTellIsNotReplayedIntegration$` | Closed Python librados views per observation through a named-target `mon_command` (Tell) to MON `b`: `mon_status` name/rank bracket, daemon-local `config get` and the `config diff get` entry of the one option `log_max_recent` (override presence/value, final value); no full configuration dump. Baseline effective value and override presence captured before A; candidates A=baseline+1 and B=baseline+2 are refused unless the baseline is 1..19998 (ceiling 20000). A while its reply is withheld by an exact-target gate that arms only on the first Tell (97) frame written to that MON, B explicitly established by a second client, then only the subject connection and wait end. The MON process stays alive; fresh Go and native sessions read B. Caller remains `OutcomeUnknownError`. One native restoration (`config unset`, or `config set` of the original override) followed by fresh views | [Run 40051643277](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/40051643277) passed this exact test on `08d45dac1ee50f34a5a99b812722e9b83b07d72b`; see the validation record. Process-local state: a MON restart would clear it, so a restart variant cannot prove the no-replay comparison and is not claimed. The same daemon incarnation is inferred from the surviving override B and the stable name/rank, not from a process ID. Local complete writes are not server execution counts. Secure-mode frames have no decoded TID receipt and use a size-based gate; CRC/AES is the only target. No universal exactly-once claim |
| OSD command / `TestCephAppliedOSDCommandIsNotReplayedIntegration` | OSD fixture (`CEPH_MSGR_TEST_OSD=1`, legacy-v6 profile). Required CRC row `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` and required secure row `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16`, both with selector `^TestCephApplied(OSD|PG)CommandIsNotReplayedIntegration$`. The broad OSD selection does not match the name, so no other row runs it | Closed fixture control `observe-daemon-command` on the target OSD's own admin socket: raw `config get` and the `config diff get` entry of the one option `log_max_recent`, each observation bracketed by two fresh native `osd dump` reads of that OSD (up, `up_from` epoch, public v2 address and nonce). The bracketing address, nonce included, must be the address the Go client dials. The command, bound and candidates of the MGR Tell cell, sent through `OSDConnection.Command` to the acting primary of the fixture PG. CRC: the reply to A's own transaction is decoded and held; a read-only command passes the armed gate first; A is read natively once with the caller pending. Secure: frames are not decoded; every byte received on the OSD connection of the one padded write (at least 4096 bytes to that OSD) is withheld from before that write leaves, A is awaited natively and fails on any state outside the change from the baseline to A or on the end of the caller, and bytes must then have arrived on that connection and been withheld. In both modes B is written once through the admin socket with the caller still pending, and only the subject connection is closed. Caller remains `OutcomeUnknownError` with an empty result and a retained submission transaction (CRC: equal to the written one; secure: nonzero, compared with no frame). A read on an explicitly reopened connection of the same client, an independent client and a fresh native read show B under the same incarnation. CRC: exactly one complete local write of A across every subject connection. Secure: exactly one padded write across every subject connection, and the recovery read on a later connection to the same OSD. One native restoration, then a separate fresh read | Direct native execution only; see the [validation record](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md). Process-local state on one unchanged OSD incarnation of a single-OSD fixture; an OSD restart would clear it and is the existing crash tests' subject, without a state comparison. Local complete writes are not server execution counts. In secure mode no write, transaction or reply frame is identified: the withheld bytes are not attributed to a reply, and the count is of padded writes. Secure evidence is AES256K IPv6 with a container test process only. No universal exactly-once claim |
| PG command / `TestCephAppliedPGCommandIsNotReplayedIntegration` | The same fixture, rows and selector as the OSD command cell | The same control with a PG target: of the primary's `pg <pgid> query` reply only the PG ID, the state string, whether a `scrubber` section exists and whether it lists `test_sequence` are kept, bracketed by the same `osd dump` reads. A is `scrubdebug <pgid> set sessions` through `OSDConnection.PGCommand` with the caller's explicit PG and primary; the baseline must be the unset flag. CRC: the reply to A's own transaction is held, and the counter is then listed natively with the caller pending. Secure: the padded-write mode of the OSD command cell; the listed counter is awaited natively (the flag has only its unset and set state). The native overwrite B is `scrubdebug <pgid> unset sessions`, and after the subject connection is closed the caller remains `OutcomeUnknownError`; a `query` on a reopened connection, an independent client and a fresh native read show the counter not listed. Exactly one complete local write of A (CRC) or one padded write (secure). One further native `unset`, then a separate fresh read | The state is one boolean of an upstream test hook, the only reversible, synchronously applied and independently observable PG-routed command at the pinned tag (the record lists the others). B equals the baseline, so the final restoration is not separately observable: the state after it equals the state before it. One primary on a single-OSD fixture; no replica or acting-set change. Local complete writes are not server execution counts; the secure mode identifies no frame, as for the OSD command cell |
| MDS command / `TestCephAppliedMDSCommandIsNotReplayedIntegration` | Owned MDS command fixture (`CEPH_MSGR_TEST_MDS_FRESH=1`, `CEPH_MSGR_TEST_MDS_COMMANDS=1`, single-OSD legacy-v6). Required CRC row `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` and required secure row `mds-commands.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand`, selector `^TestCephAppliedMDSCommandIsNotReplayedIntegration$`, the one further selector that fixture admits | The same control with an MDS target: the active MDS's own admin socket (`config get` and the `config diff get` entry of `log_max_recent`), bracketed by two fresh native `fs dump` reads of that daemon (`up:active`, GID, v2 address and nonce), whose address must be the address the Go client dials. The command, bound and candidates of the MGR Tell cell, sent through `MDSConnection.Command` with the dedicated `mds allow *` identity. CRC: held reply to A's own transaction, A natively with the caller pending, `OutcomeUnknownError` with the written transaction retained and exactly one complete local write of A. Secure: the padded-write mode of the OSD command cell on the MDS connection. In both modes one native overwrite B, B through a reopened connection, an independent client and a fresh native read under the same GID, one native restoration and a separate fresh read | Direct native execution only; see the [validation record](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md). One active rank-0 MDS incarnation; no failover, standby or multi-rank cell. Local complete writes are not server execution counts; the secure mode identifies no frame, as for the OSD command cell, and its evidence is AES256K IPv6 with a container test process only. No universal exactly-once claim |

The six previously omitted client-package tests were the applied MON/MGR
mutations, MGR setup failover, natural MGR failover, MGR receive stall and MON
write stall. The separate Close-during-MGR-handshake test is also included above.
They demonstrate existing code paths; adding their names is not a new run.
No product command API or automatic replay behavior is changed by this checkpoint.

## Audit of the OSD, PG and MDS command channels

Reconciled on 2026-11-10 against the test bodies, not the earlier rows.

| Channel | What the existing tests send and observe | What they did not establish | Result |
| --- | --- | --- | --- |
| OSD command | Read-only commands compared byte for byte with a native client; signed refusals; concurrency with object requests; a command to a paused OSD ended by cancellation or Close (uncertain and never resent); a command after and during an OSD kill or stop; a reply above the frame limit; capability changes. Two mutating commands are sent and acknowledged normally: a runtime `config set` before `bench` in `TestCephOSDCommandCrashInFlightIntegration`, and `injectdataerr` in the scrub-list tests | No mutating command whose reply is lost after it was applied, no independent native read of the changed state, and no state comparison across the loss: the in-flight crash tests restart the daemon, which discards runtime state | New cell `TestCephAppliedOSDCommandIsNotReplayedIntegration` |
| PG command | `query` on the primary compared with a native client; `-EAGAIN` on a replica, `-ENOENT` for an absent PG and for an OSD outside the acting set, each returned once and not retried. All read-only | Any mutating PG-routed command | New cell `TestCephAppliedPGCommandIsNotReplayedIntegration`, on the one reversible PG-routed state of the pinned OSD; the [record](../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md#command-choice) lists why the other PG-routed commands are unsuitable |
| MDS command | `version` and an unknown prefix compared with a native client; read-only denial; concurrency; target cache and limits; a pre-canceled command; ticket renewal; a `version` whose reply is paused and then ended by cancellation or Close (unknown outcome with its submission); `status` per rank. All read-only | Any mutating command, and therefore any applied state behind a lost reply | New cell `TestCephAppliedMDSCommandIsNotReplayedIntegration` |

Two corrections to the channel table came out of this audit.
`TestCephOSDScrubListIntegration` was listed under the PG command, but it
sends its deep scrub as a MON command and uses the OSD command channel only
for `injectdataerr`; it is now listed, with its paging variant, under the OSD
command. The OSD crash, in-flight crash, frame-limit and outside-acting-set
tests and the per-rank MDS `status` were missing and are listed. Adding these
names is not a new run.

## Remaining dedicated acceptance

These #63 cells remain separate from the implemented wire paths and historical
records. Absence of independent native acceptance does not establish a defect.

- **Lost reply after remote execution** has one bounded cell per channel:
  MON and MGR command, exact and current MON Tell, exact and current MGR
  Tell, and OSD, PG and MDS command. Each has a closed independent native
  observer and one owned native restoration. They are single cells, not
  acceptance of every command of a channel.
- **Secure-mode attribution:** the two MGR Tell cells and the OSD, PG and MDS
  command cells run in secure mode without decoded frames (withheld received
  bytes, padded-write counts). The MON and MGR command cells and the MON Tell
  cells keep their earlier secure limits. No secure cell identifies a write,
  a transaction or a reply frame; that needs frame-level instrumentation
  inside the session, after decryption, which no test has.
- **Identity of the natively mutated daemon:** the MGR Tell, OSD, PG and MDS
  observers send their one overwrite and one restoration only after a fresh
  map read shows the daemon incarnation that supplied the baseline, and
  report whether the reads after the call still show it. The MGR Tell, OSD
  and PG mutations use the daemon's admin socket, which is addressed by name
  and does not report that incarnation, so for those three a daemon replaced
  between that read and the call is reported afterwards, not prevented. The
  MDS mutations are sent by the native client as `tell mds.<GID>` with the
  baseline GID: a replacement MDS cannot receive them, the call fails and the
  receipt records it. MDS reads still use the admin socket.
- **Daemon replacement while a reply is withheld** is covered for MON and MGR
  commands only. The Tell and daemon command cells change process-local
  state, which a restart clears, so they keep the daemon and end only the
  connection.
- **Bounded large replies per channel:** large replies are tested for the
  MON channel (`TestCephLargeMonitorReplyIntegration`) and, as a refused
  reply frame, for the OSD channel
  (`TestCephOSDCommandFrameLimitIntegration`). Other channels have no such
  case.
- **Cancellation and shutdown after a command was sent** and **unavailable
  target versus protocol refusal** are stated per channel in the two sections
  below, with what each still lacks.
- **Subscription dependencies of operations:** the selected AES/CRC IPv4 host
  [ServiceMap lifecycle cell](../integration/fixture/SERVICE_MAP_LIFECYCLE_VALIDATION.md)
  keeps one Go watch through orderly Stop of its actual MON, requires a native
  registration created after recovery in a newer Go map, and bounds native/Go
  removal after explicit holder release. The
  [ServiceMap limits cell](../integration/fixture/SERVICE_MAP_LIMITS_VALIDATION.md)
  (`TestCephMonServiceMapLargeMapIntegration`, secure AES256K IPv4 container
  and CRC AES IPv4 host) admits a native map of about 1.58 MiB with its raw
  metadata, ends only the watch at a 256 KiB watch budget, and fails only the
  carrying MON session at a 1 MiB frame limit and at the fixed collection
  cap, with the client recovering each time. ServiceMap lifecycle across
  SIGKILL of the watch's MON, a killed holder and other profiles remain
  unverified. A request-specific CRC
  [map-version connected-Stop test](../integration/fixture/MON_MAP_VERSION_STOP_VALIDATION.md)
  preserves actual EOF after a withheld native reply, then checks a new
  explicit request on another admitted MON. Two
  [map-version kill cells](../integration/fixture/MON_MAP_VERSION_KILL_VALIDATION.md)
  (`TestCephMonMapVersionConnectedKillIntegration`,
  `TestCephMonMapVersionUnprocessedKillIntegration`; CRC, AES IPv4 host and
  AES256K IPv6 container) kill the connected MON with SIGKILL, once after a
  withheld reply and once while the out-of-quorum MON has read the request
  without answering. Secure-profile request-specific observation remains
  unverified.

## Cancellation and shutdown after a command was sent

Reconciled on 2026-11-14 against the test bodies. "Sent" has a different
meaning in each test, stated in the third column; none of them counts daemon
executions. A canceled or closed call that had started transmission must
return `OutcomeUnknownError` with its cause, and nothing may transmit the
command again.

| Channel | Caller cancellation after send | Close after send | How the test knows the command was sent |
| --- | --- | --- | --- |
| MON command | `TestCephLostMutationReplyIntegration`: `OutcomeUnknownError` with `context.Canceled`. `TestCephStreamCancellationIntegration` (MON): the same for a cancellation during a partial write, and the incomplete frame is not applied | No deterministic case. `TestCephRepeatedShutdownUnderTrafficIntegration` closes under traffic and accepts `ErrClosed` or a transport cause without knowing what was sent | A second client reads the applied `config-key` value before the cancellation; for the stream test, a relay stopped after one full chunk |
| MGR command | `TestCephManagerReceiveStallIntegration`: `OutcomeUnknownError` with `context.Canceled`; the next command on the stalled session is uncertain with `ErrKeepaliveTimeout`, and a later one is served by a new session. `TestCephStreamCancellationIntegration` (MGR) | No deterministic case, as for the MON command | One padded write of at least 4096 bytes on the MGR connection, then bytes received and withheld on it |
| Current MON Tell, current MGR Tell, exact MON Tell, exact MGR Tell | `TestCephSentTellCancellationIntegration` (`MonTell_cancel`, `MgrTell_cancel`, `MonTellTo_cancel`, `MgrTellTo_cancel`): `OutcomeUnknownError` with `context.Canceled` and an empty result; after the withheld connection is ended, a new invocation through the same API is answered, at the latest after one invocation that failed on the ended connection | The same test (`MonTell_close`, `MgrTell_close`, `MonTellTo_close`, `MgrTellTo_close`): `Client.Close` returns, the call ends with `OutcomeUnknownError` and `ErrClosed`, and a later invocation is `ErrClosed` without an unknown outcome | One padded write of at least 4096 bytes to the target's fixture endpoint (the connected MON, MON `b` or the active manager) returned, and bytes then arrived on that connection and were withheld. No write of that size precedes it. The command is a read-only `version`, so no daemon state is compared. After either ending the padded-write count is still one. CRC and secure rows; nothing is decoded |
| OSD command | `TestCephOSDCommandsIntegration` (paused-OSD subtest): a deadline on a command to the paused primary gives `OutcomeUnknownError` with a nonzero submission transaction and no result | The same subtest: `OSDConnection.Close` during a second command gives `OutcomeUnknownError` with `ErrClosed` | Not observed on the wire: inferred from the nonzero submission transaction, and for Close from a 200 ms wait. The command is read-only. "Never resent" is the product contract and the absence of a reconnect, not a counted write |
| PG command | The same subtest: a deadline on a PG `query` to the paused primary gives `OutcomeUnknownError` with `context.DeadlineExceeded`, a nonzero submission transaction and the caller's PG metadata | Not covered for `PGCommand`: the subtest's Close case sends a plain OSD command on the same handle | As for the OSD command |
| MDS command | `TestCephMDSCommandsIntegration` (`sent_cancel_preserves_unknown_submission`): `OutcomeUnknownError` with `context.Canceled`, the submission of a later transaction on the same connection generation, and no redial | The same test (`sent_close_preserves_unknown_submission`): `OutcomeUnknownError` with `ErrClosed` | A relay whose reads are paused reports a blocked read, and its write-chunk counter advanced |

Cancellation before anything is sent is a different result and is covered
where the tests could hold a command back: a pre-canceled or admission-held
MON command (`TestCephRecoveryIntegration`, `TestCephMapWaitIntegration`), a
MGR command during the MGR handshake or discovery
(`TestCephCloseDuringManagerHandshakeIntegration`,
`TestCephManagerAvailabilityIntegration`), a pre-canceled exact MGR Tell
(`TestCephNamedManagerTellIntegration`, on a name that is no longer active)
and a pre-canceled MDS command with an empty submission
(`TestCephMDSCommandsIntegration`). Each returns the context or `ErrClosed`
cause; the absence of an unknown outcome is asserted by all of them except
the pre-canceled MON command and the pre-canceled exact MGR Tell.

The applied-mutation cells above end their call by closing the test's own
connection wrapper, not by the caller: they show connection loss after an
applied command, which this table does not repeat.

Not covered: `Client.Close` with a sent MON or MGR command outstanding as a
deterministic case; Close during a sent PG command; a mutating command for
the Tell cancellation cell; a wire-level observation of the sent OSD and PG
command in the paused-OSD subtest.

## Unavailable target versus protocol refusal

Reconciled on 2026-11-14. An unavailable or wrong target ends the call without
a reply: a local error that is known not to have been sent, or, once
transmission may have started, an unknown outcome. A protocol refusal is a
completed reply: the daemon's signed negative code in `Result.Code` and a
`CommandError`, never an unknown outcome and never retried.

| Channel | Unavailable or wrong target | Protocol refusal by the daemon |
| --- | --- | --- |
| MON command | All MONs stopped: `context.DeadlineExceeded` without an unknown outcome (`TestCephCompleteMonitorOutageIntegration`). Connected MON paused after admission: `OutcomeUnknownError` with `ErrKeepaliveTimeout` (`TestCephPausedMonitorIntegration`). Closed client: `ErrClosed` (`TestCephContextRecoveryIntegration`) | -22 for an unknown command and -13 for a read-only identity (`TestCephCommandErrorsAndBinaryOutputIntegration`, `TestCephReadOnlyPermissionsIntegration`) |
| MGR command | No active manager: `context.DeadlineExceeded`, and `ErrClosed` for waiters at Close, without an unknown outcome (`TestCephManagerAvailabilityIntegration`, a zero-manager fixture). All MONs stopped: `context.DeadlineExceeded` (`TestCephCompleteMonitorOutageIntegration`). Active manager paused: `OutcomeUnknownError` with `ErrKeepaliveTimeout` (`TestCephPausedManagerIntegration`) | -22 and -13 in the same tests as the MON command |
| Current MON Tell, current MGR Tell | Closed client only: `ErrClosed` without an unknown outcome (`TestCephTellIntegration`, `TestCephTellRecoveryIntegration`, `TestCephSentTellCancellationIntegration`). No test sends either Tell while no MON is reachable or no manager is active | -22 for an unknown daemon-local command and -13 for the read-only identity (`TestCephTellIntegration`) |
| Exact MON Tell | A name that is not a MON of the map: `ErrMonitorNotFound` without an unknown outcome and without a dial. A failed dial to the named MON: an error without an unknown outcome and no fallback to another MON; no sentinel is asserted. Closed client: `ErrClosed` (all `TestCephNamedMonitorTellIntegration`) | -22 and -13 (`TestCephNamedMonitorTellIntegration`) |
| Exact MGR Tell | A name that is not in the MgrMap: `ErrManagerNotFound` without an unknown outcome. A standby: `ErrManagerNotActive`, with no write (`TestCephAppliedNamedManagerTellIsNotReplayedIntegration`). The earlier name after a failover: one of the two. Closed client: `ErrClosed` (`TestCephNamedManagerTellIntegration`) | -22 and -13 (`TestCephNamedManagerTellIntegration`) |
| OSD command | Closed handle: `ErrClosed` without an unknown outcome and with an empty submission. Paused OSD: `OutcomeUnknownError` (both `TestCephOSDCommandsIntegration`). Killed OSD: an error; whether it is an unknown outcome depends on whether the session had already ended and is logged, not asserted (`TestCephOSDCommandCrashIntegration`) | -22 for an unknown prefix and invalid arguments and -13 for a read-only identity, each equal to the native client's code (`TestCephOSDCommandsIntegration`) |
| PG command | A PG command whose explicit primary is not the connection's OSD: `ErrOSDCommandTarget` without an unknown outcome and with an empty submission. Closed handle: `ErrClosed` likewise (both `TestCephOSDCommandsIntegration`) | -11 on a replica (three-OSD rows only), -2 for a PG the OSD does not have and a negative code from an OSD outside the acting set, each returned once (`TestCephOSDCommandsIntegration`, `TestCephOSDCommandOutsideActingSetIntegration`) |
| MDS command | Closed handle: `ErrClosed` without an unknown outcome and with an empty submission. At open: a changed target, the connection limit, a malformed target and the disabled option are refused without a dial (`TestCephMDSCommandsIntegration`) | The native code for an unknown prefix and -13 for the read-only identity (`TestCephMDSCommandsIntegration`) |

Not covered: a current MON or MGR Tell while its target is unreachable; a
sentinel for the failed dial of an exact MON Tell; `ErrMonitorTargetChanged`
and `ErrManagerTargetChanged` in an integration test; a command through a
failed OSD or MDS dial; an OSD, PG or MDS command to a stopped daemon with an
asserted classification (the OSD crash tests assert only that it fails and
never silently recovers); a PG command to a down or paused primary other
than the canceled case above.

Boundaries: wildcard expansion, local admin sockets and HTTP module endpoints
are not missing Messenger codecs. The active-only MGR rule has a pinned-source
explanation and is kept explicit instead of rerouting. No channel has a
typed administrative API.

The guard test in `command_channel_matrix_test.go` fails if a test selector named
here no longer exists in either integration suite.
