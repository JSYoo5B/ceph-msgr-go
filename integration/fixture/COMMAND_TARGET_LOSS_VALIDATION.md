# Close after a sent command, unreachable targets and stopped daemons

This is a bounded checkpoint of #63. It closes the cases that
[the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md) listed as not
covered in its sections on cancellation and shutdown after a command was sent
and on an unavailable target versus a protocol refusal:

1. `Client.Close` with a sent ordinary MON or MGR command outstanding.
2. Cancellation, `OSDConnection.Close` and `Client.Close` after an OSD and a
   PG command were observably written.
3. Every Tell API while no MON is reachable, and both manager Tells while no
   manager is active.
4. An asserted classification of an OSD, PG and MDS command to a stopped
   daemon.
5. The cause of a failed dial of an exact MON Tell, and
   `ErrManagerTargetChanged` on both sides of the transmission boundary.

Everything is in tests, one closed fixture control, catalog rows and records.
The product API and its behavior are unchanged.

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## The rule the cells assert

A command ends as `OutcomeUnknownError` exactly when its transmission had
started. Everything before that is known not to have been sent and returns
its own cause: the caller's context, `ErrClosed`, a target refusal, a dial
failure or the terminal cause of a session that had already ended. A
completed reply is neither.

"Transmission started" is local: the client's writer took the command and
began the write. It is not a statement about the daemon. Where a cell says a
command was sent, it means one padded local write returned; where a daemon
was already stopped, that write reached no daemon at all and the outcome is
still unknown, because the client cannot tell.

## Rows and tests

| Required row | Tests the gate requires | Profile |
| --- | --- | --- |
| `mon-mgr.aes.ip4.crc.host.t-senttellcancellation` | `TestCephSentTellCancellationIntegration` (client suite) | CRC, AES, IPv4, host test process |
| `mon-mgr.aes256k.ip4.host.t-senttellcancellation` | the same | secure, AES256K, IPv4, host test process |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0` | `TestCephOSDCommandsIntegration`, `TestCephOSDCommandCrashIntegration` (API suite) | CRC, AES, IPv4, host test process, one BlueStore OSD |
| `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0` | the same | secure, AES256K, IPv6, container test process, one BlueStore OSD |
| `mon-mgr.aes.ip4.crc.host.t-suite-afb7b2` | `TestCephCompleteMonitorOutageIntegration` (client suite), `TestCephNamedMonitorTellIntegration`, `TestCephNamedManagerTellIntegration` (API suite) | CRC, AES, IPv4, host test process |
| `mon-mgr.aes256k.ip4.host.t-suite-afb7b2` | the same | secure, AES256K, IPv4, host test process |
| `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscommands` | `TestCephMDSCommandsIntegration` (API suite) | CRC, AES, IPv4, host test process |
| `mds-commands.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscommands` | the same | secure, AES256K, IPv6, container test process |
| `mon-mgr.aes256k.ip4.managers-0` | at least one PASS; its only selected test is `TestCephManagerAvailabilityIntegration` | secure, AES256K, IPv4, no initial manager |

The two `t-suite-b300c0` rows (selector
`^TestCephOSDCommand(s|Crash)Integration$`) and the two `t-suite-afb7b2` rows
(selector
`^TestCeph(CompleteMonitorOutage|NamedMonitorTell|NamedManagerTell)Integration$`)
are new. Their tests need no opt-in and also run in the broad OSD and
`mon-mgr` rows, where the result gate requires no single test; the new rows
narrow the selection so that the gate requires each of them to PASS. The
other rows existed and run a changed test body.

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCommand(s|Crash)Integration$' \
sh integration/run.sh
```

## The observation used in the API suite

`commandHold` (`integration/command_hold_test.go`) wraps the connections one
test client dials to one daemon endpoint. It decodes nothing, so it is the
same in CRC and secure mode, and it identifies no frame, transaction or
reply.

- Armed on a write: the first write of at least 4096 bytes binds its
  connection before the bytes leave. The subject command is padded to more
  than 40 KiB and the test checks that no write of that size precedes it and
  that the count of such writes is one at the end.
- Held from a moment: every selected connection is held from that call on.
- A held connection parks the result of each `Read`, received bytes and
  errors alike. The session therefore learns neither of a reply nor of the
  end of its peer until the test releases the hold or the connection is
  closed.
- It reports when a padded write on a held connection returned, with or
  without an error, and when a selected connection was closed. A session
  closes its connection only after it recorded its terminal cause, so the
  close means that the session has ended.

The cluster-free test `TestCommandHoldParksBytesAndErrors` covers selection
by endpoint, binding only to the first padded write after arming, parked
bytes, a parked end of the peer, release, close and the two classification
helpers. It establishes no native result.

The client suite uses its existing padded gate
(`cephmsgr/integration_tell_reply_gate_test.go`).

## 1. Close with a sent MON or MGR command

`TestCephSentTellCancellationIntegration` gains `MonCommand` (a read-only
`version` MON command, to any MON) and `MgrCommand` (a read-only `pg stat`
module command, to the active manager), each with `_cancel` and `_close`,
beside the four Tell APIs. The sequence and its limits are those of
[TELL_CANCELLATION_VALIDATION.md](TELL_CANCELLATION_VALIDATION.md): one padded
write returned, bytes then arrived on its connection and were withheld, the
caller was still pending; after `Client.Close` the call ends with
`OutcomeUnknownError` and `ErrClosed`, a later invocation is `ErrClosed`
without an unknown outcome, and the padded-write count is still one.

## 2. OSD and PG commands ended by the caller

Subtest `written commands ended by the caller are uncertain and never
resent` of `TestCephOSDCommandsIntegration`, six cases: `Command` and
`PGCommand`, each ended by `cancel`, `OSDConnection.Close` and
`Client.Close`. Each case dials a client of its own, whose connection to the
acting primary is wrapped by the hold.

1. Control: the unpadded command (`version`, or a PG `query`) is answered.
   The OSD was dialed once and no write reached 4096 bytes.
2. The padded command is submitted once. Its write binds the hold, returns,
   and bytes then arrive on that connection and are parked. The caller is
   still pending.
3. The call is ended. It must return `OutcomeUnknownError` with
   `context.Canceled` or `ErrClosed`, an empty result, a nonzero submission
   transaction for that OSD, and for `PGCommand` the caller's PG metadata.
4. The handle is closed (after a cancellation by the test, because its
   connection can deliver nothing). The same command on it must be
   `ErrClosed` without an unknown outcome and with an empty submission.
5. After `cancel` and `OSDConnection.Close` an explicitly opened handle is
   answered and the OSD was dialed exactly twice. After `Client.Close`
   `OpenOSD` is `ErrClosed` and the OSD was dialed once.
6. The count of padded writes is still one.

The paused-OSD subtest of the same test no longer infers the send. It uses
a client of its own whose one OSD connection is observed without being
held: its deadline `Command`, its deadline `PGCommand` and the `Command`
that `OSDConnection.Close` ends are each padded to a little over 8 KiB, and
each is asserted, or for Close awaited, as one returned local write. The
200 ms wait before Close is gone. When the daemon is resumed the count of
padded writes is three and the OSD was dialed once; the explicit new handle
dials once more. Three small commands stay within the socket buffers of a
peer that reads nothing.

## 3. No MON reachable, no active manager

`TestCephCompleteMonitorOutageIntegration` stops all three MONs, waits until
the client's MON session has ended, and then calls `MonCommand`,
`MgrCommand`, `MonTell`, `MgrTell`, `MonTellTo` (MON `b`) and `MgrTellTo` (the
manager that was active before the outage), each under a 100 ms deadline, in
each of its three cycles. Each must end with `context.DeadlineExceeded`,
without an unknown outcome and with an empty result. The four manager and
exact-name calls do not reach their running targets: every channel first
waits for an admitted MON, and the exact-name targets are resolved from maps
that need one.

`TestCephManagerAvailabilityIntegration`, on the fixture without an initial
manager: `MgrTell` under a 30 ms deadline ends with
`context.DeadlineExceeded` without an unknown outcome; four of the eight
callers that wait for a manager when the client is closed use `MgrTell`, and
each ends with `ErrClosed` without an unknown outcome; `MgrTellTo` of the
name `a`, which the MgrMap does not list, is refused at once with
`ErrManagerNotFound`.

A Tell to a target that is paused after admission is not in this cell; the
paused-daemon tests send the ordinary commands only.

## 4. Commands to a stopped daemon

A command to a stopped daemon has one of two classifications, and which one
depends only on whether the client's session had already recorded the end of
its connection when the command was issued. The existing crash test logged
which one it got. The cells now put one session on each side on purpose and
assert both.

| Side | How the test puts a session there | Required result |
| --- | --- | --- |
| Ended | A client with a one-second keepalive. After the daemon is stopped the test waits until that client closed its connection to the daemon (and, for the MDS, until `MDSConnection.Done` is closed) | An error that is not an unknown outcome and not `ErrClosed`, with no result and no transaction (OSD, PG) or an empty submission (MDS). Asked twice |
| Unaware | A client whose keepalive never probes within the test (ten minutes, or the MDS helper's hour). From before the daemon is stopped, everything it would receive on the daemon connection is parked, the end of the peer included | The padded command is written once. When that write returned, the hold is released, so the session learns that its peer is gone. The call must be `OutcomeUnknownError` without `ErrClosed`, with no result, a nonzero transaction and, for a PG command, PG metadata. A further command on the handle is refused |

`TestCephOSDCommandCrashIntegration` does this for `Command` and `PGCommand`
with SIGKILL of the single BlueStore OSD: one ended client asked with both
forms, and one unaware client per form, because a session serves one such
command. After the OSD restarts under a new address nonce the old handles
are still refused and a new connection answers.

`TestCephMDSCommandsIntegration` does it in a last subtest, `stopped daemon
is known unsent after the session ended and unknown before`, with an orderly
stop of the fixture's only MDS. Neither handle dials again. The MDS is not
restarted: the fixture is disposable and the step is the test's last.

The stop uses the fixture's daemon control with a new kind, `stop-mds`. It
is admitted only for the action `stop` and only in the public MDS command
acceptance (`CEPH_MSGR_TEST_MDS_COMMANDS=1` without the applied-command
selector); `TestDaemonControlAdmitsOnlyStoppingTheCommandFixtureMDS` in the
fixture module covers the refusals.

The daemon-free test in `internal/session/transport_loss_test.go` shows the
same rule at the session: a started call keeps an unknown outcome when the
peer goes away, a call that was admitted but not started gets the bare cause
with its allocated transaction, and a call issued after the loss gets the
session's cause with no transaction.

### The earlier undecided observation

[TELL_CANCELLATION_VALIDATION.md](TELL_CANCELLATION_VALIDATION.md) recorded
that the first invocation after the test closed a connection underneath a
live session was seen as an unknown outcome and, once, as a bare transport
error, and left open whether these should be one classification. They are
the two sides above. An invocation that the session admitted and began to
write before it recorded the loss is an unknown outcome. An invocation
issued after the session recorded the loss, or admitted but not yet started
when it did, was never written and gets the cause the locally closed
connection reports. In that test the side is not controlled, so it accepts
either for that one invocation; here it is controlled and each side is
asserted. No product behavior was changed.

One detail of the unknown side is conservative. In the runs below, every
such invocation of that test that ended as an unknown outcome carried the
cause `set tcp ...: use of closed network connection`: the writer had taken
the command and then failed to set the write deadline on the connection the
test had closed, before it wrote a byte. The session marks a call as started
when its writer takes it, not when the first byte leaves, so this call is an
unknown outcome although nothing was written. That errs toward uncertainty
and never toward a false "not sent". Whether the product should narrow it is
not decided here.

## 5. Dial cause of an exact MON Tell; a changed exact MGR target

`TestCephNamedMonitorTellIntegration`: the test's dialer refuses the dial to
MON `b` with its own error inside a `*net.OpError` of the dial. `MonTellTo`
must return that error as its cause, without an unknown outcome, without a
result, without `ErrMonitorNotFound`, `ErrMonitorTargetChanged` or
`ErrClosed`, and (as before) without dialing another MON. The product has no
sentinel of its own for a failed dial and none was added: the dialer belongs
to the caller, and its error is the cause.

`TestCephNamedManagerTellIntegration`, failover subtest: two further clients
name the active manager while the test fails it over.

- Unsent: the client's dial to the named manager is held by the test's
  dialer. After `mgr fail` the test waits until that client's own MgrMap no
  longer shows the manager, and only then lets the dial fail. `MgrTellTo`
  must return `ErrManagerTargetChanged` (which wraps `ErrManagerChanged`)
  without an unknown outcome and without the dial failure as its result.
- Sent: the client's padded `version` Tell was written to the named manager
  and bytes received afterwards were parked. When the client's MgrMap
  changes, the call must end with `OutcomeUnknownError` whose cause includes
  `ErrManagerTargetChanged`.

Both clients dialed the named manager once each and no other manager, and
the padded command was written once.

`ErrMonitorTargetChanged` has no integration case. It needs a named MON
whose own MonMap no longer associates the name with the dialed address while
the primary's map still does, that is a membership or address change between
two reads, which no fixture control produces. The daemon-free tests in
`cephmsgr/named_mon_binding_test.go` and `cephmsgr/named_mon_recovery_test.go`
cover it with a scripted peer.

## What this does not show

- That a daemon executed or did not execute anything. No cell counts daemon
  executions; the commands are read-only and no daemon state is compared.
- Any frame. A padded write is a local write of at least 4096 bytes, and the
  parked bytes are not attributed to a reply, in CRC and in secure mode.
- A mutating command ended by the caller, `Client.Close` with a sent MDS
  command, a Tell to a paused target, `OpenOSD` or `OpenMDS` to a stopped
  daemon, an OSD stopped in order without a command in flight, a restarted
  MDS, or `ErrMonitorTargetChanged` against a real MON.
- Any profile other than the rows above.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 hosts, Docker 29.9.1, the
image above, on source commit `49c6c359e34f4f7bf5288e9421c98f2d3ecfa18c`,
uploaded as an archive of that commit. It holds the tests, the fixture
control and the catalog rows; the commits after it in the same pull request
change documentation only. Each run used the environment that the native
workflow passes for its catalog row and the same result gate
(`tools/native_test_results.py snapshot` before and `verify` after). One
`go test` process ran on an engine at a time. Every row below passed on its
first run on that commit.

| Row | Gate | Tests | Fixture run |
| --- | --- | --- | --- |
| `mon-mgr.aes.ip4.crc.host.t-senttellcancellation` | expected 1, `PASS` with all twelve subtests | 0.04 s | 44 s |
| `mon-mgr.aes256k.ip4.host.t-senttellcancellation` | the same | 0.04 s | 34 s |
| `osd.aes.ip4.crc.host...legacy-v6.t-suite-b300c0` (new) | expected 2, both `PASS` | commands 5.26 s, crash 11.52 s | 76 s |
| `osd.aes256k.ip6.container...legacy-v6.t-suite-b300c0` (new) | expected 2, both `PASS` | commands 5.46 s, crash 11.32 s | 66 s |
| `mon-mgr.aes.ip4.crc.host.t-suite-afb7b2` (new) | expected 3, all `PASS` | outage 9.42 s, exact MON Tell 0.35 s, exact MGR Tell 11.78 s | 57 s |
| `mon-mgr.aes256k.ip4.host.t-suite-afb7b2` (new) | expected 3, all `PASS` | outage 10.02 s, exact MON Tell 0.92 s, exact MGR Tell 11.78 s | 58 s |
| `mds-commands.aes.ip4.crc.host...legacy-v6.t-mdscommands` | expected 1, `PASS` with the new last subtest | 15.22 s | 110 s |
| `mds-commands.aes256k.ip6.container...legacy-v6.t-mdscommands` | the same | 15.42 s | 108 s |
| `mon-mgr.aes256k.ip4.managers-0` | at least one `PASS`: its one selected test | 12.24 s | 34 s |

Observed in these runs, as logged by the tests:

- Killed OSD, both `b300c0` rows, `Command` and `PGCommand`: on the ended
  session the cause was `EOF` without an unknown outcome; on the unaware
  session the padded write returned without an error and, after the release,
  the call ended as an unknown outcome with cause `EOF` and transaction 3.
- Stopped MDS, both rows: the same, with transaction 2.
- Exact MGR Tell across the failover (CRC row): the sent call ended with
  "request outcome unknown: active manager changed" joined with "named
  manager target changed"; the unsent call with "named manager target
  changed: active manager changed".
- The six written OSD and PG command cases ended with `context.Canceled` or
  `ErrClosed` inside an unknown outcome, each with transaction 2.

The changed tests also run in broad rows. On the same commit:

| Row | Selection | Result |
| --- | --- | --- |
| `mon-mgr.aes.ip4.crc.host` | the row's own broad selection, unchanged | gate passed in 563 s; the outage test (9.72 s), the exact MON Tell test (0.28 s) and the exact MGR Tell test (10.07 s) passed among the others; the sent cancellation and manager availability tests skip there by design |
| `osd.aes256k.ip4.host`, `osd.aes.ip4.crc` | narrowed for the run to `^TestCephOSDCommandsIntegration$` | `PASS` on both MemStore rows, where the crash test skips |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3`, `osd.aes256k.ip6.osd-store-bluestore.osd-replicas-3` | narrowed for the run to `^TestCephOSDCommand(s|Crash)Integration$` | both tests `PASS` on both three-OSD rows |

The four narrowed runs are selector overrides of broad rows, applied to the
uploaded copy only, not required-row acceptance. The other broad rows were
not run.

### Checks that the assertions fail on wrong behavior

Nine temporary product changes were each applied to an uploaded archive, run
and discarded with the run; the repository was not changed. Every run failed
the result gate. The changes to an ended session, to `MonCommand` and
`MgrCommand` and to `MonTell` and `MgrTell` ran on the parent commit
`ffcb79aff93b33443ea99c5f9d9970494b5f689c`, the others on the commit above;
the assertions they fail are the same on both.

| Temporary change | Rows | Failing assertion |
| --- | --- | --- |
| A command on a session that had already ended is reported as an unknown outcome | CRC `b300c0`, CRC `t-mdscommands` | Crash test: "Command after its session had ended was not a known-unsent refusal". MDS: "a command after its session had ended was not a known-unsent refusal" |
| A started call whose transport ends with EOF gets the bare cause | the same two | Crash test: "Command admitted on a session unaware of the kill was not an unknown outcome with its transaction". MDS: "a command admitted on a session unaware of the stop was not an unknown outcome with its transaction" |
| `MonCommand` and `MgrCommand` return the bare cause of a sent command | both `t-senttellcancellation` rows | The four `MonCommand` and `MgrCommand` subtests fail; the eight Tell subtests pass |
| `MonTell` and `MgrTell` report a deadline before admission as an unknown outcome | CRC `afb7b2`, `managers-0` | Outage test: "untransmitted outage wait has incorrect result MonTell". Availability test: "MGR Tell discovery wait did not preserve known non-execution" |
| A sent exact MGR Tell whose manager changed no longer names the changed target | CRC `afb7b2` | "a sent named Tell across the failover lost its unknown outcome or the changed-target cause" |
| An obsolete manager dial failure is returned instead of resolving the changed target | CRC `afb7b2` | "an unsent named Tell across the failover was not a known-unsent changed-target refusal", with the held dial's error |
| The failed dial of an exact MON Tell is replaced by a generic error | CRC `afb7b2` | "unavailable named TCP target lost known non-execution or the dialer's own cause" |
| The OSD command channel transmits a canceled sent command once more, bounded by two seconds | CRC `b300c0` | `Command cancel` and `PGCommand cancel`: "the padded command was written again, or the OSD was dialed without the caller", with a count of 2. Paused-OSD subtest: "cancelled command to a paused OSD lost uncertainty or was not written once" |
| A sent OSD or PG command ended by Close returns the bare `ErrClosed` | CRC `b300c0` | The four `OSDConnection.Close` and `Client.Close` cases: "lost its unknown outcome, its cause or its submission". Paused-OSD subtest: "Close during a sent OSD command lost uncertainty or the Close cause" |

In the three `afb7b2` runs whose API-suite test failed, the client suite did
not run and the gate also reported the outage test as missing.

### Failures during development

None of the new assertions failed against the unchanged product. Every row
passed at its first run, both on the working tree and on each commit. The
padding of the commands to a paused or stopped daemon was reduced from a
little over 40 KiB to a little over 8 KiB before the commit above, as a
precaution against a full socket buffer on a peer that reads nothing; the
40 KiB version had passed its runs too.

No hosted CI run of these rows is recorded here.
