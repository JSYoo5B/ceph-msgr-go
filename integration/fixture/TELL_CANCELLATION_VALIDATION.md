# Cancellation and Close after a Tell was sent

This is a bounded checkpoint of #63 for the four Tell APIs:
`Client.MonTell`, `Client.MonTellTo`, `Client.MgrTell` and `Client.MgrTellTo`.
Before it, no test canceled the caller or closed the client while a Tell that
had been written was unanswered: the applied-mutation cells
([exact MON Tell](MON_TELL_OUTCOME_VALIDATION.md),
[current MON Tell](MON_CURRENT_TELL_OUTCOME_VALIDATION.md),
[MGR Tell](MGR_TELL_OUTCOME_VALIDATION.md)) end their call by closing the
test's own connection wrapper. The product API and replay behavior are
unchanged. The per-channel statement is in
[the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md#cancellation-and-shutdown-after-a-command-was-sent).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Executable boundary

Test: `TestCephSentTellCancellationIntegration` in the `cephmsgr` suite, eight
subtests: `MonTell`, `MonTellTo`, `MgrTell` and `MgrTellTo`, each with `_cancel`
and `_close`. It ends MON and MGR sessions on purpose, so it skips unless
`-test.run` is exactly `^TestCephSentTellCancellationIntegration$`, and it
needs the ordinary three-MON, two-MGR fixture.

| Required row | Profile |
| --- | --- |
| `mon-mgr.aes.ip4.crc.host.t-senttellcancellation` | CRC, AES client and service keys, IPv4, host test process |
| `mon-mgr.aes256k.ip4.host.t-senttellcancellation` | secure, AES256K client and service keys, IPv4, host test process |

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephSentTellCancellationIntegration$' \
sh integration/run.sh
```

The command is the read-only daemon-local
`{"prefix":"version","format":"json"}`. No daemon state is changed or
compared, and the test needs no native control: what it observes is the
client's own connections. Nothing is decoded, so the sequence is the same in
CRC and secure mode.

## Sequence

Each subtest dials its own client. Every connection of that client is wrapped
by the padded gate of the MGR Tell cells' secure mode
(`paddedReplyGate`, `cephmsgr/integration_tell_reply_gate_test.go`) and by a
counter of writes of at least 4096 bytes. The gate can bind only to a
connection whose fixture role may carry the API's command: any MON for
`MonTell`, MON `b` for `MonTellTo`, and the active manager, read from the
MgrMap of a separate client, for `MgrTell` and `MgrTellTo`.

1. Control: the unpadded command through the API under test is answered. No
   write has reached 4096 bytes and the gate is unbound.
2. The same command, padded to more than 40 KiB, is submitted once. The gate
   binds to its connection before the bytes leave and from then on withholds
   every byte received on it.
3. Sent: the padded write returned and is counted once, its connection's role
   is an admitted one, and bytes have arrived on that connection and been
   withheld. The caller is still pending.
4. `_cancel`: the call's context is canceled. `_close`: `Client.Close` is
   called and must return without error.
5. The call must end with `OutcomeUnknownError` whose cause is
   `context.Canceled` or `ErrClosed` respectively, and with an empty result.
6. `_cancel`: the withheld connection is closed by the test, because it can
   deliver no further reply. A session that still owns it learns of the loss
   from its next use, so the first new invocation through the same API may
   fail on the ended connection, as an unknown outcome or as a transport error
   without a result; it may not report cancellation, `ErrClosed` or a result.
   The invocation after such a failure, or the first one, must be answered.
   `_close`: a new invocation must return `ErrClosed` without an unknown
   outcome and without a result.
7. The count of padded writes is still one and the gate still holds only the
   first connection: nothing transmitted the padded command again.

## What this does not show

- That the daemon executed the command or sent a reply. The withheld bytes
  are not decoded; on a MON connection they can belong to any message.
- A mutating Tell ended by the caller. The applied-mutation cells cover an
  applied Tell whose connection is lost.
- That step 6's first invocation is answered. After the test closes a
  connection underneath a live session, that invocation's result depends on
  whether the session has already noticed. The test accepts either and
  requires the next one.
- Cancellation while a write is partially done (the MON and MGR command
  channels have `TestCephStreamCancellationIntegration`), and cancellation of
  a Tell that is still waiting for admission or a session.
- A server execution count, or any profile other than the two rows.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 host, Docker 29.9.1, the
image above, three MONs and two MGRs without OSDs, on source commit
`df89356b87ddc6093fdcb2d4dd8557898ad7d19d`, uploaded as an archive of that
commit. It holds the test and the catalog rows; the commits after it in the
same pull request change documentation only. Each run used the environment
that the native workflow passes for its catalog row and the same result gate
(`tools/native_test_results.py snapshot` before and `verify` after). One
`go test` process ran on the engine at a time.

| Row | Gate | Test | Fixture run |
| --- | --- | --- | --- |
| `mon-mgr.aes.ip4.crc.host.t-senttellcancellation` | expected the one client test, observed `PASS` with all eight subtests, no problems | 0.03 s | 43 s |
| `mon-mgr.aes256k.ip4.host.t-senttellcancellation` | the same | 0.03 s | 35 s |

In both rows the padded write went to `mon.a` on local connection 1 for
`MonTell`, to `mon.b` on connection 3 for `MonTellTo` and to the active
`mgr.a` on connection 2 for `MgrTell` and `MgrTellTo`. Every `_cancel` call
ended with `OutcomeUnknownError` whose cause is `context.Canceled`, and every
`_close` call with `OutcomeUnknownError` and `ErrClosed`. The padded-write
count stayed one in all sixteen subtests.

The same test body had passed twelve earlier runs on the uncommitted change,
six on each row. In those runs the first new invocation of step 6 failed on
the ended connection in 14 of the 48 `_cancel` subtests (`MgrTellTo` 7,
`MgrTell` 5, `MonTell` 2), each time as an unknown outcome, and was answered
in the others; the invocation after it was always answered. For `MonTellTo`,
whose connection serves one command, it was always answered.

### Checks that the assertions fail on wrong behavior

Two temporary product changes were each applied to an archive of that commit,
run on both rows and discarded with the run; the repository was not changed.
Every run failed the result gate.

| Temporary change | Result on both rows |
| --- | --- |
| The four Tell APIs return the bare cause of a sent command instead of `OutcomeUnknownError` | All eight subtests fail: "a sent ... ended by cancel (or close) lost its unknown outcome or its cause" |
| The four Tell APIs transmit a canceled sent command once more, bounded by two seconds | The four `_cancel` subtests fail: "the padded Tell was written again or the gate was rebound", with a count of 2. The `_close` subtests pass, because the change does not transmit on a closed client |

### Failures during development

Two earlier versions of step 6 failed, both in the test and not in the
product. Their passes on other subtests are not used as evidence.

1. The first version closed the withheld connection and required the next
   invocation to be answered. It failed in the first run of each row, in
   different subtests (`MonTell_cancel` and `MgrTell_cancel` on the CRC row,
   `MgrTellTo_cancel` on the secure row), with
   `request outcome unknown: set tcp ...: use of closed network connection`.
   The test had closed the connection underneath a session that the client
   still held, and that invocation was the session's first use of it. This is
   the expected result of a command started on a connection that then fails;
   the test's requirement was wrong.
2. The second version accepted an unknown outcome for that invocation. It
   failed once (`MgrTellTo_cancel`, CRC row) with a bare
   `use of closed network connection`: the same condition reported as a
   transport error without an unknown outcome. The test now accepts either
   for the one invocation that meets the ended connection, refuses a result,
   cancellation or `ErrClosed` there, and requires the next invocation to be
   answered.

Whether the bare transport error and the unknown outcome for the same
condition should be one classification is a product question this cell does
not decide; neither invents a result or retransmits.

No hosted CI run of these rows is recorded here.

## Daemon-free checks

`TestPaddedReplyGateWithholdsEveryByteAfterTheFirstPaddedWrite`
(`cephmsgr/tell_reply_gate_test.go`) covers the gate: an unselected role or a
smaller write never binds it, a later padded write never rebinds it, and
bytes received after the binding reach no reader. It establishes no native
result.
