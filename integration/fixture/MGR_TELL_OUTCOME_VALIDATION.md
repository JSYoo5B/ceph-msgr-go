# Independently observed MGR Tell mutations and no replay

This is a bounded implementation checkpoint of #63, after the
[MON](MON_COMMAND_OUTCOME_VALIDATION.md), [MGR](MGR_COMMAND_OUTCOME_VALIDATION.md),
[exact MON Tell](MON_TELL_OUTCOME_VALIDATION.md) and
[current MON Tell](MON_CURRENT_TELL_OUTCOME_VALIDATION.md) cells. It covers
`Client.MgrTellTo` and, in the [current-manager cell](#current-manager-cell)
added afterwards, `Client.MgrTell`, each in CRC mode and in a
[secure mode](#secure-mode) without decoded frames. The targeted OSD, PG and
MDS cells are recorded [separately](DAEMON_COMMAND_OUTCOME_VALIDATION.md), and
[the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md) lists what
remains. The product API and replay behavior are unchanged; see
[the contract](../../cephmsgr/MGR_TELL_CONTRACT.md).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Pinned source facts

These come from the source review recorded on #63 for the reference tag. They
were not re-derived for this change; the native observations below are
consistent with them.

- [`src/mgr/DaemonServer.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc)
  `DaemonServer::_handle_command`: an MCommand with a nonzero FSID passes the
  allow-all MGR capability check and is queued to the receiving manager's own
  admin-socket command handler. It does not go through the module dispatcher.
- [`src/common/admin_socket.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/admin_socket.cc):
  the executor replies with MCommandReply on the originating connection with
  the request's transaction ID.
- [`src/common/ceph_context.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/ceph_context.cc)
  registers the daemon-local `config set`, `config get`, `config unset` and
  `config diff get` handlers in every daemon process, and the logging observer
  that applies `log_max_recent`.
- [`src/common/config.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/config.cc):
  a runtime set or unset changes the override source of the option in that
  process only. The configuration diff exposes the override and the final
  value per option and can enumerate other settings.

The target name is a MgrMap association, not a wire field: the request carries
the command and the cluster FSID. The Go client binds the name, daemon GID and
address vector from its authenticated MgrMap and checks the daemon GID and
address in the handshake ([contract](../../cephmsgr/MGR_TELL_CONTRACT.md)).
Which daemon executed the command is therefore established here by the changed
state on that daemon's own admin socket, not by the request.

## Command choice

The mutation is
`{"prefix":"config set","var":"log_max_recent","val":["<A>"],"format":"json"}`
sent through `MgrTellTo`. Two alternatives are rejected for this cell:

- `balancer mode` is a module command. It is not routed through the Tell
  admin-socket path, and its state is persistent cluster configuration, which
  the [MGR command cell](MGR_COMMAND_OUTCOME_VALIDATION.md) already covers.
- The centralized `config set` with `who` writes the MON configuration
  database, a different layer from the daemon's runtime override.

`log_max_recent` is a positive in-memory crash-log retention count, so the
cell only raises it: the baseline must be 1..19998 with an unambiguous
override (absent, or equal to the effective value), A is baseline+1 and B is
baseline+2, never above 20000. Anything else is refused before a mutation.

## Executable boundary

Test: `TestCephAppliedNamedManagerTellIsNotReplayedIntegration` in the
`cephmsgr` suite, on the ordinary three-MON/two-MGR fixture without OSDs. It
needs no opt-in parameter. Required row:
`mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell` (Linux/amd64, CRC, AES
client and service, IPv4, host test process). The test is also a candidate of
the broad ordinary `mon-mgr` rows in both connection modes. The sections up to
the execution evidence describe the CRC mode of the exact-name cell; the
[current-manager cell](#current-manager-cell) and the
[secure mode](#secure-mode) are described after them. Reproduce with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$' \
sh integration/run.sh
```

### Native observer

The fixture control `observe-mgr-tell` (`mgr_tell_outcome.go`) accepts a
request ID, a 32-hex token, a closed phase and an owned manager name (`a` or
`b`). It accepts no command, option, value or path, and it is admitted only on
the ordinary two-manager fixture. Every call goes through the provider's
`ManagerContainer.AdminSocket` handle of that daemon, a local Unix socket that
does not use the Messenger or CephX, with a 10-second bound per call. One
observation is:

1. a fresh native `mgr dump`: availability, active name and GID, epoch and
   standby names;
2. `config get log_max_recent`, kept as the daemon's original bytes;
3. `config diff get log_max_recent`, of which only the owned entry (source
   names, override presence and value, final value), the top-level key names
   and the entry count are kept;
4. a second fresh `mgr dump`.

The Go helper accepts a state only when both map reads show the same available
active name and GID, that name is the requested target, and both option reads
succeeded.

| Phase | Target | Action |
| --- | --- | --- |
| `initial` | active manager | Observe and record the baseline once. A repeated request cannot replace it |
| `applied`, `recovered`, `restored` | baseline target | Observe |
| `overwrite` | baseline target | Claim, then `config set log_max_recent <baseline+2>` once, then observe |
| `restore` | baseline target | Claim, then `config unset log_max_recent` (introduced override) or `config set` of the original override once, then observe |
| `bystander-initial`, `bystander-final` | the other manager | Observe only |

Each mutation writes its claim file before the native call. A second request
of the same phase is refused, and a call that fails or does not return leaves
its outcome uncertain for the caller. Receipts are published atomically before
the acknowledgement and appended to `mgr-tell-outcome-native.jsonl`, which the
diagnostics allowlist retains together with `mgr-tell-outcome-identity.json`.

The baseline belongs to one manager process, so its owner record also keeps
the active GID. Immediately before `overwrite` and `restore` the control reads
the MgrMap again and sends the command only if it shows that same name and GID
as the available active manager. Otherwise the receipt says `refused`: nothing
is claimed and nothing is sent, so a manager restarted or replaced under the
same name never receives a value derived from another process's baseline. An
owner record without a GID and a failed map read are refused the same way. The
two map reads after the call are compared with the recorded GID and reported as
`same_incarnation_after`; the Go cells count a mutation only when it was not
refused, returned successfully and that report is true.

The admin socket is addressed by daemon name and reports no GID, so identity is
not proven atomically with the command. A manager replaced between the map read
and the admin-socket call (two consecutive calls of the control) is not
prevented; it is reported by the reads after the call.

### Reply gate

A test-only connection wrapper (`cephmsgr/integration_tell_reply_gate_test.go`)
is installed on every connection the subject client dials, including later
ones. It is armed with the digests of A's complete front and bulk data. When
that exact command is first written as a complete type 97 frame, the gate
binds to that connection and to the transaction ID in the frame, before the
bytes leave. On the read side it decodes the stream from the banner on and
releases only complete frames. The frame that is a type 98 message with the
bound transaction ID on the bound connection is held: no byte of it is
returned to the Go session, and a channel reports the hold. Every complete
frame before it is delivered first, also when it arrived in the same read as
the reply and when the caller reads with a short buffer. Frames after the held
reply on that connection stay withheld with it, because the stream is ordered.
Every frame on every other connection passes unchanged. A stream the wrapper
cannot decode is forwarded unchanged and recorded as an error that fails the
test.

The hold is the barrier of the test. It shows that the daemon replied to A,
so the following native read is taken once and is not repeated until it
agrees. No assertion depends on a wall-clock margin.

### Sequence

1. Prepare an active and a standby manager with the fixture's existing
   `prepare-mgr-failover` control and take the active name from an independent
   Go client.
2. `initial` on the active manager: the bracketed baseline. `bystander-initial`
   on the standby. Both Go clients wait for a MgrMap with the native active
   name and GID, and the independent client reads the baseline through
   `MgrTellTo`.
3. Controls before A on the subject client, with the gate armed: `MgrTellTo`
   of A to the standby's exact name returns `ErrManagerNotActive` and writes
   nothing; a read-only `config get` Tell to the active name passes the gate on
   the shared MGR session with its own transaction.
4. Submit A once. Wait for the hold, and then for the writer to publish its
   completed local write: the receipt and the padded-write count are recorded
   after the local write returns, which the held reply does not order.
   Require one complete local write of A, on
   the same connection as the read-only Tell, to the peer that the fixture
   topology identifies as the named manager, with the bound transaction, and
   the held frame to be type 98 with that transaction.
5. `applied`: A as effective value and as runtime override on the named
   daemon, with the caller still pending.
6. `overwrite`: B, written natively once and read back. The caller is still
   pending and the padded-write count is still one.
7. Close only the bound connection. The original call must return
   `OutcomeUnknownError` with an empty result and without `ErrManagerChanged`.
8. A new read-only `MgrTellTo` on the subject client reads B over another
   connection; the independent client reads B; `recovered` shows B natively
   under the same active name and GID; the subject client's MgrMap still names
   that daemon.
9. Across all subject connections: still exactly one write of A with its
   original identity, exactly two read-only writes (the control and the
   recovery read), and the gate held only that one frame. The recovery read
   has the control's content and its reply does not order its own local write
   either, so the test first waits for the second record to be published.
   `bystander-final` equals `bystander-initial`.
10. Cleanup with a fresh 60-second context: `restore` once, then `restored`
    must show the baseline value and provenance. A failed or unreturned
    restoration is reported and never repeated.

## Daemon-free checks

- `cephmsgr/tell_reply_gate_test.go`: with three-byte read fragments, the
  banner and earlier frames pass byte-identical; a Tell reply with another
  transaction and another reply type with the bound transaction pass; the bound
  reply is held without releasing a byte and Close ends the read with
  `net.ErrClosed`; a different command does not bind, a repeated identical
  write does not rebind, and the bound transaction on another connection
  passes; an undecodable stream is forwarded unchanged with a recorded error.
- `cephmsgr/mgr_tell_outcome_receipt_test.go`: open requests are rejected
  before anything is published; a receipt matches only the wanted value and
  override presence on the named daemon under an unchanged available name and
  GID, and not for another name, GID or admin socket, a failed read or a
  changed bracket.
- `integration/fixture/mgr_tell_outcome_test.go`: closed inputs; baseline
  ownership by token and target; refusal of a baseline outside the bound, of a
  non-active target and of a changed or unreadable MgrMap; overwrite and
  restoration derived from the recorded baseline and submitted once, for both
  an absent and a pre-existing override; a failed mutation kept uncertain with
  path-like text masked; projection of only the owned diff entry.

- `cephmsgr/tell_reply_gate_test.go` also covers the padded-write gate of the
  secure mode (only the first write of the threshold size to a selected role
  binds, bytes received before it pass, every byte received after it on that
  connection is withheld, other connections keep delivering) and a regression
  for both gates: the held connection is closed from the test and from a
  session-like reader over a connection that fails a second Close, with the
  banner in the same read as the reply.
- `TestTellReplyGateDeliversTheWholePrefixBeforeParking`: a peer writing
  three-byte fragments and a peer writing the banner, three passing frames,
  the bound reply and a later frame in one write, each read with a 1-byte, a
  7-byte and a 64 KiB buffer. The reader receives exactly the bytes before the
  bound reply and then the close error.
- `TestHeldReplyDoesNotOrderTheLocalWriteReceipt`: the peer reads all of A
  and its reply is held while the local write has not returned. No receipt
  and no padded-write count exist at that point; the waits the cells use
  return only after the write returns, and a wait with an ended context
  fails.
- `TestRepeatedReadReceiptWaitsForItsOwnWrite`: the same read is consumed in
  full on two connections while the second local write has not returned. One
  record exists; a wait by content alone is satisfied by the first, and the
  wait for the second record returns only after that write returns.
- `TestMgrTellMutationsAreBoundToTheBaselineIncarnation` (fixture): for both
  mutations and for a manager replaced under the same name, failed over,
  unreadable or recorded without a GID, the receipt is `refused`, no claim
  file exists and the replacement receives no mutating call; a replacement
  after the check is reported by `same_incarnation_after`.
  `TestNativeMutationCountsOnlyWhenSentToTheBaselineIncarnation` covers the
  Go side.
- `cephmsgr/mgr_tell_outcome_receipt_test.go` also covers the secure mode's
  wait: only a value and an override that each belong to the previous or to
  the wanted state are read again; a third value, a third override, an
  unreadable, unbracketed or foreign observation is refused.

These checks establish no native result.

## Current-manager cell

Test: `TestCephAppliedCurrentManagerTellIsNotReplayedIntegration`, the same
body with `MgrTell` as the API under test. `MgrTell` takes no name: it uses
the active manager of the client's authenticated MgrMap. Required row:
`mon-mgr.aes.ip4.crc.host.t-appliedcurrentmanagertell`, the profile of the
exact-name row with selector
`^TestCephAppliedCurrentManagerTellIsNotReplayedIntegration$`. Differences
from the sequence above:

- The baseline is taken on the natively observed active manager, and both Go
  clients wait for a MgrMap with that name and GID, as before. The test does
  not pass that name to the API under test.
- The read-only control before A and the recovery read after the loss go
  through `MgrTell`. The independent Go reader keeps using the exact name.
- The standby-name refusal is a property of `MgrTellTo` and is not repeated.
  The standby manager is still read before and after as an unchanged
  bystander.
- **Observer binding.** After the hold, the manager that received A is taken
  from the written frame: the fixture role of the endpoint that the bound
  connection was dialed to. The cell fails unless that role is the manager the
  baseline belongs to, and every later native state is requested for that
  manager and must be bracketed by its name and GID. The fixture refuses a
  request for any other manager under this baseline.
- The caller must end with `OutcomeUnknownError` and without a target-change
  cause, and the subject client's MgrMap must still name the same manager.

## Secure mode

Secure frames cannot be decoded by a connection wrapper, so neither the
written command nor a reply frame can be identified. The cells keep the same
native observer and sequence with these substitutions, using only the
connection wrapper that earlier lost-reply tests already use
(`testcluster.LostReplyConn`):

- **Gate.** A is padded to more than 40 KiB. On the first write of at least
  4096 bytes to a manager, the gate binds to that connection before the bytes
  leave and from then on withholds every byte received on it. No reply to that
  write can reach the session. Before A, the test requires that no write of
  that size has happened, so the padded write is A.
- **Barrier.** The binding shows that the padded write was issued, not that
  the daemon replied. The applied state is therefore awaited: the `applied`
  observation is repeated until it shows A. An observation is two admin-socket
  calls, so only a value and an override that each belong to the baseline or
  to A are read again. Any other state, an unreadable or unbracketed read, or
  the end of the original caller fails at once. The wait ends on the state
  itself, bounded by the test context; no assertion depends on a wall-clock
  margin. After the native overwrite the caller is checked to be still
  pending.
- **Transmission count.** Exactly one write of at least 4096 bytes across
  every subject connection, before and after the recovery read. The same
  manager must have been dialed again after the held connection, and the
  receipt lists every such dial (`later_dials_to_receiver`). A dial is
  recorded when it succeeds, before any handshake or command, so the list
  does not say which of them carried the recovery read.

What the secure mode does not establish: the transaction and sequence of A,
that the withheld bytes contain a reply to A (they can be an acknowledgement
or any other frame), or the identity of any frame. The applied native state
shows that the daemon executed A; the withheld stream shows that the caller
could not have been told. Frame-level attribution in secure mode would need
instrumentation inside the session, after decryption, which no test has.

Required row: `mon-mgr.aes256k.ip4.host.t-suite-e9be0b` (secure, AES256K
client and service, IPv4, host test process), selector
`^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$`.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 host, Docker 29.9.1, the
image above, three MONs and two MGRs without OSDs, on source commit
`f315ea5907c537a0b4f47d5a23a38013385a2f6f`. That commit holds the test, the fixture control and the catalog row;
the commits after it in the same pull request change documentation only. Each
run used the environment that the native workflow passes for its catalog row
and the same result gate (`tools/native_test_results.py snapshot` before and
`verify` after).

Required row `mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell`, selector
`^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$`: the gate expected
`client/TestCephAppliedNamedManagerTellIsNotReplayedIntegration` and observed
it as `PASS` with no problems. The test took 7.52 seconds and the fixture run
46 seconds.

| Observation | Retained result |
| --- | --- |
| Baseline | `mgr.a`, active GID 4167, MgrMap epoch 4 before and after; effective 10000, no override; diff reply of 35 entries |
| Bystander | `mgr.b` (standby), effective 10000, no override, under the same active `a`/4167 |
| Controls before A | `MgrTellTo` of A to `b`: `ErrManagerNotActive`, no write. Read-only Tell to `a`: local connection 2, TID 1, sequence 1, passed the armed gate |
| Original CRC submission of A=10001 | `mgr.a`, local connection 2, client global ID 4497, TID 2, sequence 2, complete local write |
| Withheld frame | type 98, TID 2, sequence 2, 132 bytes, on connection 2 |
| Native A while the reply was withheld | effective 10001, override 10001, `a`/4167, epoch 4; diff reply of 36 entries |
| Native overwrite B=10002 | `config set log_max_recent 10002` exited successfully once; effective 10002, override 10002 |
| Original caller after connection 2 was closed | `OutcomeUnknownError` (cause: use of closed network connection), empty result, no target-change cause |
| Fresh reads | Subject client: B over local connection 3, TID 1, sequence 1. Independent Go client: B. Native: effective 10002, override 10002, `a`/4167, epoch 4 |
| Local transmissions of A | exactly one, unchanged identity; padded writes 1 |
| Bystander at the end | `mgr.b` effective 10000, no override |
| Native restoration | One `config unset log_max_recent`, exited successfully; a separate fresh read shows effective 10000 and no override, diff reply of 35 entries |

Seven fixture containers were terminated by the run. Only the provider's
reaper container remained on the host for its usual grace period after the
fixture exited; no independent final network inventory was taken.

Broad row `mon-mgr.aes.ip4.crc.host` (no selector), run on a working tree
with the same Go test and fixture code as that commit: the gate passed with
231 observed top-level tests, 61 `PASS` and 170 optional `SKIP`, in 542
seconds. The cell ran in the client suite after the API suite's manager
failovers, against `mgr.b` with active GID 15604 at MgrMap epoch 61, and
passed in 7.32 seconds with the same connection, transaction and frame
pattern. The broad gate has no named expectation, so this shows that the cell
coexists with the other tests of that row, not required-row acceptance.

### Checks that the assertions fail on wrong behavior

Three temporary changes were each run once on the required row, with the
same test and fixture logic, and then discarded. Every run failed the result
gate at the stated assertion. In the first two the native restoration still
returned the baseline. In the third the changed observer addressed the other
manager for its restoration too, so that run says nothing about the named
daemon's final state; its fixture was discarded like the others.

| Temporary change | First failing assertion |
| --- | --- |
| Product: `Client.command` sends an uncertain named Tell once more on a new session | The original caller returned no error: "applied Tell mutation lost its unknown outcome when its connection ended" |
| Test: the submitted command sets the baseline value instead of A | `applied`: effective 10000 with override 10000 is not the wanted state on the named daemon |
| Fixture: the observer reads the other manager's admin socket | `applied`: effective 10000 with no override is not the wanted state on the named daemon |

No hosted CI run of this row is recorded here. Final-head hosted execution, if
requested, is recorded on the pull request, which supersedes this note for the
final SHA.

### Current-manager cell and secure mode

Direct native execution, not hosted CI: Linux amd64 host, Docker 29.9.1, the
image above, three MONs and two MGRs without OSDs, on source commit
`ce61db517ae4b73f923d0da1d97ceff7f47ac3f9`. That commit holds the tests,
the gates, the fixture control and the catalog rows; the commit after it in
the same pull request changes documentation only. Each run used the
environment that the native workflow passes for its catalog row and the same
result gate. One `go test` process ran on the engine at a time.

| Row | Selector | Gate | Tests | Fixture run |
| --- | --- | --- | --- | --- |
| `mon-mgr.aes.ip4.crc.host.t-appliedcurrentmanagertell` | `^TestCephAppliedCurrentManagerTellIsNotReplayedIntegration$` | expected the one client test, observed `PASS`, no problems | 7.82 s | 44 s |
| `mon-mgr.aes256k.ip4.host.t-suite-e9be0b` | `^TestCephApplied(Named|Current)ManagerTellIsNotReplayedIntegration$` | expected both client tests, observed both `PASS`, no problems | named 7.52 s, current 7.92 s | 51 s |
| `mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell` (unchanged row, re-run on that commit) | `^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$` | expected the one client test, observed `PASS`, no problems | 7.82 s | 45 s |

Current-manager cell, CRC row:

| Observation | Retained result |
| --- | --- |
| Baseline | `mgr.a`, active GID 4167, MgrMap epoch 4 before and after; effective 10000, no override; bystander `mgr.b` effective 10000, no override |
| Control before A | Read-only `MgrTell`: local connection 2, TID 1, sequence 1, passed the armed gate |
| Original CRC submission of A=10001 through `MgrTell` | written to the endpoint of `mgr.a`, local connection 2, client global ID 4497, TID 2, sequence 2, complete local write |
| Withheld frame | type 98, TID 2, sequence 2, 132 bytes, on connection 2 |
| Native A on the receiving manager while the reply was withheld | effective 10001, override 10001, `a`/4167, epoch 4 |
| Native overwrite B=10002 | `config set log_max_recent 10002` exited successfully once; effective 10002, override 10002 |
| Original caller after connection 2 was closed | `OutcomeUnknownError` (cause: use of closed network connection), empty result, no target-change cause |
| Fresh reads | Subject client through `MgrTell`: B over local connection 3, TID 1, sequence 1. Independent Go client: B. Native: effective 10002, override 10002, `a`/4167 |
| Local transmissions of A | exactly one, unchanged identity; padded writes 1 |
| Bystander at the end | `mgr.b` effective 10000, no override |
| Native restoration | One `config unset log_max_recent`, exited successfully; a separate fresh read shows effective 10000 and no override |

Secure row, both cells (the same values in each; the client global ID was
4497 for the exact-name and 4605 for the current-manager cell):

| Observation | Retained result |
| --- | --- |
| Baseline | `mgr.a`, active GID 4167, epoch 4; effective 10000, no override; bystander `mgr.b` unchanged |
| Controls before A | Exact-name cell: `MgrTellTo` of A to `b` refused locally with no write. Both: a read-only Tell returned the baseline and no write had reached 4096 bytes |
| Padded write of A=10001 | to the endpoint of `mgr.a` on local connection 2; from before it left, every byte received on connection 2 was withheld |
| Native A, awaited with the caller pending | effective 10001, override 10001, `a`/4167, epoch 4 |
| Native overwrite B=10002 | exited successfully once; effective 10002, override 10002; the caller was still pending |
| Original caller after connection 2 was closed | `OutcomeUnknownError` (cause: use of closed network connection), empty result, no target-change cause |
| Fresh reads | Subject client: B over later local connection 3 to `mgr.a`. Independent Go client: B. Native: effective 10002, override 10002, `a`/4167 |
| Local transmissions | padded writes 1 across every subject connection; no transaction or reply receipt exists in this mode |
| Native restoration | One `config unset log_max_recent`, exited successfully; a separate fresh read shows effective 10000 and no override |

After each run only the provider's reaper container remained on the host for
its usual grace period; no independent final network inventory was taken.

Both cells were also run once on that commit, with the secure row's selector,
on three other profiles of the catalog whose broad selection now includes
them: `mon-mgr.aes256k.ip6` (secure, IPv6, container test process),
`mon-mgr.aes256k.ip6.crc` (CRC, IPv6, container test process) and
`mon-mgr.aes.ip4.release-20.2.3` (secure, AES, the catalog's pinned 20.2.3
image). All six tests passed their gate, each in 7.0 to 7.4 seconds. These
are selector overrides of broad rows, so they show that the cells run on
those profiles, not required-row acceptance.

#### Checks that the assertions fail on wrong behavior

Three temporary changes were each run on both new rows and then discarded;
`git status` was clean afterwards. Every run failed the result gate. In the
first two rows the native restoration still returned the baseline.

| Temporary change | Current-manager CRC row | Secure row (both cells) |
| --- | --- | --- |
| Product: `Client.command` sends an uncertain MGR Tell once more on a new session | The original caller returned no error: "applied Tell mutation lost its unknown outcome when its connection ended" | The same assertion in both cells |
| Test: the submitted command sets the baseline value instead of A | `applied`: effective 10000 with override 10000 is not the wanted state on the receiving daemon | `applied`: "a state outside the change from the previous to the wanted one", at the first observation |
| Fixture: the observer reads the other manager's admin socket | `applied`: effective 10000 with no override is not the wanted state. The restoration also went to the other manager | The awaited state never appears, and the cell fails when the caller's session ends by its keepalive timeout: "original caller ended before the native state was observed", after 64 seconds. The restoration also went to the other manager, so these runs say nothing about the receiving daemon's final state |

The last row shows the cost of the secure mode: without a reply barrier, a
change that is never observed is refused only when the caller ends or the
test context expires, not at once.

A fault of the test gates themselves was found during these runs and fixed
before the evidence above: closing the held connection could report "use of
closed network connection" when the session closed it first. The
[daemon command record](DAEMON_COMMAND_OUTCOME_VALIDATION.md#first-failure)
keeps that first failure.

No hosted CI run of these rows is recorded here.

### After review of the pull request

Two review findings on this cell were addressed after the evidence above.

- The reply gate parked without delivering frames that arrived before the
  bound reply in the same read. The read loop had been corrected with the
  close order; the reported shape is now pinned by
  `TestTellReplyGateDeliversTheWholePrefixBeforeParking`, which delivers 0 of
  612 bytes with the earlier read loop.
- The native `overwrite` and `restore` were not bound to the manager process
  that supplied the baseline. With a fake fixture backend, a baseline of
  `mgr.a` GID 4167 with override 12000 followed by GID 4999 with override
  15000 made `restore` return success and set the replacement to 12000. The
  control now refuses in that case, as described under the native observer.

The three MGR Tell rows were run again on source commit
`def4650d68d8486e1a3e52a1026e309e318e0803`, which contains both changes, on
the same host, image and gate. Each gate expected exactly its selected client
tests and observed them as `PASS` with no problems and no skip:
`mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell` (8.22 s, run 46 s),
`mon-mgr.aes.ip4.crc.host.t-appliedcurrentmanagertell` (8.12 s, run 43 s) and
`mon-mgr.aes256k.ip4.host.t-suite-e9be0b` (8.53 s and 8.42 s, run 53 s). Every
native overwrite and restoration receipt in these runs reports
`refused=false`, `ok=true` and `same_incarnation_after=true`. The refusal
itself was exercised with the fake backend only: no native run replaced a
manager between phases. The temporary changes above were not repeated on that
commit.


## Limits

One active manager on one unchanged incarnation, addressed by its exact name
or as the current manager, one originally absent override, a recovery window
of a few seconds, and two required profiles: CRC with AES client and service
keys and secure with AES256K keys, both IPv4 with a host test process. Not
established:

- the pre-existing-override restoration branch against a daemon (daemon-free
  fixture tests only);
- secure-mode frame attribution (see [secure mode](#secure-mode)), and
  required rows for other key types, IPv6 or the container runtime;
- what `MgrTell` does when the active manager changes while a reply is
  withheld: the cell keeps one manager active;
- atomic identity of a native mutation's target: the incarnation is checked by
  a map read immediately before the admin-socket call and again after it, not
  by the admin socket itself;
- manager replacement or restart while the reply is withheld, which would
  clear the process-local override and is covered for module commands by the
  [MGR command cell](MGR_COMMAND_OUTCOME_VALIDATION.md);
- a server execution count. One complete local write and a surviving native
  overwrite show that the client did not transmit A again and that A was not
  applied after B. They do not prove exactly-once execution in general.

The same daemon incarnation is inferred from the unchanged active GID in every
bracket and the surviving override, not from a process ID.
