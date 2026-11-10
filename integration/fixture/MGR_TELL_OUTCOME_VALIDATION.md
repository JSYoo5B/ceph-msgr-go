# Independently observed exact-name MGR Tell mutation and no replay

This is a bounded implementation checkpoint of #63, after the
[MON](MON_COMMAND_OUTCOME_VALIDATION.md), [MGR](MGR_COMMAND_OUTCOME_VALIDATION.md),
[exact MON Tell](MON_TELL_OUTCOME_VALIDATION.md) and
[current MON Tell](MON_CURRENT_TELL_OUTCOME_VALIDATION.md) cells. It covers
`Client.MgrTellTo` only. It is not complete command-channel acceptance: current
`MgrTell` and targeted OSD/PG/MDS applied-mutation cells remain open in
[the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md). The product API
and replay behavior are unchanged; see
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
broad CRC rows and skips in secure rows, because its reply gate decodes CRC
frames. Reproduce with:

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

### Reply gate

A test-only connection wrapper (`cephmsgr/integration_tell_reply_gate_test.go`)
is installed on every connection the subject client dials, including later
ones. It is armed with the digests of A's complete front and bulk data. When
that exact command is first written as a complete type 97 frame, the gate
binds to that connection and to the transaction ID in the frame, before the
bytes leave. On the read side it decodes the stream from the banner on and
releases only complete frames. The frame that is a type 98 message with the
bound transaction ID on the bound connection is held: no byte of it is
returned to the Go session, and a channel reports the hold. Every other frame
on every connection passes unchanged. A stream the wrapper cannot decode is
forwarded unchanged and recorded as an error that fails the test.

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
4. Submit A once. Wait for the hold. Require one complete local write of A, on
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
   recovery read), and the gate held only that one frame.
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

These checks establish no native result.

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

## Limits

One exact active name on one unchanged manager incarnation, one originally
absent override, CRC with AES client and service keys, IPv4, host runtime and
a recovery window of a few seconds. Not established:

- the pre-existing-override restoration branch against a daemon (daemon-free
  fixture tests only);
- secure-mode frame attribution, other key types, IPv6 or the container
  runtime;
- manager replacement or restart while the reply is withheld, which would
  clear the process-local override and is covered for module commands by the
  [MGR command cell](MGR_COMMAND_OUTCOME_VALIDATION.md);
- current `MgrTell`, and targeted OSD/PG/MDS applied mutations;
- a server execution count. One complete local write and a surviving native
  overwrite show that the client did not transmit A again and that A was not
  applied after B. They do not prove exactly-once execution in general.

The same daemon incarnation is inferred from the unchanged active GID in every
bracket and the surviving override, not from a process ID.
