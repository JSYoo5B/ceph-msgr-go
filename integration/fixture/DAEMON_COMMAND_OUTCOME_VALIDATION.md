# Independently observed OSD, PG and MDS command mutations and no replay

This is a bounded implementation checkpoint of #63, after the
[MON](MON_COMMAND_OUTCOME_VALIDATION.md), [MGR](MGR_COMMAND_OUTCOME_VALIDATION.md),
[MON Tell](MON_TELL_OUTCOME_VALIDATION.md),
[current MON Tell](MON_CURRENT_TELL_OUTCOME_VALIDATION.md) and
[MGR Tell](MGR_TELL_OUTCOME_VALIDATION.md) cells. It covers one applied
command with a withheld reply on each explicitly targeted daemon channel:
`OSDConnection.Command`, `OSDConnection.PGCommand` and `MDSConnection.Command`,
each in CRC mode and, since 2026-11-14, in a [secure mode](#secure-mode)
without decoded frames.
The product API and replay behavior are unchanged; see the
[OSD and PG command contract](OSD_COMMAND_VALIDATION.md) and the
[MDS command contract](../../cephmsgr/MDS_COMMAND_CONTRACT.md).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Pinned source facts

Read at the reference tag for this change.

- [`src/osd/OSD.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc)
  `OSD::handle_command`: an MCommand from a session without the allow-all OSD
  capability is answered with `-EACCES`; any other is queued to the OSD
  process's admin-socket command handler.
- [`src/mds/MDSDaemon.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/MDSDaemon.cc)
  `MDSDaemon::handle_command`: the same for the MDS, after its own allow-all
  capability check and JSON parse.
- [`src/common/admin_socket.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/admin_socket.cc)
  `AdminSocket::do_tell_queue`: the executor replies with MCommandReply on the
  originating connection with the request's transaction ID.
- [`src/common/ceph_context.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/ceph_context.cc)
  and [`src/common/config.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/config.cc):
  every daemon process registers the daemon-local `config set`, `config get`,
  `config unset` and `config diff get` handlers, and a runtime set or unset
  changes the override source of the option in that process only. These are
  the facts the [MGR Tell record](MGR_TELL_OUTCOME_VALIDATION.md) relies on.
- [`src/osd/OSD.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc)
  `OSD::asok_command` and the registrations at the end of
  `OSD::final_init`: the commands that take a `pgid` and are routed to a PG are
  `query`, `log`, `list_unfound`, `mark_unfound_lost`, `scrub`, `deep-scrub`,
  `scrub-abort`, `schedule-scrub`, `schedule-deep-scrub` (and their old
  `pg <pgid> <cmd>` forms) and `scrubdebug`.
- [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc)
  `PrimaryLogPG::do_command` executes them. Routing differs by command
  ([`OSD::asok_command`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L2828-L2833)):
  `query` and the other PG commands are routed only to a PG whose primary is
  this OSD and are refused elsewhere, while `scrubdebug` is routed with
  `only_primary` false, so an OSD that holds a non-primary copy of the PG also
  processes it. In this record the primary is therefore selected by the
  fixture and by the primary-only `query` used as the observation, not by the
  `scrubdebug` dispatch.
- [`src/osd/scrubber/pg_scrubber.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/scrubber/pg_scrubber.cc)
  `PgScrubber::asok_debug`: `scrubdebug <pgid> set|unset sessions` sets or
  clears one boolean of the PG's scrubber. `PgScrubber::dump_scrubber`: while
  it is set, the `scrubber` section of the primary's `query` output lists a
  `test_sequence` counter. The source marks this as a test-only feature.

Which daemon executed a command is established here by the changed state on
that daemon's own admin socket, not by the request: MCommand carries the
command and the cluster FSID, and the target is the connection the caller
opened.

## Command choice

**OSD and MDS.** The mutation is the daemon-local
`{"prefix":"config set","var":"log_max_recent","val":["<A>"],"format":"json"}`,
the command, option and bound of the MON and MGR Tell cells: the baseline must
be 1..19998 with an unambiguous override, A is baseline+1 and B is baseline+2.
It changes one process-local runtime override and is restored by one
`config unset` (or `config set` of an original override).

**PG.** The PG-routed commands of the pinned OSD leave one candidate:

| Command | Why it is or is not usable for this cell |
| --- | --- |
| `query`, `log`, `list_unfound` | Read-only |
| `mark_unfound_lost` | Refused unless the PG has unfound objects with nothing left to probe; it then deletes or reverts objects and cannot be restored |
| `scrub`, `deep-scrub`, `schedule-scrub`, `schedule-deep-scrub` | Request or schedule a scrub. The reply reports the request; the scrub and its stamps happen later, are also changed by periodic scrubs, and cannot be restored |
| `scrub-abort` | Changes only a scrub that is running or requested |
| `scrubdebug <pgid> block`, `unblock`, `set block` | Fake a blocked scrub range for a scrub in progress |
| `scrubdebug <pgid> set sessions` | Sets one boolean synchronously; visible only as the `test_sequence` entry of the primary's `query`; cleared by `unset sessions` |

So the cell uses
`{"prefix":"scrubdebug","pgid":"<pgid>","cmd":"set","value":"sessions"}`. The
state is binary: the baseline must be the unset flag, A sets it, and the
native overwrite B unsets it again. B therefore equals the baseline, but it is
still distinct from A, so a replay of A after B would list the counter again.
Restoration is one more native `unset`. This flag is an upstream test hook: the
cell shows the PG command channel's outcome and no-replay boundary, not a
production PG administration operation. No other reversible, synchronously
applied and independently observable PG-routed command exists at this tag.

## Executable boundary

Tests, all in the `cephmsgr` suite without an opt-in parameter of their own:

| Test | API | Required CRC row | Required secure row |
| --- | --- | --- | --- |
| `TestCephAppliedOSDCommandIsNotReplayedIntegration` | `OSDConnection.Command` | `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` | `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` |
| `TestCephAppliedPGCommandIsNotReplayedIntegration` | `OSDConnection.PGCommand` | the same row | the same row |
| `TestCephAppliedMDSCommandIsNotReplayedIntegration` | `MDSConnection.Command` | `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` | `mds-commands.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` |

The OSD and PG cells need `CEPH_MSGR_TEST_OSD=1` with the legacy-v6 profile;
the broad OSD selection does not match their names, so only an explicit
selector runs them. The MDS cell needs `CEPH_MSGR_TEST_MDS_COMMANDS=1`, whose
fixture admits exactly this one further selector. With
`CEPH_MSGR_TEST_CONNECTION_MODE=crc` the cells decode frames; in the default
secure mode they run their [secure mode](#secure-mode). Reproduce the CRC
OSD and PG row with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 \
CEPH_MSGR_TEST_RUN='^TestCephApplied(OSD|PG)CommandIsNotReplayedIntegration$' \
sh integration/run.sh
```

For the MDS cell add `CEPH_MSGR_TEST_MDS_FRESH=1` and
`CEPH_MSGR_TEST_MDS_COMMANDS=1` and select
`^TestCephAppliedMDSCommandIsNotReplayedIntegration$`. The secure rows use the
profile of the existing secure rows of each fixture: AES256K client and
service keys, IPv6, the test process in a container, no connection-mode
parameter, BlueStore for the OSD and PG row and MemStore for the MDS row.

### Targets

The caller supplies every target, as the product requires. The tests obtain
them as follows; none of this is product behavior.

- OSD and PG: the PG is the fixture's object PG. Fresh MON reads of the test
  client (`pg map` and `osd dump`) give its acting primary, the map epoch and
  that OSD's v2 address with nonce.
- MDS: the fixture's owned filesystem ID, rank 0 and the active daemon's v2
  address with nonce from the fixture's MDS oracle, with the dedicated
  `mds allow *` command identity.

The native baseline observation must be bracketed by the daemon whose address,
nonce included, equals the address the Go client dials. This binds the
observer's admin socket to the daemon incarnation the command is sent to.

### Native observer

The fixture control `observe-daemon-command` (`daemon_command_outcome.go`)
accepts a request ID, a 32-hex token, a closed phase and a closed target:
`osd <id>`, `pg <osd id> <pgid>` or `mds <name>`, each of which must be owned
by the fixture. It accepts no command, option, value or path. Every read and
the OSD and PG mutations go through that daemon's admin socket, a local Unix
socket that does not use the Messenger or CephX, with a 10-second bound per
call. The two MDS mutations are sent by the fixture's native client as
`tell mds.<GID>` with the GID recorded with the baseline, on that client's own
connection. One observation is:

1. a fresh native map read of the daemon: for an OSD its `osd dump` entry (up,
   `up_from` epoch, public v2 address and nonce), for an MDS its `fs dump`
   entry (`up:active`, GID, v2 address and nonce);
2. the owned state: `config get log_max_recent` (original bytes) and the owned
   entry of `config diff get log_max_recent`, or for the PG cell
   `pg <pgid> query`, of which only the PG ID, the state string, whether a
   `scrubber` section exists and whether it lists `test_sequence` are kept;
3. a second fresh map read.

The Go helper accepts a state only when both map reads show the same up
daemon, incarnation and address, that incarnation is the one recorded at the
baseline, and the state reads succeeded. A PG query without a `scrubber`
section (the OSD is not the active primary) is unreadable.

| Phase | Action |
| --- | --- |
| `initial` | Observe and record the baseline once, bound to the token and target. A repeated request cannot replace it, and a request for another target is refused |
| `applied`, `recovered`, `restored` | Observe |
| `overwrite` | Claim, then `config set log_max_recent <baseline+2>` or `scrubdebug <pgid> unset sessions` once, then observe |
| `restore` | Claim, then `config unset log_max_recent`, `config set` of the original override, or `scrubdebug <pgid> unset sessions` once, then observe |

Each mutation writes its claim file before the native call. A second request
of the same phase is refused, and a call that fails or does not return leaves
its outcome uncertain for the caller. Receipts are published atomically before
the acknowledgement and appended to `daemon-command-outcome-native.jsonl`.

The baseline belongs to one daemon process, so its owner record also keeps
its incarnation (the OSD's `up_from` epoch or the MDS GID) and its public address with nonce. Immediately before `overwrite` and `restore` the control reads
the map again and sends the command only if it shows that same up
incarnation and address. Otherwise the receipt says `refused`: nothing
is claimed and nothing is sent, so a daemon restarted or replaced under the
same name never receives a value derived from another process's baseline. An
owner record without an incarnation and a failed map read are refused the same way. The
two map reads after the call are compared with the recorded incarnation and reported as
`same_incarnation_after`; the Go cells count a mutation only when it was not
refused, returned successfully and that report is true.

The admin socket is addressed by daemon name and reports neither value, so
for the OSD and PG cells identity is not proven atomically with the command.
An OSD replaced between the map read and the admin-socket call (two
consecutive calls of the control) is not prevented; it is reported by the
reads after the call.

An MDS mutation cannot reach a replacement. A decimal `mds.<GID>` target that
is not a rank of the filesystem
([`FSMap::parse_role`](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/FSMap.cc))
resolves only while that GID is in the FSMap, the command is sent to that
incarnation's own addresses, and a GID is never reused. If the daemon is
replaced after the preflight read, the command fails, the receipt records the
failure and `same_incarnation_after` is false, and the replacement keeps its
own override. The reads on the admin socket remain addressed by name and are
bracketed by the map reads as before.

### Reply gate and sequence

This is the CRC mode; the [secure mode](#secure-mode) lists its substitutions.
The reply gate is the transaction-specific gate of the
[MGR Tell cell](MGR_TELL_OUTCOME_VALIDATION.md#reply-gate), installed on every
connection the subject client dials. OSD, PG and MDS commands are the same
MCommand (97) and MCommandReply (98) messages on the daemon session. The hold
of the reply to A's own transaction is the barrier: the following native read
is taken once, and no assertion depends on a wall-clock margin.

1. `initial` on the target: the bracketed, restorable baseline, whose daemon
   address is the address the Go client dials. An independent Go client reads
   the same baseline through the channel under test.
2. On the subject client, with the gate armed: a read-only command (`config
   get`, or the PG `query`) passes the gate on the daemon connection with its
   own transaction.
3. Submit A once. Wait for the hold, and then for the writer to publish its
   completed local write: the receipt and the padded-write count are recorded
   after the local write returns, which the held reply does not order.
   Require one complete local write of A,
   on the same connection as the read-only command, to the target daemon's
   endpoint, with the bound transaction, and the held frame to be type 98 with
   that transaction.
4. `applied`: A on the daemon's own admin socket, with the caller still
   pending.
5. `overwrite`: B, written natively once and read back. The caller is still
   pending and the padded-write count is still one.
6. Close only the bound connection. The original call must return
   `OutcomeUnknownError` with an empty result, and its retained submission
   transaction must be the transaction that was written.
7. The ended connection does not recover by itself. The test closes the handle
   and opens it again on the same client; a read-only command reads B over
   another connection; the independent client reads B; `recovered` shows B
   natively under the same daemon incarnation.
8. Across all subject connections: still exactly one write of A with its
   original identity, exactly two read-only writes (the control and the
   recovery read), and the gate held only that one frame. The recovery read
   has the control's content and its reply does not order its own local write
   either, so the test first waits for the second record to be published.
9. Cleanup with a fresh 60-second context: `restore` once, then `restored`
   must show the baseline. A failed or unreturned restoration is reported and
   never repeated.

## Secure mode

Secure frames cannot be decoded by a connection wrapper, so neither the
written command nor a reply frame can be identified. The cells keep the same
targets, native observer, overwrite, recovery reads and restoration, with the
substitutions of the [MGR Tell cells](MGR_TELL_OUTCOME_VALIDATION.md#secure-mode)
and one further requirement:

- **Gate.** A is padded to more than 40 KiB. On the first write of at least
  4096 bytes to the target daemon's endpoint, the gate binds to that
  connection before the bytes leave and from then on withholds every byte
  received on it. No reply to that write can reach the session. Before A, the
  read-only command is answered on the same client and the test requires that
  no write of that size has happened on any subject connection, so the padded
  write is A.
- **Barrier.** The binding shows that the padded write was issued, not that
  the daemon replied. The applied state is therefore awaited: the `applied`
  observation is repeated until it shows A. For the option cells an
  observation is two admin-socket calls, so only a value and an override that
  each belong to the baseline or to A are read again; the PG flag has only its
  unset and set state. Any other state, an unreadable or unbracketed read, or
  the end of the original caller fails at once. The wait ends on the state
  itself, bounded by the test context.
- **Withheld bytes.** After A is observed, bytes must have arrived on the
  bound connection and been withheld. This shows that the daemon sent
  something after the padded write that the session never received. It does
  not show what: the bytes are not decoded.
- **Caller.** After the native overwrite the caller is checked to be still
  pending. When the connection is closed it must return
  `OutcomeUnknownError` with an empty result. Its retained submission
  transaction must be nonzero; it is compared with no frame.
- **Transmission count.** Exactly one write of at least 4096 bytes across
  every subject connection, before and after the recovery read. The same
  daemon endpoint must have been dialed again after the held connection, and
  the gate must still hold only the first one. The receipt lists every such
  dial (`later_dials_to_target`). A dial is recorded when it succeeds, before
  any handshake or command, so the list does not say which of them carried
  the recovery read; `recovery_connection` is written in CRC mode only,
  from the decoded read.

What the secure mode does not establish: the transaction and sequence of A on
the wire, that the withheld bytes contain the reply to A (they can be an
acknowledgement or any other frame), or the identity of any frame. The applied
native state shows that the daemon executed A; the withheld stream shows that
the caller could not have been told. Frame-level attribution in secure mode
would need instrumentation inside the session, after decryption, which no
test has.

## Daemon-free checks

- `TestNativeDaemonCommandBetweenAdmitsOnlyTheChangeFromPreviousToWanted`
  (`cephmsgr/daemon_command_outcome_receipt_test.go`): the secure mode's wait
  reads again only the previous state, the wanted state and the two torn
  option reads; a lost application that only adds an override of the baseline
  value, a third value or override, another incarnation, nonce or target, a
  failed read and a PG reply without a scrubber section or of another PG are
  refused; a torn read never ends the wait.
- `TestPaddedReplyGateWithholdsEveryByteAfterTheFirstPaddedWrite`
  (`cephmsgr/tell_reply_gate_test.go`): the gate of the secure mode.
- `cephmsgr/daemon_command_outcome_receipt_test.go`: open requests are rejected
  before anything is published; a receipt matches only the wanted state on the
  target under one unchanged up incarnation and address, and not for another
  daemon, incarnation, nonce, target kind or PG, a failed read or a changed
  bracket; the PG projection reads the flag only from a scrubber section of
  the requested PG.
- `integration/fixture/daemon_command_outcome_test.go`: closed targets and
  phases; baseline ownership by token and target; refusal of an unstable,
  unreadable or out-of-bound baseline, of a set PG flag and of a PG without a
  scrubber section; overwrite and restoration derived from the recorded
  baseline and submitted once, for an absent and a pre-existing override; a
  failed mutation kept uncertain with path-like text masked; projection of
  only the owned diff entry and of only the PG flag; the `osd dump` and
  `fs dump` identity projections; admission of the one further MDS selector.
- `TestDaemonCommandMutationsAreBoundToTheBaselineIncarnation` (fixture): for
  the OSD, PG and MDS targets, both mutations and a daemon with a new map
  incarnation, a new address nonce, an unreadable map or an owner record
  without an incarnation, the receipt is `refused`, no claim file exists and
  the replacement receives no mutating call; a replacement after the check is
  reported by `same_incarnation_after`.
- `TestMDSCommandMutationsReachOnlyTheBaselineGID` (fixture): the MDS
  mutations are built as `tell mds.<GID>` and a zero GID is rejected; with a
  replacement that comes up after the preflight read, with its own override,
  the one call addressed to the baseline GID fails, no mutating admin-socket
  call is made and the replacement keeps its override, for both mutations.
- `cephmsgr/tell_reply_gate_test.go`: the gate (see the MGR Tell record),
  including `TestHeldReplyDoesNotOrderTheLocalWriteReceipt` and
  `TestRepeatedReadReceiptWaitsForItsOwnWrite`.

These checks establish no native result.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 host, Docker 29.9.1, the
image above, on source commit `ce61db517ae4b73f923d0da1d97ceff7f47ac3f9`.
That commit holds the tests, the gate, the fixture control and the catalog
rows; the commit after it in the same pull request changes documentation
only. Each run used the environment that the native workflow passes for its
catalog row and the same result gate (`tools/native_test_results.py snapshot`
before and `verify` after). One `go test` process ran on the engine at a time.

| Row | Selector | Gate | Tests | Fixture run |
| --- | --- | --- | --- | --- |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` | `^TestCephApplied(OSD|PG)CommandIsNotReplayedIntegration$` | expected both client tests, observed both `PASS`, no problems | OSD 5.02 s, PG 4.52 s | 60 s |
| `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` | `^TestCephAppliedMDSCommandIsNotReplayedIntegration$` | expected the one client test, observed `PASS`, no problems | 5.52 s | 112 s |

| Observation | OSD command | PG command | MDS command |
| --- | --- | --- | --- |
| Target and baseline | `osd.0`, up, `up_from` epoch 5, address equal to the dialed one; effective 10000, no override | PG `2.0` on `osd.0`, the same OSD bracket; `active+clean`, scrubber section present, counter not listed | the active rank-0 MDS, `up:active`, GID 4695, address equal to the dialed one; effective 10000, no override |
| Control before A | `config get`: connection 2, TID 1, sequence 1, passed the armed gate | `query`: connection 2, TID 1, sequence 1 | `config get`: connection 2, TID 1, sequence 1 |
| Original CRC submission of A | `config set` 10001: connection 2, TID 2, sequence 2, complete local write | `scrubdebug 2.0 set sessions`: connection 2, TID 2, sequence 2 | `config set` 10001: connection 2, TID 2, sequence 2 |
| Withheld frame | type 98, TID 2, sequence 2, 747 bytes | type 98, TID 2, sequence 2, 122 bytes | type 98, TID 2, sequence 2, 132 bytes |
| Native A while the reply was withheld | effective 10001, override 10001 | counter listed (query reply of 6755 bytes) | effective 10001, override 10001 |
| Native overwrite B | `config set log_max_recent 10002` exited successfully once; effective 10002, override 10002 | `scrubdebug 2.0 unset sessions` exited successfully once; counter not listed (6720 bytes) | `config set log_max_recent 10002` exited successfully once; effective 10002, override 10002 |
| Original caller after connection 2 was closed | `OutcomeUnknownError` (cause: use of closed network connection), empty result, retained transaction 2 | the same | the same |
| Fresh reads | Reopened connection of the subject client: B over connection 3, TID 3, sequence 1. Independent Go client: B. Native: B under the same bracket | the same pattern; counter not listed | the same pattern; B under GID 4695 |
| Local transmissions of A | exactly one, unchanged identity; padded writes 1 | the same | the same |
| Native restoration | One `config unset log_max_recent`, exited successfully; a separate fresh read shows effective 10000 and no override | One more `scrubdebug 2.0 unset sessions`, exited successfully; a separate fresh read shows the counter not listed | One `config unset log_max_recent`, exited successfully; a separate fresh read shows effective 10000 and no override |

The recovery read on the reopened connection carried TID 3 with sequence 1 in
all three cells: the transaction counter is not per connection. After each run
only the provider's reaper container remained on the host for its usual grace
period; no independent final network inventory was taken.

### Checks that the assertions fail on wrong behavior

Three temporary changes were each run on both rows and then discarded;
`git status` was clean afterwards. Every run failed the result gate at the
stated assertion.

| Temporary change | OSD command | PG command | MDS command |
| --- | --- | --- | --- |
| Product: `OSDConnection.command` and `MDSConnection.Command` close the handle, open it again and send an uncertain command once more | The original caller returned no error: "applied command lost its unknown outcome when its connection ended" | the same assertion | the same assertion |
| Test: the submitted command does not establish A (it sets the baseline value; for the PG it sends `unset`) | `applied`: effective 10000 with override 10000 is not the wanted state | `applied`: the counter is not listed | `applied`: not the wanted state |
| The observer reads another target | Fixture reads the option on `mon.a`'s admin socket: `applied` shows 10000 with no override | Test asks the observer for PG `1.0` while the command goes to `2.0`: `applied` shows the counter not listed | Fixture reads the option on `osd.0`'s admin socket: `applied` is not the wanted state |

In the first two rows the native restoration still returned the baseline. In
the third the restoration followed the changed observer, so those runs say
nothing about the target's final state; their fixtures were discarded.

### First failure

The first run of the product change above on the OSD and PG row failed the
OSD cell at a different point than expected:

```
close the subject connection close tcp <local>-><relay>: use of closed network connection
```

The cause was in the test gate, not in the product or the cell's logic.
Closing the held connection released the withheld read before closing the
underlying connection; the session closes its connection as soon as that read
ends, and when its close came first the test's own close returned an error
that the cell treats as a failure. The same order existed in the exact-name
MGR Tell cell. Both gates now close the underlying connection exactly once
and give every caller that result, with a daemon-free regression that fails
on the previous code. The run was repeated after the fix and failed at the
assertion in the table. The earlier passing runs do not explain this failure
and are not used as evidence here; every result above is from the commit that
contains the fix.

### After review of the pull request

A review finding on the MGR Tell observer applied to this control as well: the
native `overwrite` and `restore` were not bound to the daemon process that
supplied the baseline. With a fake fixture backend, an OSD baseline with
override 12000 followed by a new incarnation with override 15000 made
`restore` return success and set the replacement to 12000. The control now
refuses in that case, as described under the native observer.

Both rows were run again on source commit
`def4650d68d8486e1a3e52a1026e309e318e0803`, which contains the change, on the
same host, image and gate. Each gate expected exactly its selected client
tests and observed them as `PASS` with no problems and no skip: the OSD and PG
row (5.62 s and 5.02 s, run 61 s) and the MDS row (6.22 s, run 112 s). Every
native overwrite and restoration receipt in these runs reports
`refused=false`, `ok=true` and `same_incarnation_after=true`. The refusal
itself was exercised with the fake backend only: no native run replaced a
daemon between phases. The temporary changes above were not repeated on that
commit.

### Secure mode (2026-11-14)

Direct native execution, not hosted CI: Linux amd64 host, Docker 29.9.1, the
image above, on source commit `df89356b87ddc6093fdcb2d4dd8557898ad7d19d`,
uploaded as an archive of that commit. It holds the tests and the catalog
rows; the commits after it in the same pull request change documentation
only. Each run used the environment that the native workflow passes for its
catalog row and the same result gate. One `go test` process ran on the engine
at a time. Every row passed on its first run on that commit; the two secure
rows had also passed their first run on the uncommitted change.

| Row | Gate | Tests | Fixture run |
| --- | --- | --- | --- |
| `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` (new, secure) | expected both client tests, observed both `PASS`, no problems | OSD 5.42 s, PG 4.91 s | 69 s |
| `mds-commands.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` (new, secure) | expected the one client test, observed `PASS`, no problems | 5.91 s | 109 s |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16` (CRC, body changed) | expected both client tests, observed both `PASS`, no problems | OSD 5.72 s, PG 4.92 s | 61 s |
| `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand` (CRC, body changed) | expected the one client test, observed `PASS`, no problems | 6.12 s | 110 s |

Secure rows:

| Observation | OSD command | PG command | MDS command |
| --- | --- | --- | --- |
| Target and baseline | `osd.0`, up, `up_from` epoch 5, IPv6 address equal to the dialed one; effective 10000, no override | PG `2.0` on `osd.0`, the same bracket; `active+clean`, scrubber section present, counter not listed | the active rank-0 MDS, `up:active`, GID 4659, IPv6 address equal to the dialed one; effective 10000, no override |
| Control before A | the read-only command returned the baseline; no write had reached 4096 bytes | the same | the same |
| Padded write of A | to the OSD's endpoint on local connection 2; from before it left, every byte received on connection 2 was withheld | the same | to the MDS's endpoint on local connection 2 |
| Native A, awaited with the caller pending | effective 10001, override 10001 | counter listed (query reply of 6754 bytes) | effective 10001, override 10001 |
| Withheld bytes | bytes arrived on connection 2 after the padded write and reached no reader | the same | the same |
| Native overwrite B | `config set log_max_recent 10002` once; effective 10002, override 10002; the caller was still pending | `scrubdebug 2.0 unset sessions` once; counter not listed (6720 bytes) | `config set log_max_recent 10002` once; effective 10002, override 10002 |
| Original caller after connection 2 was closed | `OutcomeUnknownError` (cause: use of closed network connection), empty result, retained submission transaction 2 (compared with no frame) | the same | the same |
| Fresh reads | Reopened connection of the subject client: B over later local connection 3 to the same endpoint. Independent Go client: B. Native: B under the same bracket | the same pattern; counter not listed | the same pattern; B under GID 4659 |
| Local transmissions | padded writes 1 across every subject connection; no transaction or reply receipt exists in this mode | the same | the same |
| Native restoration | One `config unset log_max_recent`; a separate fresh read shows effective 10000 and no override | One more `scrubdebug 2.0 unset sessions`; a separate fresh read shows the counter not listed | One `config unset log_max_recent`; a separate fresh read shows effective 10000 and no override |

After each run no fixture container remained on the host and the uploaded
work directory was removed; no independent final network inventory was taken.

Three temporary changes were each applied to an archive of that commit, run
on both secure rows and discarded with the run; the repository was not
changed. Every run failed the result gate at the stated assertion.

| Temporary change | OSD command | PG command | MDS command |
| --- | --- | --- | --- |
| Product: `OSDConnection.Command`, `OSDConnection.PGCommand` and `MDSConnection.Command` close the handle, open it again and send an uncertain command once more | The original caller returned no error: "applied command lost its unknown outcome when its connection ended" | the same assertion | the same assertion |
| Test: the submitted command does not establish A (it sets the baseline value; for the PG it sends `unset`) | `applied`: effective 10000 with override 10000 is "a state outside the change from the previous to the wanted one", at the first observation | The awaited state never appears, and the cell fails when the caller's session ends by its keepalive timeout: "original caller ended before the native state was observed", after 63 seconds | as for the OSD command |
| Fixture: the observer reads another daemon | The option is read on `mon.a`'s admin socket: the awaited state never appears and the cell fails when the caller ends, after 63 seconds | `pg query` on that socket is unreadable: "native baseline is not one restorable bracketed state of the target daemon", before anything is sent | The option is read on `osd.0`'s admin socket: as for the OSD command, after 64 seconds |

In the first two rows the native restoration still returned the baseline. In
the third the restoration followed the changed observer, so those runs say
nothing about the target's final state; their fixtures were discarded. As in
the MGR Tell cells, the secure mode refuses a change that is never observed
only when the caller ends or the test context expires, not at once. The
temporary changes were not repeated on the two CRC rows on this commit.

No hosted CI run of these rows is recorded here. Final-head hosted execution,
if requested, is recorded on the pull request, which supersedes this note for
the final SHA.

## Limits

One daemon incarnation per cell on single-OSD fixtures and a recovery window
of a few seconds. Not established:

- the pre-existing-override restoration branch against a daemon (daemon-free
  fixture tests only);
- secure-mode frame attribution (see [secure mode](#secure-mode)): no secure
  cell identifies a write, a transaction or a reply frame;
- profiles other than the two required ones per cell (CRC with AES keys, IPv4
  and a host test process; secure with AES256K keys, IPv6 and a container
  test process), replicated or erasure-coded
  pools, a replica or a changed acting set for the PG cell, an MDS standby,
  failover or another rank;
- a daemon restart while the reply is withheld, which would clear the
  process-local state and is the subject of the existing OSD crash tests
  without a state comparison;
- a production PG administration operation: the PG cell's state is an
  upstream test-only flag, and its overwrite equals its baseline, so the final
  restoration is not separately observable;
- atomic identity of a native OSD or PG mutation's target: the incarnation is
  checked by a map read immediately before the admin-socket call and again
  after it, not by the admin socket itself (the MDS mutations are addressed to
  the baseline GID);
- a native run in which an MDS is replaced between the preflight read and the
  mutation: that interval is exercised by the fixture's daemon-free test with
  a fake backend only;
- `scrubdebug` on a non-primary OSD: the cell has one OSD, which is the
  primary;
- a server execution count. One complete local write and a surviving native
  overwrite show that the client did not transmit A again and that A was not
  applied after B. They do not prove exactly-once execution in general.

The same daemon incarnation is taken from the unchanged map entry (the OSD's
`up_from` epoch or the MDS GID, and the address nonce) in every bracket and
from the surviving override, not from a process ID.
