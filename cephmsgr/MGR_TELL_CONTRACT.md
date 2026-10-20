# Named active MGR tell

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

`Client.MgrTellTo(ctx, name, command)` sends raw daemon-local commands to the
exact bare name of the currently available active manager. It shares the
ordinary MGR connection, service ticket, transaction sequence and MaxInFlight
budget. Command construction, schemas and interpreting output remain the
consumer's responsibility. No opt-in, feature advertisement or new session
profile is introduced.

## Target and lifetime

The name is exact: empty names, removing a `mgr.` prefix, interpreting a numeric
rank or expanding wildcards are unsupported. An absent name returns
`ErrManagerNotFound`; a known standby or unavailable active name returns
`ErrManagerNotActive`. These are local refusals before a command is sent.
StandbyInfo has no command address, and this API does not connect to standby
managers or wait for them to become active. `MgrTell` continues to mean the
current active daemon without a caller-selected name.

After command-slot admission, the operation selects the authenticated MgrMap's
name, daemon GID and complete retained v2 address vector, including nonce, along with the
client GlobalID. That binding is fixed before waiting for MGR setup. Setup
never switches the operation to a replacement daemon. Messenger authenticates
the MGR service and checks ServerIdent's daemon GID and the selected exact
address; the name association comes from the authenticated MON map, not a
separate daemon-name credential.

The existing MgrMap codec retains usable v2 addresses for MGR connections. The
binding covers that vector; it does not claim preservation of or sensitivity
to changes in discarded v1 addresses, and no v1 connection is attempted.

All calls sharing the active MGR session check its immutable binding at the
single writer's existing BeforeWrite boundary. A changed name, GID, address
vector or client identity rejects an unstarted call. A lost MON admission also
refuses unstarted writes. Ordinary MgrCommand/MgrTell retain their existing
known-unsent ErrRetired preparation retry; named calls never use that retry to
select another daemon. Epoch-only updates, standby metadata changes and ticket
renewal with the same client GlobalID do not change the binding.

Name changes retire the old MGR session even if GID and addresses are unchanged.
Named calls report `ErrManagerTargetChanged`, also matching `ErrManagerChanged`,
when their selected daemon changes. A local admission check cannot establish
that the remote active state will remain unchanged after it returns. Commands
whose transmission has started preserve `OutcomeUnknownError` on map change,
loss, cancellation or Close. Neither named nor ordinary commands automatically
re-execute uncertain input. A new invocation may explicitly select the newly
active name. Client Close owns setup, sessions and their cleanup.

## Wire and source

This API reuses TellCommand: MCommand type 97, authenticated nonzero cluster
FSID, a vector containing the caller's exact JSON string, header TID, and raw
bulk data. It does not substitute MMgrCommand or zero-FSID MCommand, which route
to module commands. Existing command reply decoding preserves signed status,
status text and binary data; negative status returns CommandError rather than
AuthenticationError. The server requires allow-all MGR capabilities for tell.

Pinned source anchors:

- `src/mon/MgrMap.h:189–258,280–293`: standby name/GID/modules/features; only
  the active daemon has an address vector.
- `src/mgr/MgrClient.cc:198–233,527–560`: native named tell selects only the
  matching active name. Native pending-command retransmission is deliberately
  not copied into the Go command contract.
- `src/mgr/MgrClient.cc:189–194`: an ordinary CLIENT sends no MMgrOpen.
- `src/mgr/DaemonServer.cc:1547–1560,1628–1671`: nonzero FSID selects the admin
  socket path; allow-all permissions are checked before dispatch.
- `src/mgr/MgrStandby.cc:301–305,430–457`: the active manager publishes its
  server addresses; standby operation does not supply another command route.
- `src/pybind/ceph_argparse.py:1597–1606`: CLI tell forwards the exact MGR
  target name to the native manager client.

The referenced Ceph source files carry LGPL-2.1 notices. The Go binding and
admission logic are independently written; no C++ implementation is copied.

## Verification boundary

Synthetic tests cover exact names, local standby/absent-name refusal, shared
TIDs and raw binary/signed errors, immutable address copies, name-only map
changes, setup obsolescence, a blocked writer with queued unstarted work,
context cancellation, Close and no replay. The ordinary MGR recovery and
ticket-renewal regressions remain applicable to the shared path.

`TestCephNamedManagerTellIntegration` and the closed `observe-named-mgr` fixture
control passed seven cases each on CRC/AES IPv4 host and secure/AES256K IPv6
container configurations. Fresh native before/after MgrMap identity views and
exact-name tell observations match the Go results, including allow-all denial,
concurrent mixed command/tell calls, ticket renewal and explicit new-name
selection after failover. The native command is read-only; the disposable
failover mutation is submitted once. See the [acceptance record](../integration/fixture/MGR_NAMED_TELL_VALIDATION.md)
for source hashes, raw command results, identities and owned cleanup. These
focused runs do not establish full CI coverage. No
standby command service, arbitrary named-daemon address discovery or typed
manager operations are claimed.
