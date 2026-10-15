# Disposable PG-wide backoff recipe

Reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
This describes the source-grounded fixture recipe and the separate evidence
boundaries for local acknowledgement submission and native acknowledgement
reception.

`PrimaryLogPG::do_request` sends a PG-wide MOSDBackoff BLOCK when the connection
negotiates RADOS_BACKOFF and the PG is down, incomplete, or peered but inactive
(`PrimaryLogPG.cc:1868-1901`). This last state can be held without stopping the
primary: on a single-OSD disposable pool, temporarily set size=2 and min_size=2.
The recipe waits for the actual primary's `pg query` to report `peered` without
`active`; it does not infer this condition from a MON command succeeding.
Restoring the original size/min_size lets the PG activate. `PG::on_activate`
calls `release_pg_backoffs`, which sends UNBLOCK (`PG.cc:570-657,1423-1429`).
`pauserd` is not used: `Objecter::_op_should_pause` gates native client sends,
so that flag alone does not establish that a direct Go OSD connection receives
a backoff message.

The helper accepts only the explicit `replicated-pg-v8` profile, exactly one
live OSD, eight fixed PGs, hashpspool, a replicated pool and the first existing
object in the native OMAP-write ownership receipt. It compares the pool ID,
pool name, FSID and live policy before every mutation, including restoration.
Three-replica and legacy-v6 fixtures are refused. The helper changes no global
cluster flag, CRUSH map, runtime OSD configuration or unrelated pool. The opt-in
fixture bootstrap sets `osd_op_history_size=1024`: pinned `TrackedOp.cc:101-117`
retains the longest completed operations, so the default twenty-entry limit
could otherwise discard a fast ACK before the independent observation.

Write the public input `osd-backoff-input.json` as:

```json
{"object":"<first fixture-owned OMAP-write object>"}
```

Use `ControlDaemon(ctx, dir, "verify", "osd-backoff", action)`, with actions:

- `start`: save original pool policy, native placement, PG query and native
  object bytes/version; start a token-scoped worker; return only after size=2,
  min_size=2 and the primary's peered/inactive state are independently observed.
- `observe`: return the current immutable-scope receipt without changing policy.
- `finish`: signal the worker, wait for restoration and an active PG, and verify
  independently observed object bytes/version match the original snapshot.

`osd-backoff-result.json` contains `token`, `object`, `ready`, `done`, `restored`,
`source_commit`, `before`, `inactive` and, after restoration, `after`. Each phase
contains `pool` (native OSD dump descriptor), `placement` (native `osd map` JSON),
`pg` (native `pg query` JSON), and `epoch` (MON OSD dump epoch). `before`/`after`
also contain `object_state`, the independent librados snapshot. The actual
applied primary map epoch is `inactive.pg.epoch`; `placement.pgid` is the
actual PG, while `placement.raw_pgid` preserves the raw object hash.

The Go test must separately retain actual BLOCK, its explicit ACK, UNBLOCK,
submission identity and no-replay evidence. These native state receipts do not
claim that any backoff message was sent or acknowledged. The optional native
ACK reception receipt described below is a separate observation. The original STAT
that provokes BLOCK is not replayed by this helper. Finish the scope before any
new caller mutation, and finish restoration before removing the owned object.

After readiness, the worker waits at most 180 seconds without a finish signal,
then restores automatically. Every native CLI command and independent
librados snapshot subprocess has a five-second process timeout; PG transitions
have a bounded twenty-second observation window. `finally` attempts restoration
even after an uncertain first configuration result. Restoration lowers min_size,
restores the recorded size, then explicitly restores the original min_size:
`OSDMonitor.cc:8559` resets min_size when size changes. Restoring size one uses
the required `--yes-i-really-mean-it` flag on this already size-one disposable
pool. Failed or unrestored scopes cannot overlap a subsequent start.

Each scope retains `osd-backoff-<token>-status.json` and
`osd-backoff-<token>-worker.log`. Common `osd-backoff-status.json` and the owned
relative `osd-backoff-worker.log` view identify the current scope. The token
receipt is published last so a previous worker cannot overwrite the next
scope's common view. The diagnostic collector should include public
`osd-backoff-input.json`, `osd-backoff-result.json`, `osd-backoff-status.json`,
`osd-backoff-command.json`, `osd-backoff-worker.log` and the Go integration
receipt `osd-backoff-results.json`; no credentials belong in these files. The
optional public `osd-backoff-ack-input.json` and `osd-backoff-ack-receipt.json`
join this allowlist when native ACK reception is requested.

Local mock tests verify ownership/admission, state/epoch interpretation,
explicit size-one restoration, restoration after uncertain results and deadline,
original object verification, scope isolation and process timeouts. Fixture
controller tests reject non-opt-in, non-single-OSD and invalid actions before
calling a daemon. Actual daemon acceptance is a separate required run.


## Independent native ACK reception

A successful `AckBackoff` call means the original-epoch ACK was accepted by the
local send path. It does not establish daemon receipt or a native state change.
PG query contains no backoff state, and native UNBLOCK is sent even if BLOCK
has not been acknowledged (`PG.cc:626-655`). BLOCK/UNBLOCK alone therefore
cannot fill that verification gap.

After explicitly submitting the ACK, a Go test can publish this optional public
`osd-backoff-ack-input.json`:

```json
{"global_id":42,"id":1,"map_epoch":14,"pgid":"2.2"}
```

`observe` and `finish` then wait at most ten seconds for native
`ceph tell osd.0 dump_historic_ops`, bounding each command to two seconds.
The input must match the current token scope's inactive PG and original applied
epoch. The observer matches all of the following in a completed native record:

- `MOSDBackoff::print` supplies exact PG, `ack-block` opcode, ID and map epoch
  (`messages/MOSDBackoff.h:75-81`, `common/ceph_strings.cc:402-409`).
- `OpRequest::_dump` independently supplies `client.<global_id>` from the
  received request source (`osd/OpRequest.cc:53-65`).
- The record contains `reached_pg`, set by `OSD::dequeue_op` before calling the
  PG request handler (`osd/OSD.cc:9929-9936`).

Only `token`, `global_id`, `source_type=8`, `source_number`, `osd_id=0`, `pgid`,
`id`, `map_epoch`, `op=2`, `reached_pg`, source commit and the literal evidence
label `native completed op history; reception only` are retained. Arbitrary
history descriptions, object bounds, client addresses and other operation
records are discarded. The result includes `ack_receipt`; the same minimal
receipt is retained in common and token-specific ACK-receipt files. A validated
token-specific cache avoids losing an already established observation during
later cleanup. Starting another scope archives and removes only an ACK input
that matches the completed previous scope; mismatches are refused and retained.

This establishes exact native ACK reception at the PG boundary. It does not
directly observe `Session::Backoff::STATE_ACKED` or the late-ACK deleting branch;
those states are only explicitly printed by `Session.cc:43-88` with debug OSD
logging. The tests can separately establish functional release by a fresh
caller APPEND and independent exact-once native bytes/version checks after a
late ACK. Neither observation is described as a general remote-execution or
durability acknowledgement for user I/O.

The first secure/AES256K/IPv6 single-OSD live run retained at
`/private/tmp/ceph-osd-backoff-secure-aes256k-ipv6-first` passed actual PG `2.2`
BLOCK at epoch 14, UNBLOCK at epoch 15, ID 1, restoration, and one caller APPEND
with native exact-once postconditions. Its retained log reports the integration
PASS and owned-container cleanup. That run predates the independent ACK
history receipt: its explicit ACK evidence covers local submission only.
The new exact-source ACK observer has mock regression coverage; a subsequent
live run is required to establish that additional native-reception claim.
