# Owned native PG split acceptance

This is a development-only consumer recipe. Product builds never invoke it.
`TestCephOSDSplitIntegration` opts in only with `replicated-pg-v8`, one OSD,
and the existing fixture control directory. The recipe refuses other profiles,
clusters, pools, non-UUID ownership receipts, autoscaling, or a pool outside the
bounded eight-to-sixteen PG transition. It changes only the owned pool's
`pg_num` and `pgp_num` targets through normal MON commands.

The protocol and control audit uses Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`:

- `src/mon/OSDMonitor.cc:8593–8757` distinguishes target counts from actual
  counts. `pg_num_actual` decreases by at most one, and cannot fall below actual
  PGP. The recipe does not set actual counts or bypass MGR.
- `src/mgr/DaemonServer.cc:3340–3595` advances PG counts and merges individual
  clean PGs. A successful configuration command alone does not prove a split or
  restoration.
- `src/osd/PG.cc:1926–1988` can discard an old-epoch request before its body is
  decoded. With `RESEND_ON_SPLIT`, an epoch older than `last_epoch_split` is one
  such guard. `src/osd/OSD.cc:7616–7745` dispatches the explicit actual PG.
- `src/messages/MOSDOp.h:622–641` prints a partially decoded envelope with
  request identity, actual PG, raw PG/hash and map epoch. Native completed
  operation history plus `reached_pg` proves reception of that envelope. It
  does not decode an APPEND, establish execution, or turn an unknown result
  into a rollback or nonexecution guarantee.

The Go acceptance selects an owned UUID head whose independently obtained raw
hash enters a child when eight PGs split into sixteen. Before changing the
pool, it sends one APPEND and independently observes exact bytes, version and
mtime while withholding its Go reply. That wait remains unknown after local
cancellation across the split. The new map is not evidence that this request
executed; the earlier native object observation supplies that evidence.

After sixteen actual/target/pending PGs and PGPs are active and clean, a separate
connection sends a distinct APPEND with the original parent and epoch. It
retains the unknown result, original tuple and one OSD dial. Native historic
operation identity is matched to this exact client, incarnation, transaction,
parent, raw hash and epoch. An unchanged object snapshot is recorded only as a
time-bounded observation. The caller then obtains fresh native placement,
reads the child and sends a different mutation once, comparing the result with
native bytes, version and mtime. Neither uncertain mutation is replayed.

`start`, `observe` and `finish` accept only an explicitly owned object. The
worker has bounded native commands and state waits, a 120-second ready hold,
and unconditional configuration restoration after any possibly submitted pool
change. It first restores the PGP target and actual PGP count to eight, then
restores the PG target and waits for all actual/target/pending counts and clean
PGs. It compares the caller's current state immediately before restoration
with the state after the merge, so authorized child mutations are preserved.
The original fixture head must remain unchanged. Object cleanup follows only
confirmed pool restoration; a failed restoration retains owned heads and its
failure evidence.

Each run has a token-specific status, worker log and optional stale request
receipt. Before a new run, an existing stale input must validate against the
completed prior scope; only then is it archived under that prior token and
removed. Orphan or mismatched input is rejected. Old native history cannot be
attributed to a new token merely because the daemon retained it.

Public diagnostics are `osd-split-{input,result,status,command,stale-input,
stale-receipt,results}.json` and `osd-split-worker.log`. The last JSON includes
both unknown Go submissions, native state observations, actual placement,
request identity, OSD dial counts, and restoration/cleanup status. Mock tests
prove control and ownership boundaries; they do not establish daemon
interoperability. Preserve first native failures and distinguish the exact
staged helper used by each run from later helper changes.

The fixed `dump_historic_ops` reader shares the backoff observer's 4 MiB
response acceptance limit and two-second timeout. Ordinary native commands
retain their 1 MiB limit. Neither the fixture's 1,024-entry history nor exact
stale-envelope identity checks are reduced. `osd-history-query.json` preserves
exit status, output lengths and hashes, limits and error classification;
it contains no history records or stderr text. The byte limits apply after
subprocess capture. A successful query alone proves no request reception:
the existing exact tuple and `reached_pg` match is still required.
See [OSD_HISTORY_QUERY_VALIDATION.md](OSD_HISTORY_QUERY_VALIDATION.md) for the
measured original-limit failure and six complete-selector reruns.
