# Explicit OSD resubmission

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Contract:
`internal/osd/RESUBMIT_CONTRACT.md`. Test: `TestCephOSDResubmitIntegration`
(two subtests, native-seeded owned objects, native observation of bytes and
version).

## Observed

- Completed request: Go APPENDs once (native: baseline plus the tail, version
  v). A resubmission with the returned `Submission` and `RetryAttempt` 1 gets
  a success reply under the same transaction ID, echoing attempt 1, with
  ONDISK, the same user version v and the logged replay version; native bytes
  and version are unchanged, so nothing executed twice. Control: a new request
  with a new ID executes a second append.
- Unknown outcome: a relay drops the reply of an APPEND that executes; the
  caller cancels and gets an unknown outcome with its `Submission`. After the
  connection is replaced (same client identity), the resubmission returns
  success with the native version and ONDISK, and native bytes show exactly one
  append. This resolves the unknown outcome without a second execution.
- Vectors whose re-execution would differ: an exclusive CREATE (re-execution
  would answer EEXIST), a DELETE (ENOENT) and a compound APPEND plus SETXATTR
  (bytes would double). Each resubmission returns the original code, per-op
  results, user version and ONDISK, and native bytes, version and attributes
  are unchanged. A third subtest (`non-idempotent vectors answered from the PG
  log`) covers them on the same profiles.
- The same subtest gained three vectors on 2026-10-18: OMAP SETVALS plus
  RMKEYS (on an object whose OMAP was set first), an object-class write CALL
  (`hello.record_hello` on an absent object, which answers `-17` EEXIST when it
  runs again) and COPY_FROM2 from another owned object. Each resubmission
  returned the original code, per-op results and user version, and the native
  object's bytes and version were unchanged. Passed in legacy-v6 single
  BlueStore (secure AES256K IPv4) and replicated-pg-v8 three BlueStore
  (secure AES256K IPv6).
- First attempt of the live test failed only on a test bug (it expected the
  seed baseline on an object that starts absent); the product was unchanged.
- Local refusals and the retry-echo check are in `cephmsgr/osd_resubmit_test.go`.

## Snapshotted write (2026-10-20)

`TestCephOSDResubmitSnapshotIntegration`: a self-managed snapshot id is
allocated through `Client.PoolOp` on an owned pool, and a WRITEFULL carrying
that SnapContext executes (native observation sees the new head and the
original bytes in the clone) while a relay withholds its reply; the cancelled
wait leaves an unknown outcome. The same client resubmits the write under the
original identity: the reply is ONDISK with the native head version, and
300 ms later the native head and its single clone are unchanged. Passed in
legacy-v6 single BlueStore (secure AES256K IPv4) and replicated-pg-v8 three
BlueStore (secure AES256K IPv6).

## Profiles

Passed in legacy-v6 single BlueStore, legacy-v6 three BlueStore replicas,
legacy-v6 CRC with AES key and service cipher, replicated-pg-v8 single
BlueStore, request-v9/reply-v8 with secure AES256K over IPv6 on single
MemStore, and legacy-v6 secure AES256K over IPv6 on single MemStore.

## Primary change

`TestCephOSDResubmitFailoverIntegration` (legacy-v6, three BlueStore replicas)
uses one client throughout, because the resubmission is bound to its GlobalID.
It runs an exclusive CREATE to completion on a reserved absent object, then an
APPEND whose reply a relay drops after native observation shows it executed.
The primary is SIGKILLed (exit 137); the caller gets an unknown outcome with
its `Submission`. Native placement then shows a newer epoch and two acting
replicas without the killed primary, holding the appended bytes at the
executed version. Both requests are resubmitted to the replacement primary
with their original transaction IDs and incarnation, `RetryAttempt` 1 and the
new placement: the CREATE returns 0 (not EEXIST) with its original user
version and the APPEND returns 0 with the native version, both with ONDISK,
and native bytes and version show exactly one append. Passed in CRC with the
legacy key and in secure AES256K (key and service cipher), 2026-10-17.

## After PG log trimming

`TestCephOSDResubmitAfterTrimIntegration` characterizes the limit. A Go
APPEND of `once` completes on an owned pool (user version 3). The test then
sets `osd_min_pg_log_entries` and `osd_max_pg_log_entries` to 10,
`osd_pg_log_trim_min` to 1 and `osd_pg_log_dups_tracked` to 10 for all OSDs
through MON `config set` (removed again with `config rm` in cleanup), and
writes 200 one-byte appends to another object of the same PG. Resubmitting
the original APPEND under its transaction ID, incarnation and attempt 1 then
succeeds with a new user version (204), and native bytes read `onceonce`: the
OSD no longer remembered the request and executed it again. The client has
no way to detect this; resubmission resolves an unknown outcome only while the
request is still within the PG's log and duplicate tracking, and nothing
resubmits automatically. Passed in legacy-v6 single BlueStore (secure AES256K
IPv4) and replicated-pg-v8 three BlueStore (secure AES256K IPv6).

## Across a PG split (2026-10-18)

`TestCephOSDSplitResubmitIntegration` (owned single-OSD split fixture, the
split oracle of OSD_SPLIT_VALIDATION): an APPEND to an object whose hash moves
to the split child executes (native bytes observed) while a relay withholds
its reply; cancelling the wait leaves an unknown outcome with the original
identity. The oracle then raises the pool's PG count and holds it. The same
client resubmits the APPEND under the original transaction ID and
incarnation, routed to the child PG by the new placement: the reply is
ONDISK with code 0 and the user version native observation recorded, and
300 ms later the native object still has one copy of the tail at that
version. A second APPEND, sent to the child with its reply withheld, executes
and ends unknown; the oracle merges the PGs back, and the resubmission to the
parent PG is answered from the merged log in the same way (one copy of the
second tail, the native version). Passed in replicated-pg-v8 single BlueStore
(child 2.f of parent 2.7, epochs 12 -> 17 -> 59; a first run without the merge
phase also passed) and request-v9/reply-v8 secure AES256K IPv6 single
MemStore (child 2.8 of parent 2.0, epochs 11 -> 16 -> 58).

## After a restart of the primary (2026-10-18)

`TestCephOSDResubmitRestartIntegration` (legacy-v6, BlueStore): an APPEND that
creates an owned object executes (native bytes observed) while a relay
withholds its reply, and ends unknown. The object's primary OSD is SIGKILLed
and restarted; after the PGs are clean it is the primary again under a new
address nonce. The same client resubmits the APPEND to it under the original
identity: code 0, ONDISK, the native user version, and 300 ms later the native
object still has one copy. The restarted daemon answered from the PG log it
reloaded from BlueStore. Passed in single BlueStore (secure AES256K IPv4,
osd.0, epoch 13 -> 17) and three BlueStore replicas (secure AES256K IPv6,
osd.1 primary before and after, epoch 19 -> 23).

Not claimed: for read vectors, for a rollback vector and opcodes not listed above (the server may not log every opcode); resubmission through a different client
identity (refused locally).
