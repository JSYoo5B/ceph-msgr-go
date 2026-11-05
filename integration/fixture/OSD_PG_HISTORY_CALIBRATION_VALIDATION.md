# Primary PG-history snapshot: presence calibration

This is a test-only fixture oracle for issue #9 and its calibration on one
known retained request. It answers one narrow question: does a bracketed
snapshot of the primary OSD's native PG log contain an exact request identity?
It is not a product API. It does not establish that an identity is absent,
and it is not evidence about replay or deduplication.

## Source basis

Ceph v20.2.4, source `7f793731f1b39eb4f465e960113d2363c311b964`:

- `src/osd/OSD.cc:2775-2825` routes the PG `log` command to the PG's primary
  shard and answers `-EAGAIN` ("not primary") on any other OSD. Repeating the
  command against replicas cannot observe their history.
- `src/osd/OSD.cc:4458-4463` registers the `pg <pgid> log` form.
- `src/osd/PrimaryLogPG.cc:1076-1083` dumps `pg_log_t` in an `op_log`
  section. `src/common/Formatter.cc:207-226` drops the name of the top-level
  section, so the JSON document is `{"pg_log_t": {...}}`.
- `src/osd/osd_types.cc:5355-5373` (`pg_log_t::dump`) emits `head`, `tail`,
  the ordinary `log` array and the separate `dups` array.
  `osd_types.cc:5096-5118` emits each entry's `reqid` and `extra_reqids`;
  `osd_types.cc:5234-5246` emits each duplicate's `reqid`.
- `src/osd/osd_types.h:160-180,235-241` defines `osd_reqid_t` (entity name,
  incarnation, TID), prints it as `<entity>.<incarnation>:<tid>` and compares
  all three fields. `RetryAttempt` is not part of the key.

The offline `ceph-objectstore-tool --op log` is not used. Its pinned mount
policy (`src/tools/ceph_objectstore_tool.cc:3992-4011`) does not list `log`
as a read-only operation, so it is not classified as a read-only oracle here.

## Oracle contract

`tools/osd_pool_oracle.py` mode `pghistory` takes an owned-pool object and one
to eight full identities (`type`, `num`, `inc`, `tid`). It runs these steps
once, without retrying any of them:

1. Collect the native placement of the object (`osd map` plus `osd dump` at
   one epoch) as the *before* bracket.
2. Send `{"prefix":"pg","pgid":<pgid>,"cmd":"log","format":"json"}` through
   librados `rados_osd_command` to the *before* primary OSD, by its explicit
   ID. A command sent this way names the responding OSD.
3. Collect the *after* bracket the same way.

`tools/osd_pg_history_oracle.py` then builds a receipt. The receipt is
complete only if all of the following hold:

- the native return code is 0;
- the reply is at most 256 KiB (a larger reply is not copied and is reported
  with its length);
- the reply is one strict UTF-8 JSON document with no duplicate keys and
  exactly the pinned shape (`pg_log_t` with `head`, `tail`, `log`, `dups`;
  entries with the pinned required fields and no unknown fields);
- every `reqid`, including those in `extra_reqids` and `dups`, parses;
- log versions increase strictly inside (tail, head] and the last entry is
  `head` (a cut-short array fails here even when it is valid JSON), and no
  duplicate is newer than `tail`;
- the before and after placements are identical: epoch, up and acting sets,
  primary, target OSD, PG, shard, pg_temp and address.

Any other result is reported as incomplete, with its reasons. The Go test
then fails. A complete receipt lists, for each identity, every position in
the ordinary `log`, in an entry's `extra_reqids` and in `dups`. It also
carries the complete raw reply as base64, its length and SHA-256. A match
compares the whole key; a TID, an entity or a log-tail threshold alone never
counts. The receipt always states `primary_only: true` and
`absence_established: false`.

Cluster-free tests in `tools/test_osd_pg_history_oracle.py` pin the parser
with synthetic JSON. The JSON is laid out like the pinned dump code, but no
daemon produced it. The tests cover matches in each of the three tiers, eleven
near-miss identities (each field of the key changed, plus a missing entity
number), truncation at five cut points, a valid but cut-short log array,
reshaped documents and placement changes in each bracket field. They also
cover a refused (`-11`) or oversized reply and the request validation that
happens before any native contact. A mutation check ran eleven code mutants
against these tests in a scratch copy: TID-only, incarnation-blind,
type-blind and number-blind log matching; skipped or weakened `extra_reqids`
and `dups` matching; and removed bracket, head and return-code checks. The
tests killed all eleven.

## Native calibration scenario

`TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration` runs on
the four-BlueStore fixture (`osd-replicas-4`, `replicated-pg-v8`, secure,
aes256k, IPv4, container). It uses an owned optimized isa k=2,m=2 pool with
eight PGs, the profile of the earlier EC resubmission cells:

1. Verify the pool, profile and placement facts. The Go client then sends a
   seed WRITEFULL and one APPEND under client incarnation 7, both with ONDISK
   replies, on one connection to the native primary. Native readback must
   show the seed plus one tail at the reply's user version.
2. Take one primary snapshot for eight identities: the APPEND and the seed
   by their full keys, and six near misses. The near misses are the APPEND
   TID under incarnation 8, the seed TID under incarnation 7 (both real TIDs
   of this client), the APPEND TID under the seed's incarnation, the next
   entity number, type `osd`, and a TID that was never used.
3. Require a complete receipt and identical brackets. The snapshot primary,
   acting/up sets, PG, shard and pg_temp must equal the placement the
   requests used. The digest and length of the decoded raw reply must match.
   The Go test separately counts each exact quoted `"reqid":"..."` string in
   the raw reply and requires that count to equal the parser's position count.
4. Require each known request exactly once, as an ordinary `modify` entry
   with return code 0, for the object's printed head `hobject_t`, at the
   reply's replay version, with the seed before the APPEND. All near misses
   must have no positions.

Cleanup removes the owned pool and profile. The scenario changes no daemon
configuration, kills no OSD and resubmits nothing.

The dedicated mandatory row is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2primaryhistorycalibration`.
The fixture admits the selector only on the four-OSD topology. The native
result of a pull-request head is recorded in that pull request and its #9
progress comment, with the tested SHA and run ID. A pass at one SHA is not
evidence for a later revision.

## Limits

- Only presence on the primary at one bracketed epoch is calibrated. An
  identity missing from a complete snapshot does not show that it is absent
  from replica or shard-local history, from other epochs, or from the PG's
  history in general. The proposed history-eviction resubmission cell needs a
  separate, safe, complete shard-local observation recipe for every acting
  shard. That oracle remains open.
- The native calibration exercises only the ordinary `log` tier. Positions in
  `extra_reqids` and `dups` are pinned only by the synthetic cluster-free
  tests. No native request was moved into `dups` (by trimming) or recorded as
  an extra request ID.
- No PG-log retention bound, trim boundary, timed window or exactly-once
  behaviour is established. No replay safety follows from a match.
- Placement and epochs are bracketed only around the command. A placement
  change between the Go requests and the snapshot is checked only for the
  primary, acting/up sets, PG, shard and pg_temp; the epoch may be newer.
- One profile and one EC width were selected. Replicated pools and other EC
  profiles are not part of this calibration.
