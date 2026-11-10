# Destination PG-log control for a successful COPY_FROM2

This is a test-only positive control for issue #7. It answers one narrow
question about a copy that succeeded: does the destination's native PG log
bind the committed copy to the source object's history by full request
identity? It adds no product API, opcode or flag. It injects no fault and
changes no daemon configuration.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

- The source's [COPY_GET handler](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L9439-L9446)
  collects request identities only when its cursor is complete, and asks for
  at most ten.
- The [lookup](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.h#L477-L506)
  walks the ordinary log entries of that object, newest first. It takes each
  entry's indexable request identity with its user version and appends the
  entry's existing extra request IDs. It does not enumerate the separate
  duplicate index.
- The destination's [Objecter result](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L9564-L9585)
  collects those identities, and the success path of
  [finish_copyfrom](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L10133-L10137)
  moves them into the operation context.
- [Log construction](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L9145-L9151)
  swaps them into the last destination entry as `extra_reqids`.
- The [copy-error injector](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L9834-L9845)
  branches to EIO before the final transaction is prepared. This control
  leaves it off; the two late-error cells cover that branch
  ([single chunk](OSD_COPY_FROM_LATE_ERROR_VALIDATION.md),
  [multi-chunk](OSD_COPY_FROM_MULTICHUNK_ERROR_VALIDATION.md)).

No Ceph source is incorporated into the product.

## Fixture, roles and selection

`TestCephOSDCopyFromDestinationHistoryIntegration` uses the ordinary OSD
fixture and the two owned plain replicated pools of
`tools/osd_pool_oracle.py` (slot 1: the source, slot 2: the destination). Both
heads must be absent before the test, and their native placements must name
different PGs. The Go client (the fixture's admin identity) sends both
requests, once each, to the native placement's primary:

1. One source write, WRITEFULL plus SETXATTR in one request, under client
   incarnation 7. Its reply must carry ONDISK. Native readback of the source
   must show those bytes and that xattr at the reply's user version.
2. One COPY_FROM2 to the destination under client incarnation 9, pinned to
   that source user version. Its reply must carry ONDISK and echo the pinned
   version.

Each reply must keep its transaction, GID, incarnation, object, PG pool and
attempt 0. A failed or unknown request stops the test, and nothing is resent.

The observer is the bracketed primary PG-history snapshot of
[OSD_PG_HISTORY_CALIBRATION_VALIDATION.md](OSD_PG_HISTORY_CALIBRATION_VALIDATION.md)
(`tools/osd_pool_oracle.py` mode `pghistory`, parsed fail-closed by
`tools/osd_pg_history_oracle.py`). The oracle already accepts either owned
pool slot; the Go wrapper `primaryPGHistory` now passes the slot, so it can
snapshot the destination pool's PG as well. The wrapper reports an extra
request ID's `user_version` to the test.

Catalog row
`osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcopyfromdestinationhistory`
selects only this test and uses no opt-in parameter, so the test also belongs
to the broad legacy OSD selection.

## Expectations

Independent native readback first: the destination holds the source's bytes
and xattr inventory at the copy reply's user version, and the source's bytes
and version did not change.

Each snapshot must be complete, with identical before and after brackets,
and its primary, acting and up sets, PG, shard and pg_temp must equal the
placement the request used. Each snapshot takes eight full identities
(entity type and number, incarnation, TID): the source write's key, the
copy's key and six near misses.

| Snapshot | Identity | Required positions |
| --- | --- | --- |
| Source PG | source write | exactly one ordinary `modify` entry, return code 0, for the source head's printed `hobject_t`, at the write reply's replay version |
| Source PG | copy | none; the snapshot holds no extra request ID at all |
| Destination PG | copy | exactly one ordinary `modify` entry, return code 0, for the destination head's printed `hobject_t`, at the copy reply's replay version |
| Destination PG | source write | exactly one position: the first extra request ID of that same entry (same index, object and version), with `user_version` equal to the source write's user version |
| Both | six near misses | none |

The destination snapshot must hold exactly one extra request ID in total. The
near misses are each request's real TID under the other request's
incarnation, the source write's key with the next incarnation, the next
entity number, entity type `osd`, and a TID that was never used.

`copyFromDestinationBinding` makes the destination decision. The cluster-free
`TestCopyFromDestinationHistoryBinding` pins it with synthetic positions: the
bound entry is accepted, and seventeen contradictory histories are refused
(the copy missing, duplicated, only a duplicate, only an extra request ID,
failed, a delete, on another object or at another version; the source write
missing, duplicated, an entry of its own, only a duplicate, on another entry
or object, not the first extra request ID, or with another or no user
version).

## Reproduction

On an authorized Linux runner, hold its run lock, use the pinned image and:

```sh
CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 \
CEPH_MSGR_TEST_RUNTIME=container \
CEPH_MSGR_TEST_KEY_TYPE=aes256k \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_CONNECTION_MODE=secure \
CEPH_MSGR_TEST_IP_FAMILY=6 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCopyFromDestinationHistoryIntegration$' \
go -C integration/fixture run . run -project ../..
```

Request the scenario ID above with the CI comment command. Basic PR CI
builds and unit-checks the fixture but does not run this daemon test. The
native result of a pull-request head is recorded in that pull request with
the tested SHA. A pass at one SHA is not evidence for a later revision.

## Local native calibration

This run preceded the pull request and is not an exact-SHA CI receipt. It
used the catalog row's environment and the pinned image on a Linux amd64
host with Docker 29.9.1 and Go 1.26.9, at test commit
`525e6cd` (later commits of the same pull request change documents only).
The result gate recorded `TestCephOSDCopyFromDestinationHistoryIntegration`
as PASS with no problem; the test took 10.23 s and the owned-pool oracle
confirmed both pools removed.

| Observation | Value |
| --- | --- |
| Source write | `client.4659.7:1`, user version 3, replay version `20'3` |
| Copy | `client.4659.9:2`, replay version `20'3` |
| Source PG `3.0` (osd.0, epoch 20) | 3 log entries, 0 extra request IDs, 0 duplicates; `client.4659.7:1` is the `modify` entry of `3:f48e855b:::copy-history-source:head` at `20'3` |
| Destination PG `4.0` (osd.0, epoch 20) | 3 log entries, 1 extra request ID, 0 duplicates; `client.4659.9:2` is the `modify` entry of `4:0f02ada3:::copy-history-destination:head` at `20'3`, and that entry's `extra_reqids` is `[{"reqid":"client.4659.7:1","user_version":"3"}]` |
| Near misses | no position in either snapshot |

The two other entries of each PG are the owned-pool oracle's own probe write
and delete from pool creation. They carry other client identities and no
extra request ID. The destination entry's dumped `mtime` is `0.000000`
although the request carried one; this is recorded as observed and is not
asserted.

A mutation check ran against the same fixture. With the copy replaced by a
plain Go WRITEFULL plus SETXATTR of the same bytes and xattr to the
destination, the native data and xattr checks still passed and the test
failed at the destination binding: "the source write's identity occurs 0
times at the destination". The mutation was then discarded.

## Limits

- This is a positive control. A destination entry with the extra request ID
  is already past the destination commit. A missing extra request ID cannot
  time a crash, and it cannot distinguish an unfinished source read from an
  uncommitted destination transaction. Destination loss after source
  completion and before destination commit still needs an independent,
  deterministic barrier; post-reply history and chunk counts cannot supply
  one. That cell stays open in #7.
- Only presence on each primary at one bracketed epoch is observed. The
  required empty positions (the copy's key in the source PG, the near
  misses) describe those two complete snapshots only. They do not establish
  absence on replicas, at other epochs or in the PG's history in general.
- One fresh source head with one write was used, so exactly one identity was
  transferred. The ten-identity cap, a source with several entries, existing
  extra request IDs on the source, the duplicate index and trimmed logs were
  not exercised.
- No replay or deduplication behaviour follows from a match. The test
  resubmits nothing.
- One profile ran: secure, aes256k client and service keys, IPv6, container
  runtime, legacy-v6, one BlueStore OSD. COPY_FROM (the older opcode),
  multi-chunk copies, snapshots, erasure-coded pools and replicated
  topologies with more than one OSD are not part of this control.
