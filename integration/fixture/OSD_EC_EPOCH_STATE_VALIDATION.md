# Watch, snapshot and PG backoff state across EC epoch changes

These are test-only controls for issue #9, work item 2. The earlier controls
covered an in-flight mutation, a degraded write and primary replacement on the
optimized isa k=2,m=2 profile. These three cover the other state that work
item names (a watch, self-managed snapshots and backoff controls) across the
epoch changes that lost OSDs cause. Each control answers a narrow question:

- `TestCephOSDErasureOptimizedK2M2WatchFailoverIntegration`: does one Go
  watch follow a primary replacement under a primary-first pg_temp and the
  original primary's return, through the caller's explicit RECONNECT alone?
- `TestCephOSDErasureOptimizedK2M2SnapshotFailoverIntegration`: do clones
  made through the original primary and through the replacement primary read
  back the same, through Go and natively, under the replacement and after the
  return?
- `TestCephOSDErasureOptimizedK2M2PGBackoffIntegration`: what does an EC PG
  that is peered below `min_size` send this client, how does the client's
  backoff gate treat further requests for that PG, and what releases it?

The controls add no product API, opcode or flag. OSDs leave through the
fixture's forced kill of an owned OSD and return through its start, the
daemon control the existing EC outage tests use. No other fault is injected,
no cluster-wide flag is set and no daemon configuration changes. The fixture
gains two read-only owned-pool oracle modes, which need no capability:
`pgstate` (two placement records around one `pg query` of the primary the
first record names, as the `pginterval` mode observes, without any judgement
or command) and `watchers` (the pinned `rados listwatchers` for one owned
object). The placement reader now accepts more than one empty
(`CRUSH_ITEM_NONE`) position in an erasure-coded up or acting set; repeated
real OSDs are still refused, and an empty position outside an erasure-coded
pool is refused.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

Watches:

- A registration is stored in the object's `object_info_t::watchers`, and
  RECONNECT requires an existing entry for the same cookie and entity
  ([watch ops](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L7040-L7076)).
  LIST_WATCHERS reports those stored entries
  ([list](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L6544-L6568)).
- A primary that loads the object registers each stored watch as
  disconnected, with the registration's own timeout
  ([populate](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L11811-L11845)),
  and an interval change discards the in-memory watches of the old primary
  ([on change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L12040-L12053)).
  The control registers with a 120 s timeout so that each new primary sees
  the RECONNECT well before a disconnected registration could expire.

Snapshots:

- A write whose SnapContext is newer than the snapset's sequence clones the
  head before it changes it, through whichever primary leads the PG
  ([make_writeable](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8641)).
  Nothing in it depends on the primary's shard or a pg_temp.

PG backoff:

- On a connection that negotiated RADOS_BACKOFF, a PG that is down,
  incomplete, or peered and not active answers a client op with a PG-wide
  backoff and drops the op
  ([PG backoff](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L1868-L1893)).
  The BLOCK names the PG's `spg_t`, the primary's map epoch and the range
  from the PG's first object to the next PG's
  ([BLOCK](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc#L570-L594),
  [range](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L879-L901)).
- An EC PG whose acting set has fewer than `min_size` shards activates as
  peered, not active
  ([peered](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L6932-L6935)).
  With k=2,m=2 and `min_size` 3, two lost members leave k=2 shards: enough to
  peer, one too few to be active.
- Every new interval runs `start_peering_interval`, which calls
  `clear_primary_state` before `on_change`
  ([interval](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L807-L828)).
  The PG's `clear_primary_state` releases the PG's backoffs with UNBLOCK at
  the new map epoch
  ([clear](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc#L352-L361),
  [release](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc#L596-L657)).
  `on_change`'s later `clear_backoffs`, which sends nothing, then finds them
  gone
  ([on_change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L13038-L13039)).
  So a primary that stays up across the interval change sends UNBLOCK.
- The client's gate refuses, for an erasure-coded PG, a request for a
  blocked range on any shard, because a request may carry its shard in
  primary-first numbering
  ([backoff contract](../../internal/osd/BACKOFF_CONTRACT.md)).

## Method

Each test creates its own optimized isa k=2,m=2 pool (eight PGs,
`hashpspool`) through the owned-pool oracle, checks the pool facts against the
Go client's authenticated map, and requires the object's PG active+clean under
a primary on shard 0 without a pg_temp before any Go I/O. Each observation of
a PG state is one `pgstate` receipt. The test judges it itself: two identical
placement records around a query the expected primary answered for its own
shard, with the expected sets and state, an interval that started after the
recorded one, and the primary's map epoch not behind the placement's.

Watch failover. A second Go client (the watcher, with every OSD dial counted)
registers a watch with generation 3 at the shard-0 primary. The native list
must show exactly that client and cookie, and a notification from the
fixture's client must complete with the watcher's ACK. The primary's OSD is
killed. Once the PG is active under the first parity shard's OSD with the
primary's position empty under a primary-first pg_temp, the native list must
still show the registration, and the watcher must have dialed nothing. The
caller then RECONNECTs the same handle through that primary with
`PrimaryFirst{PoolSize: 4, NonprimaryShards: 1}`. The reply must keep the
cookie, advance the generation to 4 and carry the primary-first placement.
Exactly that one connection must have been dialed. A notification through the
replacement must reach the reconnected handle with the replacement as its
source and complete with one ACK. The original OSD is started. Once every PG
is clean and the PG is active+clean on its original placement in a newer
interval, the native list must still show the registration, and the watcher
must have dialed nothing. The caller RECONNECTs once more, to the returned
primary, without primary-first numbering, at generation 5, again with exactly
one dial. A notification completes again, and after the UNWATCH the native
list is empty.

Snapshot failover. One WRITEFULL, then one self-managed snapshot id
allocated through `Client.PoolOp` and an overwrite with that SnapContext,
through the shard-0 primary. Go reads at the snapshot id and at the head,
native reads at both and LIST_SNAPS must agree: one clone for the id and the
head, with the head's user version. The primary's OSD is killed as above, and
the same reads and LIST_SNAPS through the replacement must give the same
result. A second id is allocated, and one overwrite with SnapContext
`{seq: s2, snaps: [s2, s1]}` goes through the replacement with primary-first
numbering while shard 0 is absent. It must clone the head again. Go and native
reads at both ids and the head, and LIST_SNAPS (two clones and the head,
snapset sequence s2), must agree. The original OSD is started; after every PG
is clean the same reads through the returned primary must give the same bytes,
the same head user version and a LIST_SNAPS result equal to the one under the
replacement.

PG backoff. One WRITEFULL seeds the object, and the native pre-state is read.
A second Go client (dials counted) opens one connection to the shard-0 primary
and sends one STAT on it. The OSDs of shard 3 (the second parity shard) and of
shard 1 (the nonprimary data shard) are killed, leaving shards 0 and 2. The PG
must be observed peered and undersized, optionally degraded, and not active,
under the unchanged primary, with exactly positions 1 and 3 empty and no
pg_temp. One APPEND+SETXATTR (incarnation 41) is then sent on that
connection. The next backoff event must be a BLOCK from that primary on this
session for the PG on actual shard 0, at an epoch not behind the peered
primary's, covering exactly the PG's range (for eight PGs the bit-reversed
seed order gives the end). The caller ACKs it. The APPEND must not have been
answered. A READ of the same object, and a STAT that names parity shard 2
with primary-first numbering, must each be refused locally with
`ErrOSDBackoff`, without a transaction on the wire, and not as an unknown
outcome. The shard-3 OSD is started. The next event must be the matched
UNBLOCK of the same ID, session and range at a newer epoch, and the PG must be
observed active and undersized without position 1 in an interval that started
at or before that UNBLOCK's epoch. The caller ends the APPEND's local wait. It
must end with `OutcomeUnknownError` and its original transaction,
incarnation, epoch, primary and shard. The native object must equal the
pre-state. The caller then resubmits the same operations once, under the
original submission (`Resubmit`, `RetryAttempt` 1), on the same connection,
routed by the current placement. The reply must be ONDISK with the original
transaction and the native object must hold the tail once at the reply's user
version. The client must have dialed no other connection. The shard-1 OSD is
started; once every PG is clean and the PG is active+clean on its original
placement under the unchanged address, the native state must be unchanged.

Every test removes its owned pool and profile on cleanup. A killed OSD that
the test did not start again is started by the cleanup, which waits for every
PG to be clean.

## Observations

Linux amd64 host, Docker 29.9.1, pinned image above, row parameters as the
catalog gives them (replicated-pg-v8, secure aes256k, IPv4, container, four
BlueStore OSDs, `osd-erasure-width`), each run wrapped in the CI result gate.
Source: an archive of commit `7009a1a` (the last test commit); the later
commits change documentation only.

Watch failover (20.88 s): PG `3.3`, acting `[0 2 3 1]`, primary osd.0 on
shard 0 at epoch 25. The native list showed client.4761 with cookie 1, and the
first notification completed with its ACK. After osd.0 was killed, epoch 26
named osd.2 on nonprimary shard 1 (state unknown) and epoch 27 osd.3 on
parity shard 2 (peering). At epoch 28 the PG was
`active+undersized+degraded` under osd.3 on shard 2 with a pg_temp, acting
`[NONE 2 3 1]`, interval since 27. The native list still showed the
registration. The RECONNECT through osd.3 with
`PrimaryFirst{PoolSize: 4, NonprimaryShards: 1}` advanced the generation from
3 to 4 with one dial. The notification through osd.3 came from osd.3 and
completed with one ACK. osd.0 returned under a new nonce on the endpoint it
had before. At epoch 32 the PG was `active+clean` on `[0 2 3 1]` (interval
since 31), and the native list again showed the registration before the
second RECONNECT. That RECONNECT went to osd.0 without primary-first
numbering, at generation 5. Its notification completed, and after the UNWATCH
the native list was empty.

Snapshot failover (26.03 s): PG `3.0`, acting `[1 2 0 3]`, primary osd.1 on
shard 0. Snapshot id 2 kept the 6,000-byte first write; the head was 4,500
bytes at user version 3. After osd.1 was killed the PG was active under osd.0
on parity shard 2 with a pg_temp at epoch 29 (acting `[NONE 2 0 3]`). The same
reads and LIST_SNAPS through osd.0 matched. With id 3 allocated, the 7,500-byte
overwrite through osd.0 returned ONDISK at user version 5. LIST_SNAPS then
listed clone 2 (snapshot 2, 6,000 bytes), clone 3 (snapshot 3, 4,500 bytes) and
the head, with snapset sequence 3. Go and native reads at ids 2 and 3 and at
the head returned the first, second and third payloads. After osd.1 returned
(epoch 34, `active+clean` on `[1 2 0 3]`, interval since 33), every read
through osd.1, the head version and LIST_SNAPS were unchanged.

PG backoff (24.70 s): PG `3.4`, acting `[1 2 3 0]`, primary osd.1 on shard 0
at epoch 25, native version 1. osd.0 (shard 3) and then osd.2 (shard 1) were
killed. Epoch 26 showed `[1 2 3 NONE]` peering, and epoch 27 `[1 NONE 3 NONE]`
peering. At epoch 28 the primary reported `undersized+degraded+peered` on
`[1 NONE 3 NONE]` (interval since 27). The APPEND (tid 2, incarnation 41,
epoch 25) drew BLOCK id 1 for `3.4` shard 0 at epoch 28. The range went from
hash 4 (snapshot 0) to hash 2 (CEPH_NOSNAP), the next PG of eight in
bit-reversed order. After the ACK, the READ (tid 3) and the STAT naming wire
shard 2 with primary-first numbering (tid 4) were refused locally. osd.0 was
started. UNBLOCK id 1 arrived at epoch 31, matched. At epoch 32 the PG was
`active+undersized+degraded` on `[1 NONE 3 0]` with its interval since 31. The
APPEND's local wait ended with `OutcomeUnknownError` (context canceled) and
its original identity, and the native object was still at version 1. The
resubmission (tid 2, `RetryAttempt` 1) returned ONDISK at epoch 32 with replay
version `32'2`. The native object then held the tail once at version 2. No
further OSD connection was dialed. After osd.2 returned the PG was
`active+clean` on `[1 2 3 0]` at epoch 37 (interval since 35) under osd.1's
unchanged address, and the native state was unchanged.

In the same session these regression rows passed the gate:
`t-osderasureoptimizedk2m2failover`, `...inflight`,
`...snapshotanddegradedwrite`, `...requestheldatdyingprimary`,
`...partialwriteprimaryreplacementshardhistory` (which uses the moved
`replacedOn` judgement) and `...partialwriteshardreturnshardhistory`. All
nine rows ended with zero testcontainers containers and the run directory
removed.

One behaviour mutation per control, applied to the same archive and run in
one session, failed each test as intended. Without primary-first numbering,
the RECONNECT to the replacement got no answer and ended as an unknown outcome
at the test's deadline. An overwrite through the replacement under the old
SnapContext made no second clone, so the Go read at the second id returned
other bytes. Without the explicit ACK, the gate still refused the resubmission
locally after the UNBLOCK (`ErrOSDBackoff`), as the backoff contract requires
for an UNBLOCK that arrives before the ACK.

First failure. The first working-tree run (on base `6477aa3` with these
changes uncommitted) passed the backoff and snapshot rows, but the watch row
failed at its last dial check. The returned osd.0 listened on the endpoint
the client had dialed before the outage, under a new nonce. The check
counted only endpoints the client had never dialed, so it saw no new dial.
The test was wrong, not the client. `checkOSDDials` now counts the dials at
every OSD endpoint, earlier ones included; its cluster-free test covers the
reused endpoint. Every step up to that check behaved as recorded above.

## Limits

- One profile: replicated-pg-v8, secure aes256k, IPv4, container, optimized
  isa k=2,m=2 on four BlueStore OSDs. Not run: the nonoptimized pool,
  legacy-v6 (its v6 requests carry no shard), other widths and plugins.
- The fixture topology is deterministic, so each run repeats one placement.
  The replacement primary always held parity shard 2 (wire shard 1).
- The BLOCK came from a primary on shard 0 without a pg_temp. A BLOCK from a
  primary that leads under a primary-first pg_temp, which would name an
  actual shard while requests carry the wire shard, was not produced.
- The native reception of the ACK on the EC PG is not observed. The
  replicated backoff recipe's op-history observer is specific to its
  single-OSD pool.
- No object-level backoff on an EC pool, notification to a registration
  while it is disconnected, watch timeout or expiry, snapshot removal or
  trimming, or a write during recovery.
- The read after the return does not show which shard's chunk the returned
  primary used. Identical bytes, clones and versions are the claim.
