# Replica reads

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime, three BlueStore replicas.
Contract: `internal/osd/REPLICA_READ_CONTRACT.md`. Test:
`TestCephOSDReplicaReadIntegration`, on an owned three-replica pool
(OSD_POOL_VALIDATION.md describes the oracle; its `place` mode gained an
optional acting-set target OSD, reported with the primary in `primary_id`).

## Observed

- A native-seeded object is read by Go from each of the three acting-set
  members with BALANCE_READS and with LOCALIZE_READS, as one vector of Read,
  Stat and GetXattr: bytes and attribute equal the native values and the
  receipt user version equals the native version, on the primary and on both
  replicas.
- Without a flag, a read sent to a replica gets no reply within four seconds.
- First attempt failed: right after the seed, replica osd.1 returned `-11`.
  The source shows why: a replica serves an object only when its
  `pg_committed_to` has passed the object's last write, and the primary
  advances that only with a later write in the same PG. The test therefore
  writes a second object that maps to the same PG, then reads; that second
  write is part of the passing recipe, not a retry.
- A write vector carrying a flag is refused before submission.

## Bounces (`TestCephOSDReplicaReadBounceIntegration`)

The pool oracle's `osdflags` mode sets and unsets only `norecover` and
`nobackfill` (unset again on every path).

- Unstable write: after a Go WRITEFULL that is the PG's last write, a
  BALANCE_READS read at a replica gets a known `-11` (EAGAIN) with retry
  attempt 0 (nothing resent); native bytes show the write. After one more Go
  write in the same PG the replica serves the new bytes.
- Missing object: with recovery held, a replica's OSD is SIGKILLed and leaves
  the acting set; a Go write (ONDISK on two replicas) and a same-PG write
  follow; the OSD restarts and rejoins the acting set without the object.
  After another same-PG write (ruling out the unstable cause) its balanced
  read still gets `-11`, while the primary serves the new bytes. With the
  flags unset and PGs clean, the recovered replica serves them; in one
  replicated-pg-v8 run it bounced once more right after recovery and served on
  the next read.
- Passed in legacy-v6 three BlueStore (secure/AES256K IPv4 and CRC/AES) and
  replicated-pg-v8 three BlueStore secure/AES256K IPv6, 2026-10-17.

## Profiles

Passed: legacy-v6 three BlueStore (secure/AES256K defaults), legacy-v6 CRC with
AES key and service cipher, replicated-pg-v8 three BlueStore (HASHPSPOOL, 8 PG;
`Placement.Primary` names the PG primary while the connection targets a
replica). Single-OSD profiles do not apply.

Not claimed: EC replica reads, reads racing an in-flight write or a primary
change, the other read opcodes beyond those listed, automatic replica
selection or `-EAGAIN` redirect to the primary (the caller decides), MemStore.
