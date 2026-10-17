# Standalone COPY_FROM / COPY_FROM2 verification

Run 2026-10-17 against Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
product checkpoint `a4fd05f` plus the integration test
`integration/osd_copyfrom_test.go`. Product contract:
`internal/osd/COPY_FROM_CONTRACT.md`.

## What the evidence is

The independent native client observes state; it does **not** execute
copy-from. Public librados `copy_from2` always sends flags 0 and cannot select
`TRUNCATE_SEQ`, so there is no native twin for the flagged form, and no fresh
native-encoder vector exists for either opcode. The Go request is the only
COPY_FROM sender; correctness is established by native observation of the
source and destination (bytes, xattrs, OMAP inventory and header, version) and
by the daemon accepting the wire. Consequently the encoded bytes are verified
by daemon behavior, not byte-compared with a native encoder.

## Cases (`TestCephOSDCopyFromIntegration`, seven subtests)

- COPY_FROM2 and COPY_FROM (flags 0) to an absent destination: native
  destination equals the native-seeded source exactly (binary data, two
  xattrs, four OMAP entries including an empty value and a NUL key, and an OMAP
  header set by Go); the source is unchanged; ONDISK; the reply union echoes the
  request; the receipt user version equals the native-observed version.
- COPY_FROM2 over a populated destination (stale data, an extra xattr, an extra
  OMAP key): the destination becomes an exact copy; the extras are gone.
- Source version: an exact version succeeds. A larger source version returns
  `-75` (EOVERFLOW) and a smaller one `-34` (ERANGE); both leave the
  destination absent. Per-op results for these asynchronous failures are `-115`
  (EINPROGRESS), not the aggregate code; Go preserves both raw and treats the
  vector as a known server refusal.
- Missing source: aggregate `-2`, per-op `-115`, destination not created.
  Copy onto itself: aggregate and per-op `-22`, source unchanged.
- TRUNCATE_SEQ (sequence 5, size 4) versus a plain copy of the same source: a
  later sequence-4 write at offset 40 is applied in full to the plain copy
  (object grows to 100 bytes, source sequence 0) but trimmed to nothing by the
  sequenced copy (still 32 bytes), so the flag installs the supplied sequence.

- Snapshot source: after the head is overwritten under a SnapContext (the
  original becomes a clone), COPY_FROM2 with the snapshot id yields the
  original bytes and xattrs, and a head copy yields the new bytes; the reply
  echoes the snapshot id. (Seventh subtest, added after the first profile runs;
  rerun on all four profiles, 7/7.)

## Profiles

All passed on the first complete run after test-expectation corrections (the
very first run failed three subtests because the test assumed per-op results
equal the aggregate and a plain copy would not grow the object; the product
and daemon behavior were unchanged, and the failures are why the table above
records `-115`):

| Profile | Runtime / store | Result |
| --- | --- | --- |
| legacy-v6, default secure CephX, IPv4 | container, single BlueStore | 7/7 |
| legacy-v6, 3 replicas | container, BlueStore | 7/7 |
| replicated-pg-v8 (HASHPSPOOL, 8 PG) | container, single BlueStore | 7/7 |
| replicated-pg-request-v9-reply-v8, secure, AES256K, IPv6 | container, single MemStore | 7/7 |

## Lost reply (`TestCephOSDCopyFromLostReplyIntegration`)

Legacy-v6 only. A relay drops every reply from the destination's primary. The
COPY_FROM2 executes (native observation sees the source's bytes and xattr on
the destination), then the caller cancels: Go returns an unknown outcome with
its local transaction, global ID and map epoch and no server reply. A fresh
session reads the copied bytes under a later transaction; a native observation
after a delay shows the same version and mtime, so nothing was replayed.
Passed on a single BlueStore and on three BlueStore replicas (one run each).

## SIGKILL (`TestCephOSDCopyFromCrashIntegration`)

BlueStore only. After an ONDISK COPY_FROM2, the native client observes the
destination's bytes, xattr, OMAP inventory, version and mtime; the fixture OSD
(the one the placement oracle names for the fixture object) is then SIGKILLed
(exit 137, no OOM) and restarted. Native observation after recovery matches
exactly and a fresh Go session reads the copied bytes under a later transaction.
Passed on a single BlueStore and on three replicas. In the three-replica run
the killed OSD was the fixture object's primary and not necessarily the
destination's. This is daemon termination, not host power loss or device
durability, and no copy was in flight when the daemon died.

Additional legacy-v6 three-BlueStore container runs (2026-10-17) passed all
three tests (main 7 subtests, lost reply, SIGKILL): CRC mode with AES key and
service cipher over IPv4, and secure mode with AES256K key and service cipher
over IPv6.

## Source failover during a copy

`TestCephOSDCopyFromSourceFailoverIntegration` (three BlueStore replicas)
seeds a 64 KiB source with an xattr in one owned pool and picks a destination
in the other whose primary differs from the source's. The source primary is
frozen with `docker pause`, then one Go COPY_FROM2 is sent to the destination
primary. The destination OSD's internal read of the source cannot finish
until the frozen OSD is marked down and the source PG gets a new primary; the
copy then completes with ONDISK, and the destination's native bytes, xattrs
and version match. The frozen OSD is afterwards resumed, SIGKILLed and
restarted (a daemon frozen past its heartbeat grace did not return to clean
PGs within the fixture's wait when only thawed).

| Profile | Copy time | Source primary | Destination primary |
| --- | --- | --- | --- |
| replicated-pg-v8, secure AES256K IPv4 | 28.8 s | osd.2 -> osd.1 (epoch 24 -> 26) | osd.1 |
| legacy-v6, CRC with AES key and service cipher | 27.6 s | osd.0 -> osd.1 (epoch 25 -> 27) | osd.1 |

## Destination primary killed during a copy (2026-10-18)

`TestCephOSDCopyFromDestinationCrashIntegration` (three BlueStore replicas)
uses the same source/destination pair selection as the source failover test.
The source primary is frozen (`docker pause`), one Go COPY_FROM2 is sent to the
destination primary, and 3 s later, with the copy still waiting on its source
read, the destination primary is SIGKILLed (exit 137). The request ends at once
as an unknown outcome (connection EOF) with its transaction and map epoch;
nothing is resent. Both OSDs are restored (the frozen one resumed, killed and
started, the killed one started) and the PGs return to clean. Native
observation then finds the destination absent (`-2`), and the same 5 s later.
The test accepts an exact copy as the other non-partial outcome; only the
absent one was observed.

| Profile | Source primary | Destination primary (killed) |
| --- | --- | --- |
| legacy-v6, secure AES256K IPv4 | osd.0 | osd.1 (epoch 25) |
| replicated-pg-v8, secure AES256K IPv6 | osd.2 | osd.0 (epoch 37) |

## Compound copy (2026-10-18)

`TestCephOSDCopyFromCompoundIntegration` sends one vector of COPY_FROM2
(native-seeded source with xattrs `keep` and `drop`), SETXATTR `added` and
RMXATTR `drop` to an absent destination. It completes with ONDISK, and the
native destination has the source's bytes, exactly the xattrs `keep` and
`added`, and the reply's user version. A vector with SETXATTR before the copy
is refused before submission. Passed in legacy-v6 single BlueStore (secure
AES256K IPv4) and replicated-pg-v8 three BlueStore (secure AES256K IPv6).

Not claimed: native-encoded vectors, compound vectors other than a copy
followed by xattr writes,
`MAP_SNAP_CLONE` and the cache-tier flags, fadvise hints (cross-pool and
cross-namespace copies are in OSD_POOL_VALIDATION.md), a destination primary
killed after the copy's source read completed, EC, and host runtime profiles. The in-OSD copy of a source in another PG is
exercised only incidentally by the 8-PG profile.
