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

## Cases (`TestCephOSDCopyFromIntegration`, six subtests)

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

## Profiles

All passed on the first complete run after test-expectation corrections (the
very first run failed three subtests because the test assumed per-op results
equal the aggregate and a plain copy would not grow the object; the product
and daemon behavior were unchanged, and the failures are why the table above
records `-115`):

| Profile | Runtime / store | Result |
| --- | --- | --- |
| legacy-v6, default secure CephX, IPv4 | container, single BlueStore | 6/6 |
| legacy-v6, 3 replicas | container, BlueStore | 6/6 |
| replicated-pg-v8 (HASHPSPOOL, 8 PG) | container, single BlueStore | 6/6 |
| replicated-pg-request-v9-reply-v8, secure, AES256K, IPv6 | container, single MemStore | 6/6 |

Not claimed: native-encoded vectors, compound vectors, source snapshots,
`MAP_SNAP_CLONE` and the cache-tier flags, fadvise hints, cross-pool copy,
copy under daemon failover or SIGKILL, response-loss behavior, EC, CRC mode
and host runtime profiles. The in-OSD copy of a source in another PG is
exercised only incidentally by the 8-PG profile.
