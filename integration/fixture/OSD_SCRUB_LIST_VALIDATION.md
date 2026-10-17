# Scrub error listing (SCRUBLS)

Run 2026-10-18, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, three BlueStore replicas. Test:
`TestCephOSDScrubListIntegration`. Contract: `internal/osd/SCRUB_LIST_CONTRACT.md`.

The pool oracle's `inconsistent` mode runs the native
`rados list-inconsistent-obj` and `list-inconsistent-snapset` for an owned
pool's PG and reports the interval and names.

- A native-seeded object's fresh PG has no scrub results: Go gets a known -2
  (ENOENT) and the native CLI reports no scrub information.
- The test sets `bluestore_debug_inject_read_err` for the OSDs (removed with
  `config rm` afterwards), injects a data error for the object on one replica
  with that OSD's `injectdataerr` command through the Go OSD command API, and
  starts `pg deep-scrub` through a Go MON command.
- Once the scrub has recorded it, one Go SCRUBLS returns exactly one raw value
  containing the object name; the native CLI lists exactly that object under
  the same interval. A snapset listing under that interval returns none (as
  native), and a request pinned to the next interval is a known -11 (EAGAIN).
- A first run with the raw object hash as the PG seed was refused with -22
  (`do_scrub_ls` compares the PG id unfolded); the contract and test use the
  actual PG seed. A second run without the BlueStore option listed nothing:
  the injected error only reads as EIO with that option.

| Profile | Primary | Replica with the error | Interval | Raw value |
| --- | --- | --- | --- | --- |
| legacy-v6, secure AES256K IPv4 (one PG) | osd.0 | osd.1 | 20 | 1213 bytes |
| replicated-pg-v8, secure AES256K IPv6 (eight PGs) | osd.0 | osd.1 | 23 | 1213 bytes |

Not claimed: decoding of the inconsistent-object wrapper, paging with
StartAfter across several values, snapset inconsistencies, EC shards, and
repair.
