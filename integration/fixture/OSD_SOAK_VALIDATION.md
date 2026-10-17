# OSD soak

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Test: `TestCephOSDSoakIntegration`, selected alone by
`CEPH_MSGR_TEST_OSD_SOAK=<duration>` (at least 30 s, OSD fixture only).

## Workload

Four workers, each with its own client (OpenOSD shares one handle per OSD
within a client, and Close ends it for every holder) and its own
native-reserved object, run at a steady pace (2 ms between operations):

- APPEND of 1-512 random bytes (most operations), tracked as expected bytes;
- a full READ compared with the expected bytes;
- SETXATTR `soak` with a changing value;
- a TRUNCATE to zero once the object passes 64 KiB;
- connection replacement (Close and OpenOSD);
- an APPEND under a 1-15 ms deadline. When it ends as an unknown outcome the
  worker resolves it only by explicit resubmission under the original
  transaction ID on a fresh connection, retrying with a raised attempt while
  the resubmission is itself unknown.

At the end the native client must read exactly each worker's expected bytes
and xattr, at least 100 appends, 10 reads and 10 replacements must have run,
and goroutines must return to their starting level.

## Observed

| Profile | Duration | Appends | Reads | Replacements | Unknown, all resolved |
| --- | --- | --- | --- | --- | --- |
| legacy-v6, three BlueStore, secure AES256K IPv4 | 2 min | 76032 | 12671 | 12669 | 768 |
| replicated-pg-v8, three BlueStore, secure AES256K IPv6 | 1 min | 42007 | 6999 | 6999 | 241 |

Every short-deadline append that expired had been transmitted (none ended
unsent), and every one resolved on its first resubmission without a second
execution: the native bytes matched. Goroutines stayed at 47 throughout.

Two earlier attempts failed for test reasons, kept here because they show
real limits: sharing one client let one worker's Close end another worker's
handle (`client closed`); and unthrottled 4 KiB appends filled the 1 GiB
sparse BlueStore devices within about 40 seconds, after which writes (and the
native cleanup) were never answered, as a full pool does by design. A reread
of a 16 MiB object was also refused locally by the frame bound.

Not claimed: soak under daemon failure, MON or OSD map changes, ticket
renewal during the soak (the default ticket lifetime exceeds these runs),
EC pools, or durations beyond the runs above. CI runs a 4-minute row on
legacy-v6 three BlueStore.
