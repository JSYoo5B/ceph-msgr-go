# OSD soak

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Tests: `TestCephOSDSoakIntegration` and
`TestCephOSDSoakFailoverIntegration`, selected alone by
`CEPH_MSGR_TEST_OSD_SOAK=<duration>` (at least 30 s, OSD fixture only); each
runs for that duration.

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

A later pg-v8 run passed the workload (72142 appends, 1010 unknown, all
resolved) but its native cleanup of the owned objects took more than 60
seconds right after the writes; the soak cleanups now allow 180 seconds, and
the rerun passed with 847 unknowns resolved.

## Under OSD failure (`TestCephOSDSoakFailoverIntegration`)

Three BlueStore replicas only. Four workers, each with its own client and
object, append 1-256 bytes every 10 ms, set an xattr, truncate past 32 KiB and
reread every 25 operations. Every 15 seconds the test goroutine SIGKILLs one
OSD of a worker object's acting set (alternately its primary and another
member), restarts it after 12 seconds and waits for clean PGs; one OSD is down
at a time. Workers behave as a caller: a write without a reply within 6
seconds ends unknown and is resubmitted only under its original transaction
ID, incarnation and a raised attempt, to the primary of a freshly queried
native placement; a write that never left is sent again as a new request;
failed reads are asked again. Native oracle and daemon control calls are
serialized because they share control files.

| Profile | Duration | OSD kills | Appends | Unknown, all resolved |
| --- | --- | --- | --- | --- |
| legacy-v6 (one PG, primary osd.1 each time), secure AES256K IPv4 | 2 min | 3 | 23078 | 21 |
| replicated-pg-v8, secure AES256K IPv6 | 2 min | 3 (osd.0, osd.2, osd.2) | 25125 | 13 |

Native bytes and xattrs matched for every object after the PGs were clean
again, so no write executed twice across primary changes and no write was
lost. The pace keeps the writes of other workers during one resolution well
inside the PG log's duplicate tracking; a resubmission later than that could
execute again, which is the documented limit of server-side duplicate
detection.

Two earlier attempts failed for test reasons, kept here because they show
real limits: sharing one client let one worker's Close end another worker's
handle (`client closed`); and unthrottled 4 KiB appends filled the 1 GiB
sparse BlueStore devices within about 40 seconds, after which writes (and the
native cleanup) were never answered, as a full pool does by design. A reread
of a 16 MiB object was also refused locally by the frame bound.

Not claimed: soak under MON failure, two OSDs down at once, PG splits, ticket
renewal during the soak (the default ticket lifetime exceeds these runs),
EC pools, or durations beyond the runs above. CI runs both for 4 minutes on
legacy-v6 three BlueStore.
