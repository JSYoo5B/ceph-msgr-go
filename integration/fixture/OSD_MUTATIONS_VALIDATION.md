# O0/O1 initial mutation validation

Run window: 2026-10-15, Asia/Seoul. Development branch:
`codex/osd-mutations`, based on the fixture CI correction `a25426b`.
This records local native acceptance, not completed remote CI for this branch.

Provider: published ceph-testcontainers-go v0.2.0. All MON/MGR/OSD daemons and
their stop/start lifecycle use its public APIs. Native librados ctypes calls
run only in the disposable oracle container, outside product dependencies.
No native daemon bootstrap script or product CGO/native dependency was added.

Pinned source/image: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`,
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Docker Desktop Linux arm64; host relay tests use Darwin arm64. The topology is
three MONs, two MGRs, one OSD, replicated size/min_size 1, legacy CRUSH,
HASHPSPOOL disabled and no upmap. This does not prove multi-replica or EC I/O.

| Profile | Actual acceptance |
| --- | --- |
| secure/aes256k, IPv4, BlueStore, container | all nine OSD/MON-map tests passed, including 21 mutation/guard/compound subcases, lost reply, orderly OSD restart, existing native stat/read, renewal and feature rejection |
| CRC/aes256k, IPv4, MemStore, host relay | 21 subcases plus lost reply, native stat/read, renewal and feature rejection passed |

Native vector comparisons use a separate native object reset to the same
baseline. Both paths receive explicit matching nanosecond mtimes. Native
version guards compare the corresponding native object's current version plus
the same relative assertion; different objects need not share version numbers.
Aggregate/per-op errors and independent binary readback are compared. Coverage:
hole writes, shorter/empty replacement, append, truncate shrink/extension,
interior/tail/beyond-EOF zero, create and exclusive refusal, delete/ENOENT,
version match/older/newer, extent match/mismatch, rollback after staged write,
FAILOK commit and ordered payload segmentation.

The lost-reply test sends one append and blocks receive bytes only after OSD
authentication. An independent native read proves execution before cancellation.
The canceled call retains OutcomeUnknownError and local submission identity.
A caller-chosen fresh session reads the bytes with a larger TID and does not
repeat the append. The native observer checks the bytes again after reopening.

BlueStore restart acceptance checks ONDISK completion, orderly Stop/Start,
independent native readback and a new Go read with native-refreshed epoch and
endpoint nonce. Original fixture bytes/mtime and routing metadata are restored
for subsequent tests. It does not simulate a power failure or verify replica
recovery. MemStore ONDISK is deliberately not treated as restart persistence.

All CGO=0 root Go tests and vet passed, as did the fixture module tests/vet.
Race tests passed for internal/osd, internal/session and cephmsgr. Existing
independent native decoder bytes for stat/read remain unchanged. New unit tests
check packed mutation unions and data boundaries without an encoder/decoder
roundtrip, reject unsupported fields/flags and read-budget overflow, require
ONDISK and preserve unknown-outcome identity. Full native payload encoder
fixtures for new opcodes remain outstanding.

## Observed failures and corrections

The first native vector experiment reached FAILOK comparison and found native
librados returning -EIO while the Go wire aggregate was success. Native bytes
and nanosecond mtime were identical and the comparison per-op value was -4095.
Pinned Objecter.h's cmpext callback throws for mismatch even under FAILOK.
The test now explicitly compares that native wrapper error separately from the
raw wire aggregate and verifies the committed data and per-op code. This was
not converted into a fabricated server refusal or hidden by rerunning.

The first BlueStore full experiment passed all 21 vectors and restart readback
but failed a fresh lost-reply handshake because the receive fault was still
armed. It also exposed stale nonce metadata for later read tests after OSD
restart. The fault is now disarmed before new authentication, and native
refreshed routing is published to subsequent fixture consumers. The final full
BlueStore suite passed all nine tests; the first failed run remains recorded
separately from that later acceptance.

Local logs: `/tmp/osd-mutations-secure.log` (initial FAILOK experiment),
`/tmp/osd-mutations-bluestore.log` (first full experiment),
`/tmp/osd-mutations-bluestore2.log` (focused correction),
`/tmp/osd-mutations-bluestore-full.log` (final full suite),
`/tmp/osd-mutations-crc.log` (CRC host acceptance).
These paths are local execution evidence, not tracked repository artifacts.

Remaining work is listed in [the wire contract](../../internal/osd/CONTRACT.md):
broader replicated profiles, mixed read-output/mutation vectors, forced-crash
durability, native mutation payload encoder fixtures and unsolicited event/
control contracts. The SPEC O0/O1 milestones remain partially complete.
