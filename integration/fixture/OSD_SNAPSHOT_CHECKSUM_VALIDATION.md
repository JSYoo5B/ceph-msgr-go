# Caller-selected snapshot CHECKSUM — 2026-10-16

Reference: Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

## Product change

The existing bounded standalone CHECKSUM method now accepts ReadSnapshot
for CRC32C, xxHash32 and xxHash64. No new public method or native product
dependency was added. MAPEXT and LIST_WATCHERS remain head-only. Write
SnapContext, ORDERSNAP, operation-level ROLLBACK snap fields, compound
checksums and implicit whole-object requests remain rejected. The caller
selects the snapshot and supplies placement; the product does not discover
snapshots or retry a request automatically. The OSD does not echo the read
snapshot in its reply. Backoff admission continues to match the head identity.

Pinned PrimaryLogPG.cc:2179,2319-2380 resolves the selected object before
CHECKSUM dispatch at 6151. Objecter/IoCtxImpl read-snap propagation and
common/Checksummer.h apply the same algorithm and extent rules to that
selected object. A later snap may resolve to the current head; an absent
clone remains a signed server error. Snapshot and object bytes/state are
independent of local context cancellation.

## Evidence design

The retained native algorithm vectors remain the independent seed/union/
reply-width proof. This change did not generate new native snapshot/checksum
combination vectors. Focused tests compose those already verified fields
with the existing v6/v8 read-snapshot tail and explicitly label that scope.
They exercise invalid snapshot/write contexts, head aliases, all algorithms,
known-unsent guards, raw replies and signed errors. Normal and race checks
passed. Whole-product CGO-disabled tests and vet passed, and the Python
oracle's eight ABI/bounds/restoration tests passed.

The independent live oracle selects the same snapshot with the native C
rados_ioctx_snap_set_read before rados_checksum. It independently observes
the selected object before/after, always restores LIBRADOS_SNAP_HEAD in a
finally block, and observes unchanged head state separately. A second native
observation after the Go operation verifies both head and selected snapshot
bytes/version/mtime again. Native read retries are separate from Go policy.

The 39 new cases (13 per algorithm) cover an old clone with holes, rewritten
head, unchanged object, object created after the selected snap, deleted head
with retained clone, later/above-sequence snaps resolving to head, aligned
and unaligned trimming, exact/past clone EOF and signed ENOENT. A separate
24-case head suite runs in the same fixture invocation to verify the native
adapter's unchanged head behavior.

## Results

The initial CRC/AES IPv4 three-BlueStore invocation passed all 39 snapshot
cases (22.60 s) and 24 head cases (14.63 s), but the new snapshot results file
was omitted from the fixture diagnostic allowlist. The temporary source
output was removed during successful fixture cleanup, so that run's log is
retained as a pass with incomplete detailed evidence, not as a raw-receipt
audit. It is preserved at
`/private/tmp/ceph-snapshot-checksum-crc-aes-20261016-first.log`.

The collector now retains `osd-snap-checksum-results.json`; its regression
check verifies the archived copy is private. The unchanged product and test
were then rerun against the same immutable image. The second CRC invocation
passed all 39 snapshot cases (22.87 s) and 24 head cases (14.87 s), with the
39-case raw receipt retained. The nested fixture's whole tests and vet passed.

CRC detailed artifact root:
`/private/tmp/ceph-snapshot-checksum-crc-aes-20261016-second/`.
`osd-snap-checksum-results.json` SHA-256:
`0fc0bbf15c1472a5ddc9ddf7b183f5913ee21d78b3f50515947f0bbe6d3922aa`.

The first secure/AES256K IPv6 container invocation passed 39 snapshot cases
(22.29 s) and 24 head cases (13.60 s) on single MemStore. Both detailed
acceptances use the immutable image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
These local results are distinct from remote CI status.

Secure detailed artifact root:
`/private/tmp/ceph-snapshot-checksum-secure-aes256k-ipv6-20261016-first/`.
`osd-snap-checksum-results.json` SHA-256:
`e237c0fb8a49a0d805ad348452ef139c65ec71caf513b37e623eb45b1de834b1`.

The independent raw-receipt audit checked all 39 cases per run, including
little-endian count/value widths, full xxHash64 bits, aggregate versus per-op
errors, selected old-clone versus new-head models, and each after-Go native
observation. Both runs removed four owned heads (one already absent),
removed the two owned self-managed snaps and waited for clone/whiteout trim
and clean PGs. Every fixture-owned container was terminated (10 CRC, 8 secure).

The CRC manifest retains the historical hardcoded provider-version label.
The secure run also independently verifies the corrected runtime provider
metadata: v0.3.0, status module, matching original/effective dependency paths
and checksum. See [provider identity](PROVIDER_IDENTITY_VALIDATION.md);
old receipts were not rewritten.
Frozen new live test source:
`bb001effa48b677eb0e4f17b2621c183fddc7d9f80ea1a0fdb1fcecce0ffa6d7`.
Frozen independent adapter:
`013f30a47384c4af901e5e2ba74b4e335293034af492180102bdac853edd3883`.
Frozen snapshot admission:
`ba98153bce336f925a4adce44cdf087b7b4920ef5d2d154e5a107b9e4024b21a`.

## Limits

This acceptance concerns bounded replicated-pool read requests, self-managed
snapshots, and the explicitly run configurations. It does not establish EC,
pool snapshot modes, automatic snapshot management, mutation retries, broader
request compounds or snapshot MAPEXT support. Earlier head-only evidence is
preserved in the algorithm and read-metadata acceptance records.
