# RBD metadata visibility during an unacknowledged notification

This is a bounded issue #12 checkpoint in
`TestCephRBDBaseIntegration/header_watch_notify`. It reuses the existing
owned layering-only image and its two native resizes; it adds no mutation,
product API, command schema or routing policy. Image/class interpretation
and the deliberate acknowledgement omission remain test-only consumer code
in `integration/rbd_base_test.go`.

## Reference and observed boundary

Reference: Ceph Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`.
[`C_NotifyUpdate` in `src/librbd/Operations.cc`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librbd/Operations.cc#L120-L158)
sends the update notification after successful mutation and converts that
notification's `ETIMEDOUT` to operation success. The
[`Notifier` timeout](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librbd/watcher/Notifier.cc)
is five seconds. These LGPL-2.1 sources are references; no source is copied.
This timeout rule is specific to this HeaderUpdate completion path.

The native worker's `watch` action is unchanged:

1. Open the image and register librbd's header watch.
2. Resize head from 512 KiB to 640 KiB, with Go acknowledging HeaderUpdate.
3. Resize head to 768 KiB, with Go intentionally omitting its acknowledgement.
4. Publish completion, close the image, and observe native stat, snapshot
   metadata and the raw librados class outputs.

The new observation starts only after receiving step 3's notification from
the exact native GlobalID, on Go's own cookie, with its distinct NotifyID
and exact `07010400000003000000` payload. It checks the cumulative native
state and outstanding control action, reads `rbd.get_size(CEPH_NOSNAP)` and
`rbd.get_size(snapshot ID)` once each through Go's explicit OSD connection,
and checks the state/control again. Before and after must both name the
same token, request ID and native GlobalID, still end at
`resized-acknowledged` with two phases, and retain 640 KiB as the previous
completed size. Completion racing ahead fails this checkpoint.

The raw reads must already return order 16 with head size 786432 and
snapshot size 524288. They retain request placement, TID/submission,
GlobalID, PG, epoch, per-op code and raw output in the ordinary request
ledger. After the notifier timeout, the independent native receipt must be
complete with applied mutation, final head size 786432, unchanged snapshot,
exactly one `update notification timed-out` error line, and byte-identical
head/snapshot class outputs. The first acknowledged resize remains the
under-five-second control; the second takes at least the timeout minus the
existing 100 ms clock slack. Duration alone does not establish the new
intermediate state.

## Evidence and reproduction

`rbd-base-results.json` records notification identity and raw payload,
receive time, `submitted` versus `intentionally_omitted` ACK disposition,
ACK completion/error when applicable, and `ResizePending` with both native
state snapshots, both outstanding-control checks, the Go read interval and
decoded sizes. An omitted ACK is distinguishable from a missing notification
or a failed ACK submission. Existing final receipts and cleanup checks are
retained. No resize is replayed after an uncertain result.

Run the existing closed RBD selector on an approved Linux runner:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$' \
sh integration/run.sh
```

The second selected profile sets both key types to `aes256k`, mode `secure`,
IPv6, runtime `container` and store `memstore`. Both use the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
three MONs, two MGRs, one OSD and the native oracle client. The RBD fixture
still rejects replicated-PG profiles. GitHub default CI remains separate
from live native acceptance.

## Verification status

Both selected Linux/amd64 profiles passed on 2026-10-23 from exact source
`794847b5d53a3f6a840b1919f20545af759ddccc`, Go 1.26.9 and fixture provider
`ceph-testcontainers-go` v0.3.0. All 1824 tracked source files in the native
checkout matched the committed source manifest. The SHA-256 of
`integration/rbd_base_test.go` is
`ef6fdeca268414425b907f95a6df24c75a44c901c4461998c16a6d5d45bcc4ba`.
Only documentation/evidence references were added after these executions;
final-head Basic CI is a separate result.

| Observation | CRC/AES, IPv4 host, BlueStore | Secure/AES256K, IPv6 container, MemStore |
| --- | --- | --- |
| Run interval, UTC | 08:29:44 to 08:31:23 | 08:31:23 to 08:32:54 |
| Selected native suites / leaves | 5 / 27 passed, no failure or skip | 5 / 27 passed, no failure or skip |
| Go / native GlobalID; Go cookie | 4794 / 4800; 38 | 4767 / 4773; 38 |
| Acknowledged / intentionally omitted NotifyID | 107374182403 / 107374182404 | 98784247811 / 98784247812 |
| Pending read TIDs, head / snapshot | 45 / 46, submission identity matched | 45 / 46, submission identity matched |
| Pending read reply OSDMap epoch | 25 | 23 |
| Before / after native phase | `resized-acknowledged` / same, two phases | same |
| Before / after control still outstanding | true / true | true / true |
| Head / snapshot bytes | `1000000c0000000000` / `100000080000000000` | same |
| Decoded order; head / snapshot size | 16; 786432 / 524288 | same |
| Two raw reads' interval | 1.270752 ms | 0.987375 ms |
| Native acknowledged resize | 6.446053 ms | 0.864632 ms |
| Native unacknowledged resize | 5.002396189 s | 5.002142048 s |
| Native timed-out HeaderUpdate log count | 1 | 1 |
| Final native class output comparison | head and snapshot byte-identical | same |

The full selector also passed the existing clone, journal, exclusive-lock
and write acceptances. Their success is regression evidence; it does not
add lost-journal-reply or RequestLock ACK-loss coverage to this new leaf.
The first attempts of both profiles passed; no retry replaced a failed
result. Linux checks passed before native execution: the four RBD
test-only tests with `-race`, integration `go vet`, `CGO_ENABLED=0 go build
./...`, and 635 Python tool tests (76-scenario catalog valid). Unit results
are distinct from the two live native runs.

Both cleanup receipts report all 26 journaled native fixture actions
`applied`, removal of the owned pool with native absence by name/ID,
deletion of both identities, `completed: true` and no error. Direct
post-run inspection of each ownership receipt found none of its eight
containers and no owned network. The transient runtime reaper was also
absent on the later check. No host configuration or unowned resource was
changed. Public records retain only the notification/request identity and
relevant metadata; incidental native log timestamps/thread IDs and fixture
addresses are not reproduced here.

## Remaining scope

This proves no journal replay, simultaneous wire appends, RequestLock ACK
loss, dead-owner/blocklist handoff, lost mutation reply, replicated-primary
fault or complete RBD client equivalence. The state samples observe native
publication and outstanding control; they do not instrument the internal
native callback or provide cross-object atomicity. Issue #12 stays open.
