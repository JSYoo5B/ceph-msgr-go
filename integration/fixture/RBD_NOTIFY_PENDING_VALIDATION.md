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

Implementation is present. New native runs and their exact source receipts
are pending; the historical base acceptance does not prove this checkpoint.

## Remaining scope

This proves no journal replay, simultaneous wire appends, RequestLock ACK
loss, dead-owner/blocklist handoff, lost mutation reply, replicated-primary
fault or complete RBD client equivalence. The state samples observe native
publication and outstanding control; they do not instrument the internal
native callback or provide cross-object atomicity. Issue #12 stays open.
