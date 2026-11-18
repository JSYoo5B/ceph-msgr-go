# COPY_FROM flag, snapshot and version classification

Reference: Tentacle v20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`.
The source-derived `ASSERT_VER` branch distinguishes lower (-34 ERANGE) and
higher (-75 EOVERFLOW) requested versions. The asynchronous copy retains the
per-op -115 EINPROGRESS marker when this source read fails. Native execution
must verify that representation; a CLI exit status is not a wire errno.

## Recipe and oracle

`TestCephOSDCopyFromClassificationIntegration` adds the missing cross-product
of both copy opcodes, a real snapshot clone versus a later head, and source
version zero/exact/lower/higher. Existing `TestCephOSDCopyFromIntegration`
already tests head-version mismatches and an unpinned snapshot separately.
Before each of the 16 cells, the test writes and independently observes a
case-specific populated sentinel: distinct bytes, marker and destination-only
xattrs, OMAP entry/header and mtime. Its native version must match the seed
receipt. Success must replace that state with the selected source; refusal
must preserve the complete native sentinel state. The preceding copy is never
used as the next version case's precondition.
It does not inject faults or change product admission.

Native librados snapshot reads (including decoded object_info) establish
separate clone/head bytes, xattrs, user versions and mtime. Go requests then
carry each source snapshot/version selection; native destination readback
checks bytes, xattrs, OMAP/header, version and mtime on success and complete
unchanged state on failure. Full public replies retain signed aggregate and
per-op results, echoed source union, TID/submission identity and epochs.
Success requires ONDISK. Both source states must remain unchanged.

The same live connections also reject FAILOK/EXCL operation flags, non-FADVISE
source hints, SNAPDIR, orphan truncate size, normalized truncate pair,
destination read snapshots and COPY_FROM truncate input. A before/after
read pair must have consecutive TIDs and unchanged native destination state.
This checks known-unsent admission and continued use; rejected internal copy
flags themselves are not representable in the public input and have exhaustive
v6/v8/v9 encoder coverage instead.

Owned heads and self-managed snapshots are removed, native listing confirms
clone/whiteout trimming and the fixture must return to active+clean. The
receipt `osd-copyfrom-classification-results.json` is written after cleanup.
No native twin COPY_FROM encoder/executor is added; native readback and the
previous native request vectors are distinct evidence.

Catalog row:
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromclassification`.

## Execution record

On 2026-11-18, Linux amd64, Docker 29.9.1, Go 1.26.9, the catalog row above
passed at source `f8b133b` against the pinned v20.2.4 image digest
`6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The required parent passed in 12.97 s, with all 16 nested cells PASS and no
skip or missing-test gate problems. The source clone was snapshot 2 at user
version 2; the later head was version 4. Both opcodes returned 0/0 on
zero/exact versions, -34/-115 on lower versions, and -75/-115 on higher
versions (aggregate/per-op). All 15 public local refusals preserved TID
continuity and native destination state. Three heads and one snapshot were
removed; native clone/whiteout trim completed in 602 ms and active+clean was
confirmed. Eight fixture containers terminated and a subsequent inventory
contained no testcontainers containers.

That initial execution did not archive the new JSON filename because it was
missing from the diagnostic collector allowlist. Its assertions and text
results passed, but it is not a retained raw-receipt claim. The follow-up
adds that explicit filename and re-executes the row to retain the receipt.

The historical retained run at `522ada1dc5a42c7d1a4e0a9b7869da2b795b3d99` (tree
`7eb07caf8e00d224d916417444510f57e63d5cff`) passed the same row in 13.16 s.
All 16 cells passed with the same version/result classification above, 15
known-unsent refusals and `cleaned: true`. Native trim took 603 ms and the
fixture was active+clean. Eight fixture containers terminated and the final
testcontainers inventory was empty. The archived public JSON is 46,360 bytes;
SHA-256 `c002958ccf5004827617bcf3cf9029dbdd1d82914f68920faac91d932ae0a8b8`.
It retains the raw signed per-op results (including -115), complete public
replies and before/after native destination observations. The executed
validation copy had the exact source tree above. That earlier implementation did not re-seed every version case: exact-version
success and subsequent version refusals could begin with source-equivalent
destination state. Its PASS therefore does not establish distinguishable
replacement/preservation for those cases. The per-case sentinel correction
below supersedes that aspect of the historical evidence. Exact-SHA hosted CI remains
distinct from these local Linux executions.

The initial prepush attempt found an invalid new coverage-row format and
existing paired-suite guards that treated a dedicated modern row as a suite.
The corrected row has its own check while the six original suite gates remain.
The coverage merge-test helper now creates parent directories for row files;
the first real row file exposed its previous root-files-only assumption.
A separate verification-copy error (missing Git tracked-file metadata) was
fixed in that copy. A later document check caught a link in a receipt-ID-only
column; the link now belongs to the contract description column. These were
prepush/tooling failures, not failed daemon cells.

## Boundaries

Cache-tier FLUSH/IGNORE_OVERLAY/IGNORE_CACHE/RWORDERED, direct clone mapping,
unknown copy bits and invalid truncate forms remain locally refused; they
were not sent to a daemon. Version UINT64_MAX and snapshot MAXSNAP only have
numeric encoder evidence. Destination SnapContext/ORDERSNAP, removed snapshots,
nonexistent-source snapshots, snapshot plus TRUNCATE_SEQ, additional profiles,
and fault timing are separate cells. No universal snapshot/flag/version
cross-product support is claimed.


## Per-case sentinel correction

Review found that a preceding successful copy made later case preconditions
indistinguishable from the source. The corrected test seeds all sixteen cases
independently and retains each seed reply alongside its native before/after
states. Native revalidation and a negative control are pending for this
correction; the historical results above are not substituted for them.
