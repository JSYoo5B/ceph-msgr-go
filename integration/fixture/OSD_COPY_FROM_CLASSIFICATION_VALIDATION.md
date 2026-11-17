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
The new test deliberately uses a populated destination for all 16 cells.
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

Native execution pending. No result is claimed by this recipe alone.

## Boundaries

Cache-tier FLUSH/IGNORE_OVERLAY/IGNORE_CACHE/RWORDERED, direct clone mapping,
unknown copy bits and invalid truncate forms remain locally refused; they
were not sent to a daemon. Version UINT64_MAX and snapshot MAXSNAP only have
numeric encoder evidence. Destination SnapContext/ORDERSNAP, removed snapshots,
nonexistent-source snapshots, snapshot plus TRUNCATE_SEQ, additional profiles,
and fault timing are separate cells. No universal snapshot/flag/version
cross-product support is claimed.
