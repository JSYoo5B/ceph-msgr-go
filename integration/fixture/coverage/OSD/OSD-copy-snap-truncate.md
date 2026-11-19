# OSD-copy-snap-truncate

This file holds one OSD row of the [native path coverage map](../README.md).

## Rows

| ID | Path | Class | Setup and control | Independent observation | Fault and cleanup | Evidence | Visibility |
| --- | --- | --- | --- | --- | --- | --- | --- |
| OSD-copy-snap-truncate | COPY_FROM/COPY_FROM2 from a truncated head and its untruncated clone, the TRUNCATE_SEQ override and destination SnapContexts | `released` | Single BlueStore CRC/AES IPv4 host replicated-pg-reply-v8; self-managed snapshots, sequenced source truncate, per-case destinations | Native oracle: source/destination bytes, xattrs, user versions, mtime, object_info truncate pair, rados listsnaps clones/overlaps/seq and destination clone reads; server result: signed aggregate/per-op reply | No injected fault; remove owned heads/snapshots, wait for native clone trimming and active+clean | `TestCephOSDCopyFromSnapTruncateIntegration`; [record](../../OSD_COPY_FROM_SNAP_TRUNCATE_VALIDATION.md) | dedicated row; truncate-in-vector and invalid SnapContext forms only have known-unsent evidence |

## Updates

2026-11-20: added truncated-source, TRUNCATE_SEQ and destination SnapContext
copy cells. Native execution is recorded separately from local admission.
