# OSD-copy-classification

This file holds one OSD row of the [native path coverage map](../README.md).

## Rows

| ID | Path | Class | Setup and control | Independent observation | Fault and cleanup | Evidence | Visibility |
| --- | --- | --- | --- | --- | --- | --- | --- |
| OSD-copy-classification | COPY_FROM/COPY_FROM2 clone/head source versions and local flag restrictions | `released` | Single BlueStore CRC/AES IPv4 host replicated-pg-reply-v8; self-managed clone and later head | Native oracle: source clone/head reads and destination bytes/xattrs/OMAP/header/version/mtime; server result: signed aggregate/per-op reply | No injected fault; remove owned heads/snapshot, wait for native clone trimming and active+clean | `TestCephOSDCopyFromClassificationIntegration`; [record](../../OSD_COPY_FROM_CLASSIFICATION_VALIDATION.md) | dedicated row; unsupported copy flags only have known-unsent encoder evidence |

## Updates

2026-11-18: added the snapshot/version cross-product and individual copy-flag
classification. The record separates native execution from local admission;
no cache-tier or fault result is inferred.
