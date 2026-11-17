# COPY_FROM snapshot/version classification

| Path | Fixture/control | Oracle and receipt | Boundary |
| --- | --- | --- | --- |
| COPY_FROM/COPY_FROM2 from clone/head with version zero/exact/lower/higher | Single BlueStore replicated-pg-reply-v8 CRC/AES IPv4 host, self-managed snapshot | `TestCephOSDCopyFromClassificationIntegration`; native source clone/head and destination state; [record](../../OSD_COPY_FROM_CLASSIFICATION_VALIDATION.md) | Unsupported copy flags remain known-unsent; no new fault or cache-tier claims |

2026-11-18: added a bounded classification recipe; execution status is in the
linked validation record. Owned objects/snapshots are removed with native
clone-trim and active+clean confirmation.
