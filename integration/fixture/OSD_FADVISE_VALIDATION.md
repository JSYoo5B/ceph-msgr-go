# FADVISE cache hints

Run 2026-10-18, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Test: `TestCephOSDFadviseIntegration`.

On an owned pool, a Go WRITEFULL with DONTNEED|NOCACHE and an APPEND with
RANDOM complete, and the native client reads exactly the written bytes. A
compound CMPEXT (WILLNEED) and READ (SEQUENTIAL|WILLNEED) then return the
same bytes; the reply echoes the requested hint flags (0x10 and 0x18). A
COPY_FROM2 with source hints DONTNEED|NOCACHE copies the same bytes (native
read of the copy), and its reply echoes the hint mask in the union. STAT and
CREATE carrying a hint are refused locally before submission.

Passed in legacy-v6 single BlueStore (secure AES256K IPv4), replicated-pg-v8
three BlueStore (secure AES256K IPv6) and request-v9/reply-v8 single MemStore
(secure AES256K IPv6); the COPY_FROM2 case was added later and passed in the
first and last of these.

Added 2026-10-18: a WRITESAME with DONTNEED (64 bytes of a 2-byte pattern
appended at the end) matches native bytes; a SPARSE_READ with SEQUENTIAL
returns the native bytes and the same data as an unhinted one; a CRC32C
CHECKSUM with WILLNEED|NOCACHE returns the same value as an unhinted one.
Passed in legacy-v6 single BlueStore and request-v9/reply-v8 secure AES256K
IPv6 single MemStore.

Not claimed: any cache or I/O-pattern effect of the hints inside BlueStore
(they are advisory and invisible in results).
