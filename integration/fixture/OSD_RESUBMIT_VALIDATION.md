# Explicit OSD resubmission

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Contract:
`internal/osd/RESUBMIT_CONTRACT.md`. Test: `TestCephOSDResubmitIntegration`
(two subtests, native-seeded owned objects, native observation of bytes and
version).

## Observed

- Completed request: Go APPENDs once (native: baseline plus the tail, version
  v). A resubmission with the returned `Submission` and `RetryAttempt` 1 gets
  a success reply under the same transaction ID, echoing attempt 1, with
  ONDISK, the same user version v and the logged replay version; native bytes
  and version are unchanged, so nothing executed twice. Control: a new request
  with a new ID executes a second append.
- Unknown outcome: a relay drops the reply of an APPEND that executes; the
  caller cancels and gets an unknown outcome with its `Submission`. After the
  connection is replaced (same client identity), the resubmission returns
  success with the native version and ONDISK, and native bytes show exactly one
  append. This resolves the unknown outcome without a second execution.
- Vectors whose re-execution would differ: an exclusive CREATE (re-execution
  would answer EEXIST), a DELETE (ENOENT) and a compound APPEND plus SETXATTR
  (bytes would double). Each resubmission returns the original code, per-op
  results, user version and ONDISK, and native bytes, version and attributes
  are unchanged. A third subtest (`non-idempotent vectors answered from the PG
  log`) covers them on the same profiles.
- First attempt of the live test failed only on a test bug (it expected the
  seed baseline on an object that starts absent); the product was unchanged.
- Local refusals and the retry-echo check are in `cephmsgr/osd_resubmit_test.go`.

## Profiles

Passed in legacy-v6 single BlueStore, legacy-v6 three BlueStore replicas,
legacy-v6 CRC with AES key and service cipher, replicated-pg-v8 single
BlueStore, request-v9/reply-v8 with secure AES256K over IPv6 on single
MemStore, and legacy-v6 secure AES256K over IPv6 on single MemStore.

Not claimed: resubmission after PG log trimming, after a primary change or
daemon restart, for read vectors, for OMAP/CALL/snapshot/COPY_FROM vectors
(the server may not log every opcode); resubmission through a different client
identity (refused locally).
