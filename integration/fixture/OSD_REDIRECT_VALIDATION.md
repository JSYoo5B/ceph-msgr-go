# Manifest redirect

Run 2026-10-18, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Test: `TestCephOSDRedirectIntegration`. Contract:
`internal/osd/MANIFEST_CONTRACT.md`.

Two owned pools; the native client seeds a target in pool 2 and two objects
in pool 1.

- A Go SET_REDIRECT turns pool 1's object into a redirect to the pool 2
  target (ONDISK). A Go READ of it and a native read of it both return the
  target's bytes; the target's bytes and version are unchanged.
- The first run read the right bytes but failed validation: the proxied
  read's reply echoed the requested length (4096) with a 23-byte payload, and
  the client treated that as a malformed reply (unknown outcome). The read
  validator now accepts an echoed request length; the rerun passed.
- A Go WRITEFULL to the redirected object is proxied to the target: it
  completes with ONDISK, native reads of the target return the new bytes,
  and the redirected object keeps reading them.
- A Go UNSET_MANIFEST leaves the object present and empty for Go and native
  reads.
- Known refusals: a redirect of an absent object -2, of an object to itself
  -22, and UNSET_MANIFEST on a plain object -95. A redirect target without
  OSDSetRedirect and OSDSetRedirect without a target are refused locally,
  before submission.

Passed in legacy-v6 single BlueStore (secure AES256K IPv4), replicated-pg-v8
three BlueStore (secure AES256K IPv6) and request-v9/reply-v8 single MemStore
(secure AES256K IPv6).

The write case was added afterwards; it passed in all three profiles.

Not claimed: other write opcodes through a redirect, reference-counted
redirects, SET_CHUNK, TIER_PROMOTE/FLUSH/EVICT and
dedup pools.
