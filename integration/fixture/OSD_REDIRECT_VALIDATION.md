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
- With `IgnoreRedirect` (CEPH_OSD_FLAG_IGNORE_REDIRECT) a Go READ of the
  redirected object returns its own, truncated and empty, data; with
  `OrderReadsWrites` (RWORDERED) or `SkipRWLocks` (SKIPRWLOCKS) the read
  still returns the target's bytes; SKIPRWLOCKS on a write is refused locally
  (the SKIPRWLOCKS case passed in legacy-v6 and replicated-pg-v8 three
  BlueStore).
  Added later; passed in legacy-v6 single BlueStore and request-v9 single
  MemStore.
- A Go WRITEFULL to the redirected object is proxied to the target: it
  completes with ONDISK, native reads of the target return the new bytes,
  and the redirected object keeps reading them.
- An APPEND and a SETXATTR sent to the redirected object are proxied the same
  way (2026-10-18): both complete with ONDISK, native observation of the
  target shows the appended bytes and the new xattr, and a Go read through
  the redirect returns the target's bytes. Passed in legacy-v6 single
  BlueStore and request-v9/reply-v8 secure AES256K IPv6 single MemStore.
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

- A DELETE of a redirected object is proxied as well (2026-10-18): it
  completes with ONDISK, native observation finds the redirect target absent
  (`-2`), and a Go STAT with IGNORE_REDIRECT shows the manifest object still
  present. Passed in legacy-v6 single BlueStore and request-v9/reply-v8
  secure AES256K IPv6 single MemStore.

- An OMAP SETVALS sent to the redirected object is proxied (2026-10-18): it
  completes with ONDISK, and a Go GETVALSBYKEYS addressed directly to the
  target in pool 2 (not through the redirect) returns the value. Same two
  profiles.

Not claimed: other write opcodes through a redirect, reference-counted
redirects, SET_CHUNK, TIER_PROMOTE/FLUSH/EVICT and
dedup pools.
