# Full-pool request flags (FULL_TRY / FULL_FORCE)

Run 2026-10-17 against Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Contract:
`internal/osd/FULL_CONTRACT.md`. Test: `TestCephOSDFullPoolFlagsIntegration`,
using the owned-pool oracle (`tools/osd_pool_oracle.py` modes `quota`,
`poolinfo`; see OSD_POOL_VALIDATION.md).

## Setup

An owned pool receives three native-written objects, then a native
`osd pool set-quota max_objects 2`. The test polls the native pool dump until
the flag names contain `full_quota` (a few seconds in every run). Every request
below carries a fresh placement epoch obtained after the flag appeared.

## Observed

- Default flags: a Go WRITEFULL receives no reply. With a five second context
  it ends as an unknown outcome (the request was sent). Native observation shows
  no object was created. This is why callers need FULL_TRY.
- `FullTry`: a known server refusal with signed code `-122` (EDQUOT) and no
  object created. Not an unknown outcome.
- `FullForce`: the write completes with ONDISK, the native object holds the
  bytes and its version equals the receipt's user version; a forced DELETE of a
  seeded object also completes and the object is gone natively.
- Misuse (both flags, or a flag on a read vector) is refused before submission
  with no transaction allocated.

## Profiles

Passed first run in legacy-v6 single BlueStore, legacy-v6 three BlueStore
replicas, replicated-pg-v8 single BlueStore, and request-v9/reply-v8 secure
AES256K IPv6 single MemStore.

Not claimed: `-ENOSPC` from an OSD-fullness (not quota) full flag, FULL_FORCE
semantics for the MDS path, behavior under failover while full, EC, or other
request flags (ENFORCE_SNAPC, FLUSH). The `-122` value is the signed Linux
code the server sends; it is preserved raw, not translated to a host errno.
