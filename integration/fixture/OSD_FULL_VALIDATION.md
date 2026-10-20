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

## OSD fullness (`TestCephOSDFullRatioIntegration`)

The pool oracle's `fullratios` mode reports and sets the cluster-wide
nearfull, backfillfull and full ratios (disposable fixture only); the test
records the original values and restores them, also on failure. With the
ratios lowered to 0.0005/0.0008/0.001, below the BlueStore OSDs' actual use,
the OSDs report full and the MON flags the owned pool `full` (not
`full_quota`) within one or two polls.

- A plain WRITEFULL is never answered and ends as an unknown outcome; the
  object stays absent natively.
- With FULL_TRY the OSD answers `-28` (ENOSPC), a known refusal with no
  effect, where the quota case answers `-122`.
- With FULL_FORCE the write completes with ONDISK and matching native bytes
  and version; a read of a seeded object is answered normally.
- After the ratios are restored the MON clears the flag (three or four epochs
  later), the reported ratios equal the originals, and a plain write is
  answered with ONDISK.
- Passed in legacy-v6 single BlueStore (osd.0 full) and replicated-pg-v8
  secure AES256K IPv6 with three BlueStore replicas (two OSDs full when the
  pool was flagged), 2026-10-17. MemStore is not used: fullness is measured
  on the BlueStore devices.

## Primary failure while full (2026-10-20)

`TestCephOSDFullFailoverIntegration` (legacy-v6, three BlueStore replicas)
lowers the ratios until all three OSDs report full. A plain WRITEFULL to an
owned object is held unanswered by its primary; 2 s later that primary is
SIGKILLed and the call ends as an unknown outcome (connection EOF). Once
native placement names a new primary, a FULL_TRY write there gets a known
`-28` and a FULL_FORCE write completes with ONDISK, matching native bytes and
version. The ratios are restored first; the dead OSD stays listed as full
until it restarts and reports, so the test restarts it, waits for the full
set to empty, then for clean PGs. The held write's object is then still
absent natively: it never executed. Passed in secure AES256K IPv4 and CRC
mode with AES key and service cipher over IPv6.

Not claimed: FULL_FORCE semantics for the MDS path, EC, or other
request flags (ENFORCE_SNAPC, FLUSH). The `-122` value is the signed Linux
code the server sends; it is preserved raw, not translated to a host errno.
