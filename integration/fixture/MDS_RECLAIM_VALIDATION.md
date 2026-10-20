# CephFS session reclaim

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers MDS sessions fixture (`CEPH_MSGR_TEST_MDS_FRESH=1`,
`CEPH_MSGR_TEST_MDS_SESSIONS=1`, `legacy-v6`), container runtime. Contract:
the "Session reclaim" section of `cephmsgr/MDS_SESSION_CONTRACT.md`. Test: the
new leaf `reclaim_evicts_the_target_session` of
`TestCephMDSSessionsIntegration` (two more Go clients, G and H, same read-only
fresh identity; each opens a session with a unique `uuid` metadata key).

## Observed

- Both sessions are visible to the independent native `session ls` with their
  uuid metadata.
- H reclaims a uuid no session carries: result 0, no addresses, epoch not set;
  native state is unchanged (both sessions present, nothing blocklisted).
- H reclaims G's uuid: result 0, a nonzero OSD epoch and exactly one address
  that equals the native address of G's session (same IP, port 0 and nonce).
  Native observation then shows G's session removed, G's address blocklisted
  and H's session still open. G's local session ends as Lost; H stays Open.
- `FinishReclaim` is accepted by the MDS and leaves H's session untouched.
- Wire encoding and reply decoding are checked against the daemon (it decodes
  our message and we decode its reply); there is no fresh native-encoder vector
  for these two messages (the native build cache is not available), and the
  native libcephfs `start_reclaim` was not used as a twin sender.

## Profiles

The whole sessions suite, including the new leaf, passed on the first run in
CRC-free default secure CephX with AES256K over IPv4 on BlueStore (default
recipe) and with secure AES256K over IPv6 on MemStore.

Not claimed: `-EPERM` for a different CephX identity (the MDS code path answers
EPERM and then continues, so it is not exercised), reclaim during MDS failover,
the waiting for the blocklist epoch on OSDs, native-client targets that set
their uuid with `ceph_set_uuid`, multi-rank filesystems, and any reclaim flag
other than RESET.
