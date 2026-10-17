# MON statfs (MStatfs)

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Contract:
`cephmsgr/STATFS_CONTRACT.md`. Test: `TestCephMonStatfsIntegration`, with the
pool oracle's new `clusterstat` mode (native `rados_cluster_stat`, which sends
the same MStatfs).

## Observed

- Cluster form: Go's figures (KB, KB used, KB available, object count) equal
  the native client's for the same moment; the test polls until a Go/native
  pair matches because the MGR stat service updates every few seconds. The
  figures are plausible (used and available not above the total, objects > 0).
- Pool form: for the fixture pool and an owned pool the MON answers with the
  pool's own figures and a committed version not below the cluster's; the owned
  pool reports its three native-seeded objects once the stat service has
  caught up (waited up to two minutes). There is no native twin for the pool
  form (libcephfs would be needed), so only consistency with the cluster form
  and the object count are checked.
- An absent pool id gets no reply: the call ends by its context as an unknown
  outcome and the session keeps working afterward.
- Every monitor: one client seeded only with each of the three monitors
  gets the cluster form with the same capacity; two of them answered as
  peons. Passed in the same two profiles as the capability case.
- Capability denial: `client.configwatch`, whose only MON grant is the `fsid`
  command (no `pg` read), gets no reply for either form; each call ends by its
  4-second context as an unknown outcome, the same session then runs its
  allowed `fsid` command, and the admin identity still gets figures. Added
  later the same day; passed in legacy-v6 single BlueStore and
  request-v9/reply-v8 secure AES256K IPv6 single MemStore.
- Wire: byte-level unit tests (request layout, 56-byte reply) and a synthetic
  MON; the daemon decoding our request and our decoding its reply is the
  interoperability evidence. No native-encoder vector exists.

## Profiles

Passed first complete run in legacy-v6 single BlueStore, legacy-v6 three
BlueStore replicas, replicated-pg-v8 single BlueStore and request-v9/reply-v8
secure AES256K IPv6 single MemStore.

Not claimed: the FSID-mismatch ignore, equality of pool
figures with libcephfs `statfs`, behavior across MON failover, and any scaling
of the figures.
