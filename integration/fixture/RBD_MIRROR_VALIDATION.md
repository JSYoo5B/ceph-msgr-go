# RBD mirror replication and source-path recovery

This is the local native recipe for issue #124 (tracked under #14): native RBD
snapshot mirroring on an owned disposable profile, one directed fault on the
source OSD's path to the rbd-mirror daemon, its restoration and the catch-up
of distinguishable data. It covers rows SVC-2 and SVC-3 of the
[native path coverage map](NATIVE_PATH_COVERAGE.md). The product implements no
daemon role and no mirroring policy; mirroring here is entirely native. The
cephfs-mirror daemon needs its own setup and oracle and is not covered (SVC-4,
SVC-5).

Reference: Ceph Tentacle v20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
for every daemon, the rbd-mirror daemon and the setup clients. Provider:
`github.com/JSYoo5B/ceph-testcontainers-go v0.5.0` (`go.mod` in this
directory). Netem helper:
`ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1@sha256:bb7ea0d86f0d2c4cb5720998eac27ce9c6f8d89bc431540c1d2cc71ac51c1c9a`,
the provider's `fault.NetemHelperImage` pinned to the registry digest of the
manifest list its tag resolves to (linux/amd64 manifest
`sha256:d4eb0514dba959843ce88f32bd0a14d69cf3c7608a2e356bd7520a8fed42fd2d`,
linux/arm64 manifest
`sha256:7d58ef4cc609258af4b85932b542665000b013eaa21942c262e0fac6ac642ec7`).

The tag is mutable. Take a new pin from the registry, as the
`Docker-Content-Digest` of the tag's manifest, and confirm it with a pull that
no local image satisfies. The first pin of this branch,
`sha256:63d17894d30de34fbf6c956b38e2576002aed58748b779b04c5e4632fc9dfdc3`,
was copied from a Docker engine whose containerd image store had pulled the tag
on 2026-10-18. The tag was republished on 2026-10-25 and the registry no longer
serves that older manifest list. The engine that produced the local native runs
still held it, so those runs resolved the pin locally and passed, while a fresh
GitHub Actions runner failed in the `start` phase of `cut` because the helper
reference was not found.

## Selection and profile

The catalog row
`rbd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmirrorpathrecovery`
selects only `TestCephRBDMirrorPathRecoveryIntegration`. It needs no workflow
opt-in: the fixture admits the checkpoint, and sets the test marker
`CEPH_MSGR_NATIVE_RBD_MIRROR=1`, only for this exact selector on the
single-BlueStore, legacy-v6, IPv4 OSD fixture without the RBD base, MDS,
cache-pool, MGR-service, soak or MON-freeze profiles. Any broad suite skips
the test before it reads a fixture input
(`TestRBDMirrorRequiresExclusiveMarker`). Request it with
`/ci run rbd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmirrorpathrecovery`
once this row is on the default branch.

| Role | Topology |
| --- | --- |
| Source | The fixture's own cluster: 3 MON, 2 MGR, one BlueStore OSD, aes256k keys and service cipher, secure Messenger v2 only, IPv4 bridge network |
| Destination | An owned cluster started by the fixture: 1 MON, 1 MGR, one BlueStore OSD, the same key type, cipher and Messenger policy, on its own bridge network |
| Pool | `msgr-mirror` on both clusters: replicated, size 1, one PG, application `rbd`, initialized with `rbd.InitPool` |
| Mirror | `rbd/mirror.Run`: image scope, snapshot mode, one daemon, two setup clients; the daemon and the setup clients are attached to both networks |
| Image | `msgr-mirror/path`: 4 MiB, 1 MiB objects, `layering,exclusive-lock`, imported natively from the fixture's client container |

## Recipe

The test drives four fixture actions in order (`rbd-mirror start`, `cut`,
`restore`, `finish`). The fixture admits each action once and only after the
previous one succeeded; `finish` is admitted after any earlier phase so that a
failed checkpoint still releases what it owns. Every action has its own bound
(10, 3, 6 and 3 minutes) and writes the receipt `rbd-mirror-receipt.json`. No
mutation is retried automatically.

1. **start**: start the destination, create and initialize both pools, start
   the mirror, import the seed image (deterministic bytes, seed 17), enable
   snapshot mirroring and wait (at most 4 minutes) until the destination's
   native export digest equals the seed's, then until the destination's native
   status reports the first snapshot in sync. It records both FSIDs, networks
   and versions, the global image ID, both image IDs, the daemon name, its own
   destination-side Cephx user, instance ID and its IPv4 address on the source
   network, and the source-side Cephx user of the destination pool's one
   receiving peer (`rbd mirror pool info`).
2. The test opens its own Go session to the same source OSD and reads the
   fixture's native oracle object (stat and read), comparing the native bytes.
3. **cut**: `fault.ApplyNetworkProfile` on the source OSD's public endpoint with
   the daemon's source-network address as `Peer` and 100% loss: the OSD drops
   every packet it sends to the daemon and only those. The lease must target
   the source OSD container, and the held lease's own netem helper (a
   container of the helper image in the network namespace of that OSD
   container) must exist. The source client then
   writes a distinguishable 1 MiB patch at offset 2 MiB (seed 99, through
   `rbd import-diff`) and takes a mirror snapshot. For 30 seconds, sampled
   every 5 seconds, the daemon must stay `up+replaying` at the baseline remote
   and local snapshot timestamps. The destination export must still equal the
   seed and the source export the patched image. The netem status must be
   applied with a nonzero drop counter.
4. The test reads the oracle object again on the same Go session to the same
   OSD while the fault holds. It must succeed with the native bytes.
5. **restore**: re-read the netem status (still applied, drops not lower),
   restore the lease, record that the restored lease refuses a further status
   read, confirm that no container of the helper image remains in the source
   OSD container's network namespace (helpers of the same image for other
   targets are not counted), then wait (at most 4 minutes) for the
   destination export to equal the patched image and for the native status to
   report a newer snapshot in sync whose `last_snapshot_bytes` is exactly the
   1 MiB patch.
6. **finish**: inventory the containers attached to the destination network and
   the mirror's daemons, restore a lease that is still held, terminate the
   mirror and then the destination, and confirm through the Docker engine that
   each inventoried container and the destination network no longer exist.
   Each teardown call is issued at most once and whether or not the Docker
   inventory is available; an unavailable inventory only withholds the
   independent confirmation and fails the step. A call not reached before the
   step's bound ends is left to the fixture's termination, which issues only
   the calls not yet issued. The source pool, its mirroring policy
   and the peer user stay in the disposable source cluster, which the fixture
   removes as a whole. If the test ends earlier, the fixture's own termination
   runs the same cleanup before the source cluster, and the source cluster's
   termination restores a lease that is still held.

## Messenger path attribution

The fault is attributed to one peer path by independent native observations,
not only by its effect on mirror progress:

- before the fault, the source OSD's `messenger dump client` (admin socket,
  unaffected by the network fault) has a client session from the daemon's
  source-network address, and the source MONs' `sessions` list the same
  address under the daemon's source-side peer identity. The test requires that
  identity to be `client.rbd-mirror-peer`, the user that peer bootstrap
  creates (`src/librbd/api/Mirror.cc` at v20.2.4), and distinct from the
  daemon's destination-side user, and requires that one OSD session from that
  address has the nonzero GlobalID of a MON session of that entity; a MON
  session of another entity from the same address does not count;
- during the cut the OSD dump still has the daemon's session and also the Go
  session (matched by its own GlobalID) from a different address, READY and
  connected, and that Go session keeps reading from the same OSD;
- the netem drop counter of the peer-filtered qdisc is nonzero during the cut
  and not lower at the restore;
- after the restore the daemon's session from the same address is present
  again and the native mirror status reports the new snapshot in sync.

The test restates the payload formula independently and compares the
fixture's native export digests with digests it derives itself.

## Native evidence

Recorded on 2026-11-10 on a Linux amd64 host with Docker 29.9.1, outside
GitHub Actions, using the catalog row's exact environment and the native result
gate (`tools/native_test_results.py snapshot` and `verify`). Image ID
`sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
architecture amd64, daemons `ceph version 20.2.4
(7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`. No other test
ran on that Docker engine during the runs.

The accepted run is of `2040d8a`, which pins the helper to its registry
digest; later commits change only documentation. Before the run the engine had
no image for that digest, so the helper was pulled from the registry during the
run; afterwards the engine reported it with image ID and RepoDigest
`sha256:bb7ea0d86f0d2c4cb5720998eac27ce9c6f8d89bc431540c1d2cc71ac51c1c9a` and
linux/amd64 manifest
`sha256:d4eb0514dba959843ce88f32bd0a14d69cf3c7608a2e356bd7520a8fed42fd2d`, and
the receipt recorded the same helper image ID. It passed
`TestCephRBDMirrorPathRecoveryIntegration` in 234 s (292 s for the whole
fixture run); the gate verdict was pass with no problems. The cut dropped 20
packets over 5 samples and 33 s, the destination caught up 46 s after the
restore, and 6 owned containers and the destination network were removed; no
Testcontainers container or extra network remained on the engine.

The earlier accepted run of `d19effd` (the review fixes for teardown, helper
scope and peer identity) resolved the first pin from that engine's cached
image, not from the registry. It passed in 203 s (260 s for the whole fixture
run) with gate verdict pass. Its receipt records:

- source: the fixture's cluster, 3 MON and 2 MGR, `osd.0`, pool size 1 with
  one PG; destination: 1 MON, 1 MGR and 1 OSD, a different FSID and network;
  both report `20.2.4`;
- the global image ID, distinct source and destination image IDs, daemon `a`
  with its own destination-side Cephx user `client.rbd-mirror.<id>`, attached
  to two networks, and `client.rbd-mirror-peer` as the destination pool's
  receiving peer user;
- before the fault, one READY source OSD session from the daemon's address,
  whose nonzero GlobalID equals that of the one source MON session from the
  same address, whose entity is `client.rbd-mirror-peer`;
- during the cut, one netem helper in the source OSD container's network
  namespace, 20 netem drops and 5 samples over 33 s, all `up+replaying` at
  the unchanged snapshot timestamps; destination digest of the seed, source
  digest of the patched image; the Go session's stat and read of the native
  oracle object on the same OSD succeeded;
- after the restore, the lease refused further status reads ("the network
  delay was restored"), no helper remained in the source OSD container's
  namespace, the destination reached the patched digest 44 s later and the
  native status reported a newer snapshot in sync with `last_snapshot_bytes`
  1048576;
- cleanup: 6 owned containers and the destination network independently
  confirmed absent. After the run, no Testcontainers container or extra
  network remained on the engine.

An earlier run of `c1cf75f`, before these fixes, also passed (204 s, catch-up
after 46 s); that revision did not check the peer entity or the helper scope.

Three earlier runs of intermediate revisions of this branch failed on the same
row, each in the test's own expectations, not in the native path; they are
kept here as first failures:

1. The cut sampler stopped before the full window (4 samples over 25 s); the
   test asked for at least 5 samples and 25 s. The sampler now always takes a
   last sample after the 30 s window.
2. With that change the window held (33 s), but the test still asked for 6
   samples; each native status read takes about 2 s on top of the 5 s
   interval. The test now requires the 30 s window and at least 4 samples.
3. The cut and the Go read during it passed, but the restore step read the
   lease status after `Restore`, which the provider refuses once restored.
   The step now records that refusal and relies on the helper-container count
   and the catch-up.

In all three failed runs the checkpoint's own `finish` (or the fixture's
termination) removed the destination, the daemon and the setup clients, and
no container remained afterwards. The later pass does not change what those
runs observed. This record is not GitHub Actions evidence; a `/ci run` of the
row on the tested SHA remains separate.

## Limits

- One profile: aes256k keys and service cipher, secure mode, IPv4, container
  runtime, single BlueStore OSD per cluster, one daemon, snapshot mode in
  image scope. Journal mode, pool scope, namespaces, several daemons, IPv6,
  CRC mode, host runtime and separate cluster networks are not exercised.
- The fault is outbound loss from the source OSD to the daemon. The daemon's
  path to the source MONs, the destination side, inbound or connection-setup
  faults, delay and partial loss are not exercised. Only one fault and one
  restore are observed; this is not an endurance run.
- The daemon-to-OSD frames themselves are not captured (secure sessions are
  encrypted, D2D-19); attribution relies on native session state, netem
  counters and mirror status.
- The Go client is only the control session to the source OSD. This does not
  establish a Go RBD, mirroring or journal implementation.
