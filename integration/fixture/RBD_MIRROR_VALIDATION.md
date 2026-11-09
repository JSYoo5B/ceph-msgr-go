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
`ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1@sha256:63d17894d30de34fbf6c956b38e2576002aed58748b779b04c5e4632fc9dfdc3`,
the provider's `fault.NetemHelperImage` pinned to its digest.

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
   and versions, the global image ID, both image IDs, the daemon name, Cephx
   user, instance ID and its IPv4 address on the source network.
2. The test opens its own Go session to the same source OSD and reads the
   fixture's native oracle object (stat and read), comparing the native bytes.
3. **cut**: `fault.ApplyNetworkProfile` on the source OSD's public endpoint with
   the daemon's source-network address as `Peer` and 100% loss: the OSD drops
   every packet it sends to the daemon and only those. The source client then
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
   read, confirm that no container of the helper image remains, then wait (at most 4 minutes) for the
   destination export to equal the patched image and for the native status to
   report a newer snapshot in sync whose `last_snapshot_bytes` is exactly the
   1 MiB patch.
6. **finish**: inventory the containers attached to the destination network and
   the mirror's daemons, terminate the mirror and then the destination, and
   confirm through the Docker engine that each inventoried container and the
   destination network no longer exist. The source pool, its mirroring policy
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
  address under the daemon's source-side peer identity; the test requires
  that one OSD session from that address has a GlobalID of those MON
  sessions;
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

The accepted run passed `TestCephRBDMirrorPathRecoveryIntegration` in 204 s
(258 s for the whole fixture run); the gate verdict was pass with no problems.
Its receipt records:

- source: the fixture's cluster, 3 MON and 2 MGR, `osd.0`, pool size 1 with
  one PG; destination: 1 MON, 1 MGR and 1 OSD, a different FSID and network;
  both report `20.2.4`;
- the global image ID, distinct source and destination image IDs, daemon `a`
  with its own Cephx user, attached to two networks;
- initial data at the destination about 2 s after enabling mirroring, with the
  first snapshot in sync and 4 MiB copied;
- before the fault, one READY source OSD session from the daemon's address,
  whose GlobalID equals that of the source MON session named
  `client.rbd-mirror-peer` from the same address;
- during the cut, 20 netem drops and 5 samples over 33 s, all `up+replaying`
  at the unchanged snapshot timestamps; destination digest of the seed, source
  digest of the patched image; the Go session's stat and read of the native
  oracle object on the same OSD succeeded;
- after the restore, the lease refused further status reads ("the network
  delay was restored"), no helper container remained, the destination reached
  the patched digest 46 s later and the native status reported a newer
  snapshot in sync with `last_snapshot_bytes` 1048576;
- cleanup: 6 owned containers and the destination network independently
  confirmed absent. After the run, no Testcontainers container or extra
  network remained on the engine.

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
