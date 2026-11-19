# Service-induced paths

This file holds the `SVC` rows of the [native path coverage map](README.md).
The index states the classes, how to read the columns and the selection
visibility terms.

## Rows

| ID | Path | Class | Setup and control | Independent observation | Fault and cleanup | Evidence | Visibility |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SVC-1 | RGW-induced RADOS objects, bucket index OMAP and garbage collection | `primitive` | v0.3.0 `rgw.Run`; separate [small-object checkpoint](../RGW_NATIVE_VALIDATION.md), with S3 as input only | Gateway GlobalID/TID-attributed OSD history, native head bytes and actual index shard locally; manifest-ordered stripe reconstruction and GC queue locally for the multi-stripe checkpoint | Owned gateway restart and small-object deletion locally; multi-stripe deletion with one explicit GC process locally | `TestRGWNativeCheckpointIntegration`; `TestRGWNativeMultiStripeIntegration`; `ctg@v0.4.0:TestRGWRadosReceipt` | Selected local native checkpoints, run by the development workflow with pull-request-recorded results and selectable as `rgw.aes256k.ip4.container.t-rgwcheckpoint` and `rgw.aes256k.ip4.container.t-rgwmultistripe` (`group:rgw`) through the trusted catalog; backend OSD and manager faults are row [SVC-rgw-backend-fault](SVC/SVC-rgw-backend-fault.md); issue #13 remains open |
| SVC-2 | rbd-mirror daemon in steady state | `released` | v0.5.0 `rbd/mirror.Run` between the fixture's single-BlueStore cluster (source) and an owned one-OSD destination cluster, snapshot mode, image scope, one daemon | Native oracle: destination export digest of the seeded image, the destination's snapshot progress and both clusters' FSIDs and image identities | Owned destination, daemon and setup clients terminated and independently confirmed absent | `rbd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmirrorpathrecovery`, `TestCephRBDMirrorPathRecoveryIntegration`, [RBD_MIRROR_VALIDATION.md](../RBD_MIRROR_VALIDATION.md) | one selected aes256k/secure/IPv4 container profile |
| SVC-3 | Source OSD to rbd-mirror daemon with one path cut | `released` | v0.5.0 `fault.ApplyNetworkProfile` on the source OSD with the daemon's source-network address as `Peer` and 100% loss | Native oracle: unchanged snapshot progress and destination digest during the cut, netem drop counters, the source OSD's `messenger dump` session from the daemon's address whose GlobalID a source MON session of the source peer user `client.rbd-mirror-peer` from that address carries; the Go session to the same OSD keeps reading the native oracle object | One-direction loss and restore; catch-up of a distinguishable 1 MiB patch with its native byte count | `rbd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmirrorpathrecovery`, `TestCephRBDMirrorPathRecoveryIntegration`, [RBD_MIRROR_VALIDATION.md](../RBD_MIRROR_VALIDATION.md); `ctg@v0.4.0:TestRBDMirrorPeerPathCut` | one selected aes256k/secure/IPv4 container profile |
| SVC-4 | cephfs-mirror daemon in steady state | `primitive` | v0.3.0 `RunMirror` in the `cephfs` package | None locally | None locally | none locally | no local or upstream steady-state receipt is cited here; CephFS mirroring needs its own setup and oracle |
| SVC-5 | Source OSD to cephfs-mirror daemon with one path cut | `primitive` | v0.4.0 `fault.ApplyNetworkProfile` with `Peer` expresses it | Mirror snapshot progress and netem counters in the upstream receipt | One-direction loss and restore | `ctg@v0.4.0:TestCephFSMirrorPeerPathCut` | upstream receipt only |

## Updates

Updated on 2026-11-20 for issue #13. SVC-1 points to the new row
SVC-rgw-backend-fault for backend OSD and manager faults instead of stating
that none exists. Its class, recipe and evidence are unchanged.

Updated on 2026-11-20 for issue #13. SVC-1 names the two catalog rows that
route the trusted native workflow to its checkpoints. The class stays
`primitive`: the rows select the same checkpoints and add no backend fault.

Updated on 2026-11-10 for issue #124. The former SVC-2 and SVC-3 rows named
the rbd-mirror and cephfs-mirror daemons together. They are now split by
daemon: SVC-2 and SVC-3 name the rbd-mirror daemon and are `released` with
the local catalog recipe of [RBD_MIRROR_VALIDATION.md](../RBD_MIRROR_VALIDATION.md);
SVC-4 and SVC-5 keep the cephfs-mirror paths `primitive`. No other row
changed.
