# Disposable native MON filesystem map observation

`tools/mds_maps_oracle.py` is development-only and has closed `start`, `observe`
and `finish` actions. Each takes the fixture root and output directory; input
`mds-maps-input.json` must be `{}`. It never starts an MDS, mounts a filesystem,
opens an MDS session or performs CephFS I/O. The admitted fixture is one OSD with
`replicated-pg-v8`, a matching pinned Tentacle daemon version, a recorded cluster
FSID and valid owned container identities. That fixture profile is a test recipe,
not an MDS wire-protocol requirement. No provider repository is accessed.

The recipe follows pinned Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, especially
`mon/FSCommands.cc` (`FsNewHandler`, `RemoveFilesystemHandler`),
`mon/MDSMonitor.cc` (`fs dump`, `fs get`, map subscriptions), and
`mds/FSMap.cc` (`create_filesystem`, `commit_filesystem`). These LGPL-2.1 sources
are references; no implementation is copied.

`start` fails closed if a filesystem or standby already exists. It journals one
UUID-derived filesystem name and metadata/data pool names before any mutation.
It creates only those two replicated pools (16 metadata PGs, one data PG,
replication size one, autoscaling off), observes and records each native pool ID,
then issues one `fs new` without force, overlay, recovery or caller-selected
FSCID. The native FSID, ownership journal and confirmed identity pairs are checked
before every mutation. There is no mutation retry.

A successful result preserves the full independent `ceph fs dump` and `ceph fs
get` JSON. It also exposes `fsid`, `fs_name`, `fscid`, `fsmap_epoch`,
`default_fscid`, `mdsmap_epoch`, `metadata_pool` and `data_pools`. The new MDSMap
has an enabled filesystem, named pools, no daemon entries and no up/in/failed/
stopped/damaged ranks. `observe` makes at most five attempts to obtain consistent
FSMap and per-filesystem map JSON; only reads are repeated. FSMap and per-FS epochs
remain distinct values.

The pinned CLI has no `mds getmap` or `fs getmap` command producing these exact
subscription payloads. Native JSON fields are the independent live comparison;
this oracle does not claim byte-for-byte native CLI generation of FSMapUser or
MMDSMap blobs. Separate fresh C++ native vectors verify those binary codecs, and
live subscription tests retain their received raw fronts/maps.

`finish` removes only a filesystem whose recorded FSCID, UUID name and pool IDs
still match, with no MDS or rank state. It then deletes the two confirmed owned
pools only if the native `mon_allow_pool_delete` policy is already true and no
filesystem references them. It never changes that global policy. Otherwise,
`cleanup.retained_for_fixture_teardown` explicitly lists the pools; final removal
belongs to the owned disposable cluster teardown. A pending mutation is never
resent, and an unconfirmed native ID is never adopted for destructive cleanup.
Uncertain outcomes and retained names remain in the journal and result. If an
uncertain removal's resource is still visible, finish preserves that pending
marker and retains all remaining owned resources for teardown; it never sends
the removal again. Confirmed absence may be reconciled with reads before other
confirmed cleanup proceeds. Repeated finish calls return the identical cached
result, including unresolved outcomes and retention details, without native
commands.

Public evidence files are `mds-maps-owned.json`, `mds-maps-result.json`, and
`mds-maps-commands.json`, plus the input file. Commands preserve bounded stderr,
exit status, timeout state, output size and SHA-256 without recording credentials
or keyring contents. The exclusive control lock is closed before removal. A
successful finish returns the native final empty FSMap snapshot and is locally
idempotent. Mock tests verify scope, identity checks, uncertain outcomes and both
cleanup policies; they do not establish live daemon interoperability.

## Verified native CLI lifecycle smoke, 2026-10-15

The first actual CLI lifecycle run passed `TestCephMDSOracleSmoke` in 12.32s.
This was an isolated development snapshot based on commit `88f5dab`, using
single-OSD BlueStore, the `replicated-pg-v8` fixture, CRC, AES client/service keys,
host runtime and IPv4 on the pinned arm64 Tentacle image. The copied oracle SHA-256
was `54e4c9d223b09d1fc175a003d38601dc7e5e7f23fdc8ffcaaba7defa337b2ab2`;
`snapshot-proof.json` records this exact producer and its dependency hashes.
The working oracle was independently rehashed to confirm the same bytes.

The retained `start`, `observe` and `finish` results agree on cluster FSID
`bbcd1993-424e-4a1a-b226-f4f021d06467` and token
`b56dd4a6252a4987bf2a9d3185e3676d`. The owned filesystem
`ceph-msgr-m0-b56dd4a6252a4987bf2a9d3185e3676d` received FSCID 1, metadata pool
ID 3 and data pool ID 4. Native FSMap epochs progressed from 1 before creation,
to 2 with the new filesystem, to 3 after removal. The independent `fs get` and
`fs dump` observations agreed on MDSMap epoch 2, `enabled=true`, empty daemon/up
maps and empty in/failed/stopped/damaged rank sets. There was no MDS daemon.

All 52 recorded native commands completed with return code zero and no timeout.
The four mutation receipts are exactly the two UUID pool creations, `fs new`
and the confirmed filesystem's `fs rm`. Finish recorded no pending or unresolved
mutation, an empty final FSMap and default FSCID -1. Pool deletion was disabled;
no pool-delete command or global configuration change was sent. Both named pools
were explicitly retained for disposable fixture teardown. The run log then
records termination of all eight distinct containers in the ownership receipt,
including the OSD and the native oracle/test container. This is teardown evidence,
not evidence that the native MON accepted pool deletion.

Evidence is retained under `/private/tmp/ceph-mds-oracle-smoke-first/`:

| Evidence | SHA-256 |
| --- | --- |
| `snapshot-proof.json` | `cdd8745bb5d731a159639d875119e1a0b000d3aab1ca456b83f10f9403357c15` |
| `mds-maps-results.json` | `80f26bdbb36eb1e3c879f0bbfa3b9570b376a3f8da3be6b79ae13c350aa79db5` |
| `mds-maps-owned.json` | `4796b55c4ec3b19c111d6728cde349f5d02d0ede88c16ed7fbacb1fda5b5befe` |
| `mds-maps-commands.json` | `e08d540d5a4991a85c5629c9e2d6570a9125ecdb8d02d993484d1929879221e1` |
| `fixture-ownership.json` | `98dab5ae0d05ee5ed7fd97d81952318d30982376a9db7a2beffbfb37078609df` |
| `run.log` | `cbc46b22bc8c21e96affb5517e92aa55781f0aa5869fc32a9cffd865f91cab00` |

This run verifies only the development oracle's native CLI creation, observation,
identity checks and cleanup policy. It does not verify the public Go filesystem
map subscriptions, raw subscription payload equivalence to CLI JSON, MDS service
authentication, an MDS connection/session, a filesystem mount or CephFS I/O.
