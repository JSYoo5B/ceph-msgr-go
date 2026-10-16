# Explicit caller-routed upmap verification

Prepared 2026-10-16 against Ceph 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`.
This is a verification-only extension of the existing replicated-PG OSD
protocol profile. It adds no placement algorithm, automatic routing or retry,
inner OSDMap decoder, new Messenger feature bit, or typed management API.
The two actual daemon profiles below passed their first executions.

## Closed scope and observations

The existing three-BlueStore fixture and its original three-replica pool
remain unchanged. The recipe creates one UUID-owned, HASHPSPOOL replicated
pool with size/min_size 2, eight PGs, autoscaling off, and two fixed head
objects. Native pool compatibility must already permit upmap; the recipe
reads that setting and never raises it or changes global deletion policy.

The seven control actions accept only an empty JSON object:
`prepare`, `items`, `restore-items`, `full`, `restore-full`, `observe`, `finish`.
The items transition replaces a baseline secondary with the spare OSD while
preserving the primary. After restoration, the full transition changes both
membership and primary. Both transitions must restore exact baseline vectors.
The two objects may share a PG; the recipe accounts for that explicitly.

Each native observation retains a coherent map epoch, raw hash, actual PG,
primary address/nonce, up/acting vectors and active+clean PG query, along with
independent librados bytes, stat2 and own-object version. Go supplies those
observed targets explicitly and issues STAT/READ/APPEND without automatic
reselection. The append is submitted once; an unknown result is retained and
fails acceptance. Native readback must contain the sentinel exactly once and
match the returned version. Own-object version must increase, without assuming
an increment of exactly one. The original fixture object and pool policy are
checked throughout.

## Bounds, ownership and cleanup

Native children have eight-second deadlines within a 135-second action;
the fixture control has 150 seconds and the Go caller 160 seconds.
Results and command counts are bounded. A kernel file lock is automatically
released on process death; its inode is not unlinked. A scoped watchdog can
attempt the closed cleanup after an abandoned lease, preserving the original
failure instead of reporting a successful acceptance.

Mutation intents are journaled before execution. Observed pool name/ID or
policy replacement refuses further mutation. Native object observations bind
the ioctx to the frozen numeric pool ID and independently check that ID.
CLI name-based mutations have a preflight check, not an atomic remote lease;
the UUID scope assumes a single owner in the disposable fixture. Confirmed
or uncertain commands are never replayed through another action alias.
After the first action failure only cleanup is permitted.

Cleanup distinguishes override restoration, both object removals, baseline
preservation and retained resources. The owned pool is deliberately retained
for disposable fixture teardown even when deletion is enabled. Therefore
`finished=true` means the control finished, not that every resource was deleted.
Container/network teardown is verified separately from in-cluster cleanup.

## Verification and actual daemon results

The 18 new mock-native regressions passed independently, including a real
POSIX child SIGKILL followed by same-inode lock reacquisition. The complete
Python suite passed 407 tests. Root Go tests/vet and nested fixture tests/vet
passed; source-level controls are separate from live interoperability.

Both native runs used frozen source
`9c1bb40f69262a21580dfd8a17153d5e05849f22`, based on main `57895ba`.
The actual linked fixture provider was v0.3.0, checksum
`h1:LRXc5mVpqw+/xDRWsv+m8ouLzkVDtB385RoQZ/7ygOY=`. Both used Linux arm64
Ceph image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Native require_min_compat_client remained luminous in the retained dump.

| First execution | Actual configuration | Cases / time | Result |
| --- | --- | --- | --- |
| CRC | AES client/service keys, IPv4, host client, three BlueStore OSDs, request v8/reply v6 | items + full, 31.80 s | PASS, exit 0 |
| Secure | AES256K client/service keys, IPv6, container client, three BlueStore OSDs, request v8/reply v6 | items + full, 31.86 s | PASS, exit 0 |

Each run retained eight control receipts and ten Go request/reply records.
In both runs, items PG 3.5 changed [2,1] to [2,0] and back; full PG 3.3
changed [0,2] to [1,0] and back. Native coherent observation epochs were
20/22/24/26/28 (CRC) and 21/23/25/27/29 (secure). Both owned heads retained
the single submitted append, native own version matched its Go reply, and
original pool policy/object state remained unchanged. Successful mutations
reported ONDISK; hardware power loss and mutation failover during upmap were
not tested. These two runs used different PGs; same-PG handling has model
coverage, not a native same-PG execution claim.

Both recipes restored the overrides and removed two objects with no cleanup
errors. One empty owned pool per run was retained for fixture teardown;
`pool_removed=false` remains explicit. A separate read-only Docker inventory
confirmed all 20 owned containers and both fixture networks absent:
`/private/tmp/upmap-owned-cleanup-audit-20261016.json`.

Original artifacts and adjacent stdout logs:

- `/private/tmp/upmap-crc-aes-ipv4-3blue-v8-20261016-first`
- `/private/tmp/upmap-secure-aes256k-ipv6-3blue-v8-20261016-first`

Raw results SHA-256, respectively:
`30d522d197d514b5d9fd89e020e56179bf890f9bc4da36bec9e55f4034b76a01` and
`6749921a7c46b19e329ffbba4a54218ff3d0b1662db42de64227eeba4ecdd8ff`.
An independent audit of both original runs is retained at
`/private/tmp/upmap-independent-audit-20261016.json`; it independently decoded
raw replies and checked native observations, command intents and cleanup.
No historical proof was rewritten. Live native worker-death/watchdog recovery,
reply-v8 upmap acceptance, EC, optimal CRUSH, automatic routing, and complete
remote CI success are not claimed.

To reproduce either row, select `CEPH_MSGR_TEST_OSD=1`,
`CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8`,
`CEPH_MSGR_TEST_OSD_REPLICAS=3`, `CEPH_MSGR_TEST_OSD_STORE=bluestore`,
`CEPH_MSGR_TEST_RUN=^TestCephOSDUpmapIntegration$`, a unique diagnostics
directory, and that row's key/service cipher, connection mode, IP family
and runtime before running `sh integration/run.sh` from the project root.
