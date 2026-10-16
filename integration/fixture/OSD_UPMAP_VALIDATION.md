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

## CI authentication window correction

Run `38888886288`, job `116933272778` at source `648140e` failed in the
CRC upmap row after items passed and before a full-transition Go request
was submitted. The original log and downloaded diagnostics are retained at
`/private/tmp/ceph-ci-fix-20261016-job-116933272778-original.log` and
`/private/tmp/ceph-ci-upmap-648140e-first/ceph-diagnostics`.
MON logs report an unavailable AUTH service secret and rejection of the
old ticket for the same Go GlobalID 4752. Cleanup still restored overrides
and removed both owned objects. The truncated logs do not establish a
product renewal defect or the exact sequence that lost the reclaim proof.
The secure row at `1b84aa1`, run `38890511851`, job `116938585002`,
independently showed the same MON rejection for Go GlobalID 4755 and
AUTH secret ID 6; its original diagnostics are retained at
`/private/tmp/ceph-ci-upmap-1b84-secure-first`. It also completed cleanup.

The initial correction at `2696f06` gave the exact three-BlueStore
`replicated-pg-v8` upmap selector a 120-second AUTH ticket while retaining
12-second service tickets. This
isolates long native placement controls from aggressive AUTH reclaim-proof
expiry and continues service renewal. Other selectors and dedicated
6.5-second AUTH tests retain their existing contracts. No product
authentication, rejection, retry, GlobalID, or mutation behavior changes.
Each native control now records public AUTH/OSD deadlines, readiness,
GlobalID and rejection before and after execution, without ticket secrets.
Every successful Go reply must retain the initial GlobalID.

The corrected source passed root and fixture Go tests/vet with CGO disabled.
Fresh Ceph 20.2.4 arm64 executions, using the image above, passed without
retry: CRC/AES/IPv4 host in 36.20 seconds and secure/AES256K/IPv6 container
in 35.79 seconds. Each retained eight native controls and ten Go replies,
restored both overrides, removed both owned heads and explicitly retained
the empty owned pool for fixture teardown. The receipt deadlines advanced
during native controls, so longer AUTH validity did not remove renewal
from these runs. No initial GlobalID changed and no rejection was hidden.

The exact tested source SHA-256 values are
`f01e97f5049dbc75647c2649dd4de43ef602a2966db672df6df100c3422b72b4`
(`provider.go`) and
`73efc4192d1ad3ecad4dc54043073764297e3419cf8c8a7e4d0b71eeba796c85`
(`../osd_upmap_test.go`), based on `04b58b4`; only validation prose was
edited afterward. Fresh artifacts are retained separately at
`/private/tmp/ceph-ci-upmap-auth-crc-20261016` and
`/private/tmp/ceph-ci-upmap-auth-secure-20261016`, with result SHA-256
`6d08842cb92bb60a60aaf1a52ce0ddeb868b48dbf1c6cc853e503ac190c3b4a8`
and `a0ff3ff7a5ea563edb4151bdf3e27076b07f2eb49549c1a29a81a54c28646bef`.
The read-only inventory
`/private/tmp/ceph-ci-upmap-auth-cleanup-audit-20261016.json` confirms all
20 owned containers and both networks absent. These local results do not
establish full GitHub CI success.

### Service-window follow-up

The first exact-`2696f06` CI attempt, run `38894829102`, passed both Windows
unit jobs, Windows fixture tools and CRC upmap job `116952687062` (55.26 s).
Secure upmap job `116952687149` failed with a different error: items passed,
then `OpenOSD` reached the caller's 20-second deadline before any full-phase
request was submitted. This was not an observed `-13` rejection.

The new public timeline shows valid AUTH and unchanged GlobalID 4737
throughout. The full control ended at 11:40:28.782724736 UTC with OSD ticket
expiry 11:40:28.530546077; that ticket was still unchanged at 11:40:48.784.
It was renewed by the final cleanup observation. Every observation reported
MON ready and no rejection. The artifact lacks per-attempt setup outcomes,
so it does not prove whether a slow/failed candidate or scheduling caused
the delayed publication. The client correctly refused the expired service
proof and ended before submitting a full-phase mutation. Native cleanup
restored overrides and removed both heads.

The original failure is retained separately at
`/private/tmp/ceph-ci-final2696-job-116952687149-first-failure.log` and
`/private/tmp/ceph-ci-final2696-secure-upmap-12042919864-first-failure/ceph-diagnostics`.
The downloaded artifact ZIP SHA-256 is
`ff9e32a3ed3c08d57673413130006d4a2d573a55df71997558296426a305d680`;
the original Go result SHA-256 is
`1881e3c11d9caf8b846ecd1fa4e01132a5e855116846f1a42ecf3b5d0687b662`.

The final fixture policy therefore gives that exact placement-only selector
both AUTH and service TTLs of 120 seconds, as other long-control profiles
already use. The short-ticket opt-in is excluded from this selector override
and retains AUTH 6.5 seconds/service 12 seconds. All other ordinary selectors
retain 12-second service tickets. This separates placement verification from
aggressive renewal timing; it is not a product renewal fix or a renewal
interoperability claim. Caller deadlines, native bounds, error handling,
request counts, identity checks and mutation replay policy are unchanged.

The 120/120 policy passed the complete fixture Go tests/vet and fresh,
first-attempt native runs: CRC/AES/IPv4 host 32.91 seconds and
secure/AES256K/IPv6 container 35.09 seconds. Each retained eight controls and
ten successful replies under the original GlobalID, completed items/full
transitions and exact restoration, submitted each APPEND once, and removed
both owned heads with the empty pool retained for fixture teardown. The
public ticket windows covered these executions; these runs do not exercise
renewal. The tested `provider.go` SHA-256 is
`ff479de5c640edc6c837743b77257df62889bcd71ff19f51a43f8b3c54fbf5aa`;
the integration test SHA above remains unchanged.

Fresh artifacts are
`/private/tmp/ceph-ci-upmap-window-crc-20261016` and
`/private/tmp/ceph-ci-upmap-window-secure-20261016`, with result SHA-256
`01bb0e2d620660aeb1f1f3a3910be1da6d567789e05a1c13ae8e1867cb724f8f`
and `1121da8786876c4dbffbce7f9b59949f2ce7dd7cee7c9f6c259713b6f5750145`.
The independent inventory
`/private/tmp/ceph-ci-upmap-window-cleanup-audit-20261016.json` confirms
all 20 owned containers and both networks absent. Final-head GitHub results
remain separate from this local evidence.
