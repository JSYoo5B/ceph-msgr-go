# ENFORCE_SNAPC validation — 2026-10-18

Five Linux runs on the final v2 source passed
`TestCephOSDEnforceSnapContextIntegration` and all three selected leaves each
(**15 leaves, no skips**). They verify Go `OSDRequest.EnforceSnapContext` on
explicitly targeted replicated WRITEFULL requests with one owned pool snapshot,
using legacy-v6, v8/reply-v6, v8/reply-v8 and v9/reply-v8 profiles. The independently audited receipt is
[OSD_ENFORCE_SNAP_CONTEXT_LINUX_20261018.json](OSD_ENFORCE_SNAP_CONTEXT_LINUX_20261018.json).
Two earlier successful legacy runs are retained separately in the same receipt.
They precede a test-only correction: reply PG seed is the raw object hash for
legacy-v6 and the caller-supplied actual PG seed for modern profiles. The final
five runs and six local checks all use that corrected v2 source. Product code
did not change between the first and final source snapshots.

## Server contract and independent comparison

Pinned Ceph is Tentacle 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
`src/osd/PrimaryLogPG.cc:4213–4229` selects the pool's SnapContext for a
writing request in pool-snapshot mode when ENFORCE_SNAPC is absent. With
ENFORCE_SNAPC, it retains the caller's sequence and filters the caller's snap
list. ORDERSNAP rejects a sequence below the object's snapset sequence with
`-EOLDSNAPC` (`-85`) before selecting a new object version or mtime.

The librados C API does **not** expose an ENFORCE_SNAPC operation flag. This
acceptance therefore does not claim a directly equivalent forced-native
operation. Its independent native evidence consists of:

- One ordinary native WRITEFULL control with C API ORDERSNAP `512`, without
  ENFORCE_SNAPC, preceded by a persisted once-only intent.
- Selected-snapshot and head STAT2/READ observations, including each object's
  own user version and nanosecond mtime.
- Native `rados listsnaps --format json`, preserving both parsed clone data
  and the exact stdout bytes.

The forced request's `-85` is an actual Go-to-OSD server result, interpreted
against the pinned source branch. Native observations establish the resulting
object state independently. Native connections retain their own exact GIDs.
No total native C API call count is inferred: each run preserves **seven
native observation receipts** and **six Go requests**; a native observation
itself performs several calls.

## Final v2 live configurations

All five runs used Linux amd64 and the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

| Configuration | Request / reply profile | Key / service | Network / runtime | OSD shape | UTC start → end | Test | Containers absent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| crc-host | legacy-v6 / v6 | AES / AES | IPv4 host | 3 BlueStore, replicas 3/min_size 2 | 08:04:02.301953 → 08:05:20.691828 | PASS 10.09s | 10 |
| secure-ipv6 | legacy-v6 / v6 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:04:02.728762 → 08:05:11.406755 | PASS 9.25s | 8 |
| v8-crc-host | replicated-pg-v8 / v6 | AES / AES | IPv4 host | 3 BlueStore, replicas 3/min_size 2 | 08:04:01.270618 → 08:05:19.521929 | PASS 9.08s | 10 |
| v9-secure-ipv6 | replicated-pg-request-v9-reply-v8 / v8 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:04:03.351570 → 08:05:07.280170 | PASS 8.35s | 8 |
| reply-v8-secure-ipv6 | replicated-pg-reply-v8 / v8 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:06:00.932311 → 08:07:01.633525 | PASS 10.23s | 8 |

Each row passed the same three leaves, with six Go requests and seven native
observation receipts. In total, the final runs preserve 30 Go replies and 35
native receipts; they do not establish a total C API call count.

The fixture prepares `ceph-msgr-owned-pool-1` separately from its ordinary
pool and uses only `enforce-snap-control`, `enforce-snap-enforced` and the
single named pool snapshot `enforce-snap-S`. Pool ID `3` and snapshot ID `1`
were observed in each independent disposable cluster; those IDs are not
assumed globally unique. Each request records the caller-supplied pool, raw
object hash, primary address, acting set and OSDMap epoch. The test selects
its ordinary OSD fixture and is included by the existing broad OSD integration
selector; no separate CI matrix row is added.

## Observed sequence

Native seeds establish the same binary A bytes on the two heads. A public
MON `PoolOp(PoolSnapshotCreate)` creates S. Empty SnapContext means sequence
zero and no snapshot IDs. Every subject WRITEFULL sets ORDERSNAP.

| Go request | Caller context / ENFORCE_SNAPC | Aggregate / per-op | Independent state |
| --- | --- | --- | --- |
| Control B, TID 1 | Empty / false | 0 / 0 | Head B; snapshot S is A |
| Enforced-head B, TID 2 | Empty / false | 0 / 0 | Head B; snapshot S is A |
| Control C, TID 3 | Empty / false | 0 / 0 | Head C; S remains A |
| Enforced-head C, TID 4 | Empty / true | -85 / 0 | Head B and S unchanged |
| Enforced-head C, TID 5 | `{Seq:S, Snaps:[S]}` / true | 0 / 0 | Head C; S remains A |
| Readonly forbidden write, separate GID/TID 1 | `{Seq:S, Snaps:[S]}` / true | -1 / 0 | Head C and S unchanged |

Between the second and third Go requests, the ordinary native control writes
C successfully to the control head with ORDERSNAP `512`. Its own version
advances before the separate Go ordinary write advances it again. The two
objects' absolute versions are never compared for equality.

In all five runs, successful Go writes exposed ONDISK completion and their user
versions matched the next independent observation of that same object. The
observed version progressions differ by pool shape and placement:

| Profiles | Control B → native C → Go C | Other head B → explicit current C | Original clone versions, control / other |
| --- | --- | --- | --- |
| Two legacy-v6 rows | 8 → 11 → 12 | 10 → 13 | 4 / 6 |
| Three modern rows | 4 → 5 → 6 | 4 → 5 | 2 / 2 |

These values are observations, not API constants or a cross-object version
equality contract. Each clone retained its own seed bytes, version, mtime
`1700000500.123456789`, attributes and snapshot association. The B/C write
mtimes were checked to nanosecond precision. Both refusals preserved the head's
version, bytes and mtime plus the selected snapshot and entire parsed clone
listing.

The negative aggregate codes are retained separately from the zero per-op
results: these refusals occurred before the WRITEFULL operation executed.
No retry, explicit resubmission or unknown-outcome fault is introduced in
this live test.

## Source and raw evidence audit

The final runs used base
`source-1` plus **19 overlay files**.
The audit matched every selected overlay file and all **707 Go files** against
the preserved, executed source directory, rather than assuming the subsequently
edited working tree was identical. The sorted Go inventory SHA256 is
`93ca0d0b50cb7c9110061a6542c66550fe060148a762aa8ad57f6c382088c407`.
The JSON preserves every selected source hash and the source-manifest hash.

The reply seed is checked against the raw hash only on legacy-v6. On modern
profiles, it is checked against `request.Placement.PG.Seed` and the native
actual PG seed; the separate request object hash is still checked against the
native raw hash. GID and TID are preserved public submission metadata. The raw
Messenger header was not retained, so their wire encoding and the header's
version/compatibility fields are not independently decoded here; reply-tail
policy follows the declared negotiated profile.

Each run has ten fetched public files, individually hashed with byte counts.
The intermediate native receipts embedded in the Go evidence are typed
snapshots, accompanied by producer-recorded hashes of their original JSON
bytes. Those original intermediate JSON byte streams are not available for
independent hash recomputation. The final standalone native receipt is
independently hashed, matches its producer-recorded hash and byte count, and
is compared against its embedded observation. Native
LISTSNAPS stdout bytes are preserved independently in every receipt.

## Offline wire proof and local checks

The separate [12-vector native proof](../../internal/osd/testdata/enforce-snap-context-native-provenance.json)
and its [README](../../internal/osd/testdata/enforce-snap-context-native-README.md)
cover v6/v8/v9 × empty/nonempty SnapContext × normal/enforced WRITEFULL.
Fresh canonical native message encoding, a separate decoder and offline
header projection were verified. The audit checks segment hashes and that
normal/enforced pairs retain identical feature masks and decoded fields except
ENFORCE_SNAPC; each front pair differs by exactly one bit. This remains offline
codec evidence, distinct from the live profiles above; its header projection
does not exercise live ProtocolV2 execution.

All six local Linux checks on the final v2 source snapshot exited zero:

| Check | Command |
| --- | --- |
| Root, CGO disabled | `go test ./...` |
| Race | `go test -race ./integration ./cephmsgr` |
| Root vet | `go vet ./...` |
| Nested fixture | `go test -mod=readonly ./...` |
| Nested fixture vet | `go vet -mod=readonly ./...` |
| Python | `python3 -m unittest discover -s tools -p 'test_*.py'` — 449 tests |

Their receipts and all six logs are hashed in the JSON. The root unit run's
opt-in integration skips are distinct from the five actual native runs above.
These local checks do not establish completion of the GitHub matrix.

## Cleanup and limits

The public MON snapshot-removal result is zero. The native pool cleanup
receipt confirms deletion of owned pool 1 and that unused slot 2 was absent.
Independent resource audits confirm all 44 distinct owned containers and five
networks absent after the final five runs. This records cleanup of the disposable
resources, not a claim about other host resources.

Live coverage is WRITEFULL on head replicated objects with one pool snapshot,
in the five configurations listed. Other writing opcodes, EC, extra snapshot
combinations, unlisted profiles and live uncertain-outcome faults/resubmission
remain outside this record. Unknown-outcome/no-replay behavior has separate
synthetic coverage. ONDISK is the observed Ceph completion flag; this test adds
no power-loss durability guarantee, including for MemStore. No whole-CI success
is claimed.

## Preserved earlier successful legacy runs