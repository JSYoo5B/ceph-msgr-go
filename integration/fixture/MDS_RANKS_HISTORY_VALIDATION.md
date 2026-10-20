# MDS replay acceptance: bounded native history

On 2026-10-18, the three-rank standby-replay acceptance passed its first
corrected execution on each of the two configurations below. This change
reserves native operation-history capacity for the fixture's assertions. It
does not change the Go product's forwarding, retirement, retry or replay
policy. The [independent receipt](MDS_RANKS_HISTORY_LINUX_20261018.json)
records the selected source hashes, raw evidence hashes, native request
identities, per-rank observations and cleanup checks.

Reference: Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; native source references below
are LGPL-2.1. The fixture's CLI, native LibCephFS observer and disposable
containers are development dependencies only. The existing
[standby-replay acceptance](MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md) defines the
protocol scope; this record adds its history-observation correction.

## Preserved first CI failure

CI run `39105613739`, job `117610680671`, at source
`source-1` failed the requirement that the
native rank-1 history contain the original forwarded MKDIR with TID 6.
The Go CREATE and MKDIR had already reached rank 2 with the original TIDs
5 and 6 and successful safe results; the Go trace and CREATE extra data
retained their returned inode identities. Observed native forward counters
and explicit same-TID hops also succeeded. The failing run stopped before
the final independent native listing, so it does not prove that listing. The
failure was the subsequent historic-op observation, not evidence that the
mutation failed to forward or execute.

Pinned `common/options/mds.yaml.in:1286-1302` defaults to history size 20
and duration 600 seconds. `common/TrackedOp.cc:101-117` removes expired
entries and, at capacity, removes the shortest-duration operation first.
It does not promise to retain the newest 20 requests. Short forward-only
operations therefore lack the observation guarantee the acceptance had
assumed. `mds/MDCache.cc:9870-9898` cleans up forwarded requests into the
tracked history.

## Fixture correction

Only the standby-replay fixture reserves `mds_op_history_size=256`, before
creating either acceptance client. It reads the baseline on each of the
three owned active MDS processes and the owned standby, saves an intent
before each runtime-only set, and requires native config readback `256`.
A failed or uncertain selection stays recorded and cannot be selected
again. Duration remains 600 seconds.

The standby's runtime configuration follows its process when it becomes
rank 0's replay follower and later active rank 0. Cleanup attributes each
setting by exact daemon name and GID, rather than the rank/incarnation
that may change during promotion. Surviving processes are restored to the
saved baseline with readback. An old GID absent from the stable replacement
topology is recorded as discarded; its configuration is not applied to the
respawned process. No cluster-wide config setting is introduced.

Each `ops` observation now records native size, duration and **unfiltered**
operation count before filtering by the owned client GlobalID. A missing,
wrong-sized or saturated history fails acceptance. This verifies capacity
at the observation time; it does not promise arbitrary history retention.
The original exact GlobalID/TID, operation/path, RETRY and no-REPLAY checks
remain. Requests and partial per-rank lists are saved before a later
assertion can fail. Unit tests cover intent-before-set, uncertain results,
wrong readback, repeat-selection refusal, foreign daemons, unsafe baseline,
saturation and same-process promotion versus a new GID.

## Frozen source and local checks

Both native executions and the independent six-check run used base
`source-2` plus exactly these selected Go
files. Their working-tree bytes were independently matched to the frozen
source manifest before generating the receipt.

| Selected file | SHA256 |
| --- | --- |
| `integration/fixture/mds_cephfs_ranks.go` | `8554c0b94d7c1d6d2164e2be5e2547ba39a31ee59bc0e4ad1ca6f54297f81ba7` |
| `integration/fixture/mds_cephfs_ranks_replay_test.go` | `08982629561ded462e260bd06ec50451b0897da2568b0a1034162353fa081e19` |
| `integration/mds_cephfs_ranks_replay_test.go` | `c9d126ab29c247d78b25e39b9395d51d6b0b60f75b000943a3c95afcfabee055` |
| `integration/mds_cephfs_ranks_test.go` | `f679d77302d554dc22288171a53745429fb734d229cebd6e08bde753083a5019` |

## Native observations

Both runs used Linux amd64, provider v0.3.0, three active MDS ranks and one
owned standby. The image was
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The selector was `^TestCephMDSCephFSRanksReplayIntegration$`, using the
existing ranks and standby-replay opt-ins. No new profile was added.

| Configuration | UTC runner window | Go test | Result |
| --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 07:12:15.610–07:15:57.120 | 98.08 s | 6/6 leaves, exit 0 |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 07:12:16.517–07:15:56.374 | 97.90 s | 6/6 leaves, exit 0 |

Independent inspection of both runs established:

- The original two requests took rank 0→1→2 under TIDs 5 and 6. Source
  descriptions showed `RETRY=3`; rank-1 and rank-2 descriptions showed no
  RETRY field, which the pinned native printer emits only for a nonzero
  parsed retry. The original forward descriptions had no REPLAY marker.
  All six rank/TID entries matched the owned GlobalID and controlled path.
- Native history size/duration was `256/600` on all three ranks, with
  unfiltered counts `[10,30,2]`. The rank-2 native operations completed
  successfully after journal commit. Independent listing matched the Go
  inode numbers for `forwarded-file` and `forwarded-dir`.
- Native forward counter deltas `[2,2,0]` matched the four Go-observed hops.
  Both logical requests reached successful safe replies. The remaining
  leaves passed frozen-export mutation, standby replay takeover,
  reconnect and explicit resubmission under the original TIDs.
- The client GlobalID remained 5457 for CRC and 5427 for secure across all
  three initial sessions and the replacement rank-0 session. The old
  unsafe requests remained outcome-unknown after the peer reset. Their
  later explicit resubmissions had native `RETRY=1 REPLAY
  QUEUED_FOR_REPLAY` descriptions and successful safe, traceless replies.
  This does not authorize automatic uncertain-request replay.
- Reconnect transferred nine caps and one realm. Capability agents had no
  pending work or recorded errors at the end; native cap counts changed
  from `[9,6,4]` to `[0,0,0]`. The Go client opened no OSD connection.

Cleanup contained five runtime records per run. All four history records
had native readback `256` from baseline `20`. Three surviving processes,
including the promoted standby, restored/read back `20`. The failed old
rank-0 process's history and journal-pause records were discarded with its
old GID, for totals **three restored and two discarded**. Cleanup verified
stable topology, restored pins, absent owned namespace and sessions,
attributable blocklist removal, both client identities, then filesystem
and pool removal. Each retained absence audit reported all 12 owned
containers and its network absent: 24 containers and two networks total.
The offline audit rechecked these receipts; it did not issue new Docker
queries.

## Separate combined-check failure

```text
TestMDSClosePushLossBeforeAdmissionDoesNotResend
mds_close_push_admission_test.go:150:
fill ordered writer queue context deadline exceeded
```

## Final combined local checks

| Additional selected test file | SHA256 |
| --- | --- |
| `cephmsgr/mds_close_push_admission_test.go` | `31f947835764ff1adf8c3e7a407d4d909b3f8e66eec7a21c3476cca5eacc20ea` |
| `cephmsgr/mds_peer_test.go` | `7cd5e480dd7f5199779555b9fee67a8f78fa3e6c13d21d5fdb49626d0b515b22` |

The seven steps were `go test -race ./cephmsgr -run '^TestMDSClosePush'
-count=100` (four tests, 100 iterations), root CGO-disabled tests, integration
and cephmsgr race tests, root vet, nested fixture tests and vet, and 429
Python unit tests. Every recorded exit code was zero. The final six-file
source manifest and all seven logs plus the execution receipt are hashed
separately in the machine record. The two ACK test changes were committed
separately as `source-5`; the native evidence still belongs to its original
four-file source, without retrospective relabeling.

The original 72-success/1-failure CI result remains a failure. The final
combined local pass is not a new complete CI-matrix result. These results
do not establish failover during a forward, rank-1/2 takeover, arbitrary
history delivery, or broader storage and platform compatibility.
