# Explicit FLUSHSNAP resubmission after an MDS restart

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This extends the earlier
[first-send recovery acceptance](MDS_CEPHFS_CAPSNAP_RECOVERY_VALIDATION.md).
The later [completed-flush acceptance](MDS_CEPHFS_CAPSNAP_DEDUPE_VALIDATION.md)
adds independent durable-completion, dropped-ACK and deduplication evidence;
the results and limits below describe this earlier checkpoint.
The product still exposes raw cap messages and events; recovery selection,
snapshot generations and writeback policy belong to the test consumer.

## Mixed recovery recipe

The existing exclusive DATA profile keeps its two snapshots and A/B/C
images. Both A/B old-context OSD writebacks complete with ONDISK before
either metadata message is admitted. Their original TIDs, oldest TID,
follows, size, mtime and expected ACKs are retained.

The test temporarily changes only its owned MDS incarnation's
`debug_mds`, `debug_ms` and `mds_log_pause`. Baselines and mutation intents
are journaled before changes; admin-socket readback confirms each setting.
Debugging and a log mark precede the journal pause. Generation A is then
admitted through the old session; generation B remains unsent.

Local `SendCaps` success is queue admission. Server arrival is separate:
the fixture pairs an incoming caps log with the registered exact client
GlobalID and address nonce, its handler's exact operation/inode/TID/follows
on the same logger thread, and the same client's expected-snapshot branch.
The public log is bounded and starts after the mark. An unbound handler,
near-prefix identity, other nonce, unmatched thread or tuple does not count.
The branch establishes `_do_snap_update` entry, without establishing journal
completion or durability.

The fixture then fails its exact armed MDS GID once. The client must end
Lost with SESSION_RESET. Its old event consumer is not cancelled: `Next`
drains accepted events before returning the terminal error. Once the agent
stops, the test requires no errors or unapplied pushes, no matching A ACK,
the original pending ACK identity still present and B still unsent.

The caller reconnects to the same owned daemon's new GID, nonce and
incarnation, retaining the oldest pending cap-snap follows and latest root
and directory realms. Before event consumption starts, it installs the old
immutable generations and exact ACK expectations. ACTIVE/adoption requires
native proofs of unchanged FSID/FSCID/pools, old GID absence and baseline
runtime settings on the new incarnation; only then are the old overrides
marked discarded.

The consumer explicitly selects A's original TID for resubmission once in
the new session generation, then first-sends B. The complete selection is
validated before any send; failed or incomplete selections remain barriers.
Original admission evidence is preserved separately from the new attempt.
The ordinary send/drain path still never replays an admitted generation.
Neither path repeats object writeback.

Both new ACKs must match inode, follows, TID and dirty mask. A's ACK must be
after the new send call began; a fast ACK can precede queue-admission return,
so that return timestamp is not used as a causal lower bound. A synthetic
sender reproduces this order. Native A/B snapshot and C head bytes,
metadata and both object clone lists are checked as before, and the Go
object-request count must remain unchanged across recovery and metadata
sends. Cleanup verifies runtime, namespace, layout, identities, exact
old-address fencing, filesystem, pools and owned containers/network.

Source basis (LGPL-2.1 references, no native code copied): Client.cc
`send_flush_snap`/`kick_flushing_caps`/`early_kick_flushing_caps`;
Locker.cc `handle_client_caps`, `_do_snap_update` and
`C_Locker_FileUpdate_finish`; MDLog.cc `_submit_thread`;
MClientCaps.h `print`; DispatchQueue.cc `pre_dispatch`.

## Native results (2026-10-18)

Both final runs used Linux amd64, Go 1.26.9, a compatible Docker engine, CGO-disabled
Go test binaries and the published fixture provider v0.3.0. Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`;
the daemon reported the pinned commit. Selector:
`^TestCephMDSCephFSDataIntegration$`.

| Configuration | UTC controller time | Entire test | Restart leaf | Result |
| --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 04:22:17–04:26:47 | 159.61 s | 54.08 s | 12/12 leaves, exit 0 |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 04:22:16–04:26:42 | 158.66 s | 54.88 s | 12/12 leaves, exit 0 |

Both old-MDS logs bound A's incoming message, handler and expected snapshot
branch to the selected client, inode `0x10000000004`, TID 6, follows 2 and
snapshot 3. After natural old-agent termination A still had that pending
ACK, original admission generation 1 and no accepted ACK; B's TID 7/follows
3 remained unsent. The new generation 2 explicitly resubmitted only TID 6,
then first-sent 7. Original oldest TID 6 and dirty FILE_WR 4096 were retained.
Both new ACKs matched exactly. MDS GIDs changed 4698→7758 (CRC) and
14551→7731 (secure), with new nonces and incarnation 6→10.

The Go object-request count remained 178 across reconnect and both metadata
sends. Native A/B/C whole-file SHA-256, every 16 KiB chunk, sizes and
snapshot mtimes matched, as did Go/native clone lists and associations.
The test's Go byte comparisons are executed assertions; a complete wire
capture and object buffers are not archived. Raw OSD completion flags,
request hashes, epochs and timestamps are retained.

Both cleanup receipts confirmed the old overrides discarded only after new
baseline readback (`debug_mds=1/5`, `debug_ms=0/0`, `mds_log_pause=false`),
runtime restoration, namespace/layout restoration, both identities removed
and only the exact old MDS fencing entry removed. Fresh cleanup confirmed
filesystem and pools removed. All nine owned containers and the network
were absent after each run.

[Machine-readable evidence](MDS_CAPSNAP_RESUBMIT_LINUX_20261018.json)
records final source hashes, exact admission/resubmission/ACK metadata,
paired native logs, OSD completion evidence, native reads/clones and cleanup.
The tested snapshot was
`source-1` plus its ten listed Go files;
only documentation and this record changed afterward. Full root and fixture
Go tests, root and fixture vet, race checks for `integration` and `cephmsgr`,
and 422 Python tool tests passed on that final snapshot. These are local
selected configurations, separate from CI at the final commit.

Two earlier runs also passed with cleanup (04:17:39/41–04:22:04/10 UTC).
Independent review then identified a synthetic false-negative timing check:
a fast ACK can be received before the sender records admission return.
The assertion was corrected to the send-call start and a deterministic
fast-ACK regression added. Both final native runs above and the full checks
were repeated on the corrected source. Earlier source/evidence hashes are
preserved separately in the same machine-readable record.

## Limits

This establishes explicit same-TID resubmission after a successful old
admission and no accepted ACK. The old send itself did not return an error;
the remote outcome becomes uncertain when the session is lost. It does not
establish that the old MDS had committed or rolled back the update, that an
ACK was never transmitted on the network, or that a completed-flush
deduplication branch ran after restart. Durable completion with an ACK lost
before client acceptance requires separate evidence. Interrupted OSD
writeback, multiple writers and eviction with buffered data remain outside
this recipe. MemStore's ONDISK flag does not establish persistence across an
OSD restart.
