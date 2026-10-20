# Completed FLUSHSNAP recovery after a lost ACK

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This extends the
[explicit original-TID resubmission recipe](MDS_CEPHFS_CAPSNAP_RESUBMIT_VALIDATION.md).
Recovery choice, snapshot generations, object placement and writeback remain
test consumer responsibilities. Product APIs do not automatically replay a
cap flush or infer that a local send proves server completion.

## Acceptance recipe

The exclusive DATA profile prepares both old-context A/B object writebacks
with ONDISK replies, retaining their original flush TIDs, oldest TID, follows,
dirty mask, size, mtime and exact ACK expectations. The owned old MDS journal
is paused, only A is sent, and its incoming message, exact handler and
expected snapshot branch are bound to the Go GlobalID/address nonce.

Before releasing the journal, the fixture sets `ms_blackhole_client=true`
through the owned admin socket and reads it back. This drops client
Messenger traffic in both directions. All control observations use the
admin socket or MON maps; no native client RPC is sent through the blocked
transport. Native namespace/data operations are refused during the pause,
blackhole or an unadopted restart.

The fixture restores `mds_log_pause=false`, reads it back, and executes one
admin-socket `flush journal` barrier. Intent is recorded before the call;
an uncertain barrier is never reissued. Return code zero is required.
Separately, the native log must attribute A's dropped FLUSHSNAP_ACK through
three same-thread records: `send_message_client_counted` for the exact
GlobalID, an AsyncConnection send to the exact peer nonce, and blackholing
of the identical operation/inode/TID/follows tuple. A bare blackhole line
does not identify its client and is rejected.

The fixture fails its exact old MDS GID once. The old session must end Lost
with SESSION_RESET, and its agent drains accepted events naturally. A must
still have its exact pending ACK and no accepted ACK; B must remain unsent.
The caller reconnects with the same client identity, earliest pending
follows and latest root/directory realms, handing off the complete immutable
cap-snap and ACK state before event consumption starts.

Adoption requires the owned replacement's new GID, nonce and incarnation,
ACTIVE, unchanged filesystem/pools, old GID absence and all four original
runtime baselines. Only then can old active overrides be discarded. The
test enables debugging on the new incarnation, marks its log, and explicitly
resubmits only A's original TID. Before first-sending B, the log must bind
the new incoming message and exact handler to
`handle_client_caps already flushed tid <TID> for client.<GID>`.
The new debugging override is restored and read back.

Both exact ACKs, native A/B snapshot and C head bytes/size/mtime, and both
objects' clone lists are checked. Go object-request count must remain
unchanged across reconnect and metadata sends. Cleanup proves all runtime
baselines, namespace/layout/identities, exact owned old-address fencing,
filesystem/pools and run-owned containers/network.

Native basis (LGPL-2.1 references, no native code copied): Locker.cc
`_do_snap_update` records the client flush in the same EUpdate metablob;
`C_Locker_FileUpdate_finish` registers completion and sends its ACK after
journal completion; journal.cc restores completed flushes during replay;
`handle_client_caps` returns an ACK without reapplying a completed
FLUSHSNAP. MDSRank.cc and AsyncConnection.cc establish the counted-client,
peer-address and blackhole log call order. These source semantics are
distinct from the actual configuration evidence recorded below.

## Native results (2026-10-18)

Both final runs used Linux amd64, Go 1.26.9, a compatible Docker engine, CGO-disabled
product test binaries and fixture provider v0.3.0. The daemon reported the
pinned source commit. Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Selector: `^TestCephMDSCephFSDataIntegration$`.

| Configuration | UTC controller time | Entire test | Restart leaf | Result |
| --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 04:41:47–04:46:40 | 185.04 s | 80.57 s | 12/12 leaves, exit 0 |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 04:41:49–04:46:43 | 185.03 s | 80.82 s | 12/12 leaves, exit 0 |

Each old log retained six attributed lines: A's arrival, exact handler and
snapshot branch, then its counted-client, destination-nonce and blackhole
ACK triple. Journal barrier return code was zero. The naturally drained
old agent retained A's pending inode `0x10000000004`, TID 6, follows 2 and
dirty FILE_WR 4096 without an accepted ACK; B's TID 7/follows 3 was unsent.
The replacement log retained the new original-TID arrival, exact handler
and `already flushed tid 6` branch. Both bounded logs reported zero dropped
groups. MDS GIDs changed 4695→17192 (CRC) and 14551→17176 (secure), with
new nonces/incarnations and unchanged Go client identities.

Both new ACKs matched exactly. Go object requests remained 178 across
reconnect and metadata sends, with four earlier A/B writes carrying ONDISK
completion and preserved request hashes. Independent native A/B snapshots
and C head matched whole-file SHA-256, every 16 KiB chunk, size and mtime;
both object clone lists and associations matched. The actual Go assertions
also compare object bytes; complete wire captures/buffers are not archived.

Cleanup confirmed the original four settings (`debug_mds=1/5`,
`debug_ms=0/0`, `mds_log_pause=false`, `ms_blackhole_client=false`). The old
pause was restored before the barrier, old debug/blackhole overrides were
discarded only after replacement baseline proof, and new debug settings
were explicitly restored. Namespace, layout, both identities, exact owned
old-address fencing, filesystem and pools were cleaned. All nine owned
containers and the network were absent after each run.

[Machine-readable evidence](MDS_CAPSNAP_DEDUPE_LINUX_20261018.json) records
the tested base `source-1` plus ten listed Go-file hashes, exact cap messages,
paired native logs, journal result, object completion, native postconditions
and cleanup. The snapshot includes the independently reproduced/fixed
reconnect reservation leak and late same-TID abandonment race. Full root
and fixture Go tests/vet, race tests for `integration`, `cephmsgr` and
`internal/session`, and 422 Python tool tests passed on this combined source.
Documentation and the evidence record were updated afterward. These local
selected-profile results do not establish full CI success.

Two earlier runs also passed with cleanup (04:35:37–04:40:40 CRC,
04:35:37–04:40:31 secure). Their source base was `source-2` plus the same six
integration Go files, before the independent product request-lifetime
fixes. Their original source/evidence hashes are preserved separately in
the machine-readable record; the final combined runs above were repeated
after those fixes.

## Limits

This recipe targets a successfully admitted old cap message with completed
object writeback. It does not establish recovery from a `SendCaps` error,
interrupted object writeback, multiple writers, or eviction with buffered
data. MemStore ONDISK does not establish persistence across an OSD restart.
