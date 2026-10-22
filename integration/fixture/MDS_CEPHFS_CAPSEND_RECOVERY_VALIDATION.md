# Head UPDATE recovery after uncertain and refused cap sends

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This is the first checkpoint
of issue #10. It reuses the owned [data fixture](MDS_CEPHFS_DATA_FIXTURE.md)
and its single owned restart. It complements the
[explicit resubmission](MDS_CEPHFS_CAPSNAP_RESUBMIT_VALIDATION.md) and
[completed-flush](MDS_CEPHFS_CAPSNAP_DEDUPE_VALIDATION.md) recipes.

## Product contract under test

`SendCaps` (and `ReleaseCaps`/`SendLease`) returns an error only before the
message enters the ordered writer. Such an error means the session never
transmits the message (known unsent). A nil return is admission only. The
remote outcome stays unknown until a correlated ACK. No product code path
changed. The contract is documented in `cephmsgr/mds_caps.go` and
[MDS_CEPHFS_CONTRACT.md](../../cephmsgr/MDS_CEPHFS_CONTRACT.md), and pinned
by synthetic peers in `cephmsgr/mds_caps_outcome_test.go`. Selecting what to
send again belongs to the test-only consumer
(`integration/mds_cephfs_capsend_test.go`).

## Acceptance recipe

Selector: `^TestCephMDSCephFSCapSendRecoveryIntegration$`. Every leaf except
the restart is shared with `^TestCephMDSCephFSDataIntegration$`. The restart
leaf is `uncertain_and_refused_cap_sends_recovered_across_mds_restart`.

1. The leaf creates `capsnap-file` with Fw and a max size covering both
   generations. It writes generation A, 200000 bytes, through the Go OSD
   session (ONDISK). It then prepares UPDATE U1: dirty FILE_WR, a new flush
   TID, `FLAG_SYNC` and oldest TID U1.
2. The owned incarnation gets `debug-on`, a cap-log mark and
   `client-drop-on` (`ms_blackhole_client=true`, `debug_ms=10/10`, both read
   back). `SendCaps(U1)` is admitted. The boundary is exactly one
   ProtocolV2 `handle_message blackhole` line for U1's update/inode/TID/follows
   tuple from the registered peer nonce. That native branch runs after
   Messenger receipt and before dispatch (ProtocolV2.cc `handle_message`).
3. Generation B's tail (bytes 200000..220000) is written (ONDISK). UPDATE U2
   is prepared with its own TID and oldest TID U1. The old filtered log must
   show no source/handler pair for U1 and no line for U2's TID.
4. The owned rank-0 restart fires. The session ends Lost with
   SESSION_RESET. `SendCaps(U2)` on the Lost session must return an error
   satisfying `errors.Is(err, session.Err())`. The consumer records it as known
   unsent. U1 stays uncertain (admitted, no ACK). The old agent drains with
   no errors, unapplied pushes or ACK for either TID.
5. The client reconnects to the new GID/nonce/incarnation with its caps and
   realms and no pending flushes. Adoption follows `up:active`. The Go
   object-request count is unchanged.
6. On the new incarnation (`debug-on`, mark), the consumer validates the
   whole ordered selection. It then resubmits U1 with its original TID and
   first-sends U2. Both keep their original TID, dirty mask, follows, oldest
   TID and metadata. The cap envelope (cap ID, sequences, caps, wanted,
   barrier) comes from the current session.
7. The new filtered log must show, for each TID, the exact source/handler
   pair followed by the same-thread dirty-flush branch
   (`flush client.<gid> dirty Fw ... [inode <ino>`). It must not show
   `already flushed`, so neither flush had completed on the old incarnation.
   `debug-off` is read back.
8. Two FLUSH_ACKs must correlate with the explicit recovery sends: TID,
   inode, dirty Fw, current cap ID, received after the send started, in
   flush order. After close, Go head bytes equal A followed by B's tail.
   Native LibCephFS read (whole-file SHA-256 and every 16 KiB chunk) and
   `statx` (inode, size 220000, mtime seconds of U2) match. The object-request
   count stays unchanged across reconnect and recovery. The shared cleanup
   leaf and fixture cleanup then run as for the data acceptance.

Native basis (LGPL-2.1 references, no native code copied): ProtocolV2.cc
`handle_message` (blackhole branch after `in_seq`/ack accounting);
Locker.cc `handle_client_caps` (completed-flush branch, dirty flush and
FLUSH_ACK), `_do_cap_update`, `file_update_finish`; Client.cc
`mark_caps_flushing`, `send_cap`, `kick_flushing_caps`.

## Native results (2026-10-22)

### After rebase onto main and the symmetric runtime guard

Tested PR head `c2d6cd3da9517a8024dc294918149b3b46c0e39b`: the branch rebased
onto `3de2098eb6c9eec5a1e1228208f0714d9ace8cb3`, with `log-pause` refused
while `client-drop-on` is active. The workflow ran from that main commit:
[run 39823750027](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39823750027).
Basic PR CI also passed at this head.

| Selector | Configuration | Entire test | Restart leaf | Result |
| --- | --- | --- | --- | --- |
| cap-send | IPv4 host, CRC, AES, BlueStore | 184.42 s | 60.97 s | PASS |
| cap-send | IPv6 container, secure, AES256K, MemStore | 184.90 s | 59.92 s | PASS |
| data (regression) | IPv4 host, CRC, AES, BlueStore | 240.91 s | 105.42 s | PASS |
| data (regression) | IPv6 container, secure, AES256K, MemStore | 289.68 s | 123.80 s | PASS |

### Before the rebase

GitHub-hosted Ubuntu 24.04 amd64 runners, Go 1.27.3, CGO disabled. Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Tested PR head `6043f5464148168c6f1d39d7738442504afce482` through the trusted
default-branch workflow at `c081c22e5277f1cd479b0ca6bda7e013aa0e9d6f`:
[run 39804882976](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39804882976).

| Configuration | Test end (UTC) | Entire test | Restart leaf | Result |
| --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 15:36:28 | 208.45 s | 66.41 s | 12/12 leaves PASS |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 15:35:04 | 143.27 s | 47.06 s | 12/12 leaves PASS |

The result gate required a complete PASS of the selected top-level test in
both jobs. Every assertion listed in the recipe executed and passed. The
per-run public evidence file (`mds-cephfs-data-results.json`, including
TIDs, GIDs, cap logs and native receipts) is written inside the fixture
control directory. Successful CI runs do not archive it, so exact TIDs,
GIDs and log lines are not reproduced here.

The same PR run also used `^TestCephMDSCephFSDataIntegration$` at
`41060ddd8c975386e0d773f22835b641b6c97962` as a regression after the shared-leaf
refactor. Both profiles passed (188.64 s and 203.63 s,
[run 39804361371](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39804361371)).
The later head only admitted the explicit selector in the fixture profile.

The first request ([run 39804276917](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39804276917),
head `41060dd`) failed before any Go test started. The closed MDS profile
still pinned the data opt-in to the original selector. The fix admits only
the explicit cap-send selector with the data opt-in.

## Evidence kinds

- Synthetic: product outcome classes (`cephmsgr`), consumer
  classification/selection, fixture admission and cap-log attribution.
- Live native interoperability: the two CI jobs above.
- No native codec vectors changed.

## Limits

- Only head UPDATE was exercised live. FLUSHSNAP after an uncertain
  admission is covered by the earlier resubmission/deduplication recipes. A
  FLUSHSNAP refused by a Lost session was not exercised live.
- The uncertain send is a drop injected after Messenger receipt. A frame
  lost before the MDS Messenger received it, or a transport failure between
  admission and write, is not separately reproduced.
- Recovery sends keep the caller's original metadata. Native
  `kick_flushing_caps`/`send_cap` would send current inode metadata with the
  original TID.
- Interrupted OSD writeback, eviction with buffered data, multiple writers
  and persistence across an OSD restart (MemStore) remain outside this
  recipe.
