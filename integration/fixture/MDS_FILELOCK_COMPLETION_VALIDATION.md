# Raw FLOCK wait and interrupt completion

Run 2026-10-18 on Linux amd64 with Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The receipt retains the pinned
image digest, daemon identities, source inventory and each overlay hash.
Both source snapshots start from `source-1`; the final one includes the
request/Close fix committed as `source-2` and both new integration leaves.

This follows the nonblocking native comparison in
[MDS_FILELOCK_VALIDATION.md](MDS_FILELOCK_VALIDATION.md). The same owned empty
file, authenticated Go session, capability consumer and independent native
LibCephFS identity are reused. No product lock policy or new fixture action
is introduced. Evidence is retained in
[MDS_FILELOCK_COMPLETION_LINUX_20261018.json](MDS_FILELOCK_COMPLETION_LINUX_20261018.json).

## Local wait expiry and the original call

Owner A acquires an exclusive whole-file FLOCK. Owner B submits one exclusive
request with wait=1 using the parent operation context. A GETATTR safe reply
on the same ordered connection precedes a separate 750 ms local `Wait`, which
returns `context.DeadlineExceeded`. A is explicitly unlocked, then that same
B call receives a safe result 0 with its original TID. GETFILELOCK identifies
B's owner and authenticated GlobalID. B is explicitly unlocked and an
independent native exclusive nonblocking probe acquires and releases the lock.

The deadline bounds the local wait. It does not cancel the remote request,
remove the pending call, release its in-flight slot or undo the lock. Only
one application Submit selects B; Messenger resume observations are recorded
separately. The ordered barrier and timeout do not directly observe a native
waiting_locks entry or establish when the MDS entered its wait branch.

## Explicit interrupt

A separate leaf holds A while B's one wait=1 request outlives its local Wait.
The consumer copies B's arguments and changes only rule to FLOCK_INTR (4)
and type to UNLOCK (4). It submits that interrupt once with a distinct TID.
Both the interrupt's safe result 0 and original B call's safe result -EINTR
(-4) are required; their arrival order is not assumed. GETFILELOCK then
identifies A as still held. A is normally unlocked before an independent
native exclusive probe confirms release.

Pinned Tentacle `Client::_interrupt_filelock` copies those arguments;
`Server::handle_client_file_setlock` removes the matching waiter and wakes
the original request with -EINTR. An interrupt arriving after a grant does
not remove the held lock. This success path keeps A held until B returns
-EINTR, so it does not test that grant race. An interrupt result 0 by itself
does not prove cancellation or rollback.

## Ownership and cleanup

Every selected request retains its caller arguments, TID, admission times,
local wait times and raw replies. Full inode images, session status, resume
counters and the native before/after receipt are preserved. Both leaves
compare bytes, size, native statx version and second-resolution mtime with
the preceding nonblocking baseline; Go ChangeAttr, size and precise MTime
are also unchanged. Internal wire inode Version and MaxSize remain recorded
observations because capability bookkeeping may advance them.

Normal and failure paths share each selected unlock call. Cleanup first
unlocks A, waits again on the original B call, and normally unlocks B only
if B was granted. An uncertain interrupt is waited on again using its
original call. No acquire, unlock or interrupt is automatically resubmitted.
Unconfirmed cleanup remains a failure. SETFILELOCK's safe reply reports an
in-memory state change and does not establish journal durability.

## Session close admission

The combined source also fixes a request/Close race. A request waiting for a
shared slot could previously pass the initial Open check, wake after
REQUEST_CLOSE admission and enter the writer while Closing. Close could
also select its message while a registered request still awaited writer
admission. Two controlled regressions fail against the original source.

A context-aware session gate now orders request registration and writer
admission with Close selection and admission. Closing is checked after slot
admission. The reader and terminal resolver do not take this gate. Five
regressions cover those races, failed Close admission, transport loss and
confirmed CLOSE without consuming another owner's slot or gate token.

## Verified runs

| Profile | Phase and UTC interval | Local-expiry leaf | Interrupt leaf | Selected leaves |
| --- | --- | --- | --- | --- |
| CRC/AES, IPv4 host, one BlueStore | initial, 06:13:58–06:21:10 | 4.054 s | not selected | 24/24 |
| secure/AES256K, IPv6 container, one MemStore | initial, 06:13:56–06:20:07 | 3.754 s | not selected | 24/24 |
| CRC/AES, IPv4 host, one BlueStore | final, 06:22:21–06:29:39 | 4.053 s | 4.154 s | 25/25 |
| secure/AES256K, IPv6 container, one MemStore | final, 06:22:27–06:29:08 | 3.453 s | 3.453 s | 25/25 |

Every controller exits 0. Each final selection includes seven push, five
recovery and thirteen metadata leaves. The separate internal-client binary
has no matching test and adds no live coverage. The original B TID is 17 in
the local-expiry leaf and 25 in the interrupt leaf; the explicit interrupt
has TID 27. All four sessions record zero Messenger resumes during these
leaves. These are observed values, not a guarantee against retransmission.

All four runs use distinct fixture identities and resources. Scoped cleanup
confirms namespace/blocklist restoration, zero residual owned sessions and
removal of the four CephFS identities. Fresh cleanup removes the filesystem,
its pools and base identity. An independent audit finds each run's nine
owned containers and network absent. Hashes and sizes of all eleven fetched
public files per run are retained; private keyrings and ownership files are
excluded.

The final combined source passes complete `CGO_ENABLED=0 go test ./...`,
integration/cephmsgr race tests, root/fixture vet, fixture unit tests and 423
Python checks on Linux. The pre-fix source separately fails exactly the two
admission regressions described above; those failures and hashes are retained.
All 692 Go files in the resulting commit candidate match the tested final
snapshot. These checks do not establish complete CI at that commit.

## Limits

These are bounded, head-file, whole-file FLOCK observations. FCNTL byte-range
locks, direct native waiting-lock observation, permission denial, interrupt-after-grant behavior and typed lock
workflows are not established. Held-lock reconnect across an owned MDS restart is the
separate [held-lock restart checkpoint](MDS_FILELOCK_RESTART_VALIDATION.md).
No remote cancellation follows from a local
context expiry. Exact source inventories and the independent cleanup audit
are part of the receipt; local validation is distinct from complete CI.
