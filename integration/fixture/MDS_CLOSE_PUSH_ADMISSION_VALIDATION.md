# CLOSE sequence catch-up after queue admission

Run 2026-10-18 with Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. Evidence and source hashes are in
[MDS_CLOSE_PUSH_ADMISSION_LINUX_20261018.json](MDS_CLOSE_PUSH_ADMISSION_LINUX_20261018.json).
The snapshot starts from `source-1` and changes only `mds_session.go` plus
the new controlled-peer regression file. Cache-tier fixture work is excluded.

## Failure and correction

Close snapshots the counted-push sequence before waiting for writer queue
admission. While that queue is full, the session remains Open so a failed
admission can roll back its local selection. A push received then advances
the sequence but does not take the existing Closing-only retry path.

After admission, the old sequence reaches the MDS. Pinned
`Server.cc:830–861` drops a REQUEST_CLOSE with an old push sequence without
replying. Previously, if no further push arrived, the caller could remain
Closing indefinitely: another public Close only waited on the same selection.

The post-admission transition now checks whether the sequence advanced and
signals the existing closeNow worker. That worker reads the current sequence
and sends a fresh session CLOSE control, matching native Client::got_mds_push.
The same mutex orders this check with the push handler: an earlier push is
caught by the new check and a later one by the existing Closing path. This
does not resubmit metadata requests.

## Controlled wire regressions

The test pauses the active writer and fills its 64-entry queue. It waits for
the actual Close selection, then delivers a counted capability push through
the peer while the session is still Open. Releasing the writer produces
CLOSE sequence 0 followed by sequence 1, without a second push or public
Close call. A peer CLOSE reply completes the original call.

Against the original source, that regression observes the first CLOSE but
times out waiting for the updated one. The failed run and source/test hashes
are retained. Two additional regressions verify that failed admission stays
Open and a later explicit Close selects sequence 1, and that transport loss
preserves its original cause without reviving the session or sending another
CLOSE. These boundaries are controlled-peer tests, not native fault injection.

## Native and Linux checks

| Profile | UTC interval | Selected leaves | Controller |
| --- | --- | --- | --- |
| CRC/AES, IPv4 host, one BlueStore | 06:40:46–06:48:00 | 25/25 | exit 0 |
| secure/AES256K, IPv6 container, one MemStore | 06:40:45–06:46:57 | 25/25 | exit 0 |

Each selection includes seven push, five recovery and thirteen metadata
leaves, including both FLOCK completion paths. Scoped cleanup confirms no
owned sessions, restored namespace/blocklist and removed identities. Fresh
cleanup removes the filesystem and its pools. Each run's nine owned
containers and network are independently absent after teardown. These runs
verify the ordinary native paths on the changed source; they do not force
the native daemon's queue-admission race.

Complete CGO-disabled root tests, integration/cephmsgr race tests, root and
fixture vet, fixture unit tests and 423 Python checks pass on Linux. All 693
Go files in this commit candidate match the native/check snapshot. The
earlier `source-2` baseline separately passed all 73 CI jobs; that result does
not establish complete CI for this later change.
