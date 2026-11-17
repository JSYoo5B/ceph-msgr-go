# A confirmed prefix around an unknown extent across an owned MDS restart

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This is a further cell of the
second checkpoint of issue #10. It reuses the owned
[data fixture](MDS_CEPHFS_DATA_FIXTURE.md), its OSD client drop and its
single owned restart, like the
[writeback-fence recipe](MDS_CEPHFS_WRITEBACK_FENCE_VALIDATION.md).

## Question

The writeback-fence recipe interrupts the last write of a file and never
completes anything after it. This cell asks what a writer may claim when a
completed write lies beyond an extent whose outcome is unknown: a metadata
flush must report the contiguous completed prefix, not the largest completed
end offset, and recovery across an MDS restart must neither repeat a
completed write nor resend the unknown one, nor make the file appear to hold
data it never confirmed.

## Product contracts under test

No product code changed. The cell exercises the contracts the
writeback-fence record lists: `OSDConnection.Request` ends an admitted
request whose local wait ends without a reply with `OutcomeUnknownError`
and never retries it, `SendCaps` flushes only the metadata the caller
chooses, and `ReconnectSession` moves the same client identity and nonce to
a new MDS incarnation.

The size a flush may report and what recovery may resend are the test-only
consumer's decisions (`integration/mds_cephfs_writeback_test.go` and
`integration/mds_cephfs_writeback_mixed_test.go`): `committedEnd` is the end
of the range completed writes cover from offset 0 without a gap,
`completedEnd` the largest end any completed write reaches, a completed write
is never repeated and an uncertain one is never resent.

## Recipe

Selector: `^TestCephMDSCephFSMixedWritebackRecoveryIntegration$`. Every leaf
before the restart recipe is shared with `^TestCephMDSCephFSDataIntegration$`;
this selector then runs `mixed_writeback_prefix_flushed_across_mds_restart`
and the shared cleanup leaf. With the plan layout (64 KiB stripe unit, two
128 KiB objects per period) the leaf uses three consecutive stripe units of a
new `capsnap-file`:

| Range | File bytes | Object extent | Outcome |
| --- | --- | --- | --- |
| A | [0, 65536) | object 0, [0, 65536) | completed |
| B | [65536, 131072) | object 1, [0, 65536) | uncertain |
| C | [131072, 196608) | object 0, [65536, 131072) | completed |

`cephfsMixedWritebackPlan` checks this placement: A and C are disjoint ranges
of one object and B is the only extent of another, so writing C can neither
overwrite nor re-execute B.

1. The leaf creates the file with Fw and a max size covering C, and writes A
   with a 30 s wait. It must be `completed` (result 0, ONDISK). No metadata is
   flushed yet.
2. `osd-drop-on` sets and reads back the single OSD's client drop. B is
   written with a 5 s wait and must end `uncertain`, with its submission
   identity (GlobalID, map epoch), object, offset, length and request digest
   kept. `osd-log` must return exactly one attributed receipt and blackhole
   pair for this client. `osd-drop-off` restores and reads back both
   baselines.
3. C is written as a new deliberate request with a 30 s wait and must be
   `completed`. The leaf must have issued exactly three object writes, and
   C's object is not B's.
4. `committedEnd` must be A's end (65536) while `completedEnd` is C's end
   (196608). The consumer marks only the committed end dirty, with C's time
   as the modification time, and sends an UPDATE with `FLAG_SYNC`. The UPDATE
   must report size 65536, that mtime and the snap context's sequence; its
   FLUSH_ACK must match the TID, inode, dirty Fw and cap ID. Go STAT and the
   native object view must then show object 0 at 131072 bytes and object 1
   absent.
5. The owned rank-0 restart fires with B still uncertain and no dirty or
   flushing metadata. The session ends Lost with SESSION_RESET, the same
   client GlobalID and nonce reconnect to the new GID, nonce and incarnation
   with their caps and realms and choose no flush, and adoption follows
   `up:active`. The Go object-request count is unchanged across the restart.
   Native `statx` must report size 65536 and C's mtime.
6. On the new incarnation a native read of the file must return A's 65536
   bytes (SHA-256 and every 16 KiB chunk). The closed `object-bytes` view
   must show object 0 holding exactly A followed by C (whole-object SHA-256
   and each 16 KiB chunk) and object 1 absent, and Go STAT and the native
   object view must agree on the sizes. A Go LOOKUP must report size 65536 and
   C's mtime. A Go read of [0, 196608) must return A, a missing object for B
   (zeros) and C.
7. After the file is closed, the resend rule must refuse all three writes:
   A and C because they completed, B because its outcome is unknown, although
   the native view shows that it did not land. Neither the max_size request
   nor any cap update that the pre-restart or the post-restart agent recorded
   for the file once it was marked dirty may report a size above 65536, and no
   cap update after the accepted flush may carry dirty metadata. The agent's
   cap size grows only when the consumer marks a size dirty, so earlier cap
   messages cannot exceed the bound either.

The native read and `statx` show that the file is A alone: had a flush
reported C's end, a native read would have returned A, zeros where B was
meant to be and C, which is file data the writer never confirmed. The
object-bytes view shows separately that C's bytes reached its object. An
existing object is not a file-size acknowledgement.

## Native object-bytes view

`object-bytes capsnap-file` is a closed fixture action admitted only by the
capability `mds-writeback-mixed`, which requires `mds-writeback-fence`. It
runs where any other native action may (never during a restart, a journal
pause, an MDS or OSD client drop or the background reader). The native
identity stats the file, then, through the python rados binding, stats each
of the planned objects `<ino hex>.<objno %08x>` and reads every existing
object whole (bounded to 4 MiB, the read must return exactly the stat size).
It reports existence, size and a digest of each object's head content
(length, SHA-256, SHA-256 of each 16 KiB chunk). The decoder refuses a
digest on an absent object or in the plain `objects` view, an existing
object without one, a digest whose length differs from the size, clone
records and any file other than `capsnap-file`.

## Runs

All runs below used a Linux amd64 host with Docker 29.9.1, the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
and the catalog row's environment from `python3 tools/scenario_plan.py env`
under the result gate (`tools/native_test_results.py`), one `go test`
process per engine.

### Before the final code commit

Both new rows first ran from a `git archive` of
`8a3c5d5102a5d190cc69d0c769ac146974d14e3a`, before the size bound was
extended to the max_size request. Both passed the gate with every leaf at the
first attempt (parents 140.06 s and 138.60 s; the mixed leaf 42.44 s and
42.71 s). They are superseded by the runs below.

### At `c741f33`

These runs used source `c741f33d1dafd6b4249315fee2a00bcc6cb5d2a1`. The upload
was a `git archive` of that commit, and the commit ID was recorded on the host
before the run. Each row ran once, in one sequence on one engine, and passed
its gate with no failed, skipped or missing selected test. Only documentation
changed after that commit.

| Row | Parent | Mixed leaf | Cleanup leaf |
| --- | --- | --- | --- |
| `mds-cephfs-mixed-writeback`, CRC/AES IPv4 host BlueStore | 140.60 s | 43.43 s | 17.99 s |
| `mds-cephfs-mixed-writeback`, secure/aes256k IPv6 container MemStore | 139.57 s | 43.18 s | 17.71 s |

The rows that share the data fixture, its OSD drop and the restart helper
passed in the same sequence: both `mds-cephfs-writeback` rows (parents
196.68 s and 194.23 s), both `mds-cephfs-capsend` rows (146.41 s and
146.59 s) and both `mds-cephfs-data` rows (189.17 s and 189.36 s). After the
sequence no testcontainers container was left on the engine.

### Control run by temporary mutation

One run on the CRC/AES IPv4 host BlueStore row used a `git archive` of
`c741f33`. Before the run, one change was applied on the host and never
committed: the consumer marked `completedEnd` (C's end) dirty in place of the
committed prefix, and the local check of the UPDATE's size followed it. The
gate failed as intended in the mixed leaf. After the restart, native `statx`
reported size 196608 where 65536 was required. Every earlier leaf passed.
This shows that the native observation, not only the consumer's own
bookkeeping, refuses a flush that claims C's end.

No run of this acceptance failed except the intended control run.
Exact-head CI is separate from these runs.

## Evidence kinds

- Synthetic: the plan placement and its refused layouts, the committed and
  completed ends, the resend rule, the expected object images, the native
  content comparison and the cap-size bound
  (`TestCephFSMixedWritebackPlanBoundsAndContent`); the capability
  admission, the closed request, the admission before native work and the
  output shape of `object-bytes`
  (`integration/fixture/mds_cephfs_data_writeback_mixed_test.go`).
- Live native interoperability: the runs above. Exact-head CI is separate.
- No native codec vectors changed.

## Limits

- B's interruption is the existing drop after OSD Messenger receipt. A
  reply withheld after the write applied, a frame lost before the OSD
  received it and a partially applied request are not reproduced, so B
  resolves natively as not applied while the consumer keeps it uncertain.
- One writer, one file, three single-extent writes on two objects, a single
  OSD. The accepted flush precedes the restart; a prefix flush whose own
  outcome is uncertain at the restart (the cap-send recipe's case) is not
  combined with the mixed ledger. Truncating or extending the file over C,
  eviction with C in place, multiple writers and persistence across an OSD
  restart are not run.
- The detailed ledger (`facts.mixed_writeback` in
  `mds-cephfs-data-results.json`: the three writes with their submission
  records, the drop lines, the flush and its ACK, every cap message and the
  native views) is written inside the job and is not uploaded for a
  successful run (issue #1).

## Source and licensing

Native behavior is read from LGPL-2.1 sources at the pinned commit:
`osdc/Striper.cc` (`file_to_extents`), `client/Client.cc` (`_write`,
`check_caps`, `send_cap`), `mds/Locker.cc` (`_do_cap_update`,
`_update_cap_fields`), `msg/async/ProtocolV2.cc` (`handle_message`) and
`pybind/rados/rados.pyx` (`stat`, `read`). No upstream code is incorporated.
Native tools remain development dependencies.
