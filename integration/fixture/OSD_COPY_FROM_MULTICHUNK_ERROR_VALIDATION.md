# Selected native multi-chunk COPY_FROM2 late-error acceptance

Test/controller checkpoint: `46d77c6ea25cc815268bb7a566968b9181822269`
(native run 40033488575, attempt 1).
Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
A successful native run uploads only its result and test-gate files, so the
test also writes its bounded public observations to the job log. The values
below come from that log. Object names in it are synthetic fixture names.

This cell sits beside the [single-chunk late-error acceptance](OSD_COPY_FROM_LATE_ERROR_VALIDATION.md).
It does not replace that test, its row or its receipt.

## Source boundary

The single-chunk cell reads an 8 MiB `osd_copyfrom_max_chunk`, so its
21-byte source finishes in one chunk. The bounded native observer
(`tools/osd_omap_write_native.py`) caps binary data at 128 KiB. A larger
source inside that bound cannot reach the multi-chunk branch by itself.

- The [option declaration](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/osd.yaml.in#L1496-L1501)
  has no startup-only flag, and
  [get_copy_chunk_size](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.h#L1398-L1407)
  reads the current value for each chunk. The declaration alone does not
  establish runtime mutability, so the controller reads the value back.
- [_copy_some](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9564-L9575)
  issues one copy-get per chunk, preceded by assert-version when the source
  version is known.
- [process_copy_chunk](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9776-L9793)
  creates a temporary object with
  [generate_temp_object](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L8954-L8962)
  while the cursor is incomplete. The injector runs only after the cursor
  completes.
- The [error path](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9867-L9882)
  completes the client callback first and then submits removal of the
  temporary object. Receiving EIO is therefore not a cleanup barrier.

No Ceph source is incorporated into the product.

## Test ownership and controller

`TestCephOSDCopyFromMultiChunkLateErrorIntegration` has its own exact
selector, its own fixture-derived marker
(`CEPH_MSGR_NATIVE_COPYFROM_MULTICHUNK_ERROR`) and its own catalog row on the
CRC/AES IPv4 host, single-BlueStore, legacy-v6 fixture. The test skips before
it touches fixture state when the marker is absent. Broad suites and the
single-chunk row receive an empty marker. Cluster-free regressions cover the
skip, the environment and the controller's selector checks.

The controller uses only the owned OSD's local admin socket, and every call
has a bounded context:

1. It reads the original injector value (false) and chunk limit, lowers the
   chunk limit to 32,768 bytes once and reads it back. Only then does it
   enable the injector once and read it back.
2. Restoration resets the injector first and then the chunk limit. Each
   setting is observed before at most one restoring set. An uncertain
   acknowledgement keeps restoration ownership and is never replayed.
3. `history` returns the owned OSD's `dump_historic_ops` descriptions.
   [OpHistory::cleanup](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/TrackedOp.cc#L101-L117)
   discards the shortest operations first once the history is full. The
   default 20 entries held only slower writes in an earlier run, so this
   exclusive fixture alone starts with `osd_op_history_size = 1024`. The
   test requires every dump to stay below that capacity.
4. `staging <pgid>` enumerates the owned PG collection with the BlueStore
   [bluestore list](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueAdmin.cc#L81-L124)
   admin command, starting at GHMIN with a 4,096-entry bound.
   [_collection_list](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.cc#L13679-L13690)
   includes the temporary range for that start. Fewer entries than the
   bound establishes a complete enumeration. Entries whose pool is -2 or
   lower are reported as temporary.

`history` and `staging` are read-only. The daemon stays online, so no
offline objectstore tool is involved.

## Operations and actual result

One native C API preparation owns two names: a source with 65,536 bytes of
data, one binary xattr and two bounded OMAP entries, and an initially absent
Go destination. Go sets the source header and seeds the destination with
different bytes, xattr, OMAP/header, version and mtime. Independent native
reads record both preconditions.

Before the request, a complete enumeration shows no temporary object, and
the history has no copy-get naming the source. The Go request is submitted
once: COPY_FROM2 of the exact native source version, followed by SETXATTR
`multi-chunk-suffix`. No retry, resubmission or positive copy follows.

| Field | Observation |
| --- | --- |
| Platform/profile | Linux/amd64, CRC/AES client and service, IPv4 host, legacy-v6 request/reply, one BlueStore OSD |
| Effective chunk limit | 8,388,608 -> 32,768 -> 8,388,608, read back after each change |
| Effective injector | false -> true -> false, read back after each change |
| Go aggregate / per-op results | -5 / [-115, 0] |
| Original source union | version 3, head snapshot |
| Local submission | TID 3, global ID 4689, attempt 0, epoch 13 |
| Native pool / primary / PG | 2 / 0 / 2.0 |
| Source copy-gets in history | 3, each `[assert-version v3,copy-get max 32768]`, OSD-issued TIDs 1 to 3, in a 95/1024-entry history |
| Staging enumeration | baseline 4 objects without temporary entries; first post-reply enumeration complete without temporary entries, 401 ms after the reply |
| Integration selector duration | 6.23 seconds |

The typed Go error is OSDError with signed code -5, not
OutcomeUnknownError. The copy operation retains -115 (EINPROGRESS) and the
untouched suffix retains zero. **That zero does not prove the suffix
executed.** Native source and destination bytes, known xattr/OMAP inventory,
header, mtime and version are unchanged, and the suffix marker is absent.
After restoration, a same-connection read uses the next TID and returns the
original destination bytes and version.

Three copy-gets with the lowered maximum and the exact source version are
independent native cursor evidence that the incomplete-cursor branch ran
(64 KiB of data plus metadata over 32 KiB chunks). The temporary object
itself was not observed. The first complete enumeration after the reply
already had no temporary entry. That shows absence at that point, not when
the removal happened or that the object was ever persisted. An observed
temporary entry would have to match the pinned `temp_<pgid>_0_` identity
in pool `-2-pool`; the bounded poll allows 30 seconds.

Object cleanup has a separate 40-second context and removed both owned
names. The diagnostic allowlist includes the option, history, staging and
result receipts.

## Development iterations

The selected runs before the checkpoint are retained as development evidence.
Their heads predate a rebase onto main and remain visible only in the pull
request history:

| Run | Pre-rebase head | Outcome |
| --- | --- | --- |
| 40031201803 | `93bf54b` | Signed reply checks passed; the default 20-entry history held no copy-get |
| 40031670921 | `442800e` | Retained descriptions: the full history held only slower writes |
| 40032112563 | `26475b6` | 1024-entry history held three copy-gets printed with assert-version; the test matched only a bare copy-get |
| 40032500622 | `e8983b6` | Passed |
| 40032881589 | `c324b49` | Passed with logged observations (temporary entry absent 701 ms after the reply, after the history query) |

The later reorder enumerates staging before the history query. Each
failure has a source-backed cause; a later pass does not explain an earlier
failure by itself.

## Reproduction and limits

On an authorized Linux runner, hold its run lock, use the pinned image and:

```sh
CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCopyFromMultiChunkLateErrorIntegration$' \
CEPH_MSGR_TEST_TIMEOUT=8m \
go -C integration/fixture run . run -project ../..
```

Its scenario ID is
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcopyfrommultichunklateerror`.
Request it with the CI comment command. Basic PR CI builds and unit-checks
the fixture but does not run this daemon test.

This is one multi-chunk late server-error checkpoint. It does not
establish destination loss after source-read completion, removal timing,
mid-vector termination, arbitrary compound rollback, other chunk sizes,
snapshots, EC, modern wire versions, secure/aes256k/IPv6 or other
topologies. Keep #7 open for its remaining fault and compound requirements.
