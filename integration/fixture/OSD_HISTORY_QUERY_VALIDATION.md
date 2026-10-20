# Retained OSD history query — 2026-10-18

The six complete modern-profile CI selectors passed locally after separating
the native OSD history response limit from ordinary commands: **80 top-level
tests passed, no failures or skips**. Normal and late Backoff ACK observation,
Split/merge restoration, stale-envelope identity and subsequent Watch tests
remained enabled. Product Go code and CI selectors are unchanged by this fix.
The selected evidence is
[OSD_HISTORY_QUERY_LINUX_20261018.json](OSD_HISTORY_QUERY_LINUX_20261018.json).

## Original failure and measured reproduction

A single fresh Linux request-v9/reply-v8 CRC/AES IPv4 host/one-BlueStore run
used that exact complete selector with the **original 1 MiB limit**. Its only
helper overlay added command-size diagnostics and saved the unfiltered native
history for this measurement. It failed at the same Split observer:

- exit code 0, stderr 0 bytes, complete/non-truncated command result;
- stdout **1,312,961 bytes**, exceeding 1,048,576;
- 1,024 native retained operations;
- raw stdout SHA256
  `74e8deccfbc03f42080c798de9066150a53c6dab08ff3777e195649aa683e76b`.

This measured run was UTC 08:58:09.436067–09:01:41.112049. Twelve top-level
tests passed; Split and its six subsequent Watch tests failed. Its eight
owned containers and network were independently confirmed absent. The raw
history and first-failure artifacts remain in the local evidence directories
named in the JSON. These measured sizes are not attributed retroactively to
the four GitHub jobs.

## Narrow correction

Only the fixed read-only `tell osd.0 dump_historic_ops --format json` command
uses a 4 MiB response acceptance limit. Its timeout remains two seconds and
stderr limit 4,096 bytes. Ordinary native commands retain their 1 MiB limit.
The fixture still retains 1,024 operations; no history clearing, selector
removal, automatic mutation replay or identity-check relaxation was added.

Both Backoff and Split use the dedicated reader. The public
`osd-history-query.json` records exit code, stdout/stderr lengths and hashes,
limits, completeness, operation count and failure classification. It contains
no history descriptions or stderr text. Timeout captures are marked partial
and have no exit code. This is the latest query's receipt, not an archive of
every query. Limits apply after subprocess capture, not as a streaming heap
ceiling. A successful query is not itself a request-reception receipt.

## Complete-selector verification

All six runs used Linux amd64 and pinned Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image digest
`sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
CRC rows used AES/IPv4/host/one BlueStore; secure rows used
AES256K/IPv6/container/one MemStore. Each used the exact corresponding CI
selector, including EnforceSnapContext and ChecksumCompound.

| Request / reply | Mode | Top-level passes | Last history bytes / operations |
| --- | --- | ---: | ---: |
| v8 / v6 | CRC | 10 | 986,245 / 765 |
| v8 / v6 | secure | 10 | 994,638 / 766 |
| v8 / v8 | CRC | 11 | 1,164,984 / 903 |
| v8 / v8 | secure | 11 | 1,186,557 / 913 |
| v9 / v8 | CRC | 19 | 1,311,270 / 1,024 |
| v9 / v8 | secure | 19 | 1,321,078 / 1,024 |

All last queries returned exit 0 with empty stderr and complete output under
4 MiB. The latter four exceeded the old limit. Their public query receipts
retain output hashes and counts; unfiltered fixed-run histories were not
retained, so those hashes were not independently recomputed from raw JSON.

Independent readback matched all 80 actual top-level names to the exact CI
selectors. It checked native ACK client/PG/ID/original epoch and `reached_pg`,
stale client/incarnation/TID/PG/raw hash/epoch, preserved unknown execution
outcomes and no redial, PG restoration from 16 to 8, object state, and owned
head cleanup. All **48 unique owned containers and six networks** were
confirmed absent. Python's full 459-test suite and fixture Go test/vet passed.

Executed source was `source-2` plus the five listed helper/test/diagnostic
overlays. The preserved 1,642-file stage was rehashed against its manifest.
Its 712-file Go inventory SHA256 is
`a6ff145127c91cc724025c5cb12c8eb20ddc29c2ec0ef58a53255520705db168`;
only the fixture diagnostic allowlist differs among Go files. Later Markdown
and this selected evidence were not executed source. These local results do
not establish the status of a subsequent full GitHub CI run.
