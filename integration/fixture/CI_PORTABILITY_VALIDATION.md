# Native proof / watcher fixture portability correction — 2026-10-15

Exact original head `source-1`,
GitHub Actions run `38694566449`, reported Windows unit-test failures. No run
was restarted or canceled. Two causes were separately reproduced and corrected:

- `control_lock` held a file handle across `yield` and unlinked the file before
  closing that handle. Windows rejects this deletion with WinError 32. Closing
  the creation handle before yielding keeps exclusive path creation/ownership
  while allowing cleanup. Three additional mock regression cases cover normal
  cleanup, exception cleanup and refusing an existing lock while simulating
  Windows open-handle deletion behavior.
- `.gitattributes` fixed only the earlier nine native encoder C++ wrappers to
  LF. Windows checkout converted the three new wrappers to CRLF and their
  source hashes differed from the pinned Linux native-build provenance. The
  new `tools/*_native_encoder.cc text eol=lf` rule includes existing and future
  wrappers. The prior setting's hash mismatch and the correction's exact hash
  match were reproduced with Git's actual `core.autocrlf=true` checkout filter
  for watcher, MAPEXT and CHECKSUM sources in an isolated temporary checkout.

All 272 Python tests and focused CGO=0 fixture native-provenance Go tests
passed locally. The Windows job logs directly establish the original errors;
the checkout-filter experiment independently establishes the line-ending
cause. Successful Windows execution of this correction needs the new exact-
head CI result; the local checks are not presented as that result.

## Follow-up from source-2

Run `38698420954` at `source-3`
exposed three additional test-harness issues. Windows converted the native
helper's Linux loader paths to backslashes, and a staging test assumed POSIX
permission bits. The build helper now serializes Linux paths as POSIX paths;
permission assertions apply only on systems implementing those bits. A
PureWindowsPath regression reproduces the path issue independently of the host.

The host MON fixture also exhausted a 32 MiB log bound on unrelated daemon
debug history. A bounded-line streaming reader now passes only exact reclaim
candidate lines to the existing strict timestamp/entity/GID/suffix parser. Its
candidate evidence retains the original 32 MiB bound. Malformed candidate lines,
read failures, oversized lines and cancellation still fail. The Docker log
reader closes on context cancellation. A regression streams 64 MiB of unrelated
history around the required native evidence without retaining that history.

The exact isolated change set passed the fixture module's complete CGO=0 tests
and 285 Python tests. Actual Windows and host MON CI execution remain separate
verification; no run was canceled or retried for this change.

## Request-v9 Linux SDK proof regression — 2026-10-16

Run `38890511851` at `source-4`
failed both Windows Go-version unit jobs in the request-v9 SDK proof model.
Its text writer translated the modeled Linux CMake cache to CRLF, whereas
the native Linux producer correctly requires exact LF bytes. Commit
`source-5` writes those modeled files as
explicit bytes, verifies their bytes and hashes, and retains rejection of
CRLF, enabled Jaeger and defined HAVE_JAEGER. The native producer and its
historical proof artifacts are unchanged.

The complete Python suite passed 413 tests locally. Run `38893912494` at
that exact correction SHA passed Windows unit jobs `116949721132`
(Go 1.27.x) and `116949721304` (Go 1.24.0), plus Windows fixture tools job
`116949721240`. These completed jobs establish this Windows correction;
they do not establish completion of the whole native matrix.

## Current-main regression reconciliation — 2026-10-18

- Seven jobs failed the Python matrix contract before their Go checks ran.
  The workflow had gained a separate soak row (59 rather than 58) and split
  resubmission plus later request-v9 tests, but the expected selectors still
  described the older matrix. The checks now enumerate the actual intended
  tests, distinguish the soak row from the original 30 legacy rows and keep
  the closed selectors and configuration checks. No selected native test was
  removed. The complete Python suite passed 422 tests after this correction.
- Four jobs failed OSD map validation, then lost their fixture control worker.
  Their two fresh sessions each received the same valid latest-40 incremental
  window. The manifest listed 80 records while the independent native oracle
  permits 64. Each `(kind, epoch)` appeared twice (40 distinct records).
  The test now preserves one record per received blob and checks exact digest
  equality before deduplication. Conflicting same-epoch bytes fail without
  overwriting the first evidence. Native encoder/decoder and oracle bounds
  are unchanged. Regression coverage includes repeated 40-map windows,
  conflicting bytes, and distinct full/incremental identities.
- Single-BlueStore `MutationClose` used an old caller map epoch after the
  preceding fullness test. The retained sequence reports FULL at epoch 116,
  cleared at 120, while the old route was epoch 99. Pinned
  `PrimaryLogPG.cc:2137–2160` drops ordinary writes older than
  `last_epoch_marked_full`, even after FULL clears. Its own withheld receive
  path can observe an OSDMap without proving an executed APPEND. Individual
  discard logs were not retained, so this diagnosis is based on source and
  ordering evidence. The shared fixture now reads current native placement
  for single OSDs as well as replicated ones, before any subject request;
  object identity/namespace and baseline guards remain. It does not replay
  the request or add automatic routing to the product.
- The four-minute soak reached actual OSD/POOL_FULL on the three 1-GiB
  BlueStore devices. Unanswered writes and deadline failures are consistent
  with that server refusal. Only the closed three-BlueStore soak selection
  receives four-GiB sparse devices; full ratios, workload duration, unknown
  outcomes and explicit-only resubmission remain unchanged. See
  `OSD_SOAK_VALIDATION.md` for the retained native health evidence and finite
  capacity contract.

The native leader-stop failures are separate from a request replay bug:
`retireMonitor` drains a replaced session only for `ConnectTimeout`, then
fails its transmitted pending calls as unknown. That bounded behavior is an
existing documented product contract. The leader-election acceptance clients
now explicitly choose `ConnectTimeout=60s` before Dial, which bounds both
endpoint setup and retired-session drain. Their operation contexts remain
240/300 seconds. Only the leader-stop subject clients use that option; ordinary
pinned queries, short ticket renewal and the product's default ten-second
drain remain unchanged. Public GlobalID/FSID, ticket deadlines, readiness and
rejection observations accompany the existing native single-allocation and
new-leader checks. Unknown outcomes are never retried. The original
retirement-timeout/no-replay regression tests still pass.

Fresh local native acceptance on 2026-10-18 used the pinned Ceph 20.2.4 image
in Linux arm64 containers with secure AES256K client/service keys, IPv4 and
one BlueStore OSD. All six selected tests passed on their first corrected
execution, in this order:

| Subject | Elapsed | Independent checks |
| --- | --- | --- |
| MapVersion/Statfs during leader stop | 17.86 s | Original GlobalID, no auth rejection, native restart/election and map epoch |
| PoolOp during leader stop | 24.11 s | Exactly one allocated snapshot ID, native snap sequence, original GlobalID |
| Full-ratio transition | 20.21 s | FULL at epoch 27, cleared at 30 |
| OSD map delivery | 6.89 s | 28 distinct blobs match native decode and negotiated-feature encode |
| Mutation Close | 0.73 s | Withheld reply, independently observed execution, no request replay |
| Concurrent mutation | 1.13 s | Compound WRITE/APPEND extents/records match native bytes |

The first CI artifacts do not contain the raw map blob files, so the old
manifest cannot by itself establish native revalidation. The fresh daemon
run above supplies the actual native map comparison; the repeated-40-window
deduplication has separate regression coverage. Local arm64 results do not
establish a new amd64 or whole-matrix CI pass.
