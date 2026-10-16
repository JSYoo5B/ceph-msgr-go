# Native proof / watcher fixture portability correction — 2026-10-15

Exact original head `837ea8b64c36a40b595ba4e6769d6db7e8ba0da0`,
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

## Follow-up from ab2198c

Run `38698420954` at `ab2198c27a939d295d4e8db487fe64d073c501bf`
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

Run `38890511851` at `1b84aa13db0fc797f2048427a02f1ceaed4b5e93`
failed both Windows Go-version unit jobs in the request-v9 SDK proof model.
Its text writer translated the modeled Linux CMake cache to CRLF, whereas
the native Linux producer correctly requires exact LF bytes. Commit
`04b58b47a23439b6d6cf19710fc543a83f7dc158` writes those modeled files as
explicit bytes, verifies their bytes and hashes, and retains rejection of
CRLF, enabled Jaeger and defined HAVE_JAEGER. The native producer and its
historical proof artifacts are unchanged.

The complete Python suite passed 413 tests locally. Run `38893912494` at
that exact correction SHA passed Windows unit jobs `116949721132`
(Go 1.27.x) and `116949721304` (Go 1.24.0), plus Windows fixture tools job
`116949721240`. These completed jobs establish this Windows correction;
they do not establish completion of the whole native matrix.
