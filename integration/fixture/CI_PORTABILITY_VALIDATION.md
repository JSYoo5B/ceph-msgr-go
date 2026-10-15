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
