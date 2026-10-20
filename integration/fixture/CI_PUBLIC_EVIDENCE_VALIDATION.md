# Public container receipts and host diagnostics — 2026-10-15

The container-runtime OSD suites at head `source-1` could finish successfully
and then fail in the host harness while `saveMetadata` opened
`osd-checksum-input.json`. That input was explicitly included in the public
metadata allowlist, but the root-run container test created it with mode 0600.
The unprivileged host collector therefore could not read the bind-mounted
receipt. MAPEXT input/results and watcher results had the same mismatch;
the new PG enumeration, sparse-read and watch/notify receipts also needed
the same publication contract.

`internal/testcluster.WritePublicEvidence` now publishes these non-secret
receipts through a temporary file with explicit mode 0644. It closes the file
before renaming it, including on Windows. Replacing the directory entry also
allows a host rerun to replace a prior container-owned receipt without opening
that earlier file for truncation. The fixture output directory remains private,
and the host collector still creates its diagnostic copies with mode 0600.
Keys, credentials and configuration files do not use this helper and remain
outside the public metadata allowlist.

Local regressions check public mode/content, replacement of the earlier inode,
preservation of an existing symlink target, and temporary-file cleanup on a
failed rename. A fixture source check covers every explicitly named archived
Go-test receipt so a new owner-readable-only publication is rejected. The
collector test checks that the archived copies remain private on POSIX systems.
Both test groups passed with `CGO_ENABLED=0`.

These checks establish the publication policy and local collector behavior.
They do not establish a successful Linux root-container/unprivileged-host run
or a restarted exact-head CI result; those require the next live fixture/CI run.

## Windows receipt identity check — 2026-10-15

Exact-head run `38703760506` at
`source-2` failed only its two Windows unit
jobs. `os.Stat(path)` can load Windows file identity lazily, so comparing that
receipt after atomic replacement can inspect the replacement twice. The test
now captures identity through an open file's `Stat`, then closes the handle
before replacement. Production publication remains unchanged. Focused CGO=0
tests and Windows amd64 test cross-compilation passed; actual Windows execution
is left to the new normal CI run.
