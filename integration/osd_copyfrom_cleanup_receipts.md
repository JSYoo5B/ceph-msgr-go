# Replicated multi-chunk cleanup receipts

The primary-loss, three-chunk-loss, source-change, OMAP-edit and source-removal
runners publish each cell after its cleanups. This includes both copy opcodes,
head/clone cells and pinned/unversioned variants where present. Publication is
registered first with `publishAfterCleanups`; a function `defer` runs before
`testing.T` cleanups and must not copy these receipts.

`chunk_restored` reports the chunk-setting cleanup, separately from the copy's
result and the test's verdict. `chunk_restore_readback` retains the native
`copychunk restore` observation: OSD, action, original/effective values, claim
state and any failures. A true summary requires the correct OSD/action, a positive
original value above the lowered limit, effective value equal to that original,
no failures, the restore acknowledgement and a released claim. A failed or
aborted readback cannot establish restoration; an absent readback remains absent.
The oracle itself reads the effective setting after setting it (or observes that
an OSD restart already restored it), and only then releases the claim.

A failure in the test body still runs restoration and publishes its actual
outcome. Restoration failure still fails the test and publishes false; it is not
silenced to produce a receipt. A skipped cell publishes nothing. These fields do
not prove every other cleanup completed, nor does `chunk_restored=true` turn a
failed copy test into a pass.

The controller-substitute regression serializes each receipt shape after normal
completion, a body failure, an unsuccessful lower acknowledgement, restoration
refusal, inconsistent readback and malformed JSON. It checks the published value
against the substitute's separately retained effective setting. Native results
and temporary failure controls belong in the PR evidence, not this contract.
Older receipts published before cleanup cannot determine whether the setting
was restored from their false summary alone.
