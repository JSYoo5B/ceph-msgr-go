# Explicit replicated PG profile with v8 replies

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source facts are independently
translated from `include/ceph_features.h`, `messages/MOSDOp.h`,
`messages/MOSDOpReply.h` and `msg/Message.cc` (LGPL-2.1); no native implementation
is copied into the Go product. Native builders and Ceph fixtures are development
verification tools and remain outside product/runtime dependencies.

## Closed pre-Dial contract

Select `Options.EnableOSD = true` and
`Options.OSDProfile = cephmsgr.OSDProfileReplicatedPGReplyV8` before Dial. This is
closed enum value2; ObjectHash remains0 and ReplicatedPG remains1. The new profile
requires the existing explicitly targeted replicated contract: caller-supplied
actual PG and matching connection primary, separate raw object hash, replicated
NO_SHARD (-1), v8/compat3 requests, HASHPSPOOL and bounded BACKOFF handling.
See [O4](O4_CONTRACT.md) for those unchanged request/control/placement semantics.

Only the OSD role adds NEW_OSDOPREPLY_ENCODING bit58 to supported **and required**
features, on top of the fixed existing replicated mask. Missing bit58 fails
negotiation rather than downgrading replies. Both replicated profiles declare
the same HASHPSPOOL capability on MON. The existing MON-only CRUSH admission
exception is unchanged and is never copied to OSD. MGR and the independent
MON MDS-map opt-in retain their own feature policies; no MDS daemon feature or
service ticket is introduced. No arbitrary feature-mask API exists.

Bit58 also names CRUSH_TUNABLES5 in the pinned feature header. Negotiating it
selects the implemented reply encoding; it does not implement CRUSH evaluation,
automatic placement or compatibility with every map/pool topology. Native
acceptance for this profile is limited to the controlled legacy-CRUSH fixture
recorded below. Default optimal CRUSH, EC, MSR, SERVER_SQUID/v9,
primary changes, upmap routing and general pool compatibility are not established
by this feature opt-in or the offline vectors.

## Strict reply selection and preservation

Both public `Request` and dedicated `ListPG` use one profile-selected decoder.
ObjectHash and ReplicatedPG accept exactly v6/compat2 replies. The ReplyV8 profile
requires negotiated bit58 and accepts exactly v8/compat2 replies. The received
header cannot select a different decoder. There is no v6 fallback, version7/9
acceptance, or compatibility downgrade. Request-side v8 framing still refuses
SERVER_SQUID's complete mask and malformed incarnation masks; permitting bit58
changes only the negotiated-feature tail in the existing v8 request encoding.
The default v6 request encoder continues refusing it.

The v8 reply preserves object, pg_t, u64 flags, signed aggregate result, legacy
version, map epoch, operation unions, output lengths, retry attempt, signed
per-operation results, replay version, user version, redirect and three-u64
trace. Its distinct tail has a boolean followed by a redirect structure only
when true; the v6 unconditional redirect structure is not accepted as v8.
Nonempty middle data, truncation, invalid booleans, trailing bytes, unsupported
operations, inconsistent output lengths and configured frame limits fail.
Raw front/data and public metadata remain available on decode/validation errors
where decoding produced them; owned decoder storage does not alias peer inputs.

The reply does not carry reqid, client incarnation or an spg_t shard. Public
submission metadata therefore retains the original authenticated GlobalID,
transaction, client incarnation, epoch and placement locally. Reply identity
validation checks object, pool, **actual PG seed**, operation vector and retry
attempt0. It does not fabricate identity fields absent from the wire.

The same operation result validation applies to v6 and v8. Signed negative
aggregate/per-op status remains raw; a successful vector containing a failed
ordinary operation is contradictory unless that requested operation has FAILOK.
A successful mutation requires the requested ONDISK completion; ACK-only is not
a durability receipt. Unknown negative aggregate codes remain `*OSDError` values.
Failed replies can retain earlier operation output and nonzero legacy, replay
and user versions. A PG-log-only refusal may carry the current user version in
legacy/user fields and a distinct next replay version. A failed write sets
`update_log_only`, bypasses the direct read/error completion branch, and has
its reply versions populated in `PrimaryLogPG::execute_ctx` before the PG-log
error path (`PrimaryLogPG.cc:8991,4316-4352`, `MOSDOpReply.h:65-82`). Preserve these observations without requiring zero or
equating the three versions. Neither raw versions nor ONDISK on a negative
aggregate establish a committed object change, restart durability or proof that
an operation did not execute; independent postconditions remain separate.
Malformed or contradictory replies fail the OSD session and preserve
`OutcomeUnknownError` for the sent request. A valid redirect returns
`ErrOSDRedirect` with its raw/decoded metadata and is never followed.

## Admission, recovery and replay boundary

A common fixed capability predicate enables placement, the single-writer BACKOFF
admission hook, type61 dispatch, bounded control queue, explicit ACK and PGNLS v8
for **both** replicated profiles. Selecting a different reply codec must not
accidentally bypass those capabilities. The gate refuses queued unstarted work
only when its actual PG/shard and head hobject match a retained restriction;
unrelated work can proceed on the same connection. UNBLOCK changes local
admission only; it does not resend refused or uncertain work.

Target ID/address, maps, epochs and placement remain caller-owned inputs. The
library neither refreshes placement nor reroutes on redirects, PG changes,
connection loss or malformed replies. A canceled local wait does not cancel
remote execution. Sent mutation results retain their unknown outcome and
original submission identity; no new session or reply format triggers replay.
A caller may explicitly submit new work under its own routing/recovery policy.

## Evidence and limits

The retained `reply-v8-native-{success,enoent,unknown-error,mutation-ack,redirect}`
fixtures were freshly constructed by the independent native wrapper and separately
decoded by native code. Existing producer/source/configuration/library/binary and
per-segment provenance remains unchanged by activating this public profile.
The profile's public projection tests compare all five native replies, signed
results, completion flags, operation output, versions and conditional redirect.
Five pre-existing fresh native v6 fixtures remain accepted by both old profiles
and rejected by the new profile. Version/compatibility mutations and a missing
negotiated bit58 are refused without fallback.

Authenticated synthetic public tests exercise v8 requests/replies, ONDISK and
ACK-only mutations, FAILOK/ordinary failed guards, signed server errors, explicit
request identity, invalid versions/compatibility, a wrong actual PG, disconnection,
conditional redirects, PGNLS pages/end and strict v6 refusal, and BACKOFF
BLOCK/ACK/admission/UNBLOCK. They confirm no automatic OSD redial, redirect follow
or mutation replay. Synthetic tests are transport/policy evidence, not live Ceph
interoperability.

The [actual daemon record](../../integration/fixture/OSD_REPLY_V8_VALIDATION.md)
uses legacy CRUSH, HASHPSPOOL and eight PGs with one OSD per fixture. In CRC/AES
host IPv4 BlueStore and secure/AES256K container IPv6 MemStore, the thirteen
existing modern-profile cases each passed in prior `*-final` runs. Each overall
run nevertheless failed a new test's incorrect requirement for zero versions
on a signed refusal; its other two new cases did not run. The product decoder
was unchanged when the source-backed expectation was corrected.

The three new cases then passed in independent focused runs: CRC 2.78s and
secure 3.45s. They cover signed refusal with earlier READ output, FAILOK metadata
and allocation-hint completion, bounded xattr/OMAP output and standalone empty
watcher listing. Their logs are `/private/tmp/ceph-osd-reply-v8-crc-aes-focused.log`
and `/private/tmp/ceph-osd-reply-v8-secure-aes256k-ipv6-focused.log` (evidence
folders share the names without `.log`). Earlier setup/runtime failures remain
separate retained evidence. These are separate thirteen-case and three-case
results, without a combined sixteen-case all-pass claim. Full final CI is a
separate result. This does not expand acceptance to default CRUSH, EC, every
operation family or broader security/topology combinations; MemStore completion
does not establish restart durability.
