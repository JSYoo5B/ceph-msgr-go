# Raw MON filesystem and MDS map subscriptions

Reference: Ceph Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go and
requires no Ceph installation, CLI, native library or CGO. Native development
fixtures do not become runtime dependencies. The exact wire codecs and source
license audit are in [internal/mds/CONTRACT.md](../internal/mds/CONTRACT.md).

## Opt-in and public input

Set `Options.EnableMDSMaps = true` **before Dial**. This adds only MDSENC bit29 to
the supported and required feature masks of the client's main and independently
opened named MON connections. Default MON connections and every MGR/OSD
connection retain their existing feature contracts. There is no downgrade when
MDSENC is missing. Existing MON PGID64, MSG_ADDR2 and complete SERVER_NAUTILUS
prerequisites, together with MDSENC, select the audited current MMDSMap encoding.
No MDS CephX service ticket is requested by this option.

`WatchFSMapUser(ctx, FSMapUserOptions{StartEpoch, MaxBufferedBytes})` creates the
client's single raw `fsmap.user` stream. It provides the directory as received;
it does not select an entry or interpret the legacy-client FSCID as a default.

`WatchMDSMaps(ctx, filesystemID, MDSMapOptions{StartEpoch, MaxBufferedBytes})`
requires a caller-supplied nonnegative signed 32-bit FSCID. Zero is explicit and
valid. Negative sentinel IDs are refused. No name lookup, filesystem choice,
legacy fallback, daemon/rank selection, automatic routing or operation retry is
performed. Both kinds of watch can coexist with each other and MON commands.

`StartEpoch` is inclusive, with zero asking for the current observation. The
native MON subscription cursor is u64, but these map epochs are u32. Only the
first map at a given epoch is accepted: maps below the active requested cursor,
duplicates and decreasing epochs are ignored. The first epoch-zero map is valid.
Ordinary MON replacement re-registers at `max(StartEpoch, highestAccepted+1)`;
this cursor uses accepted maps, including unread maps, rather than maps consumed
by Next. Complete or retained history is not promised. Accepting epoch MaxUint32
queues that map and terminates with `ErrMDSMapEpochExhausted`; it never wraps or
requests a next epoch outside the codec's epoch domain. Local watch/source
identity counter exhaustion also terminates or refuses registration explicitly.

Registration success means local admission, without a server permission or
delivery result. MON may silently decline unavailable filesystem IDs or missing
subscription capabilities. A timeout, empty map or SubscribeAck does not prove
authorization, existence, daemon readiness or a safe CephFS route.

## Binding untagged replies

MFSMapUser has no cluster UUID. A map is accepted only from the watch's admitted,
current, ready MON source; that source's validated MonMap establishes the cluster
context. MMDSMap includes a UUID, which must equal the client's authenticated
cluster UUID, and its outer epoch must equal the inner map epoch.

MMDSMap does **not** echo an FSCID. Its historical map_fs_name is preserved but
cannot bind a filesystem. Before sending `mdsmap.<FSCID>`, the client reserves
that MON session for one numeric watch identity and its requested FSCID. Closing,
canceling or overflowing the watch retains this binding as a tombstone until
the MON session closes. A second MDS watch on that same source returns or ends
with `ErrMDSMapSourceBound`, even if it requests the same FSCID. The implementation
does not force a shared MON reconnect to bypass this guard. Use a fresh Client
for an independent later watch. The tombstone holds no closed watch/queue.

The same active watch can re-register its own topic after recovery restores a
still-live old MON. A failed candidate must not erase that old source's binding.
A fresh MON may acquire the existing watch's same FSCID. Maps from other sources,
unready candidates, closed clients or terminal watches are ignored, with the
source and watch identity checked again after decoding.

FSMapUser always uses the one cluster-wide topic. Closing and opening a new
FSMapUser watch is allowed on the same source. A late same-topic frame from an
older subscription can satisfy the new watch's StartEpoch and be delivered as a
raw observation. Neither message contains a subscription generation or request
ID. `SourceGeneration` is a local MON delivery-context identifier, shared by both
stream kinds on the same source. It is neither a wire generation nor proof that
a notification was generated/sent after a new subscription. It changes when a
new MON source is registered; returning to a still-live old source retains that
source's local identifier. It supplies no MDS session recovery claim.

## Bounds, ownership and termination

Each stream has a FIFO capped at 64 observations. `MaxBufferedBytes` defaults to
`Options.MaxFrameSize` and accepts 1 KiB through 1 GiB. A queued observation is
charged `768 + 8*len(RawFront)` bytes. A linked FIFO reserves each fixed queue
node explicitly without unused backing-array capacity. This conservative
allowance also covers retained front/map storage and current decoded public
slice/string/struct representations; it is not an exact wire-byte quota. The decoder separately
bounds the frame, nested sizes/counts, feature bytes and address vectors before
allocation. Transient decoding remains bounded by those codec limits.

Overflow ends only that stream with `ErrMDSMapWatchOverflow`, preserving every
previously accepted observation and ordinary shared MON commands. Maps are not
silently dropped to keep the stream running. Projection fields preserve unknown
states, flags, compatibility masks/names and required-client feature bytes;
none is interpreted as support for an MDS operation or session capability.

`Next(ctx)` supports concurrent callers and delivers each queued observation
once. An already canceled wait consumes nothing. Canceling that wait leaves the
stream active. The registration context owns the lifetime and MON recovery;
lifetime cancellation is local and supplies no remote cancellation/rollback.

`Close() error` waits for the registration worker. An active explicit close ends
with `ErrFSMapUserStreamClosed` or `ErrMDSMapStreamClosed`. Client shutdown ends
with `ErrClosed`; explicit authentication refusal and fatal source protocol
errors retain their original cause. Accepted FIFO entries drain before the
first stable terminal error. A source failure published before delayed connection
cleanup cannot be replaced by a later close, cancellation, malformed callback,
local overflow or authentication refusal. Closing is not a remote unsubscribe.

Returned RawFront and RawMap own the received bytes; RawMap and raw feature bytes
can view that owned storage. Public metadata has explicit public types and no
internal-package aliases. Treat retained bytes and slices as read-only, or
coordinate any mutation among the caller's users. Public `FilesystemID` in an
MDSMapUpdate is the local explicit binding; `MapFSName` is the received field.

## Evidence and unsupported next steps

Retained native encoder fixtures independently establish exact type103 v1/compat1
and type21 v2/compat1 envelopes plus current map fields. Fresh empty payloads
pass through canonical `Message::encode` once, including its compat normalization;
the constructor-only FSMapUser compat0 is refused. Public projection tests
compare all five native vectors, including unknown metadata, UUID and raw bytes.
Synthetic authenticated MON tests exercise exact topic registration, epoch-zero
acceptance, inclusive/highest cursors, duplicate/decreasing filtering, MON
replacement, old-source rejection, UUID/epoch failures, FIFO/byte limits, local
cancellation, tombstones, max-epoch termination, ordinary command preservation
and delayed-cleanup first-cause arbitration. Session tests cover only-bit29 MON
feature negotiation, unchanged other roles, CRC/secure opt-in and missing-feature
refusal. These synthetic results do not establish live Ceph interoperability.
The separate native MON integration test records its own exact fixture/result;
no MDS daemon is needed to verify raw maps for an empty filesystem.

The [local native acceptance](../integration/fixture/MDS_MAPS_VALIDATION.md)
passed on CRC/AES host IPv4 single BlueStore and secure/AES256K container IPv6
single MemStore. Each run combined this map case with twelve existing OSD cases.
Native fs dump/fs get JSON independently matched UUID, explicit FSCID, names,
epochs, pools and empty rank state; the test retained actual raw maps and checked
cancellation, same-source binding refusal, unknown-FSCID wait, ordinary MON
commands and zero OSD connection attempts. Tickets were allowed to renew. The
filesystem was removed; two owned pools remained for confirmed fixture teardown
under the unchanged pool-deletion policy. Earlier setup, canonical-compat and
invalid ticket-equality test failures remain recorded separately. These local
results are distinct from remote CI and from MDS daemon/session support.

This does not add MDS daemon authentication, transport/session OPEN or metadata
I/O. Pinned `ceph_mds.cc` lines180-190 chooses `stateful_server(0)` for CLIENT;
`msg/async/ProtocolV2.cc` lines2972-2989 requires stateful flags0 and a nonzero
server cookie. The current product service handshake accepts lossy sessions
only; the MDSENC map change does not alter that policy. MDS session required-client
features (`mds/Server.cc` lines585-609 and743-753) are a separate contract and
remain raw map metadata here. CLIENT_SESSION/RECONNECT/REQUEST/REPLY, caps,
leases, snapshots, quota, session replay, rank failover, CephFS and typed
filesystem workflows remain unsupported.
