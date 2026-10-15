# Bounded Tentacle watcher listing

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, audited 2026-10-15.
This is the LIST_WATCHERS subset of O3, not watch registration or event support.

## Request and reply

`LIST_WATCHERS` is `0x1209`, a READ operation. The pinned
[Objecter builder](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L1467)
adds a zero 28-byte union and no input data. The existing MOSDOp v6 request
carries the caller's explicit object, locator, raw hash and map epoch. It
adds no new session feature. No discovery, routing, retry or registration is
performed. The product accepts only a standalone head listing, with no op
flags, snapshot context, mtime or other active inputs. A local output budget
of at least ten bytes is required; the budget has no wire field and cannot
limit what the server sends.

The pinned
[handler](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L6544)
reads the committed object's watcher map and returns
`obj_list_watch_response_t` (v1/compat 1). Its list holds `watch_item_t`
(v2/compat 1): entity_name_t (u8 type, signed i64 number), cookie u64,
timeout_seconds u32, entity_addr_t. The output decoder bounds the collection
against the actual bytes before allocating. It accepts current entry v2 and
newer compatible versions with appended fields; it rejects the obsolete
address-less entry v1. Address type, nonce, wire IPv4/IPv6 family, flow/scope
metadata and an unspecified address are preserved. Entries stay in server
order. Raw listing bytes and ordinary reply identity/versions remain available.

A successful operation requires exactly one well-formed bounded listing and
a zero echoed union. A failed listing must have no output; its signed server
result is preserved. A successful empty listing occupies ten bytes. Malformed
or over-budget replies fail that connection with the existing uncertain-call
error contract, preserving submission identity and raw reply. Canceling a local
wait or closing a connection never reissues a listing. Other OSD connections
remain independent.

This is an observation of registrations at one request's execution time.
The returned entity belongs to the watcher, not the observing client. Its
timeout is the registered timeout, not remaining life or a liveness guarantee.
No subscription generation or local handle is inferred from its cookie.

## Verification and limits

[Native vector provenance](testdata/watchers-native-provenance.json) records
five fresh message encoder/decoder vectors: request, two IPv4/IPv6 entries,
empty listing, ENOENT and EACCES. The listing itself was also encoded and
separately decoded by pinned native classes. Explicit offline transport
sequence/ACK inputs do not prove a live Messenger session. Go tests compare
request front/data/header, reply metadata and public results with those bytes.
Malformed lengths/counts/versions, unknown trailers, inactive inputs, budgets,
cancellation/Close and no replay have separate unit/synthetic/fuzz coverage.

[Actual daemon acceptance](../../integration/fixture/OSD_WATCHERS_VALIDATION.md)
compares native Python librados registrations and CLI lists with Go replies.
The native helper uses its own fixture-owned object and token-scoped worker;
it unregisters both watches before removing the object and bounds a worker to
five minutes even after a failed test. It is a development-only dependency.

WATCH, NOTIFY, NOTIFY_ACK, MWatchNotify, reconnect/renewal and backoff remain
unsupported. PG enumeration, snapshot/compound listing semantics, read-only
credentials, removal races, stale registrations and broad profile coverage
need separate contracts and evidence. Full O3 acceptance is still open.

## Provenance

The referenced Objecter/PrimaryLogPG and
[watcher structs](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L6684)
carry LGPL-2.1 notices; include/rados.h permits LGPL-2.1 or LGPL-3.0. The Go
codec independently translates wire semantics, with no copied C++ implementation.
The development wrapper constructs native objects and calls unchanged pinned
encoders/decoders. No native code or library enters the product dependency graph.
