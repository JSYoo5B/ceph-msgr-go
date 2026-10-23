# CephFS client session acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI and the owned MDS are
development-only oracles. This acceptance covers the public
`MDSConnection.OpenSession`/`MDSSession` API of
[the session contract](../../cephmsgr/MDS_SESSION_CONTRACT.md): open, renewal,
stale resumption, confirmed close, explicit REJECTs and native eviction. It
adds no metadata request, capability, reconnect or file I/O. The owned recipe
and its cleanup are described in [MDS_SESSIONS_FIXTURE.md](MDS_SESSIONS_FIXTURE.md).

## Test shape

`TestCephMDSSessionsIntegration` skips unless `CEPH_MSGR_TEST_MDS_SESSIONS=1`
and requires MDS_FRESH=1, no MDS_COMMANDS and the `legacy-v6` OSD recipe. Six
independent Go clients (A to F) each enable only `Options.EnableMDS`, dial MDS
through a counting wrapper that refuses any OSD dial, and open one explicit
`MDSTarget` built from the owned native map: FSCID, rank 0 and the exact v2
`address/nonce`. Clients A to D and F use the read-only fresh identity
(`allow r fsname=<owned fs>`); E uses the foreign identity. Each session sends
metadata `entity_id` (identity without `client.`), `hostname`
(`ceph-msgr-go-<token prefix>[-suffix]`) and `root=/`.

Every native observation and mutation receipt must match the startup oracle:
FSID, FSCID, filesystem name, daemon GID, rank 0, endpoint, nonce and
incarnation, re-read after the action. The daemon's applied MDSMap epoch must
be at least the map epoch in each receipt.

The nine sequential leaves:

| Leaf | Checked behavior |
| --- | --- |
| `open_native_session` | A opens with a 2 s RenewInterval. Go status is Open, generation 1, connection generation 1, GlobalID equal to the client's, unchanged target, push sequence 0, no read-only flag. OPEN sequence, flags and metadata are empty. OPEN supported features and metric flags equal the native vector images `ffffff0000000000` and `ffff000000000000`. Cap auths are one grant: UID -1, no GIDs, empty path, the owned filesystem name, readable, not writeable, no root squash. Native `session ls` shows A `open`, CephX auth `client`/`mds-fresh-<token>`, 0 caps and leases, `client_features` `0x00000000000001ff` (also derived from `MDSClientFeatures()`), metric flags `0x0` and exactly the three metadata keys sent |
| `periodic_renewal` | `LastRenewal.Sequence` advances by three in 2 s periods; each sample has Requested <= Confirmed, strictly increasing sequences and confirmations, and at least 1 s between requests; at most one renewal outstanding; A stays natively `open` |
| `stale_and_automatic_resume` | Owned stale window applied (timeout 30 s, autoclose 120 s, daemon `mds_defer_session_stale=false`). B (second client, same identity, distinct GlobalID) opens with a 1 h RenewInterval. Its first event is STALE, at least 25 s after its last confirmed renewal. The library's automatic renewal returns B to Open with `LastRenewal.Sequence == RenewSequence >= 2`, requested after the STALE receipt. A, sampled every 200 ms, never leaves Open and has no queued event. Both are natively `open`; the window is restored and verified |
| `confirmed_close` | `Close` on A and B returns nil; Done is closed; state Closed; `Err` is `ErrMDSSessionClosed`; neither GlobalID remains in `session ls` |
| `reject_refused_sessions` | With owned `refuse_client_session=true`, C's OpenSession returns `*MDSSessionRejectedError` (flags 0, not blocklisted, reason equal to `error_string`, not `ErrMDSSessionActive`); C is absent natively. After restoration, the same handle opens a generation-2 session, closes it confirmed, and C is absent again |
| `reject_missing_required_feature` | With owned `reply_encoding` required (only `feature_9`), D's OpenSession is an explicit REJECT and D is absent natively; the requirement is removed and the map returns to its empty baseline |
| `reject_foreign_filesystem_caps` | E (foreign identity) authenticates to the MDS; OpenSession is an explicit REJECT whose reason is exactly `client doesn't have caps for FS "<owned fs>"`; E is absent natively |
| `native_eviction_is_lost` | F opens and renews; native `session evict id=<gid>` removes it and blocklists exactly its observed `IP:port/nonce`. Locally Done closes, state is Lost, `Err` is not `ErrMDSSessionClosed`, `Close` wraps `ErrMDSSessionActive`, the MDS transport ends, and `OpenMDS` to the same target on that client returns `ErrMDSSessionActive` without dialing |
| `mds_only_dials` | Each of the six clients dialed the MDS exactly once and never an OSD |

## Results

Both selected configurations passed on 2026-10-16 (KST), all nine leaves, no
skips. The tested source is HEAD `source-1`
plus only this change set, exported to a separate directory with
`git archive`; other agents' uncommitted product edits in the shared checkout
were excluded. Acceptance source SHA-256: `integration/mds_sessions_test.go`
`e177b5b4718088085a613c9d1e9c991534d04342172706153d8728d70ecdffa6`,
`integration/fixture/mds_sessions.go`
`d14d1014c5e11a41c081f3dfe3f48b0be5db481253f1eef0f2a386a53dda3d33`.

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(image ID `sha256:6bb1c8a4...8fd8f9`, arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.

| Configuration | Controller wall time (UTC) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 16:04:13 to 16:06:59 (2 min 46 s) | 80.37 s | 9/9 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 16:07:07 to 16:09:50 (2 min 43 s) | 80.09 s | 9/9 passed |

Leaf durations (CRC / secure, seconds): open 1.61 / 1.60, renewal 6.01 /
6.03, stale and resume 44.31 / 43.98, close 1.51 / 1.60, refused 9.83 / 9.61,
required feature 8.42 / 8.71, foreign caps 1.61 / 1.61, eviction 7.04 /
6.93. Each native controller action took 1.4 to 1.6 s for an observation and
about 3 to 6 s for a mutation including its settle wait.

### Observed wire and native values

Both runs observed the same values:

- Declared client features (raw): `ff01000000000000`; native
  `client_features.feature_bits`: `0x00000000000001ff`; native metric flags:
  `0x0`. The kv metadata was exactly the three keys sent.
- Live OPEN supported features `ffffff0000000000` and metric flags
  `ffff000000000000`, byte-equal to the native vector
  `mds-session-native-open` (encoded with `CEPHFS_FEATURES_MDS_SUPPORTED` and
  `CEPHFS_METRIC_FEATURES_ALL`). OPEN flags 0, sequence 0, empty metadata.
- Cap auths: `[{UID:-1 GIDs:[] Path:"" FilesystemName:<owned fs> RootSquash:false Readable:true Writeable:false}]`.
- STALE arrived 30.567 s (CRC) and 31.243 s (secure) after B's last confirmed
  renewal; B was Open again 22 ms and 20 ms after the STALE receipt, with
  renewal sequence 2. A reached renewal sequence 26 before its close.
- Native addresses: IPv4 `172.19.0.9:0/<nonce>` (the host-runtime relay's
  address, port 0) and IPv6 `[fd62:93fe:2c9e::9]:0/<nonce>`; each MDS
  connection had its own nonce.
- MDSMap epochs moved 7 (baseline) to 15 through eight owned map mutations;
  the daemon had applied each epoch before the next request.

### Verbatim native REJECT reasons

| Case | `error_string` |
| --- | --- |
| refused sessions | `` new sessions are not permitted, enable again via `ceph fs set <fs_name> refuse_client_session false` `` |
| required feature | `missing required features '0x0000000000000200'` |
| foreign caps | `client doesn't have caps for FS "ceph-msgr-fresh-<token>"` |

All three had flags 0 and no SESSION_BLOCKLISTED bit. The refusal text keeps
the literal `<fs_name>` placeholder from the pinned source.

### Eviction

The eviction receipt reached state `evicted` with the exact address
(`172.19.0.9:0/99618250`, until `2026-10-15T17:06:35.572566Z`;
`[fd62:93fe:2c9e::9]:0/2642885229`, until `2026-10-15T17:09:26.109575Z`). The
Go session became Lost 4.0 s and 3.9 s after the request was issued (the
controller's own confirmation polling is included). The local terminal cause
was `io: read/write on closed pipe`: the test routes MDS dials through an
in-process stream relay, which closes its pipe when the MDS closes the TCP
connection. The cause text is therefore shaped by the test harness; the Lost
state, the `ErrMDSSessionActive` close result and the address guard are the
verified product behavior.

### Cleanup verification

Both `mds-sessions-cleanup.json` receipts report `completed: true`: final map
equal to the baseline (refusal false, no required features, timeouts 60/300),
no reconciliation needed for either flag, daemon `mds_defer_session_stale`
back to `true`, no central `mds_defer_session_stale` entry, exactly the one
eviction entry removed, an empty final blocklist, no unowned new entries and
the foreign identity deleted. Both `mds-fresh-cleanup.json` receipts report
`sessions_restored`, filesystem, pools and fresh identity removed and
`completed: true`. All nine owned containers and the fixture network of each
run were confirmed absent with `docker inspect` afterwards. Ryuk containers
and containers of other concurrently running fixtures are not counted.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-sessions-results.json` | `93fdb1e61f4d2b38430cbf9ebdbd4da0837b0edf101d26d8acca8de7e9b636ce` | `f1ae3d27a8841b9ecd017e7ad4790b6a4a23897cc54c81c6f4fd220c7036ca90` |
| `mds-sessions-cleanup.json` | `b3d3e28778792ec83ff2d50666f235b93097a3eb8843e27156b14562a24bb090` | `1bda806f0c5e9bd125e885636ea03063d4afd3371f0703b7c7756bac508df365` |
| `mds-fresh-cleanup.json` | `0573f2fd86bd9aec4ce3df1c687cab78a7b57e1dd486ff725f33521177e79155` | `8518eb87510185197a693ec9f6f3ec8f9a7d56f75217f46133e230e129fa0f6c` |

## Earlier attempts

The first CRC run (`ceph-mds-sessions-crc-aes-native1`) failed in
`periodic_renewal`: no renewal after sequence 1 was acknowledged within 10 s.
A diagnostic rerun showed the session already Closed with
`ErrMDSSessionClosed`. The test had registered A's safety `Close` on the
first leaf instead of the parent test, so the session was closed, confirmed by
the MDS, at the end of the open leaf. This was a test defect, not a product or
native behavior; sessions spanning leaves now register their safety close on
the parent test. Both of these runs completed cleanup (`completed: true`).

Two further passing runs (`ceph-mds-sessions-crc-aes-native2`,
`ceph-mds-sessions-secure-aes256k-ipv6-native1`) were built from the shared
working tree while other agents had uncommitted product edits there. They
match the recorded results but are not the acceptance record.

## Findings and limits

- With the native default `mds_defer_session_stale=true`,
  `find_idle_sessions` never marks a session without capabilities stale.
  Without renewals such a session stays `open` until `session_autoclose` and is
  then evicted directly, with blocklisting under the default
  `mds_session_blocklist_on_timeout=true`. STALE was observed here only with
  the owned `mds_defer_session_stale=false` override.
- Adding a required client feature makes the MDS evict and blocklist every
  open session lacking it; the fixture refuses that mutation while sessions
  exist.
- Not verified live: RECALL_STATE, FLUSHMSG and its acknowledgement,
  FORCE_RO, a nonzero push sequence on close, OPEN with a nonzero sequence,
  root-claim and duplicate-uuid REJECTs, the SESSION_BLOCKLISTED REJECT (a new
  connection always has a new nonce, so this client cannot present a
  blocklisted address), server CLOSE before OPEN, canceled OPEN with
  `OutcomeUnknownError`, close-time context expiry, autoclose or timeout
  eviction, standby/failover, multiple ranks, reconnect and capabilities.
  These remain covered only by synthetic peers or not at all.
- These are local selected-configuration results, separate from remote CI.

## Autoclose eviction (2026-10-18)

Leaf `autoclose_eviction_is_lost`. The fixture gained an `autoclose-on`
action: after the stale window is restored it applies session_timeout 30 s
and session_autoclose 40 s (both at least the native minimum of 30 s) with
`mds_defer_session_stale=false`, journaled as `autoclose_window` and restored
by `stale-off` (also on cleanup). A Go session renewing every 2 s is opened
through a connection wrapper that, once armed, holds all reads and writes, so
the MDS sees a silent but open connection. Its native session is observed
first, which registers the address as owned for blocklist cleanup.

About 46 seconds after silencing, native observation shows the session gone
and its address blocklisted (`mds_session_blocklist_on_timeout`, default
true). When traffic resumes, the session ends locally as Lost with the
transport error (`write pipe: i/o timeout`), never as a confirmed close, and
it is not resurrected. Passed in CRC/AES BlueStore IPv4 and secure AES256K
MemStore IPv6; the dial-count check now expects nine session clients.

## Required reclaim_client admission (2026-10-23)

Leaf `admit_required_reclaim_client`, added when both client profiles began
declaring RECLAIM_CLIENT (bit 10; decision record
`internal/mds/CLIENT_EXTENSION_DECISIONS.md`). The fixture gained the closed
`require-reclaim-on` / `require-reclaim-off` actions
(`fs required_client_features <fs> add/rm reclaim_client`, journal kind
`required_client_features_reclaim_client`). With only `feature_10:
reclaim_client` required, client I (session profile) and client J
(`EnableCephFS`) each open a session with the read-only fresh identity. Both
OPENs advertise RECLAIM_CLIENT. Native `session ls` shows I `open` with
`client_features` `0x00000000000005ff` and J `open` with
`0x00000000001c87ff`, each with exactly the metadata sent. Both close
confirmed and are absent natively, and the requirement is removed back to the
empty baseline. The dial-count check now expects eleven session clients.

Tested source `3bfcf53687e04afb56f1e27acccbc124268dbdb5` (PR #55), GitHub
Actions run 39885092766. Both `mds-sessions` rows passed (CRC/AES host IPv4
BlueStore and secure AES256K container IPv6 MemStore). In the same suite and
run, `reclaim_evicts_the_target_session` passed with the new declaration.
The same run passed both rows of every `mds-cephfs*` group (`mds-cephfs`,
`-ranks`, `-ranks-replay`, `-completion`, `-data`, `-capsend`), whose shared
native-session check now requires `0x00000000001c87ff`.

Not verified: an open session declaring bit 10 surviving a newly added
`reclaim_client` requirement (the fixture refuses the mutation while sessions
exist), and requirements that combine several bits.
