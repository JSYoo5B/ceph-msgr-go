# Independently observed exact MON Tell mutation and no replay

This is the third bounded implementation checkpoint of #63, after the
[MON](MON_COMMAND_OUTCOME_VALIDATION.md) and
[MGR](MGR_COMMAND_OUTCOME_VALIDATION.md) cells. It is not complete
command-channel acceptance. The product API and replay behavior are unchanged.
Current `MonTell`/`MgrTell`, exact `MgrTellTo` and targeted OSD/PG/MDS
applied-mutation cells remain open in
[the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Python librados is a development-only independent observer inside the
disposable native image.

## Pinned source facts

- `src/mon/Monitor.cc` `Monitor::handle_tell_command` authorizes an MCommand
  and queues it to the admin-socket command handler of the MON process.
- `src/common/ceph_context.cc` registers the daemon-local `config set`,
  `config get` and `config unset` handlers. They take `var` and, for set, a
  `val` string array. They are distinct from the MON configuration database
  commands (`config set`/`config get` with `who`), which this cell does not use.
- `src/common/config.cc`: a runtime set or unset changes the `CONF_OVERRIDE`
  source of the option in that process; the configuration diff exposes the
  override and the final value per option. The state is process-local.
  A restart or replacement of the MON clears it and therefore cannot prove the
  no-replay comparison, so this cell never restarts the target MON.
- `src/common/options/global.yaml.in` and `src/common/ceph_context.cc`:
  `log_max_recent` is a positive integer applied by the logging observer. It
  sets in-memory crash-log retention, so the cell only raises it, by one or two
  slots, and never shrinks it.

The daemon-local reply shapes used by the oracle (`config get` returning an
object keyed by the option name, `config diff get` returning a `diff` object
keyed by option name with `override` and `final` members) are read from the
raw replies and are first confirmed by a native run on the pull request; the
oracle refuses to record a baseline whose replies do not parse as expected.

## Executable boundary

Selector: `TestCephAppliedMonitorTellIsNotReplayedIntegration` in the
`cephmsgr` suite. It uses the existing ordinary three-MON/two-MGR fixture; no
new selector opt-in or catalog row is introduced. Initial target profile:
`mon-mgr.aes.ip4.crc.host` (Linux/amd64, CRC, AES client and service, IPv4,
host test process and owned relay). Reproduce with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephAppliedMonitorTellIsNotReplayedIntegration$' \
sh integration/run.sh
```

The owned target is MON `b`. Every native observation opens one fresh
authenticated librados handle and sends named-target `mon_command` requests
(target `b`, a Tell command) in this order: `mon_status`, `config get`
var=`log_max_recent`, `config diff get` var=`log_max_recent`, `mon_status`.
The two `mon_status` reads bracket the views; a receipt is accepted only when
both report the same daemon name and rank. The `config get` result keeps its
signed code, original bytes (base64) and status. `mon_status` and the diff keep
their signed code, status, byte count and SHA-256; only the daemon name, rank
and state and the owned option's diff entry (source names, override presence
and value, final value) are persisted. No full configuration dump is requested
or persisted. If the daemon answers `config diff get` with more than the owned
option, the unrelated entries stay in worker memory only; the receipt keeps the
reply's top-level key names and entry count.

1. Generate a 128-bit random token. The initial observation captures the
   baseline effective value and runtime override presence before any mutation.
   The worker exclusively records a baseline only when the daemon identity is
   the target, the effective value is in the bound `1..19998`, and the diff
   shows either no override or one override equal to the effective value.
   Candidate values are A=baseline+1 and B=baseline+2, so B never exceeds the
   ceiling of 20000. A baseline outside the bound is refused by both the worker
   and the test (`monTellOutcomeValues`), and nothing is mutated.
2. A separate Go session reads the baseline through `MonTellTo` and must agree.
3. The subject client submits A once through `Client.MonTellTo`, padded to
   40 KiB of JSON whitespace. An exact-target gate arms receive withholding
   only for the first Tell command (message type 97) written to the connection
   whose fixture role is `mon.b`. It arms before the write, so a reply cannot
   slip through, and it never inspects another role or message type. In CRC
   mode it decodes complete validated frames; in secure modes, where frames
   cannot be decoded, it falls back to a padded-size threshold (unverified).
4. Repeated fresh native reads must show, within one `mon_status` bracket,
   effective A and an override A while the original caller is still pending
   and the blocked-receive signal has fired. Only reads repeat; a third value
   fails. In CRC mode the test-only `net.Conn` wrapper, which now also records
   Tell (97) frames beside MON (50) and MGR (0x709) commands, binds the
   original TID, sequence and `mon.b` role to the exact request front/data
   digest and a local connection ordinal. The padded local write count must be
   one.
5. A second Go client explicitly establishes distinct B with `MonTellTo`.
   Fresh native reads must confirm effective B and override B before anything
   else happens.
6. Only the subject connection and its wait end (the withheld wrapper is
   closed). The MON process is neither restarted nor replaced. The original
   caller must return `OutcomeUnknownError` with no acknowledged result. Fresh
   exact-target Go sessions (the subject client and the independent client,
   each opening its own Tell connection) and a fresh native handle must read B,
   with the same daemon name and rank. The original identified mutation keeps
   exactly one complete local write, and the fresh read uses another connection
   to the `mon.b` role. A later observation never acknowledges A retroactively,
   and local writes are not server execution counts.
7. Because the override is process-local, the same MON incarnation is inferred
   from the surviving override B together with the stable name and rank. This is
   not a process ID comparison.
8. Restoration has its own fresh bounded context. The worker exclusively marks
   restoration started, then submits exactly one native Tell mutation chosen
   from its own baseline receipt: `config unset` when no override existed,
   otherwise `config set` of the one original override. It publishes the signed
   result, bytes and status, then takes fresh native views. The test requires
   signed `0` and repeats only read observations until the effective value and
   override presence equal the baseline. A timeout keeps the started marker and
   uncertainty; restoration is never retried, and the test fails.

The controller validates request IDs, closed phases (`initial`, `applied`,
`overwritten`, `recovered`, `restore`, `restored`) and the target (`a`, `b` or
`c`), places each request atomically, runs the Python process under
`timeout 30`, and publishes its request-specific acknowledgement only after
atomic receipt publication. The external timeout is necessary because pinned
Python `mon_command` ignores its timeout argument. The worker reuses the MON
oracle's atomic publication and bounded raw result encoding. It has no
arbitrary command, option, value, target or output path, and the Go helper
rejects an invalid token, phase or target before publishing a request. The
fixture metadata allowlist includes `mon-tell-outcome-native.jsonl` and
`mon-tell-outcome-identity.json`, so failure diagnostics can retain the
original observations, partial identity progress and restoration uncertainty.

## Daemon-free checks

- `tools/test_mon_tell_outcome_oracle.py`: closed inputs, exact-target
  addressing, option-only persistence even for a wider diff reply, override
  presence separate from the effective value, the `1..19998` bound, baseline
  ownership by token and target, and a single restoration.
- `cephmsgr/mon_tell_outcome_receipt_test.go`: the candidate bound, the closed
  commands, exact reply decoding, rejection of open requests before anything is
  published, receipt state matching, Tell identity recording and the gate arming
  only for the exact target and message type.

## Execution evidence

No native execution is recorded in this file yet. Exact-head CI receipts are
recorded on the pull request that introduces this cell. No exactly-once server
execution, secure-frame transaction capture, other profile, restart of the
target MON, current `MonTell`/`MgrTell`, exact `MgrTellTo` or targeted
OSD/PG/MDS applied mutation, or completion of #63 is claimed.
