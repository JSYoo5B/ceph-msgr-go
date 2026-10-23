# Independently observed MGR applied mutation and no replay

This is the second bounded implementation checkpoint of #63, after the
[MON cell](MON_COMMAND_OUTCOME_VALIDATION.md). It is not complete
command-channel acceptance. The product API and replay behavior are unchanged.
Current/exact Tell and targeted OSD/PG/MDS native applied-mutation cells remain
open in [the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Python librados is a development-only independent observer inside the
disposable native image.

## Pinned source facts

- `src/pybind/mgr/balancer/module.py`: `balancer mode` validates the mode and
  calls `set_module_option("mode", ...)`; `balancer status` reports
  `get_module_option("mode")` as the `mode` JSON field. The option default is
  `upmap` and its allowed values are `none`, `crush-compat`, `upmap`, `read`
  and `upmap-read`.
- `src/mgr/PyModule.cc` `PyModuleConfig::set_config` sends
  `config set` (or `config rm` for no value) with who=`mgr` and
  name=`mgr/balancer/mode` to the MON, waits for its result, and only then
  updates the active MGR's module cache. A native MGR view of mode A
  therefore follows the MON commit of A.
- `src/mon/ConfigMonitor.cc` `config get` with a key returns the stored value
  or, when no entry exists, the option default followed by a newline. An
  effective-value read alone cannot establish whether an explicit entry
  exists. `config dump` lists stored entries with section, name, value and
  mask; the oracle uses it for presence.
- `src/common/config.cc` `md_config_t::set_mon_vals` delivers `mgr/` keys
  present in a MON config update to the MGR callback
  (`src/mgr/MgrStandby.cc`), but does not invoke that callback for a key that
  disappeared. A native `config rm` therefore restores the MON store, while
  the running MGR's cache is expected to keep its last value until the module
  configuration is reloaded. This is source inference only.

## Executable boundary

Selector: `TestCephAppliedManagerMutationIsNotReplayedIntegration` in the
`cephmsgr` suite. It uses the existing ordinary three-MON/two-MGR fixture; no
new selector opt-in or catalog row is introduced. Initial target profile:
`mon-mgr.aes.ip4.crc.host` (Linux/amd64, CRC, AES client and service, IPv4,
host test process and owned relay). Reproduce with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephAppliedManagerMutationIsNotReplayedIntegration$' \
sh integration/run.sh
```

Every native observation opens one fresh authenticated librados handle and
records, in order: the active MGR name/GID from `mgr dump`, `mgr_command`
`balancer status`, `mon_command` `config get` who=`mgr`
key=`mgr/balancer/mode`, the `config dump` entries with that exact option name,
and `mgr dump` again. The two identity reads bracket the views; a receipt is
accepted only when both report the same available active name and GID. The
balancer status and config get results keep their signed code, original bytes
(base64) and status. `config dump` and `mgr dump` keep their signed code,
status, byte count and SHA-256; only the owned option entries and the active
name, GID, epoch and availability are persisted, never the full configuration
or map.

1. Generate a 128-bit random token. The initial observation captures the
   baseline effective mode and the explicit entry set before any mutation. The
   worker exclusively records a baseline only when the entry set is empty or is
   exactly one unmasked who=`mgr` entry, and the MGR mode is a pinned allowed
   value. The test also requires the MON effective bytes to equal the MGR mode
   followed by a newline, and an explicit baseline value to equal that mode.
2. Choose a different mode A (`upmap`, or `none` when the baseline is
   `upmap`). Arm receive withholding on the first MGR connection and submit A
   once through `Client.MgrCommand`, padded to 40 KiB of JSON whitespace.
3. Repeated fresh native reads must show, within one bracket of the original
   active GID, MGR mode A, MON effective `A\n` and exactly one explicit
   who=`mgr` entry A, while the original caller is still pending. The blocked
   receive signal must also fire. Only reads repeat; a third mode fails.
4. In CRC mode the test-only `net.Conn` wrapper from the MON cell decodes
   complete validated CRC frames, now recording MGR command messages (type
   `0x709`) as well as MON commands. It binds the original TID, sequence and
   fixture MGR role to the exact request front/data digest and a local
   connection ordinal. The padded local write count must be one.
5. A separate Go client submits a distinct overwrite B (the baseline mode) once.
   Fresh native reads must confirm MGR mode B, MON `B\n` and one explicit
   who=`mgr` entry B, still under the original GID, before `mgr fail` replaces
   the active MGR.
6. The original caller must return `OutcomeUnknownError` wrapping
   `ErrManagerChanged`, with no acknowledged result. A caller-directed read on
   the replacement MGR must return B, and the client's MgrMap must name a
   different GID. Fresh native views must again show B, bracketed by one
   unchanged active name/GID equal to the client's replacement and different
   from the original GID. The original identified mutation keeps exactly one
   complete local write, and the recovery read uses another connection to the
   replacement MGR role. A later observation never acknowledges A
   retroactively, and local writes are not server execution counts.
7. Restoration has its own fresh bounded context. The worker exclusively marks
   restoration started, then submits exactly one native mutation chosen from its
   own baseline receipt: `config rm` who=`mgr` name=`mgr/balancer/mode` when no
   entry existed, otherwise `config set` of the one original value. It
   publishes the signed result, bytes and status, then takes fresh native
   views. The test requires signed `0` and repeats only read observations until
   the explicit entry set equals the baseline set, the MGR mode and MON
   effective value equal the baseline mode. A timeout keeps the started marker
   and uncertainty; no restoration is retried, and the test fails.

Because B equals the baseline mode, an absent baseline restored by `config rm`
leaves the MGR's cached value equal to the baseline effective mode even under
the source inference above. The restored MON store has no entry, so a later
module reload also yields the baseline default.

The controller validates request IDs and closed phases (`initial`, `applied`,
`overwritten`, `recovered`, `restore`, `restored`), places each request
atomically, runs the Python process under `timeout 30`, and publishes its
request-specific acknowledgement only after atomic receipt publication. The
external timeout is necessary because pinned Python `mon_command` and
`mgr_command` ignore their timeout arguments. The worker reuses the MON
oracle's atomic publication and bounded raw result encoding. It has no
arbitrary command, option, value or output path. The fixture metadata
allowlist includes `mgr-command-outcome-native.jsonl` and
`mgr-command-outcome-identity.json`, so failure diagnostics can retain the
original observations, partial identity progress and restoration uncertainty.

## Execution evidence

No native execution is recorded in this file yet. Exact-head CI receipts are
recorded on the pull request that introduces this cell. No exactly-once server
execution, secure-frame transaction capture, other profile, Tell or targeted
OSD/PG/MDS applied mutation, or completion of #63 is claimed.
