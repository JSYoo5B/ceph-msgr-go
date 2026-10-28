# Zero-initial-MGR service bootstrap recipe

This is a test-only consumer and fixture profile, not a new product API. Ceph
Tentacle v20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964` and pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
remain the authority. The fixture uses the released provider v0.3.0.

## Closed selection

`mgr-service.aes256k.ip4.container.managers-0.t-mgrservicecoldbootstrap` uses
secure mode, aes256k keys, IPv4, container runtime, three MONs, no initial MGRs
and no OSDs. It selects only
`^TestCephMgrServiceColdBootstrapIntegration$` with the existing
`CEPH_MSGR_TEST_MGR_SERVICE=1` and `CEPH_MSGR_TEST_MGR_COUNT=0` inputs. Other
service parents retain their one-OSD/two-MGR restrictions. Host relay mode is
not admitted: its startup-only target allowlist lacks the delayed endpoints.

The only additional native modes admitted without OSDs are `mgrstate` and
`servicewatch`, through the existing bounded native oracle. No object, pool,
metadata-update or configuration control is granted by this profile.

## Required observations

1. Independently read an unavailable native MON MgrMap before constructing the
   Go client. Require MON readiness and the corresponding unavailable Go map.
2. Service setup deadlines, cancellation and client Close must return known
   pre-send failures. Keep one further setup pending while a new MON auth
   ticket expiry is observed. While waiting, the test issues raw MON status
   probes conditionally; renewal itself, not a required probe count, establishes
   MON progress for this cell.
3. Record every subject/closed-client dial attempt in a bounded ledger. Before
   activation all attempts must match authenticated MON endpoints, with no
   MGR, unclassified or omitted-overflow attempts. The initial peer inventory
   deliberately has no MGRs. After activation, classify against authenticated
   current MGR endpoints and require a positively attributed service dial;
   never infer zero dials by discarding unknown endpoints.
4. Publish the `start-mgrs` marker once. Existing fixture control creates owned
   MGR a and b, refreshing the ownership inventory after each creation, then
   waits for active/standby readiness. An uncertain activation is not replayed.
5. The original pending open must complete with Configure. Independent native
   MgrMap and service observations bind the active daemon name/GID and service
   client GID/metadata. Canceled identities must remain absent after activation.
6. Attempt Close once, require its acknowledgement and stable native absence.
   Fixture teardown owns the delayed daemon processes, credentials and network;
   require independent final absence of the refreshed ownership inventory.

`TestColdBootstrapDialAttributionIncludesLaterManager` exercises initial
MON-only, later-unclassified, authenticated-MGR and overflow accounting without
Ceph. Fixture tests preserve exclusive profile and native-mode authority.
These synthetic checks do not establish the native observations above.

## Reproduction and evidence

On an authorized Linux runner, under the current run/check locks:

```sh
CEPH_MSGR_TEST_MGR_SERVICE=1 \
CEPH_MSGR_TEST_MGR_COUNT=0 \
CEPH_MSGR_TEST_KEY_TYPE=aes256k \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_RUNTIME=container \
CEPH_MSGR_TEST_RUN='^TestCephMgrServiceColdBootstrapIntegration$' \
sh integration/run.sh
```

Require the named API parent to report actual PASS, not SKIP or a zero exit
from an empty selector. Record exact source SHA, attempt, image/profile, the
`mgr-service-cold-bootstrap.json` receipt, native identity/removal assertions,
restored owned-resource absence and the final Basic CI result on the PR.
Local selected execution and hosted `/ci` execution remain separate evidence.
No successful execution is implied merely by adding this recipe.

## Execution attribution

Initial native run [40688440287](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/40688440287),
attempt 1, tested `1e055e98ad670af2a6d66654f47335dd834dab8d` under controller
`334c818544b006a6c345cec110256993f634a271`. The named API parent PASSed in
27.27 s without skips: native map epochs 1 to 4, before-activation dial counts
MON=3/MGR=0/unclassified=0, after-activation MON=4/MGR=1/unclassified=0, and
native service removal assertions passed. Seven container terminations were
logged, but this run did not independently inspect final resource absence.
It predates the separate read-only cleanup confirmation below.

Basic run 40688413951 at that same source failed two existing static catalog
expectations (82 versus 83 rows and two versus three service profiles). Its
fixture unit job passed; later product checks were skipped, not passed. Those
expectations are corrected without weakening the existing two profile checks.
Final-source Basic/native results are recorded separately on the PR.

The cold profile now requires `MGR_COLD_BOOTSTRAP_CLEANUP`: after the sole
termination attempt, a fresh Docker client must receive explicit not-found
results for every container in the refreshed ownership inventory and for its
network. A present resource, unreadable inventory or failed inspector cannot
certify cleanup. This performs no removal or retry itself. The compact
`MGR_COLD_BOOTSTRAP` log receipt records ticket expiry before/after the pending
setup, map identities, dial counts and local/native assertions. Raw successful
oracle transcripts are not retained by the current generic success artifact.

## Limits

This does not prove cold setup under every platform, security mode, topology or
failure. MGR runtime metadata preparation, arbitrary config broadcasts, service
failover after registration, uncertain mutation replay and the separate service
worker handoff boundary are outside this cell. MON renewal and service dispatch
are distinct from service visibility; close acknowledgement and eventual native
removal remain separate checks. No autonomous re-registration policy is added.
