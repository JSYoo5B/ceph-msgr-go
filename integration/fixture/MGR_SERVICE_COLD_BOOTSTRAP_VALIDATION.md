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
   ticket expiry is observed and raw MON status commands continue working.
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

## Limits

This does not prove cold setup under every platform, security mode, topology or
failure. MGR runtime metadata preparation, arbitrary config broadcasts, service
failover after registration, uncertain mutation replay and the separate service
worker handoff boundary are outside this cell. MON renewal and service dispatch
are distinct from service visibility; close acknowledgement and eventual native
removal remain separate checks. No autonomous re-registration policy is added.
