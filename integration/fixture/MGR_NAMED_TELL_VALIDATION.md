# Named active MGR tell acceptance

Date: 2026-10-16. Product and fixture source checkpoint:
`source-1`, independently reviewed before
either native run. The subsequent documentation commit changes no Go source.
Reference Ceph: Tentacle 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

The public API is `Client.MgrTellTo(ctx, exactBareName, Command) (Result, error)`.
It selects only the currently available active daemon by its exact map name.
It shares ordinary MGR authentication and the session; standby addresses,
automatic replacement selection and uncertain-command replay are not provided.
See [the product contract](../../cephmsgr/MGR_TELL_CONTRACT.md).

## Actual configurations and results

Both runs used the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
and fixture provider `github.com/JSYoo5B/ceph-testcontainers-go v0.3.0`, three
MONs, two MGRs, no OSD/MDS, CGO-disabled test binaries and the exclusive selector
`^TestCephNamedManagerTellIntegration$`.

| Transport / CephX key and service cipher | Go runtime / address family | Result |
| --- | --- | --- |
| CRC / aes / aes | Darwin arm64 host / IPv4 | 7/7 subtests, 11.69 s, runner exit 0 |
| secure / aes256k / aes256k | Linux arm64 container / IPv6 | 7/7 subtests, 11.06 s, runner exit 0 |

These are focused public API acceptances. The separately invoked internal
client suite had no matching test for this selector; it is not counted as more
native coverage. No whole-CI result is claimed here. There were no native
failures or native retries for either configuration.

The seven subtests establish:

1. Exact available active-name tell returns the same version JSON as a fresh
   native `ceph tell mgr.<exact-name> version --format json` client, with the
   Go connection endpoint mapped to that owned daemon.
2. Missing names, an unstripped `mgr.` prefix, wildcard and numeric-looking
   name are local ErrManagerNotFound; the known standby is ErrManagerNotActive.
3. The unknown daemon command preserves raw status -22 and its text. A
   separately authenticated read-only client preserves raw -13 and its text
   as CommandError, while an ordinary module query still succeeds.
4. Three concurrent ordinary MGR command, current-active tell and named tell
   calls preserve their independent outputs on the shared session.
5. Both AUTH and MGR ticket expiry values advance while client GlobalID,
   selected name/GID and endpoint remain stable, followed by another named tell.
6. One explicit `mgr fail` mutation causes a new active name and GID. The old
   name is refused locally, and an explicit new-name call succeeds; ordinary
   MON and MGR commands also remain usable.
7. Pre-cancelled calls are local cancellations. Close completes and subsequent
   named calls return ErrClosed.

The distinction between a queued unstarted refusal and a started unknown
outcome is tested with synthetic peers, including a blocked writer followed by
a name-only map change and a separate MON admission loss that does not Fail
the MGR session. Native versions do not prove those race boundaries. Exact
nonce-vector changes, identity changes, setup obsolescence, malformed reply/raw
data handling and no replay are also synthetic/shared-path regression evidence,
not additional native observations.

## Identity and renewal observations

Each isolated fixture independently selected `a`, daemon GID 4167, map epoch 4,
and switched to `b`, GID 4239, epoch 6. The primary Go client GlobalID remained
4470; the read-only client used 4473. These IDs coincidentally repeat across the
two independent clusters; each receipt retains its own FSID.

| Observation | CRC/aes IPv4 | secure/aes256k IPv6 |
| --- | --- | --- |
| Owned active endpoint | `172.19.0.6:6800` → `172.19.0.7:6800` | `[fd62:93fe:2c9e::6]:6800` → `[fd62:93fe:2c9e::7]:6800` |
| AUTH and MGR expiry before | `2026-10-16T18:44:28.518712+09:00` | `2026-10-16T09:45:55.098035087Z` |
| AUTH and MGR expiry after | `2026-10-16T18:44:37.527155+09:00` | `2026-10-16T09:46:04.238561006Z` |
| Old `a` call after switch | known ErrManagerNotActive | known ErrManagerNotActive |
| Final client state | Closed true | Closed true |

For each run, fresh native map/tell/map observations bracket one stable active
identity. The `after` control polls read-only maps until a higher epoch has a
different available name and GID from the saved `before` identity. It does not
retry the failover mutation or a failed native tell. The archived map receipts
contain the decoded native epoch/name/GID/availability/standby identity view;
they are not the complete raw `mgr dump` document. Go Result.Data is preserved
as raw bytes in JSON's base64 form, with Code, Message and error classification.
All three recorded named version responses per configuration decode to exactly
the corresponding native version JSON.

## Retained evidence and cleanup

Local first-run logs and private diagnostic directories remain at:

Each directory contains fixture-manifest.json, fixture-ownership.json,
named-mgr-oracle-before.json, named-mgr-oracle-after.json and
named-mgr-results.json (18 Go observations). Only allowlisted non-secret
metadata is collected. These machine-local paths are not portable repository
artifacts; the hashes below identify the retained files.

| SHA-256 evidence | CRC/aes IPv4 | secure/aes256k IPv6 |
| --- | --- | --- |
| native before receipt | `e2942a00fd4d91bca0828feb3a6ebd4edd68fbdf1a777c46786ce21007c8b95e` | `e2942a00fd4d91bca0828feb3a6ebd4edd68fbdf1a777c46786ce21007c8b95e` |
| native after receipt | `8e2855338fc1606c501d9f5809a5494e451279533768b110a793327afb7f6755` | `0291d4164c0e5e899e8a52b192394f600385ce96709c4cd33b1e874021f0acea` |
| raw Go results | `3f1312cf3fedb7980de7239604de67c8516435ddbca547eada722809968e79cd` | `97397ee877644ca34d40e78ed84c82e74dcb9febbdde9ee307e02b2d4742e2d2` |
| manifest | `e5ac089d2585b1615b97cd2ae0b22d20633f82d6f049ea6d18f4120bcbb45833` | `50831637f2f1ddbe6e6c9943d54bbd238f8551c65d34d734997834d3e966755e` |
| ownership | `382056870233cfc21a0015961b28724da69b113ffefd9d66f82f7a7372947fde` | `27e630db4e0211f8225f91c9aa45256f8cc80bd8cc23ab59f776b133524731fc` |

Both runners exited 0 after logging termination of all seven owned containers.
A separate read-only Docker inventory check found zero recorded container IDs
remaining and both owned networks absent. No unrelated resources were removed.

## Frozen source and local checks

| File | SHA-256 |
| --- | --- |
| cephmsgr/api.go | `e81b2b5b0c195a10499869ae0bdd2c014841728c0b31d4c8f75234c8f978f553` |
| cephmsgr/client.go | `47ab41431c0ca20fb93d3e1f2d8d708f697e84b188d8d1ecaab60153d626f0df` |
| cephmsgr/named_mgr.go | `7ebb233e8fe70f1dc3627c584c2109d30184ffb21aee45ad95ddffe40fff0ca7` |
| cephmsgr/named_mgr_test.go | `1f03d98125d3c9c20ec3f73d24ac42e8594b1b1ce0431fd862ab035b7fbbaf99` |
| integration/named_mgr_test.go | `3df86420629a2bda39702a15cbc4ff4ff3535909dd03d1ec716b8d151dc36c98` |
| integration/fixture/control.go | `858195452557b9e12d6bde97871040dbd8a92c460b376944ac775031508f18ec` |
| integration/fixture/named_mgr.go | `60af75ccad02d9f40df8a4f75718bde16c6ae94350a8dce2e3d97afe77ae4753` |
| integration/fixture/named_mgr_test.go | `d453e80aa82d555d455534e2f878d15e5308e2167c0bc1214b3dd773f9fe5502` |
| integration/fixture/diagnostics.go | `265aec5e562a8c0a43058e83e1f796c9c6d350db412a8476b6d1bfe77f9afc10` |

Final local checks passed: CGO_ENABLED=0 product `go test ./...`, nested fixture
`go test ./...`, both modules' `go vet ./...`, and CGO-disabled race checks for
the named-MGR/Tell/Manager tests. The final checks and
native runs are distinct from any remote CI run.

## Reproduction and remaining limits

From this nested fixture module, choose fresh diagnostic paths for each run:

```sh
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephNamedManagerTellIntegration$' \
go run . run -project ../.. -diagnostics .artifacts/named-mgr-crc

CEPH_MSGR_TEST_CONNECTION_MODE=secure CEPH_MSGR_TEST_KEY_TYPE=aes256k \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k CEPH_MSGR_TEST_IP_FAMILY=6 \
CEPH_MSGR_TEST_RUNTIME=container \
CEPH_MSGR_TEST_RUN='^TestCephNamedManagerTellIntegration$' \
go run . run -project ../.. -diagnostics .artifacts/named-mgr-secure
```

Standby tell, arbitrary named-daemon address discovery, waiting for standby
promotion, cross-name routing and automatic replay remain unsupported. Other
OS/architecture, cipher mixtures and address combinations have not been
established by these two runs. The product still implements raw communication;
schemas and operational policy remain outside it.
