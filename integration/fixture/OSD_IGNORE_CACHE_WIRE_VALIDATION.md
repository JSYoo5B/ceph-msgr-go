# IgnoreCache wire implementation checkpoint

The [implementation contract](../../internal/osd/CACHE_POOL_CONTRACT.md)
and [fresh native proof](../../internal/osd/testdata/ignore-cache-native-README.md)
describe the bounded opt-in. The [machine-readable record](OSD_IGNORE_CACHE_WIRE_LINUX_20261018.json)
retains the selected source hashes, native execution/cleanup receipt and six
Linux check results. This checkpoint establishes offline wire equality and
synthetic transport behavior; Go live cache-pool interoperability was pending
at this checkpoint. The subsequent [Go/native record](OSD_IGNORE_CACHE_VALIDATION.md)
documents its separately executed live acceptance.

The source snapshot used base `source-1`
plus exactly the 21 selected files recorded in the receipt. Its 700 Go files
have a sorted path/hash inventory digest retained there. Concurrent MDS
history-fixture changes and the unfinished Go cache fixture are excluded.
The native SDK independently generated all 24 v6/v8/v9 operation/flag vectors
on an owned Linux amd64 builder; no Go-produced bytes replaced native output.
Initial comparison exposed a test WRITEFULL input missing its required Length;
the helper was corrected to Length 4, preserving the product validator.

On 2026-10-18, from 07:10:44 to 07:11:33 UTC, the native fixture completed all checks:

| Check | Result |
| --- | --- |
| Root `CGO_ENABLED=0 go test ./...` | Pass |
| `go test -race ./integration ./cephmsgr` | Pass |
| Root `go vet ./...` | Pass |
| Separate fixture module `go test -mod=readonly ./...` | Pass |
| Separate fixture module `go vet -mod=readonly ./...` | Pass |
| Python `test_*.py` discovery | 429 tests passed |

Authenticated synthetic peers verify the default feature refusal, explicit
MON/OSD bit-35 admission, separation from MGR/MDS and the MON CRUSH exception,
all four fixed profiles, private MON plumbing, raw READ output, and uncertain
WRITEFULL identity without OSD redial or replay. Unsupported IgnoreCache
combinations are refused before shared-slot, MON-readiness and TID side effects.
An independent read-only source review found no additional blocker.

These checks do not prove actual cache promotion, whiteouts, caps enforcement,
native mutation uncertainty or durability through the Go product. The earlier
[native-only probe](OSD_IGNORE_CACHE_NATIVE_PROBE_VALIDATION.md) remains a
separate evidence class, and in-progress main CI is not counted as passed here.
