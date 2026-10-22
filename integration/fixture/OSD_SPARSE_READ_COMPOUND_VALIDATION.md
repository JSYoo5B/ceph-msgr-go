# Read-only SPARSE_READ/READ compounds (2026-10-22)

`TestCephOSDSparseReadCompoundIntegration` passed with all eight subtests in
three Linux configurations. The tested source was PR head
`f60930c8adc958c415035ef74c402e5c550a5ee5`, run by the existing default OSD
selector of three catalog rows through
[CI command run 39812625879](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39812625879).
The catalog and fixture were not changed. All runs used Ceph Tentacle 20.2.4
commit `7f793731f1b39eb4f465e960113d2363c311b964` and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
on GitHub-hosted Linux amd64 runners. Each job also ran the rest of its
default OSD selection, and each job succeeded.

| Catalog row | Request / reply profile | Key / mode | Network / runtime | OSD shape | Job | Top-level test |
| --- | --- | --- | --- | --- | --- | --- |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3` | legacy-v6 / v6 | AES / CRC | IPv4 host | 3 BlueStore, replicas 3 | [119942786921](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39812625879/job/119942786921) | PASS 4.25s |
| `osd.aes256k.ip6.osd-store-bluestore.osd-replicas-3` | legacy-v6 / v6 | AES256K / secure | IPv6 container | 3 BlueStore, replicas 3 | [119942786800](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39812625879/job/119942786800) | PASS 5.25s |
| `osd.aes256k.ip4.host` | legacy-v6 / v6 | AES256K / secure | IPv4 host | 1 MemStore, replicas 1 | [119942786837](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39812625879/job/119942786837) | PASS 5.06s |

## Cases

The exclusive OMAP-write fixture owns a dense 32-byte object, a sparse object
and a reserved absent name. The sparse object received three 4-byte Go writes
at 8192, 0x11000 and 0x20000 (size 131076). Before each compound, the
`osd-mapext` oracle ran the pinned `rados mapext` CLI over the full object and
retained C librados stat/read bytes, mtime and user version. Each per-op
expectation is the intersection of that native full map with the operation's
range clipped to the object size, with bytes taken from the native dense read.
The native client did not execute the Go compound or a partial SPARSE_READ.

Observed native full maps were `[{0,32}]` for the dense object in all rows.
For the sparse object they were `[{8192,4},{69632,4},{131072,4}]` on both
BlueStore rows and `[{0,131076}]` on MemStore, which reports the logical
range including zero-filled holes.

| Case | Compound (SR = SPARSE_READ, R = READ) | Checked |
| --- | --- | --- |
| dense-mixed | SR(0,16), R(16,16), SR(8,8) | Per-op union, result, extents, bytes; raw data partition |
| dense-eof | SR(29,100), R(30,10), SR(64,1), R(64,1) | Clipped lengths, empty map past EOF, empty READ past EOF |
| librbd-mixed | SR(0,0x10000), R(0x10000,0x1000), SR(0x11000,0x20000)+SEQUENTIAL | The native librbd layout across holes |
| sparse-pair-holes | SR(0,4096), SR(8190,10), R(8190,10) | Hole-only range, partial extent, dense READ of the same range |
| fadvise-each | SR+RANDOM, R+NOCACHE, SR+DONTNEED, R+WILLNEED | Per-op hints echoed; results unchanged |
| missing-object-ambiguous | SR, R, SR on the absent name | Native -2; Go -2 with all-zero per-op results, original unions, no output; fresh STAT on the same connection |
| refused-shapes-not-sent | SR+STAT, MAPEXT+R, FAILOK SR+R, SR+WRITE | Local error, no transaction ID |
| local-output-budget | R(0,4), SR(8190,10) with an 8-byte budget on a separate client | Outcome unknown with raw reply retained; original connection still serves the compound |

In every successful case the reply user version equalled the native stat
version, every SPARSE_READ result equalled the same operation sent alone on
the same connection, and the post-compound native observation matched the
pre-compound bytes, mtime, version and allocation map.

## Not covered

Request-v8/v9 profiles have native codec and unit evidence only; their
explicit catalog rows select other tests. Snapshot compounds, replica reads,
EC, mid-vector daemon errors, faults and explicit resubmission were not run.
No evidence JSON was archived for this test: the per-case lines above come
from the job logs.
