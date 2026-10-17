# Bounded Tentacle WRITESAME

Reference source: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, LGPL-2.1 headers and server
source. This implementation translates the wire fields; it copies no native
implementation and adds no native product dependency.

`WRITESAME` (`0x2226`) is an explicitly ordered mutation using the existing
request API. `Offset` selects its start, `Length` is the logical requested byte
count, and `Data` contains the original nonempty binary pattern. A zero logical
length is admitted with a nonempty pattern. In both verified profiles it kept
existing bytes/attributes/mtime/version unchanged and left missing objects
absent; those are native observations, not a broader persistence guarantee. A positive length must be a multiple of
the pattern length. Offset plus length must not overflow. Both encoders bound
actual frame input and the sum of logical WRITE/WRITEFULL/APPEND/WRITESAME bytes
independently by the configured frame limit. Tiny patterns cannot bypass the
expanded work bound, including in mixed or FAILOK compound vectors.

The 28-byte union contains offset u64, logical length u64, pattern length u64,
and four zero bytes. The separate payload length is the original pattern byte
count. Replies expose the pattern length independently of extent truncate
fields, require the original union and no output, and preserve signed results.
The daemon expands the pattern before invoking WRITE (PrimaryLogPG.cc:5051),
which can clip writes against a newer truncate sequence. The WRITESAME reply
keeps its original requested extent; Length does not establish actual bytes
written. No truncate fields are accepted for this different union.

Head-only mutations may carry an explicit valid write SnapContext/ORDERSNAP;
reading a snapshot in the same writing vector is refused. The existing
staged-metadata and class ordering restrictions apply. Offset-zero positive WRITESAME uses
a conservative digest boundary of one byte: the nested WRITE may be clipped
to an unknown smaller full-object size by a truncate sequence. Every subsequent
positive offset-zero READ/CMPEXT is refused, including after a staged sequenced
TRUNCATE or intervening FAILOK operations; later writes cannot erase the bound.
Cancellation or malformed completion after send retains an unknown outcome;
there is no automatic routing, retry or replay. A successful writing vector
requires ONDISK, whose persistence still depends on its storage profile.
Negative per-op results remain visible under FAILOK even when the vector
succeeds. An ONDISK flag on an error is not proof that its mutation applied.

SETXATTRS/RESETXATTRS remain unsupported. Their enum declarations and stale
Objecter builders do not establish server support: the pinned PrimaryLogPG
has no handlers and falls through to -EOPNOTSUPP. Public librados supplies
WRITESAME standalone and compound APIs, enabling a separate bounded native
oracle; it supplies no corresponding public bulk-xattr APIs.

Current evidence: focused synthetic/unit/public tests and their race run pass;
CGO-disabled product and fixture suites pass. Five fresh native v6 vectors
were directly encoded and separately decoded by pinned Ceph. Go/public and
fixture proof consumers pass. A separate public C++ compound oracle compiled
against the image librados and passed its loader audit. The logical native
success/FAILOK/zero reply inputs carry ACK alone: public consumers correctly
retain them as unknown outcomes because ONDISK is missing. Those wire vectors
do not claim daemon execution.

Actual daemon acceptance passed all 19 cases on CRC/AES IPv4 host with three
BlueStore replicas and on secure/AES256K IPv6 container with one MemStore OSD,
both explicitly legacy-v6. The failed first CRC setup attempt remains separate
from the corrected retry. Snapshot context preserves the native clone; two
sequenced-TRUNCATE preparations prove clipping while the original logical
reply extent remains unchanged. Full bytes, attributes, stat2, own version,
raw signed results and successful ONDISK receipts were checked. See
[the full evidence and limits](../../integration/fixture/OSD_WRITESAME_VALIDATION.md).
No EC, new v8 live profile, automatic placement or
mutation replay is added by this work.
