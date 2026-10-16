# Bounded Tentacle WRITESAME

Reference source: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, LGPL-2.1 headers and server
source. This implementation translates the wire fields; it copies no native
implementation and adds no native product dependency.

`WRITESAME` (`0x2226`) is an explicitly ordered mutation using the existing
request API. `Offset` selects its start, `Length` is the logical requested byte
count, and `Data` contains the original nonempty binary pattern. A zero logical
length is admitted with a nonempty pattern; its actual object/version/mtime
behavior requires native observation. A positive length must be a multiple of
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
staged-metadata and class ordering restrictions apply. Offset-zero writes add
the logical length to the conservative staged digest boundary, retaining its
minimum across FAILOK operations: following full READ/CMPEXT is refused.
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
CGO-disabled product and fixture suites pass. Fresh native vectors, a separately
built native C++ compound oracle, and two real fixture profiles are pending.
Synthetic tests do not establish native interoperability or zero-length effects.
