package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

// OSDSetAllocHint records an advisory allocation hint in the object's
// object_info and passes it to the object store. Like other writes it creates
// a missing object, advances the object version and requires ONDISK on
// success. The native librados builder always adds FAILOK; this client sends
// only the flags the caller sets.
const OSDSetAllocHint OSDOpcode = OSDOpcode(osd.SetAllocHint)

// OSDAllocHintFlags are the pinned CEPH_OSD_ALLOC_HINT_FLAG_* bits. Unknown
// bits are rejected before transmission.
type OSDAllocHintFlags uint32

const (
	OSDAllocHintSequentialWrite OSDAllocHintFlags = 1 << iota
	OSDAllocHintRandomWrite
	OSDAllocHintSequentialRead
	OSDAllocHintRandomRead
	OSDAllocHintAppendOnly
	OSDAllocHintImmutable
	OSDAllocHintShortLived
	OSDAllocHintLongLived
	OSDAllocHintCompressible
	OSDAllocHintIncompressible
	OSDAllocHintLog
)

// OSDAllocHint is the active union for OSDSetAllocHint. The reply echoes it.
type OSDAllocHint struct {
	ExpectedObjectSize, ExpectedWriteSize uint64
	Flags                                 OSDAllocHintFlags
}
