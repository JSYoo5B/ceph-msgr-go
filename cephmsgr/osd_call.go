package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

// OSDCall invokes a RADOS object class method. The class name, method name and
// input bytes are raw caller inputs; this layer adds no class-specific schema.
const OSDCall OSDOpcode = OSDOpcode(osd.Call)

// OSDCallMode selects only the request READ or WRITE flag and whether a
// mutation ModificationTime is admitted. The Tentacle OSD classifies the
// method by its registered flags, so a read-mode call to a write method still
// executes as a write; every sent CALL keeps the uncertain-outcome contract and
// is never resent.
type OSDCallMode uint8

const (
	OSDCallRead  OSDCallMode = OSDCallMode(osd.ClassRead)
	OSDCallWrite OSDCallMode = OSDCallMode(osd.ClassWrite)
)

// OSDClassInput is the active union for OSDCall; OSDOperation.Data holds the
// method input. Class and Method are 1-255 bytes without NUL. Mode is
// required. MaxOutputBytes is a positive local receipt budget, not a remote
// limit. FAILOK is not yet supported for OSDCall. A CALL after any staged write
// in the same vector, or any read, guard or CALL after a CALL, is rejected with
// ErrUnsupportedOSDOperation because a method may read committed state and
// stage writes itself.
//
// Results: the method's signed return is the per-op Result (positive values
// are preserved) and its output is Data. In a vector that stages a write,
// Tentacle refuses output above osd_max_write_op_reply_len (64 bytes by
// default) with -EOVERFLOW and does not commit. Unknown classes or methods,
// classes outside the server's load list and missing capabilities are server
// refusals returned unchanged.
type OSDClassInput struct {
	Class, Method  string
	Mode           OSDCallMode
	MaxOutputBytes uint32
}

// OSDClassResult preserves the echoed cls union. OutputLength is the method
// output length recorded by the OSD; it can exceed PayloadLength only when an
// -EOVERFLOW write-reply bound cleared the output.
type OSDClassResult struct {
	ClassLength, MethodLength, Argc uint8
	InputLength                     uint32
	OutputLength                    uint64
}

func classWireInput(input OSDClassInput) osd.ClassInput {
	return osd.ClassInput{Class: input.Class, Method: input.Method, Mode: uint8(input.Mode), MaxOutputBytes: input.MaxOutputBytes}
}
