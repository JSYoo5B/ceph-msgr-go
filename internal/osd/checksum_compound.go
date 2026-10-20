package osd

import (
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// ChecksumCompound identifies only the new multi-operation checksum subset.
// Standalone checksum requests and every other vector retain their own policy.
func ChecksumCompound(ops []Operation) bool {
	if len(ops) < 2 {
		return false
	}
	for _, op := range ops {
		if op.Code == Checksum {
			return true
		}
	}
	return false
}

func validateChecksumCompoundShape(ops []Operation) error {
	for _, op := range ops {
		switch op.Code {
		case Checksum, Read, Stat, AssertVersion, CompareExtent:
		default:
			return ErrOperation
		}
		// No FAILOK, CALL or staged mutation can make the terminal boundary
		// ambiguous. Retain only the hints this opcode already implements.
		if op.Flags & ^fadviseFlags(op.Code) != 0 {
			return ErrOperation
		}
	}
	return nil
}

func validateChecksumCompoundWork(ops []Operation, limit uint32) error {
	if !ChecksumCompound(ops) {
		return nil
	}
	if err := validateChecksumCompoundShape(ops); err != nil {
		return err
	}
	var work, input uint64
	for _, op := range ops {
		var read, bytes uint64
		switch op.Code {
		case Checksum:
			read, bytes = op.Length, checksumWidth(op.Checksum.Type)
		case Read:
			read = op.Length
		case CompareExtent:
			read, bytes = op.Length, uint64(len(op.Data))
		}
		if read > uint64(limit)-work || bytes > uint64(limit)-input {
			return wire.ErrLimit
		}
		work, input = work+read, input+bytes
	}
	return nil
}

// ValidateChecksumCompoundRequest checks only the new request's head/primary
// scope. Existing cache, snapshot, ordering and per-operation validators still
// run; this does not broaden any of them or infer remote object state.
func ValidateChecksumCompoundRequest(r Request) error {
	if !ChecksumCompound(r.Operations) {
		return nil
	}
	if err := validateChecksumCompoundShape(r.Operations); err != nil {
		return err
	}
	if r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.BalanceReads || r.LocalizeReads || r.RetryAttempt != 0 {
		return ErrRequest
	}
	return nil
}

// ChecksumCompoundPrefix returns the server-reported completed prefix of an
// already validated compound reply. Empty EOF output is not an execution bit;
// an early aggregate error with all-zero per-op results has no known prefix.
func ChecksumCompoundPrefix(r Reply) int {
	if r.Result >= 0 {
		return len(r.Operations)
	}
	for i, op := range r.Operations {
		if op.Result < 0 {
			return i
		}
	}
	return 0
}

func checksumCompoundOriginalUnion(requested Operation, op OperationResult) bool {
	if requested.Code == Checksum {
		return op.Offset == requested.Offset && op.Length == requested.Length &&
			op.Checksum == (ChecksumResult{ChunkSize: requested.Checksum.ChunkSize, Type: requested.Checksum.Type}) &&
			op.TruncateSize == 0 && op.TruncateSequence == 0
	}
	length := requested.Length
	if requested.Code == AssertVersion {
		length = requested.Version
	}
	return op.Offset == requested.Offset && op.Length == length &&
		op.TruncateSize == requested.TruncateSize && op.TruncateSequence == requested.TruncateSequence && op.Xattr == (XattrResult{})
}

// ValidateChecksumCompoundReply preserves successful output before an ordinary
// failure without treating an unexecuted rval=0 as success. The caller checks
// object/PG/TID/attempt identity and exact raw payload splitting first.
// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964:
// PrimaryLogPG.cc:8205-8213,4310-4321,9183-9222; osd_types.cc:7408-7415;
// include/types.h:617-621. LGPL-2.1; independently translated, no source copied.
func ValidateChecksumCompoundReply(requested []Operation, reply Reply) error {
	if !ChecksumCompound(requested) || len(requested) != len(reply.Operations) {
		return ErrOperation
	}
	if err := validateChecksumCompoundShape(requested); err != nil {
		return err
	}
	terminal := -1
	for i, op := range reply.Operations {
		if op.Result < 0 {
			terminal = i
			break
		}
	}
	if reply.Result >= 0 && terminal >= 0 || reply.Result < 0 && terminal >= 0 && reply.Result != reply.Operations[terminal].Result {
		return errors.New("OSD checksum compound aggregate contradicts its terminal operation")
	}
	ambiguous := reply.Result < 0 && terminal < 0
	for i, requested := range requested {
		op := reply.Operations[i]
		if op.Code != requested.Code || op.Flags&(OpExclusive|OpFailOK) != requested.Flags&(OpExclusive|OpFailOK) || uint64(op.PayloadLength) != uint64(len(op.Data)) {
			return errors.New("OSD checksum compound operation differs from request or payload metadata")
		}
		if ambiguous || terminal >= 0 && i > terminal {
			if op.Result != 0 || op.PayloadLength != 0 || !checksumCompoundOriginalUnion(requested, op) {
				return errors.New("OSD checksum compound unexecuted or ambiguous operation changed its input or returned output")
			}
			continue
		}
		success := terminal < 0 || i < terminal
		if requested.Code == Checksum {
			if err := ValidateChecksumReply(requested, op, success); err != nil {
				return err
			}
			continue
		}
		length := requested.Length
		if requested.Code == AssertVersion {
			length = requested.Version
		}
		short := requested.Code == Read || requested.Code == CompareExtent
		if op.Offset != requested.Offset || op.Length > length || !short && op.Length != length || op.TruncateSize != requested.TruncateSize || op.TruncateSequence != requested.TruncateSequence || op.Xattr != (XattrResult{}) {
			return errors.New("OSD checksum compound extent differs from request")
		}
		switch requested.Code {
		case Read:
			if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length && op.Length != requested.Length {
				return errors.New("OSD checksum compound read has inconsistent output")
			}
		case Stat:
			if op.PayloadLength > 16 || success && op.PayloadLength != 16 {
				return errors.New("OSD checksum compound stat has invalid output")
			}
		default:
			if op.PayloadLength != 0 {
				return errors.New("OSD checksum compound guard returned output")
			}
		}
	}
	return nil
}
