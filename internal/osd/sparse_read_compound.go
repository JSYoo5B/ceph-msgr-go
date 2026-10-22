package osd

import (
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// SparseReadCompound identifies the multi-operation SPARSE_READ subset: the
// native librbd object read (librbd/io/ObjectRequest.cc:238-246) composes
// SPARSE_READ and READ extents in one vector. Standalone SPARSE_READ and every
// other vector retain their own policy.
//
// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964,
// PrimaryLogPG.cc:5835-6024: both handlers read ctx->new_obs.oi, which equals
// the committed object state while no operation in the vector stages a write.
// LGPL-2.1; independently translated, no source copied.
func SparseReadCompound(ops []Operation) bool {
	if len(ops) < 2 {
		return false
	}
	for _, op := range ops {
		if op.Code == SparseRead {
			return true
		}
	}
	return false
}

func validateSparseReadCompoundShape(ops []Operation) error {
	for _, op := range ops {
		switch op.Code {
		case SparseRead, Read:
		default:
			return ErrOperation
		}
		// No FAILOK or mutation can make the terminal boundary ambiguous or
		// let a read observe staged state. Only the cache hints remain.
		if op.Flags&^OpFadviseMask != 0 {
			return ErrOperation
		}
		// The native composition sends no extent truncate fields; CephFS
		// truncate semantics stay with standalone READ.
		if op.TruncateSize != 0 || op.TruncateSequence != 0 {
			return ErrRequest
		}
	}
	return nil
}

func validateSparseReadCompoundWork(ops []Operation, limit uint32) error {
	if !SparseReadCompound(ops) {
		return nil
	}
	if err := validateSparseReadCompoundShape(ops); err != nil {
		return err
	}
	// The logical extents bound server work; the output budgets bound what
	// this client accepts. Neither may multiply the per-request frame limit.
	var work, output uint64
	for _, op := range ops {
		bytes := op.Length
		if op.Code == SparseRead {
			bytes = uint64(op.SparseRead.MaxOutputBytes)
		}
		if op.Length > uint64(limit)-work || bytes > uint64(limit)-output {
			return wire.ErrLimit
		}
		work, output = work+op.Length, output+bytes
	}
	return nil
}

// ValidateSparseReadCompoundRequest checks only the request's head/primary
// scope. Existing ordering, lock and redirect validators still run; this does
// not broaden them or infer remote object state.
func ValidateSparseReadCompoundRequest(r Request) error {
	if !SparseReadCompound(r.Operations) {
		return nil
	}
	if err := validateSparseReadCompoundShape(r.Operations); err != nil {
		return err
	}
	if r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.BalanceReads || r.LocalizeReads || r.RetryAttempt != 0 {
		return ErrRequest
	}
	return nil
}

// SparseReadCompoundPrefix returns the server-reported completed prefix of an
// already validated compound reply. An early aggregate error with all-zero
// per-op results (a missing object) has no known prefix.
func SparseReadCompoundPrefix(r Reply) int {
	return ChecksumCompoundPrefix(r)
}

// ValidateSparseReadCompoundReply preserves completed output before an
// ordinary failure without treating an unexecuted rval=0 as success. The
// caller checks object/PG/TID/attempt identity and exact payload splitting.
// Pinned PrimaryLogPG.cc:8205-8213 stops at the first negative operation;
// earlier outdata remains in the reply.
func ValidateSparseReadCompoundReply(requested []Operation, reply Reply) error {
	if !SparseReadCompound(requested) || len(requested) != len(reply.Operations) {
		return ErrOperation
	}
	if err := validateSparseReadCompoundShape(requested); err != nil {
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
		return errors.New("OSD sparse-read compound aggregate contradicts its terminal operation")
	}
	ambiguous := reply.Result < 0 && terminal < 0
	for i, requested := range requested {
		op := reply.Operations[i]
		if op.Code != requested.Code || op.Flags&(OpExclusive|OpFailOK) != 0 || uint64(op.PayloadLength) != uint64(len(op.Data)) {
			return errors.New("OSD sparse-read compound operation differs from request or payload metadata")
		}
		if ambiguous || terminal >= 0 && i > terminal {
			if op.Result != 0 || op.PayloadLength != 0 || op.Offset != requested.Offset || op.Length != requested.Length || op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Xattr != (XattrResult{}) {
				return errors.New("OSD sparse-read compound unexecuted or ambiguous operation changed its input or returned output")
			}
			continue
		}
		success := terminal < 0 || i < terminal
		if requested.Code == SparseRead {
			if err := ValidateSparseReadReply(requested, op, success); err != nil {
				return err
			}
			continue
		}
		if op.Offset != requested.Offset || op.Length > requested.Length || op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Xattr != (XattrResult{}) {
			return errors.New("OSD sparse-read compound read extent differs from request")
		}
		// A local read sets the extent length to the bytes returned (pinned
		// PrimaryLogPG.cc:5910-5921). A failed read zeroes the length, but a
		// full-object digest mismatch can leave the bytes it already read.
		if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length {
			return errors.New("OSD sparse-read compound read has inconsistent output")
		}
	}
	return nil
}
