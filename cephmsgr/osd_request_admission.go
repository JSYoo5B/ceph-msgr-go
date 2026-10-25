package cephmsgr

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

// Validate this capability before shared-slot admission, monitor readiness or
// TID allocation. Ordinary requests retain their existing validation path.
func (o *OSDConnection) validateIgnoreCache(request OSDRequest) error {
	if !request.IgnoreCache {
		return nil
	}
	// A cache-tier operation has its own contract (validateCacheOperation).
	if len(request.Operations) == 1 && osd.IsCacheOperation(uint16(request.Operations[0].Code)) {
		return nil
	}
	if !o.client.options.EnableOSDCachePools {
		return fmt.Errorf("%w: IgnoreCache requires EnableOSDCachePools", ErrUnsupportedFeatures)
	}
	if request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure {
		return fmt.Errorf("%w: IgnoreCache refuses resubmission and erasure-coded placement", osd.ErrRequest)
	}
	input := osd.Request{IgnoreCache: true, ReadSnap: request.ReadSnapshot,
		SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext,
		BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect,
		OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, RetryAttempt: request.RetryAttempt}
	if len(request.Operations) == 1 {
		op := request.Operations[0]
		input.Operations = []osd.Operation{{Code: uint16(op.Code), Flags: uint32(op.Flags), TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence}}
	}
	if err := osd.ValidateIgnoreCache(input, o.features); err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			return fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return err
	}
	return nil
}

// validateSetChunk applies the SET_CHUNK contract before shared-slot
// admission, monitor readiness or TID allocation. Requests without
// OSDSetChunk only refuse a Chunk input.
func (o *OSDConnection) validateSetChunk(request OSDRequest) error {
	chunk := false
	for _, op := range request.Operations {
		if op.Code != OSDSetChunk && op.Chunk != (OSDChunkInput{}) {
			return fmt.Errorf("%w: a chunk input belongs to OSDSetChunk alone", osd.ErrRequest)
		}
		chunk = chunk || op.Code == OSDSetChunk
	}
	if !chunk {
		return nil
	}
	if request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure {
		return fmt.Errorf("%w: OSDSetChunk refuses resubmission and erasure-coded placement", osd.ErrRequest)
	}
	input := osd.Request{MTimeSeconds: request.ModificationTime.Seconds, MTimeNanoseconds: request.ModificationTime.Nanoseconds,
		Object: request.Object.Name, Locator: osd.Locator{Pool: request.Object.Pool, Key: request.Object.Key, Namespace: request.Object.Namespace, Hash: -1},
		ReadSnap: request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots,
		OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext, FullTry: request.FullTry, FullForce: request.FullForce,
		BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect,
		OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, IgnoreCache: request.IgnoreCache,
		IgnoreOverlay: request.IgnoreOverlay, RetryAttempt: request.RetryAttempt}
	for _, op := range request.Operations {
		// SET_CHUNK carries only its flag and chunk input: every other field
		// must be zero, refused here rather than after slot admission.
		if op.Code == OSDSetChunk && !reflect.DeepEqual(op, OSDOperation{Code: op.Code, Flags: op.Flags, Chunk: op.Chunk}) {
			return fmt.Errorf("%w: OSDSetChunk carries only its flag and chunk input", osd.ErrRequest)
		}
		input.Operations = append(input.Operations, osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags), SetChunk: setChunkWireInput(op)})
	}
	if err := osd.ValidateSetChunkRequest(input); err != nil {
		return err
	}
	// The frame bound is enforced again by the encoder.
	_, _, err := osd.ValidateOperations(input.Operations, ^uint32(0))
	return err
}

// validateCacheOperation applies the cache-operation contract before
// shared-slot admission, monitor readiness or TID allocation. Requests without
// a cache opcode only refuse IgnoreOverlay.
func (o *OSDConnection) validateCacheOperation(request OSDRequest) error {
	cache := false
	for _, op := range request.Operations {
		cache = cache || osd.IsCacheOperation(uint16(op.Code))
	}
	if !cache && !request.IgnoreOverlay {
		return nil
	}
	if cache && !o.client.options.EnableOSDCachePools {
		return fmt.Errorf("%w: cache-tier operations require EnableOSDCachePools", ErrUnsupportedFeatures)
	}
	if cache && (request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure) {
		return fmt.Errorf("%w: cache-tier operations refuse resubmission and erasure-coded placement", osd.ErrRequest)
	}
	input := osd.Request{ReadSnap: request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots,
		OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext, FullTry: request.FullTry, FullForce: request.FullForce,
		BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect,
		OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, IgnoreCache: request.IgnoreCache,
		IgnoreOverlay: request.IgnoreOverlay, RetryAttempt: request.RetryAttempt}
	for _, op := range request.Operations {
		// A cache operation is inputless and flagless: every other field
		// must be zero, refused here rather than after slot admission.
		if osd.IsCacheOperation(uint16(op.Code)) && !reflect.DeepEqual(op, OSDOperation{Code: op.Code}) {
			return fmt.Errorf("%w: cache-tier operations carry no flags or input", osd.ErrOperation)
		}
		input.Operations = append(input.Operations, osd.Operation{Code: uint16(op.Code)})
	}
	if err := osd.ValidateCacheRequest(input, o.features); err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			return fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return err
	}
	return nil
}

// Only the explicit new snapshot flag takes this early preflight. Ordinary
// requests keep their existing complete validation/encoding path. Operation
// codes and CALL modes suffice to classify snapshot applicability; the full
// opcode, payload and compound validators still run before transmission.
func (o *OSDConnection) validateEnforceSnapContext(request OSDRequest) error {
	if !request.EnforceSnapContext {
		return nil
	}
	if len(request.Operations) == 0 || len(request.Operations) > osd.MaxOperations {
		return osd.ErrRequest
	}
	input := osd.Request{EnforceSnapContext: true, ReadSnap: request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence,
		Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, Operations: make([]osd.Operation, len(request.Operations))}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Class: osd.ClassInput{Mode: uint8(op.Class.Mode)}}
	}
	return osd.ValidateEnforceSnapContext(input, o.client.options.MaxFrameSize)
}

// The compound checksum and sparse-read subsets are head-only and
// caller-targeted at a replicated primary. This projection adds no policy to
// other vectors.
func (o *OSDConnection) validateChecksumCompound(request OSDRequest) error {
	if len(request.Operations) < 2 {
		return nil
	}
	checksum, sparse := false, false
	for _, op := range request.Operations {
		checksum = checksum || op.Code == OSDChecksum
		sparse = sparse || op.Code == OSDSparseRead
	}
	if !checksum && !sparse {
		return nil
	}
	if len(request.Operations) > osd.MaxOperations {
		return osd.ErrRequest
	}
	if request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure {
		return osd.ErrRequest
	}
	input := osd.Request{ReadSnap: request.ReadSnapshot, BalanceReads: request.BalanceReads,
		LocalizeReads: request.LocalizeReads, RetryAttempt: request.RetryAttempt,
		Operations: make([]osd.Operation, len(request.Operations))}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags), TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence}
	}
	if err := osd.ValidateChecksumCompoundRequest(input); err != nil {
		return err
	}
	return osd.ValidateSparseReadCompoundRequest(input)
}

// Each encoded OMAP entry needs at least two length prefixes, each assertion
// two prefixes and an operator, and the whole request data must fit one frame.
// Refuse impossible maps, summed across the vector, before copying headers.
func osdOmapEntryBound(ops []OSDOperation, limit uint32) error {
	var minimum uint64
	for _, op := range ops {
		minimum += uint64(len(op.Omap.Entries))*8 + uint64(len(op.Omap.Assertions))*12
		if minimum > uint64(limit) {
			return ErrLimitExceeded
		}
	}
	return nil
}
