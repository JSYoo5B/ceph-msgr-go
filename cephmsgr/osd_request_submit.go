package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func setChunkWireInput(op OSDOperation) osd.SetChunkInput {
	c := op.Chunk
	if c == (OSDChunkInput{}) {
		return osd.SetChunkInput{}
	}
	return osd.SetChunkInput{SourceOffset: c.SourceOffset, SourceLength: c.SourceLength, TargetObject: c.TargetName, TargetOffset: c.TargetOffset,
		Target: osd.Locator{Pool: c.TargetPool, Key: c.TargetKey, Namespace: c.TargetNamespace, Hash: -1}}
}

// Watch operations reserve event ownership before transmission. This private
// path prevents raw callers from reusing a live stream's cookie on another object.
func (o *OSDConnection) request(ctx context.Context, request OSDRequest, owner *OSDWatchStream) (OSDReply, error) {
	var result OSDReply
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, ErrClosed
	}
	if err := o.validatePlacement(request); err != nil {
		return result, err
	}
	if err := o.validateChecksumCompound(request); err != nil {
		return result, err
	}
	if err := o.validateSetChunk(request); err != nil {
		return result, err
	}
	if err := o.validateIgnoreCache(request); err != nil {
		return result, err
	}
	if err := o.validateCacheOperation(request); err != nil {
		return result, err
	}
	if err := o.validateEnforceSnapContext(request); err != nil {
		return result, err
	}
	request.Placement = cloneOSDPlacement(request.Placement)
	if len(request.Operations) == 0 || len(request.Operations) > osd.MaxOperations {
		return result, osd.ErrRequest
	}
	for _, op := range request.Operations {
		if osd.IsWatchOperation(uint16(op.Code)) {
			if owner == nil || owner.connection != o || owner.cookie != op.Watch.Cookie || owner.request.Object != request.Object || owner.request.ClientIncarnation != request.ClientIncarnation {
				return result, ErrUnsupportedOSDOperation
			}
		} else if owner != nil {
			return result, ErrUnsupportedOSDOperation
		}
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-o.ctx.Done():
		return result, ErrClosed
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := operation.Err(); err != nil {
		return result, o.operationError(err)
	}
	if _, err := c.monitor(operation); err != nil {
		return result, o.operationError(err)
	}
	c.mu.Lock()
	s, closed, globalID := o.session, c.closed || o.closed, c.auth.GlobalID
	c.mu.Unlock()
	if closed {
		return result, ErrClosed
	}
	if globalID != o.globalID {
		return result, errors.New("ceph: OSD connection belongs to an earlier client identity")
	}
	var fixedTID uint64
	if request.Resubmit != nil || request.RetryAttempt != 0 {
		prior := request.Resubmit
		if prior == nil || owner != nil || request.RetryAttempt < 1 || request.RetryAttempt > OSDMaxRetryAttempt ||
			prior.Transaction == 0 || prior.Transaction > c.osdNext.Load() || prior.GlobalID != globalID || prior.ClientIncarnation != request.ClientIncarnation {
			return result, fmt.Errorf("%w: resubmission needs the earlier submission of this client identity and a positive retry attempt", osd.ErrRequest)
		}
		fixedTID = prior.Transaction
	}
	if err := osdOmapEntryBound(request.Operations, c.options.MaxFrameSize); err != nil {
		return result, err
	}
	input := osd.Request{
		ClientInc:        request.ClientIncarnation,
		MTimeSeconds:     request.ModificationTime.Seconds,
		MTimeNanoseconds: request.ModificationTime.Nanoseconds,
		MapEpoch:         request.MapEpoch, Object: request.Object.Name, Hash: request.Object.Hash,
		Locator:    osd.Locator{Pool: request.Object.Pool, Key: request.Object.Key, Namespace: request.Object.Namespace, Hash: -1},
		Operations: make([]osd.Operation, len(request.Operations)),
		ReadSnap:   request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext, FullTry: request.FullTry, FullForce: request.FullForce, BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect, OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, IgnoreCache: request.IgnoreCache, IgnoreOverlay: request.IgnoreOverlay, RetryAttempt: request.RetryAttempt}
	for i, op := range request.Operations {
		if (op.Code == OSDSetRedirect) != (op.Redirect != OSDRedirectInput{}) || op.Code == OSDSetRedirect && op.CopyFrom != (OSDCopyFromInput{}) {
			return result, fmt.Errorf("%w: a redirect target belongs to OSDSetRedirect alone", osd.ErrRequest)
		}
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence, Data: op.Data, Version: op.Version, Xattr: xattrWireInput(op.Xattr), Omap: omapWireInput(op.Omap), Class: classWireInput(op.Class),
			AllocHint:    osd.AllocHintInput{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: uint32(op.AllocHint.Flags)},
			Watch:        watchWireInput(op.Watch),
			SparseRead:   osd.SparseReadInput{MaxOutputBytes: op.SparseRead.MaxOutputBytes},
			MapExt:       osd.MapExtInput{MaxOutputBytes: op.MapExt.MaxOutputBytes},
			Checksum:     osd.ChecksumInput{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type, Seed: op.Checksum.Seed},
			ListWatchers: osd.ListWatchersInput{MaxOutputBytes: op.ListWatchers.MaxOutputBytes},
			CopyFrom:     copyFromWireInput(op),
			SetChunk:     setChunkWireInput(op),
			Snap:         op.Snapshot, ListSnaps: osd.ListSnapsInput{MaxOutputBytes: op.ListSnapshots.MaxOutputBytes}}
	}
	var m msgr.MessageData
	var err error
	if request.Placement != nil {
		p := request.Placement.PG
		if c.options.OSDProfile.requestV9() {
			m, err = osd.EncodeRequestV9(osd.V9Request{Request: input, PG: osd.PG{Pool: p.Pool, Seed: p.Seed}, Shard: request.Placement.wireShard()}, o.features, c.options.MaxFrameSize)
		} else {
			m, err = osd.EncodeRequestV8(osd.V8Request{Request: input, PG: osd.PG{Pool: p.Pool, Seed: p.Seed}, Shard: request.Placement.wireShard()}, o.features, c.options.MaxFrameSize)
		}
	} else {
		m, err = osd.EncodeRequest(input, o.features, c.options.MaxFrameSize)
	}
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, err
	}
	var reply msgr.MessageData
	if fixedTID != 0 {
		reply, err = s.CallAt(operation, m, fixedTID)
	} else {
		reply, err = s.Call(operation, m)
	}
	submission := OSDSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, ClientIncarnation: request.ClientIncarnation, MapEpoch: request.MapEpoch, Placement: request.Placement}
	result.Submission = submission
	if err != nil {
		return result, o.operationError(err)
	}
	decoded, err := o.decodeReply(reply)
	result = publicOSDReply(decoded)
	if osd.ChecksumCompound(input.Operations) {
		// Public conversion precedes identity/completion validation so raw
		// replies remain available on failure. Compound checksum values are
		// withheld until that entire vector validates, even if its reported
		// aggregate is positive. Standalone conversion remains unchanged.
		for i := range result.Operations {
			result.Operations[i].Checksum.Values = nil
		}
	}
	if osd.SparseReadCompound(input.Operations) {
		// Decoded compound sparse-read output likewise waits for complete
		// validation; raw Data and RawData remain available.
		for i := range result.Operations {
			result.Operations[i].SparseRead = OSDSparseReadResult{}
		}
	}
	result.Submission = submission
	result.Transaction, result.GlobalID = reply.Transaction, o.globalID
	redirect := errors.Is(err, osd.ErrRedirect)
	if err == nil || redirect {
		if request.Placement != nil {
			input.Hash = request.Placement.PG.Seed
		}
		err = validateOSDReply(input, decoded, redirect)
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid OSD reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	if !redirect && osd.ChecksumCompound(input.Operations) {
		// Decode prefix values only after the complete compound validates,
		// including a valid signed error after an earlier checksum output.
		for i := 0; i < osd.ChecksumCompoundPrefix(decoded); i++ {
			if op := decoded.Operations[i]; op.Code == osd.Checksum {
				result.Operations[i].Checksum = publicChecksum(op.Checksum, op.Data)
			}
		}
	}
	if !redirect && osd.SparseReadCompound(input.Operations) {
		for i := 0; i < osd.SparseReadCompoundPrefix(decoded); i++ {
			if op := decoded.Operations[i]; op.Code == osd.SparseRead {
				result.Operations[i].SparseRead = publicSparseRead(op.Data)
			}
		}
	}
	if redirect {
		return result, ErrOSDRedirect
	}
	if result.Code < 0 {
		return result, &OSDError{Code: result.Code}
	}
	return result, nil
}

func copyFromWireInput(op OSDOperation) osd.CopyFromInput {
	if op.Code == OSDSetRedirect {
		r := op.Redirect
		return osd.CopyFromInput{Object: r.TargetName, Locator: osd.Locator{Pool: r.TargetPool, Key: r.TargetKey, Namespace: r.TargetNamespace, Hash: -1}, Version: r.TargetVersion}
	}
	in := op.CopyFrom
	if in == (OSDCopyFromInput{}) {
		return osd.CopyFromInput{}
	}
	out := osd.CopyFromInput{Object: in.SourceName, Locator: osd.Locator{Pool: in.SourcePool, Key: in.SourceKey, Namespace: in.SourceNamespace, Hash: -1}, Snap: in.SourceSnapshot, Version: in.SourceVersion, SourceFadvise: uint32(in.SourceFadvise)}
	if in.TruncateSequence != 0 || in.TruncateSize != 0 {
		out.Flags = osd.CopyFromTruncateSequence
		out.TruncateSequence, out.TruncateSize = in.TruncateSequence, in.TruncateSize
	}
	return out
}
