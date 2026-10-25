package cephmsgr

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func validateOSDReply(input osd.Request, reply osd.Reply, redirect bool) error {
	if reply.Object != input.Object || reply.PG.Pool != uint64(input.Locator.Pool) || reply.PG.Seed != input.Hash || reply.RetryAttempt != input.RetryAttempt || len(reply.Operations) != len(input.Operations) {
		return errors.New("OSD reply does not match request identity or operations")
	}
	flags, _, err := osd.ValidateOperations(input.Operations, ^uint32(0))
	if err != nil {
		return err
	}
	if !redirect && reply.Result >= 0 && osd.RequiresOnDisk(input.Operations, flags) && !reply.OnDisk() {
		return errors.New("OSD reply lacks requested ONDISK completion")
	}
	if !redirect && osd.ChecksumCompound(input.Operations) {
		return osd.ValidateChecksumCompoundReply(input.Operations, reply)
	}
	if !redirect && osd.SparseReadCompound(input.Operations) {
		return osd.ValidateSparseReadCompoundReply(input.Operations, reply)
	}
	for i, requested := range input.Operations {
		op := reply.Operations[i]
		normalizedZero := requested.Code == osd.Zero && op.Code == osd.Truncate
		length := requested.Length
		if requested.Code == osd.AssertVersion {
			length = requested.Version
		}
		// The OSD trims a WRITE that extends an object past a newer
		// truncation and echoes the shortened extent (PrimaryLogPG.cc:6768-6777).
		shortExtent := requested.Code == osd.Read || requested.Code == osd.CompareExtent || requested.Code == osd.Write
		if op.Code != requested.Code && !normalizedZero || op.Flags&(osd.OpExclusive|osd.OpFailOK) != requested.Flags&(osd.OpExclusive|osd.OpFailOK) || uint64(len(op.Data)) != uint64(op.PayloadLength) {
			return errors.New("OSD reply operation differs from request or payload metadata")
		}
		// Tentacle stops a vector on an ordinary failed operation. Only
		// FAILOK can retain a negative per-op result in a successful vector.
		// A contradictory success is not an execution/durability receipt.
		// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L8205
		// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9188
		if !redirect && reply.Result >= 0 && op.Result < 0 && requested.Flags&osd.OpFailOK == 0 {
			return errors.New("OSD successful vector contains a failed ordinary operation")
		}
		success := !redirect && reply.Result >= 0 && op.Result >= 0
		switch requested.Code {
		case osd.OmapGetKeys, osd.OmapGetValues, osd.OmapGetHeader, osd.OmapGetValuesByKeys, osd.OmapCompare,
			osd.OmapSetValues, osd.OmapSetHeader, osd.OmapClear, osd.OmapRemoveKeys, osd.OmapRemoveKeyRange:
			if err := osd.ValidateOmapReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.GetXattr, osd.GetXattrs, osd.CompareXattr, osd.SetXattr, osd.RemoveXattr:
			if err := osd.ValidateXattrReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.Call:
			if err := osd.ValidateClassReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.SetAllocHint:
			if err := osd.ValidateAllocHintReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.Rollback:
			if err := osd.ValidateRollbackReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.Watch, osd.Notify, osd.NotifyAck:
			if err := osd.ValidateWatchReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.SparseRead:
			if err := osd.ValidateSparseReadReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.MapExt:
			if err := osd.ValidateMapExtReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.Checksum:
			if err := osd.ValidateChecksumReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.ListWatchers:
			if err := osd.ValidateListWatchersReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.WriteSame:
			if err := osd.ValidateWriteSameReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.CopyFrom, osd.CopyFrom2, osd.SetRedirect:
			if err := osd.ValidateCopyFromReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.ListSnaps:
			if err := osd.ValidateListSnapsReply(requested, op, success); err != nil {
				return err
			}
			continue
		}
		if op.Offset != requested.Offset || op.Length > length || !shortExtent && op.Length != length || op.TruncateSize != requested.TruncateSize || op.TruncateSequence != requested.TruncateSequence || op.Xattr != (osd.XattrResult{}) {
			return errors.New("OSD reply extent differs from request")
		}
		switch requested.Code {
		case osd.Read:
			// A local read sets the extent length to the bytes returned; a read
			// the OSD proxies to a manifest object's redirect target echoes
			// the requested length instead (observed live, Tentacle 20.2.4).
			if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length && op.Length != requested.Length {
				return errors.New("OSD read reply exceeds requested length or has inconsistent output")
			}
		case osd.Stat:
			if op.PayloadLength > 16 || success && op.PayloadLength != 16 {
				return errors.New("OSD stat reply has invalid size and mtime output")
			}
		case osd.Isdirty:
			// ISDIRTY encodes one bool (1 byte) into its output; a successful
			// read returns exactly that, a failed one returns nothing.
			if op.PayloadLength > 1 || success && op.PayloadLength != 1 {
				return errors.New("OSD isdirty reply has invalid dirty-flag output")
			}
		default:
			if op.PayloadLength != 0 {
				return errors.New("OSD mutation/guard reply has unsupported output")
			}
		}
	}
	return nil
}

func (o *OSDConnection) operationError(err error) error {
	if o.ctx.Err() == nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return err
	}
	var unknown *OutcomeUnknownError
	if errors.As(err, &unknown) {
		return &OutcomeUnknownError{Cause: errors.Join(unknown.Cause, ErrClosed)}
	}
	return errors.Join(err, ErrClosed)
}

func publicOSDReply(r osd.Reply) OSDReply {
	result := OSDReply{Object: r.Object, PG: OSDPG{Pool: r.PG.Pool, Seed: r.PG.Seed}, Flags: r.Flags,
		Code: r.Result, MapEpoch: r.MapEpoch, RetryAttempt: r.RetryAttempt, UserVersion: r.UserVersion,
		LegacyVersion: OSDVersion{Epoch: r.LegacyVersion.Epoch, Version: r.LegacyVersion.Version},
		ReplayVersion: OSDVersion{Epoch: r.ReplayVersion.Epoch, Version: r.ReplayVersion.Version},
		Trace:         r.Trace, RawFront: r.RawFront, RawData: r.RawData,
		Redirect: OSDRedirect{Pool: r.Redirect.Locator.Pool, Key: r.Redirect.Locator.Key, Namespace: r.Redirect.Locator.Namespace,
			Hash: r.Redirect.Locator.Hash, Object: r.Redirect.Object, LegacyInstructions: r.Redirect.LegacyInstructions}}
	result.Operations = make([]OSDOperationResult, len(r.Operations))
	for i, op := range r.Operations {
		result.Operations[i] = OSDOperationResult{WriteSamePatternLength: op.WriteSamePatternLength, CopyFromVersion: op.CopyFrom.Version, CopyFromSnapshot: op.CopyFrom.Snap, Code: OSDOpcode(op.Code), Flags: op.Flags,
			Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence,
			PayloadLength: op.PayloadLength, Result: op.Result, Data: op.Data,
			Xattr:     OSDXattrResult{NameLength: op.Xattr.NameLength, ValueLength: op.Xattr.ValueLength, ComparisonOp: op.Xattr.ComparisonOp, ComparisonMode: op.Xattr.ComparisonMode},
			Class:     OSDClassResult{ClassLength: op.Class.ClassLength, MethodLength: op.Class.MethodLength, Argc: op.Class.Argc, InputLength: op.Class.InputLength, OutputLength: op.Class.OutputLength},
			AllocHint: OSDAllocHint{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: OSDAllocHintFlags(op.AllocHint.Flags)},
			Watch:     OSDWatchInput{Cookie: op.Watch.Cookie, Version: op.Watch.Version, NotifyID: op.Watch.NotifyID, Generation: op.Watch.Generation, TimeoutSeconds: op.Watch.TimeoutSeconds, ProtocolVersion: op.Watch.ProtocolVersion, Action: op.Watch.Action},
			Snapshot:  op.Snap, Checksum: OSDChecksumResult{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type}}
		if op.Code == osd.SparseRead && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].SparseRead = publicSparseRead(op.Data)
		}
		if op.Code == osd.MapExt && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Extents = publicMapExt(op.Data)
		}
		if op.Code == osd.Checksum && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Checksum = publicChecksum(op.Checksum, op.Data)
		}
		if op.Code == osd.ListWatchers && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Watchers = publicWatchers(op.Data)
		}
		if op.Code == osd.ListSnaps && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Snapshots = publicSnapshotList(op.Data)
		}
		if op.Code == osd.Isdirty && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() && len(op.Data) == 1 {
			result.Operations[i].Dirty = op.Data[0] != 0
		}
	}
	return result
}
