package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

type OSDOpcode uint16

const (
	OSDRead          OSDOpcode = OSDOpcode(osd.Read)
	OSDStat          OSDOpcode = OSDOpcode(osd.Stat)
	OSDWrite         OSDOpcode = OSDOpcode(osd.Write)
	OSDWriteFull     OSDOpcode = OSDOpcode(osd.WriteFull)
	OSDAppend        OSDOpcode = OSDOpcode(osd.Append)
	OSDTruncate      OSDOpcode = OSDOpcode(osd.Truncate)
	OSDZero          OSDOpcode = OSDOpcode(osd.Zero)
	OSDCreate        OSDOpcode = OSDOpcode(osd.Create)
	OSDDelete        OSDOpcode = OSDOpcode(osd.Delete)
	OSDAssertVersion OSDOpcode = OSDOpcode(osd.AssertVersion)
	OSDCompareExtent OSDOpcode = OSDOpcode(osd.CompareExtent)
)

// OSD reply completion flags are native OSD flags, not Messenger ACKs.
// ONDISK is a server completion report whose persistence depends on its store.
const (
	OSDReplyAck     = osd.FlagAck
	OSDReplyOnNVRAM = osd.FlagOnNVRAM
	OSDReplyOnDisk  = osd.FlagOnDisk
)

// OSDObject contains wire locator metadata prepared by the routing layer.
// Hash is the raw object-placement hash, not an actual PG number. The client
// performs no hashing or CRUSH calculation. The initial v6 encoding supports
// replicated-pool requests and no independently targeted erasure-code shard.
type OSDObject struct {
	Pool           int64
	Name           string
	Key, Namespace string
	Hash           uint32
}

type OSDOperationFlags uint32

const (
	OSDExclusiveCreate OSDOperationFlags = OSDOperationFlags(osd.OpExclusive)
	// OSDFailOK continues a compound vector after this operation's error. Its
	// negative per-op result is retained even if later mutations commit.
	OSDFailOK OSDOperationFlags = OSDOperationFlags(osd.OpFailOK)
)

// OSDOperation is one ordered operation on the request's object. Writes and
// CompareExtent require Length == len(Data); WriteFull/Append have Offset 0.
// Truncate uses Offset as the new size. Create/Delete/Stat have zero extents.
// AssertVersion uses Version, with zero extents and no payload. Only Create
// accepts ExclusiveCreate. Truncate-sequence variants are unsupported.
// Xattr is the active input union for Get/GetAll/Compare/Set/RemoveXattr;
// their extent and Version fields are zero. Data holds the SET/CMP bytes.
// Omap is the active input for the OMAP reads and mutations, with zero
// extent, Version, Data and Xattr. Its wire input length is derived locally.
// Class is the active input for OSDCall, with zero extent, Version, Xattr
// and Omap; Data holds the method input. AllocHint is the active input for
// OSDSetAllocHint, with every other field zero. Snapshot is the ROLLBACK
// target (1 through OSDMaxSnapshot) and ListSnapshots the OSDListSnapshots
// budget; both opcodes carry no other field and no flags. ListWatchers is the
// local output budget for head-only, standalone OSDListWatchers. MapExt and
// Checksum select bounded, standalone head reads using Offset/Length.
// Caller must not mutate the vector, Data or Omap inputs until Request returns.
type OSDOperation struct {
	Code           OSDOpcode
	Flags          OSDOperationFlags
	Offset, Length uint64
	Data           []byte
	Version        uint64
	Xattr          OSDXattrInput
	Omap           OSDOmapInput
	Class          OSDClassInput
	AllocHint      OSDAllocHint
	Snapshot       uint64
	MapExt         OSDMapExtInput
	Checksum       OSDChecksumInput
	ListWatchers   OSDListWatchersInput
	ListSnapshots  OSDListSnapshotsInput
}

// OSDTime is Tentacle's raw utime_t. Zero preserves the server's zero-mtime
// semantics; the client does not substitute its own clock. Nanoseconds < 1e9.
type OSDTime struct{ Seconds, Nanoseconds uint32 }

// OSDRequest is one ordered, atomic object vector on an explicitly targeted
// replicated OSD. Mutation replies request only ONDISK completion, not a
// separate preliminary ACK. There is no automatic epoch refresh, redirect,
// placement calculation, retry or mutation replay.
//
// ReadSnapshot selects the clone a read-only vector observes: zero (or
// OSDHeadSnapshot) is the head, and 1 through OSDMaxSnapshot name a snap id.
// The reply does not echo it. SnapContext and OrderSnapshots apply only to
// vectors that write, which always target the head. OrderSnapshots sets
// CEPH_OSD_FLAG_ORDERSNAP so an older SnapContext is refused with
// OSDOldSnapContext instead of writing without a clone.
type OSDRequest struct {
	ClientIncarnation uint32
	ModificationTime  OSDTime
	MapEpoch          uint32
	Object            OSDObject
	Operations        []OSDOperation
	ReadSnapshot      uint64
	SnapContext       OSDSnapContext
	OrderSnapshots    bool
}

type OSDPG struct {
	Pool uint64
	// Seed preserves the v6 reply pg_t seed, which echoes the request's raw
	// object-placement hash. It does not identify the actual mapped PG.
	Seed uint32
}

type OSDVersion struct {
	Epoch   uint32
	Version uint64
}

// OSDOperationResult preserves each operation's wire metadata and raw output.
// Read and CompareExtent may report a shorter Length at EOF. CompareExtent
// compares bytes beyond EOF with zero without returning a payload.
// Stat's bytes describe size and mtime; this layer does not convert them into a
// filesystem-style object API. The caller can interpret the wire data.
// Xattr keeps attribute lengths/comparison fields separate from extents.
// For OMAP operations, Length echoes the encoded input byte length (zero for
// GetOmapHeader and ClearOmap). Data retains the raw collection framing,
// including the page's more suffix, or the raw header; comparisons and
// mutations return none.
type OSDOperationResult struct {
	Code                         OSDOpcode
	Flags                        uint32
	Offset, Length, TruncateSize uint64
	TruncateSequence             uint32
	PayloadLength                uint32
	Result                       int32
	Data                         []byte
	Xattr                        OSDXattrResult
	Class                        OSDClassResult
	AllocHint                    OSDAllocHint
	// Snapshot echoes the ROLLBACK target. Snapshots is decoded from Data
	// for a successful OSDListSnapshots, which also keeps the raw bytes.
	Snapshot  uint64
	Snapshots OSDSnapshotList
	// Watchers is decoded on a successful OSDListWatchers; Data keeps raw bytes.
	Watchers []OSDWatcher
	// Extents decodes successful MAPEXT output; Data preserves the raw map.
	Extents []OSDExtent
	// Checksum retains the union on all replies; successful output adds Values.
	Checksum OSDChecksumResult
}

type OSDRedirect struct {
	Pool               int64
	Key, Namespace     string
	Hash               int64
	Object             string
	LegacyInstructions []byte
}

// OSDSubmission is local request metadata, retained when an admitted request
// ends with an error. It is not a server reply or proof of execution.
type OSDSubmission struct {
	Transaction, GlobalID       uint64
	ClientIncarnation, MapEpoch uint32
	Attempt                     uint32
}

// OSDReply preserves the server's completion metadata and operation outputs.
// Flags are raw Ceph OSD flags, independent of Messenger ACKs. A read-only
// success does not establish any write durability guarantee. A mutation
// success requires the server ONDISK flag; durability is storage/profile
// dependent (in particular, MemStore contents do not survive OSD restart).
type OSDReply struct {
	Submission                   OSDSubmission
	Transaction, GlobalID        uint64
	Object                       string
	PG                           OSDPG
	Flags                        uint64
	Code                         int32
	MapEpoch                     uint32
	RetryAttempt                 int32
	LegacyVersion, ReplayVersion OSDVersion
	UserVersion                  uint64
	Operations                   []OSDOperationResult
	Redirect                     OSDRedirect
	Trace                        [3]uint64
	RawFront, RawData            []byte
}

// ErrOSDRedirect accompanies a decoded redirect reply. The routing layer may
// inspect it; this client never follows a redirect automatically.
var ErrOSDRedirect = osd.ErrRedirect

// ErrUnsupportedOSDOperation rejects unknown opcodes, flags and unsupported
// compound combinations before object-request transmission. In Tentacle,
// offset-zero READ/CMPEXT after a possible full write can compare committed
// bytes with a staged digest; these combinations are conservatively rejected.
// OMAP and xattr reads after a staged write of the same metadata kind or a
// DELETE, which may read committed rather than staged state, are rejected the
// same way.
var ErrUnsupportedOSDOperation = osd.ErrOperation

type OSDError struct{ Code int32 }

func (e *OSDError) Error() string { return fmt.Sprintf("ceph: OSD result code %d", e.Code) }

// Request transmits one explicitly targeted ordered object vector. ctx
// governs the full local wait, including MaxInFlight admission. Cancellation
// after transmission starts preserves OutcomeUnknownError. Replies expose the
// wire pg_t, epoch and versions for the caller's routing layer; no retry occurs.
func (o *OSDConnection) Request(ctx context.Context, request OSDRequest) (OSDReply, error) {
	var result OSDReply
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, ErrClosed
	}
	if len(request.Operations) == 0 || len(request.Operations) > osd.MaxOperations {
		return result, osd.ErrRequest
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
		ReadSnap:   request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, Data: op.Data, Version: op.Version, Xattr: xattrWireInput(op.Code, op.Xattr), Omap: omapWireInput(op.Omap), Class: classWireInput(op.Class),
			AllocHint:    osd.AllocHintInput{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: uint32(op.AllocHint.Flags)},
			MapExt:       osd.MapExtInput{MaxOutputBytes: op.MapExt.MaxOutputBytes},
			Checksum:     osd.ChecksumInput{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type, Seed: op.Checksum.Seed},
			ListWatchers: osd.ListWatchersInput{MaxOutputBytes: op.ListWatchers.MaxOutputBytes},
			Snap:         op.Snapshot, ListSnaps: osd.ListSnapsInput{MaxOutputBytes: op.ListSnapshots.MaxOutputBytes}}
	}
	m, err := osd.EncodeRequest(input, o.features, c.options.MaxFrameSize)
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, err
	}
	reply, err := s.Call(operation, m)
	submission := OSDSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, ClientIncarnation: request.ClientIncarnation, MapEpoch: request.MapEpoch}
	result.Submission = submission
	if err != nil {
		return result, o.operationError(err)
	}
	decoded, err := osd.DecodeReply(reply, c.options.MaxFrameSize)
	result = publicOSDReply(decoded)
	result.Submission = submission
	result.Transaction, result.GlobalID = reply.Transaction, o.globalID
	redirect := errors.Is(err, osd.ErrRedirect)
	if err == nil || redirect {
		err = validateOSDReply(input, decoded, redirect)
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid OSD reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	if redirect {
		return result, ErrOSDRedirect
	}
	if result.Code < 0 {
		return result, &OSDError{Code: result.Code}
	}
	return result, nil
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

func validateOSDReply(input osd.Request, reply osd.Reply, redirect bool) error {
	if reply.Object != input.Object || reply.PG.Pool != uint64(input.Locator.Pool) || reply.PG.Seed != input.Hash || reply.RetryAttempt != 0 || len(reply.Operations) != len(input.Operations) {
		return errors.New("OSD reply does not match request identity or operations")
	}
	flags, _, err := osd.ValidateOperations(input.Operations, ^uint32(0))
	if err != nil {
		return err
	}
	if !redirect && reply.Result >= 0 && flags&osd.FlagWrite != 0 && !reply.OnDisk() {
		return errors.New("mutation reply lacks requested ONDISK completion")
	}
	for i, requested := range input.Operations {
		op := reply.Operations[i]
		normalizedZero := requested.Code == osd.Zero && op.Code == osd.Truncate
		length := requested.Length
		if requested.Code == osd.AssertVersion {
			length = requested.Version
		}
		shortExtent := requested.Code == osd.Read || requested.Code == osd.CompareExtent
		if op.Code != requested.Code && !normalizedZero || op.Flags&(osd.OpExclusive|osd.OpFailOK) != requested.Flags || uint64(len(op.Data)) != uint64(op.PayloadLength) {
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
		case osd.ListSnaps:
			if err := osd.ValidateListSnapsReply(requested, op, success); err != nil {
				return err
			}
			continue
		}
		if op.Offset != requested.Offset || op.Length > length || !shortExtent && op.Length != length || op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Xattr != (osd.XattrResult{}) {
			return errors.New("OSD reply extent differs from request")
		}
		switch requested.Code {
		case osd.Read:
			if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length {
				return errors.New("OSD read reply exceeds requested length or has inconsistent output")
			}
		case osd.Stat:
			if op.PayloadLength > 16 || success && op.PayloadLength != 16 {
				return errors.New("OSD stat reply has invalid size and mtime output")
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
		result.Operations[i] = OSDOperationResult{Code: OSDOpcode(op.Code), Flags: op.Flags,
			Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence,
			PayloadLength: op.PayloadLength, Result: op.Result, Data: op.Data,
			Xattr:     OSDXattrResult{NameLength: op.Xattr.NameLength, ValueLength: op.Xattr.ValueLength, ComparisonOp: op.Xattr.ComparisonOp, ComparisonMode: op.Xattr.ComparisonMode},
			Class:     OSDClassResult{ClassLength: op.Class.ClassLength, MethodLength: op.Class.MethodLength, Argc: op.Class.Argc, InputLength: op.Class.InputLength, OutputLength: op.Class.OutputLength},
			AllocHint: OSDAllocHint{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: OSDAllocHintFlags(op.AllocHint.Flags)},
			Snapshot:  op.Snap, Checksum: OSDChecksumResult{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type}}
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
	}
	return result
}
