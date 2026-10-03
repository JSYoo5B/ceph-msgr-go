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
	OSDRead OSDOpcode = OSDOpcode(osd.Read)
	OSDStat OSDOpcode = OSDOpcode(osd.Stat)
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

// OSDOperation is a single stat or bounded read on the request's object. Stat
// has zero Offset and Length. Read requires a positive Length; read-to-end and
// mutation opcodes are outside this milestone. Offset+Length must not overflow.
type OSDOperation struct {
	Code           OSDOpcode
	Offset, Length uint64
}

// OSDRequest specifies a first-attempt request with no snapshot context.
// MapEpoch is obtained by the caller's map layer. The client does not move a
// request to another OSD or refresh its epoch automatically.
type OSDRequest struct {
	MapEpoch   uint32
	Object     OSDObject
	Operations []OSDOperation
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
// Stat's bytes describe size and mtime; this layer does not convert them into a
// filesystem-style object API. The caller can interpret the wire data.
type OSDOperationResult struct {
	Code                         OSDOpcode
	Flags                        uint32
	Offset, Length, TruncateSize uint64
	TruncateSequence             uint32
	PayloadLength                uint32
	Result                       int32
	Data                         []byte
}

type OSDRedirect struct {
	Pool               int64
	Key, Namespace     string
	Hash               int64
	Object             string
	LegacyInstructions []byte
}

// OSDReply preserves the server's completion metadata and operation outputs.
// Flags are raw Ceph OSD flags, independent of Messenger ACKs. A read-only
// success does not establish any write durability guarantee.
type OSDReply struct {
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

type OSDError struct{ Code int32 }

func (e *OSDError) Error() string { return fmt.Sprintf("ceph: OSD result code %d", e.Code) }

// Request transmits one explicitly targeted, read-only object request. ctx
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
	input := osd.Request{MapEpoch: request.MapEpoch, Object: request.Object.Name, Hash: request.Object.Hash,
		Locator:    osd.Locator{Pool: request.Object.Pool, Key: request.Object.Key, Namespace: request.Object.Namespace, Hash: -1},
		Operations: make([]osd.Operation, len(request.Operations))}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Offset: op.Offset, Length: op.Length}
	}
	m, err := osd.EncodeRequest(input, o.features, c.options.MaxFrameSize)
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, err
	}
	reply, err := s.Call(operation, m)
	if err != nil {
		return result, o.operationError(err)
	}
	decoded, err := osd.DecodeReply(reply, c.options.MaxFrameSize)
	result = publicOSDReply(decoded)
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

func validateOSDReply(input osd.Request, reply osd.Reply, redirect bool) error {
	if reply.Object != input.Object || reply.PG.Pool != uint64(input.Locator.Pool) || reply.PG.Seed != input.Hash || reply.RetryAttempt != 0 || len(reply.Operations) != len(input.Operations) {
		return errors.New("OSD reply does not match request identity or operations")
	}
	for i, requested := range input.Operations {
		op := reply.Operations[i]
		if op.Code != requested.Code || op.Offset != requested.Offset || op.Length > requested.Length || uint64(len(op.Data)) != uint64(op.PayloadLength) {
			return errors.New("OSD reply operation differs from request or payload metadata")
		}
		success := !redirect && reply.Result >= 0 && op.Result >= 0
		switch requested.Code {
		case osd.Read:
			if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length {
				return errors.New("OSD read reply exceeds requested length or has inconsistent output")
			}
		case osd.Stat:
			// A failed vector may leave an unexecuted stat with rval 0 and no
			// output. Only an ordinary successful reply requires size+mtime.
			if op.PayloadLength > 16 || success && op.PayloadLength != 16 {
				return errors.New("OSD stat reply has invalid size and mtime output")
			}
		default:
			return errors.New("OSD reply has an unsupported operation")
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
			PayloadLength: op.PayloadLength, Result: op.Result, Data: op.Data}
	}
	return result
}
