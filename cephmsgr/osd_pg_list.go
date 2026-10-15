package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// OSDPGListTarget identifies one actual PG and the primary OSD chosen by the
// caller. Seed is already folded by that caller's map; no CRUSH or pool scan
// is performed. Primary must equal this connection's target OSD ID.
type OSDPGListTarget struct {
	Pool    uint64
	Seed    uint32
	Primary int32
}

// OSDPGListRequest asks for one head-only PGNLS page. Namespace may be the
// Tentacle all-namespaces sentinel "\x01". An empty Cursor selects the native
// minimum; subsequent requests supply the previous reply's Cursor.Raw.
// Count is a server entry limit; MaxOutputBytes is a local reply-byte budget
// (at least 49). Empty pages may still require another page. StartEpoch is
// raw sequence metadata, preserved without automatic epoch refresh/retry.
// The caller must not modify Cursor until ListPG returns.
type OSDPGListRequest struct {
	ClientIncarnation, MapEpoch uint32
	PG                          OSDPGListTarget
	Namespace                   string
	Cursor                      []byte
	Count                       uint64
	StartEpoch, MaxOutputBytes  uint32
}

// OSDPGCursor preserves a current compatible hobject_t and its raw envelope.
// Raw may be passed to the next ListPG call. Max is the global maximum
// sentinel; End in OSDPGListReply is the end-of-this-PG indication.
type OSDPGCursor struct {
	Key, Object, Namespace string
	Snapshot               uint64
	Hash                   uint32
	Max                    bool
	Pool                   int64
	Raw                    []byte
}

// OSDPGListEntry is raw naming/locator metadata, in server traversal order.
type OSDPGListEntry struct{ Namespace, Object, Key string }

// OSDPGListReply preserves the ordinary wire reply plus a decoded PGNLS page.
// End reports aggregate result 1; result 0 permits another page. Enumeration
// is an observation, not an atomic inventory or snapshot. The caller owns
// cursor progression, epoch/map changes and recovery. No page is resent.
type OSDPGListReply struct {
	OSDReply
	Cursor  OSDPGCursor
	Entries []OSDPGListEntry
	End     bool
}

// ErrOSDPGListTarget rejects a caller-selected primary that differs from
// the connection target before transmission.
var ErrOSDPGListTarget = errors.New("ceph: OSD PG listing target differs from its explicit metadata")

func pgListWireInput(r OSDPGListRequest) osd.PGListRequest {
	return osd.PGListRequest{ClientInc: r.ClientIncarnation, MapEpoch: r.MapEpoch, PG: osd.PG{Pool: r.PG.Pool, Seed: r.PG.Seed}, Namespace: r.Namespace, Cursor: r.Cursor, Count: r.Count, StartEpoch: r.StartEpoch, MaxOutputBytes: r.MaxOutputBytes}
}

// ListPG sends exactly one bounded PGNLS request to an explicit primary.
// A canceled local wait does not cancel server execution. Raw metadata and
// unknown outcomes are retained using the same rules as Request.
func (o *OSDConnection) ListPG(ctx context.Context, request OSDPGListRequest) (OSDPGListReply, error) {
	var result OSDPGListReply
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, ErrClosed
	}
	if request.PG.Primary != o.target.ID {
		return result, ErrOSDPGListTarget
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
	input := pgListWireInput(request)
	m, err := osd.EncodePGListRequest(input, o.features, c.options.MaxFrameSize)
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, err
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
	reply, err := s.Call(operation, m)
	submission := OSDSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, ClientIncarnation: request.ClientIncarnation, MapEpoch: request.MapEpoch}
	result.Submission = submission
	if err != nil {
		return result, o.operationError(err)
	}
	decoded, err := osd.DecodeReply(reply, c.options.MaxFrameSize)
	result.OSDReply = publicOSDReply(decoded)
	result.Submission = submission
	result.Transaction, result.GlobalID = reply.Transaction, o.globalID
	redirect := errors.Is(err, osd.ErrRedirect)
	if err == nil || redirect {
		err = osd.ValidatePGListReply(input, decoded, redirect)
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid OSD PG listing reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	if redirect {
		return result, ErrOSDRedirect
	}
	if result.Code < 0 {
		return result, &OSDError{Code: result.Code}
	}
	page, err := osd.DecodePGList(decoded.Operations[0].Data, request.Count)
	if err != nil {
		return result, err
	} // Already validated; retained as a defensive guard.
	result.Cursor = OSDPGCursor{Key: page.Cursor.Key, Object: page.Cursor.Object, Namespace: page.Cursor.Namespace, Snapshot: page.Cursor.Snap, Hash: page.Cursor.Hash, Max: page.Cursor.Max, Pool: page.Cursor.Pool, Raw: page.Cursor.Raw}
	result.Entries = make([]OSDPGListEntry, len(page.Entries))
	for i, e := range page.Entries {
		result.Entries[i] = OSDPGListEntry{Namespace: e.Namespace, Object: e.Object, Key: e.Key}
	}
	result.End = result.Code == 1
	return result, nil
}
