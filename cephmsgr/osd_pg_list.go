package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// OSDPGListTarget identifies one actual PG and the primary OSD chosen by the
// caller. Seed is already folded by that caller's map; no CRUSH or pool scan
// is performed. Primary must equal this connection's target OSD ID. For an
// erasure-coded PG set Erasure and Shard to the primary's acting-set position
// (replicated-PG profiles; the object-hash profile's v6 request has no shard).
type OSDPGListTarget struct {
	Pool    uint64
	Seed    uint32
	Primary int32
	Erasure bool
	Shard   uint8
	// PrimaryFirst has the meaning of OSDPlacement.PrimaryFirst.
	PrimaryFirst *OSDPrimaryFirst
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
	// Filter, when set, sends PGNLS_FILTER: the OSD returns only the entries
	// its named filter accepts. Rejected entries still advance the cursor, so
	// a filtered page may be empty without being the PG's end. The caller must
	// not modify Filter.Input until ListPG returns.
	Filter *OSDPGListFilter
}

// OSDPGListFilter names an OSD-side PGNLS filter: "plain" (built into the
// OSD) or "<class>.<filter>" from an object class the OSD loads. Input is the
// filter's raw parameters, sent unchanged. An unknown class or filter is
// refused by the OSD with -EINVAL (-EPERM when class loading is denied).
type OSDPGListFilter struct {
	Name  string
	Input []byte
}

// OSDPlainPGListFilter builds the OSD's built-in "plain" filter, which keeps
// objects whose attribute key equals value byte for byte and drops objects
// without that attribute (osd_types.cc PGLSPlainFilter). key is the OSD's
// attribute key: an xattr set through SETXATTR with name n is stored under
// "_" + n.
func OSDPlainPGListFilter(key string, value []byte) OSDPGListFilter {
	e := wire.Encoder{}
	e.String(key)
	e.U32(uint32(len(value)))
	e.Raw(value)
	return OSDPGListFilter{Name: "plain", Input: e.Data}
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
	input := osd.PGListRequest{ClientInc: r.ClientIncarnation, MapEpoch: r.MapEpoch, PG: osd.PG{Pool: r.PG.Pool, Seed: r.PG.Seed}, Namespace: r.Namespace, Cursor: r.Cursor, Count: r.Count, StartEpoch: r.StartEpoch, MaxOutputBytes: r.MaxOutputBytes, Erasure: r.PG.Erasure, Shard: primaryFirstShard(r.PG.Shard, r.PG.PrimaryFirst)}
	if r.Filter != nil {
		input.Filter = &osd.PGListFilter{Name: r.Filter.Name, Input: r.Filter.Input}
	}
	return input
}

// ListPG sends exactly one bounded PGNLS request to an explicit primary.
// A canceled local wait does not cancel server execution. Raw metadata and
// unknown outcomes are retained using the same rules as Request.
func (o *OSDConnection) ListPG(ctx context.Context, request OSDPGListRequest) (OSDPGListReply, error) {
	var result OSDPGListReply
	input := pgListWireInput(request)
	encode := func(v9, v8 bool) (msgr.MessageData, error) {
		switch {
		case v9:
			return osd.EncodePGListRequestV9(input, o.features, o.client.options.MaxFrameSize)
		case v8:
			return osd.EncodePGListRequestV8(input, o.features, o.client.options.MaxFrameSize)
		}
		return osd.EncodePGListRequest(input, o.features, o.client.options.MaxFrameSize)
	}
	validate := func(decoded osd.Reply, redirect bool) error { return osd.ValidatePGListReply(input, decoded, redirect) }
	reply, decoded, err := o.pgOperation(ctx, request.PG, request.ClientIncarnation, request.MapEpoch, "PG listing", encode, validate)
	result.OSDReply = reply
	if err != nil {
		return result, err
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

// pgOperation sends one PG-level operation to the explicit primary and
// validates its reply; a refusal returns *OSDError with the reply retained.
func (o *OSDConnection) pgOperation(ctx context.Context, target OSDPGListTarget, incarnation, epoch uint32, label string, encode func(v9, v8 bool) (msgr.MessageData, error), validate func(osd.Reply, bool) error) (OSDReply, osd.Reply, error) {
	var result OSDReply
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, osd.Reply{}, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, osd.Reply{}, ErrClosed
	}
	if target.Primary != o.target.ID {
		return result, osd.Reply{}, ErrOSDPGListTarget
	}
	if !validShardPlacement(target.Erasure, target.Shard, target.PrimaryFirst) || target.Erasure && !c.options.OSDProfile.replicatedPG() {
		return result, osd.Reply{}, fmt.Errorf("%w: a PG shard needs erasure-coded placement and a replicated-PG profile", osd.ErrRequest)
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, osd.Reply{}, ctx.Err()
	case <-o.ctx.Done():
		return result, osd.Reply{}, ErrClosed
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := operation.Err(); err != nil {
		return result, osd.Reply{}, o.operationError(err)
	}
	m, err := encode(c.options.OSDProfile.requestV9(), c.options.OSDProfile.replicatedPG())
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, osd.Reply{}, err
	}
	if _, err := c.monitor(operation); err != nil {
		return result, osd.Reply{}, o.operationError(err)
	}
	c.mu.Lock()
	s, closed, globalID := o.session, c.closed || o.closed, c.auth.GlobalID
	c.mu.Unlock()
	if closed {
		return result, osd.Reply{}, ErrClosed
	}
	if globalID != o.globalID {
		return result, osd.Reply{}, errors.New("ceph: OSD connection belongs to an earlier client identity")
	}
	reply, err := s.Call(operation, m)
	submission := OSDSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, ClientIncarnation: incarnation, MapEpoch: epoch,
		Placement: &OSDPlacement{PG: OSDPG{Pool: target.Pool, Seed: target.Seed}, Primary: target.Primary, Erasure: target.Erasure, Shard: target.Shard, PrimaryFirst: cloneOSDPrimaryFirst(target.PrimaryFirst)}}
	result.Submission = submission
	if err != nil {
		return result, osd.Reply{}, o.operationError(err)
	}
	decoded, err := o.decodeReply(reply)
	result = publicOSDReply(decoded)
	result.Submission = submission
	result.Transaction, result.GlobalID = reply.Transaction, o.globalID
	redirect := errors.Is(err, osd.ErrRedirect)
	if err == nil || redirect {
		err = validate(decoded, redirect)
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid OSD %s reply: %w", msgr.ErrFrame, label, err)
		s.Fail(cause)
		return result, decoded, &OutcomeUnknownError{Cause: cause}
	}
	if redirect {
		return result, decoded, ErrOSDRedirect
	}
	if result.Code < 0 {
		return result, decoded, &OSDError{Code: result.Code}
	}
	return result, decoded, nil
}
