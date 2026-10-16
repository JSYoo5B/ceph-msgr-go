package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

var (
	ErrOSDWatchReconnect  = errors.New("ceph: watch has no usable confirmed registration for reconnect")
	ErrOSDWatchSuperseded = errors.New("ceph: watch delivery moved to a newer connection")
)

// A private receipt proves an initial successful WATCH, not current liveness.
// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964:
// Objecter.cc:_send_linger increments register_gen for RECONNECT; PrimaryLogPG
// looks up (cookie, authenticated entity), returning ENOTCONN without WATCH
// fallback. Watch.cc:connect may redeliver in-progress notifications. No native
// implementation is copied (LGPL-2.1 source).
type osdWatchRegistration struct {
	client                  *Client
	globalID                uint64
	owner                   *OSDWatchStream // guarded by client.mu, as are the following fields
	lastSessionGeneration   uint64
	generation              uint32
	removed, removalUnknown bool
	gate                    chan struct{} // serializes explicit controls and ownership transfer
}

func (w *OSDWatchStream) confirmWatchRegistration() {
	c := w.connection.client
	r := &osdWatchRegistration{client: c, globalID: w.connection.globalID, owner: w, lastSessionGeneration: w.connection.generation, generation: w.options.RegistrationGeneration, gate: make(chan struct{}, 1)}
	r.gate <- struct{}{}
	// The initial public handle is not exposed before this successful receipt
	// is installed. The pointer is immutable thereafter.
	w.registration = r
}

func (r *osdWatchRegistration) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.client.ctx.Done():
		return ErrClosed
	case <-r.gate:
	}
	if err := ctx.Err(); err != nil {
		r.release()
		return err
	}
	return nil
}
func (r *osdWatchRegistration) release() { r.gate <- struct{}{} }

func (w *OSDWatchStream) performWatchOperation(ctx context.Context, epoch uint32, op OSDOperation) (OSDReply, error) {
	r := w.registration
	if r == nil {
		return w.performRequest(ctx, epoch, op)
	}
	// A handoff may be waiting for its ordinary server reply while holding the
	// control gate. Superseded handles must fail locally without waiting for
	// that unrelated outcome. Check again after acquisition to fence a handoff
	// that races this first check.
	r.client.mu.Lock()
	current := r.owner == w
	r.client.mu.Unlock()
	if !current {
		return OSDReply{}, ErrOSDWatchSuperseded
	}
	if err := r.acquire(ctx); err != nil {
		return OSDReply{}, err
	}
	defer r.release()
	r.client.mu.Lock()
	current = r.owner == w
	removed := r.removed
	r.client.mu.Unlock()
	if !current {
		return OSDReply{}, ErrOSDWatchSuperseded
	}
	if removed {
		return OSDReply{}, ErrOSDWatchReconnect
	}
	reply, err := w.performRequest(ctx, epoch, op)
	if op.Code == OSDWatchOperation && op.Watch.Action == OSDWatchUnwatch {
		r.client.mu.Lock()
		if err == nil {
			r.removed = true
		} else {
			var unknown *OutcomeUnknownError
			if errors.As(err, &unknown) {
				r.removalUnknown = true
			}
		}
		r.client.mu.Unlock()
	}
	return reply, err
}

// Validate the entire caller-owned wire input before transferring authority.
// This is a single fixed operation, so use the real negotiated encoder rather
// than duplicating its epoch, time, snapshot, feature and size constraints.
// Generation is fixed-width and accepts every uint32; its checked increment is
// assigned only after acquiring the registration gate.
func (o *OSDConnection) preflightWatchReconnect(request OSDRequest, cookie uint64) error {
	input := osd.Request{
		ClientInc: request.ClientIncarnation, MTimeSeconds: request.ModificationTime.Seconds, MTimeNanoseconds: request.ModificationTime.Nanoseconds,
		MapEpoch: request.MapEpoch, Object: request.Object.Name, Hash: request.Object.Hash,
		Locator:    osd.Locator{Pool: request.Object.Pool, Key: request.Object.Key, Namespace: request.Object.Namespace, Hash: -1},
		Operations: []osd.Operation{{Code: osd.Watch, Watch: osd.WatchInput{Cookie: cookie, Action: OSDWatchReconnect}}},
		ReadSnap:   request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots,
	}
	var err error
	if p := request.Placement; p != nil {
		pg := osd.PG{Pool: p.PG.Pool, Seed: p.PG.Seed}
		if o.client.options.OSDProfile.requestV9() {
			_, err = osd.EncodeRequestV9(osd.V9Request{Request: input, PG: pg, Shard: -1}, o.features, o.client.options.MaxFrameSize)
		} else {
			_, err = osd.EncodeRequestV8(osd.V8Request{Request: input, PG: pg, Shard: -1}, o.features, o.client.options.MaxFrameSize)
		}
	} else {
		_, err = osd.EncodeRequest(input, o.features, o.client.options.MaxFrameSize)
	}
	if errors.Is(err, osd.ErrFeatures) {
		return fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
	}
	return err
}

// ReconnectWatch explicitly attaches a confirmed registration to a newer OSD
// connection of the same Client and authenticated GlobalID. request supplies
// updated caller-owned routing/placement and no Operations; its object and
// client incarnation must match the original watch. The cookie is preserved,
// while registration generation increments. Nothing discovers or retries a
// target, registers a missing watch or replays a notify.
//
// Ownership transfers locally before one RECONNECT submission. The old stream
// retains its accepted events and first terminal cause, but cannot send Ping,
// Unwatch or ACK afterward. A handle returned together with an operation error
// owns the receipt, including when the remote outcome is unknown. There is no
// rollback; another attempt requires that handle and a newer connection.
func (o *OSDConnection) ReconnectWatch(ctx context.Context, previous *OSDWatch, request OSDRequest) (*OSDWatch, OSDReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, OSDReply{}, err
	}
	if previous == nil || previous.OSDWatchStream == nil || previous.notifier || previous.registration == nil {
		return nil, OSDReply{}, ErrOSDWatchReconnect
	}
	old := previous.OSDWatchStream
	r := old.registration
	if r.client != o.client || request.Object != old.request.Object || request.ClientIncarnation != old.request.ClientIncarnation {
		return nil, OSDReply{}, ErrOSDWatchReconnect
	}
	request, options, err := o.prepareWatchStream(request, old.options)
	if err != nil {
		return nil, OSDReply{}, err
	}
	if err := o.preflightWatchReconnect(request, old.cookie); err != nil {
		return nil, OSDReply{}, err
	}
	if err := r.acquire(ctx); err != nil {
		return nil, OSDReply{}, err
	}
	defer r.release()
	c := o.client
	c.mu.Lock()
	if r.owner != old {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrOSDWatchSuperseded
	}
	if r.removed || r.removalUnknown || r.generation == math.MaxUint32 || o.generation <= r.lastSessionGeneration || o == old.connection {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrOSDWatchReconnect
	}
	if c.closed || o.closed || o.session == nil || c.osds[o.target.ID] != o {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrClosed
	}
	if c.auth.GlobalID != r.globalID || o.globalID != r.globalID {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrOSDWatchReconnect
	}
	if err := o.session.Err(); err != nil {
		c.mu.Unlock()
		return nil, OSDReply{}, err
	}
	if o.watchState == nil {
		o.watchState = &osdWatchState{streams: make(map[uint64]*OSDWatchStream)}
	}
	if len(o.watchState.streams) >= 64 {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrOSDWatchLimit
	}
	if o.watchState.streams[old.cookie] != nil {
		c.mu.Unlock()
		return nil, OSDReply{}, ErrOSDWatchReconnect
	}
	r.generation++
	options.RegistrationGeneration = r.generation
	next := &OSDWatchStream{connection: o, cookie: old.cookie, request: request, options: options, changed: make(chan struct{}), registration: r}
	// Each cookie/session pair is used once. Merely incrementing the WATCH
	// generation cannot fence late events: MWatchNotify carries no such field.
	old.finishLocked(ErrOSDWatchSuperseded)
	r.owner, r.lastSessionGeneration = next, o.generation
	o.watchState.streams[next.cookie] = next
	c.mu.Unlock()
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: next.cookie, Action: OSDWatchReconnect, Generation: r.generation}}
	reply, err := next.performRequest(ctx, request.MapEpoch, op)
	if err != nil {
		next.finish(err)
	}
	return &OSDWatch{next}, reply, err
}
