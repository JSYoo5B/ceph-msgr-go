package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

const (
	OSDWatchOperation     OSDOpcode = OSDOpcode(osd.Watch)
	OSDNotifyOperation    OSDOpcode = OSDOpcode(osd.Notify)
	OSDNotifyAckOperation OSDOpcode = OSDOpcode(osd.NotifyAck)
	OSDWatchUnwatch       uint8     = osd.WatchUnwatch
	OSDWatchRegister      uint8     = osd.WatchRegister
	OSDWatchReconnect     uint8     = osd.WatchReconnect
	OSDWatchPing          uint8     = osd.WatchPing
	OSDWatchNotification  uint8     = osd.WatchEventNotify
	OSDWatchCompletion    uint8     = osd.WatchEventComplete
	OSDWatchDisconnect    uint8     = osd.WatchEventDisconnect
)

// OSDWatchInput is the active input for WATCH, NOTIFY and NOTIFY_ACK.
// Operation.Data is the user notification or acknowledgement blob. NOTIFY
// uses ProtocolVersion 1; WATCH uses Action, Generation and TimeoutSeconds.
// Direct Request submission is rejected: WatchObject and NotifyObject reserve
// a unique cookie and event consumer before transmitting these operations.
type OSDWatchInput struct {
	Cookie, Version, NotifyID                   uint64
	Generation, TimeoutSeconds, ProtocolVersion uint32
	Action                                      uint8
}

func watchWireInput(w OSDWatchInput) osd.WatchInput {
	return osd.WatchInput{Cookie: w.Cookie, Version: w.Version, NotifyID: w.NotifyID, Generation: w.Generation, TimeoutSeconds: w.TimeoutSeconds, ProtocolVersion: w.ProtocolVersion, Action: w.Action}
}

var (
	ErrOSDWatchOverflow     = errors.New("ceph: OSD watch event queue exceeded")
	ErrOSDWatchLimit        = errors.New("ceph: OSD watch stream limit reached")
	ErrOSDWatchDisconnected = errors.New("ceph: OSD watch registration disconnected")
	ErrOSDNotifyComplete    = errors.New("ceph: OSD notify completion received")
)

// OSDWatchOptions bounds the retained queue, at most 64 events. The default
// byte budget is MaxFrameSize; this conservative estimate is not a heap cap.
// TimeoutSeconds is sent unchanged. The caller owns explicit Ping scheduling;
// there is no automatic renewal, reconnection or re-registration.
type OSDWatchOptions struct{ TimeoutSeconds, RegistrationGeneration, MaxBufferedBytes uint32 }

type OSDNotifyIdentity struct{ GlobalID, Cookie uint64 }
type OSDNotifyReply struct {
	OSDNotifyIdentity
	Data []byte
}

// OSDWatchEvent preserves the authenticated source connection, cookie, notify
// identity, signed server status and distinct front/data payloads. The source
// GlobalID is this authenticated client; NotifierGlobalID is the remote sender.
// Version is the obsolete raw u64 event field, not the message header version.
// A notification is never acknowledged merely by receiving or dequeuing it.
type OSDWatchEvent struct {
	Opcode                                      uint8
	Cookie, Version, NotifyID, NotifierGlobalID uint64
	ReturnCode                                  int32
	Source                                      OSDTarget
	GlobalID, SessionGeneration                 uint64
	Payload, RawFront, RawData                  []byte
	Replies                                     []OSDNotifyReply
	Missed                                      []OSDNotifyIdentity
}

type osdWatchState struct{ streams map[uint64]*OSDWatchStream }
type queuedOSDWatchEvent struct {
	event OSDWatchEvent
	bytes uint64
}

// OSDWatchStream is bound to one connection generation and one non-reused
// locally allocated cookie. Close releases local delivery only; it sends no
// remote UNWATCH or ACK. Accepted events drain before the stable first error.
type OSDWatchStream struct {
	connection     *OSDConnection
	cookie         uint64
	notifier       bool
	request        OSDRequest
	options        OSDWatchOptions
	queue          []queuedOSDWatchEvent // all mutable state guarded by client.mu
	bytes          uint64
	terminal       error
	changed        chan struct{}
	notifyID       uint64
	notifyIDKnown  bool
	completionSeen bool
	completionID   uint64
	registration   *osdWatchRegistration // immutable after construction/initial successful WATCH
}

// OSDWatch is an explicitly registered watch, without an automatic ping loop.
type OSDWatch struct{ *OSDWatchStream }

// OSDNotify retains asynchronous completion separately from its MOSDOpReply.
type OSDNotify struct{ *OSDWatchStream }

func (w *OSDWatchStream) Cookie() uint64 { return w.cookie }

func (o *OSDConnection) newWatchStream(request OSDRequest, options OSDWatchOptions, notifier bool) (*OSDWatchStream, error) {
	request, options, err := o.prepareWatchStream(request, options)
	if err != nil {
		return nil, err
	}
	cookie, err := o.client.nextOSDTransaction()
	if err != nil {
		return nil, err
	}
	w := &OSDWatchStream{connection: o, cookie: cookie, request: request, options: options, notifier: notifier, changed: make(chan struct{})}
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || o.closed || o.session == nil {
		return nil, ErrClosed
	}
	if err := o.session.Err(); err != nil {
		return nil, err
	}
	if o.watchState == nil {
		o.watchState = &osdWatchState{streams: make(map[uint64]*OSDWatchStream)}
	}
	if len(o.watchState.streams) >= 64 {
		return nil, ErrOSDWatchLimit
	}
	o.watchState.streams[cookie] = w
	return w, nil
}

func (o *OSDConnection) prepareWatchStream(request OSDRequest, options OSDWatchOptions) (OSDRequest, OSDWatchOptions, error) {
	if err := o.validatePlacement(request); err != nil {
		return request, options, err
	}
	request.Placement = cloneOSDPlacement(request.Placement)
	if len(request.Operations) != 0 || request.ReadSnapshot != 0 && request.ReadSnapshot != OSDHeadSnapshot || request.SnapContext.Sequence != 0 || len(request.SnapContext.Snapshots) != 0 || request.OrderSnapshots || request.EnforceSnapContext {
		return request, options, osd.ErrRequest
	}
	if options.MaxBufferedBytes == 0 {
		options.MaxBufferedBytes = o.client.options.MaxFrameSize
	}
	if options.MaxBufferedBytes < 64 || options.MaxBufferedBytes > o.client.options.MaxFrameSize {
		return request, options, osd.ErrRequest
	}
	return request, options, nil
}

// WatchObject installs delivery before submitting WATCH. request supplies
// explicit routing metadata and no Operations. ctx controls registration only.
// The returned handle is also retained on an uncertain submission error so the
// caller can inspect its cookie and explicitly submit Unwatch. No replay occurs.
func (o *OSDConnection) WatchObject(ctx context.Context, request OSDRequest, options OSDWatchOptions) (*OSDWatch, OSDReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, OSDReply{}, err
	}
	w, err := o.newWatchStream(request, options, false)
	if err != nil {
		return nil, OSDReply{}, err
	}
	reply, err := w.perform(ctx, request.MapEpoch, OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: w.cookie, Action: OSDWatchRegister, Generation: options.RegistrationGeneration, TimeoutSeconds: options.TimeoutSeconds}})
	if err != nil {
		w.finish(err)
	} else {
		w.confirmWatchRegistration()
	}
	return &OSDWatch{w}, reply, err
}

// NotifyObject reserves completion delivery before NOTIFY. Its ordinary reply
// contains the assigned notify ID; Next returns the separate completion event,
// including partial replies and missed identities when ReturnCode is negative.
// An early completion retained after a lost ordinary reply cannot be checked
// against that reply's notify ID; the operation's unknown outcome remains.
// ctx cancellation never proves remote cancellation or permits automatic retry.
func (o *OSDConnection) NotifyObject(ctx context.Context, request OSDRequest, payload []byte, options OSDWatchOptions) (*OSDNotify, OSDReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, OSDReply{}, err
	}
	if options.RegistrationGeneration != 0 {
		return nil, OSDReply{}, osd.ErrRequest
	}
	w, err := o.newWatchStream(request, options, true)
	if err != nil {
		return nil, OSDReply{}, err
	}
	reply, err := w.perform(ctx, request.MapEpoch, OSDOperation{Code: OSDNotifyOperation, Data: payload, Watch: OSDWatchInput{Cookie: w.cookie, TimeoutSeconds: options.TimeoutSeconds, ProtocolVersion: 1}})
	if err != nil {
		w.finish(err)
		return &OSDNotify{w}, reply, err
	}
	id := binary.LittleEndian.Uint64(reply.Operations[0].Data)
	c := o.client
	c.mu.Lock()
	w.notifyID = id
	w.notifyIDKnown = true
	mismatch := w.completionSeen && w.completionID != id
	if mismatch {
		err = fmt.Errorf("%w: notify completion ID differs from operation reply", msgr.ErrFrame)
		// The early completion was provisional until the operation reply
		// supplied its identity. A mismatched event is never user delivery.
		w.queue = nil
		w.bytes = 0
		w.finishLocked(err)
	} else if w.completionSeen {
		w.finishLocked(ErrOSDNotifyComplete)
	}
	c.mu.Unlock()
	if mismatch {
		o.session.Fail(err)
	}
	return &OSDNotify{w}, reply, err
}

func (w *OSDWatchStream) perform(ctx context.Context, epoch uint32, op OSDOperation) (OSDReply, error) {
	return w.performWatchOperation(ctx, epoch, op)
}

func (w *OSDWatchStream) performRequest(ctx context.Context, epoch uint32, op OSDOperation) (OSDReply, error) {
	r := w.request
	r.MapEpoch = epoch
	r.Operations = []OSDOperation{op}
	return w.connection.request(ctx, r, w)
}

// Ping explicitly renews the existing registration on this session. The caller
// supplies its chosen current map epoch; a failed ping does not reconnect.
func (w *OSDWatch) Ping(ctx context.Context, epoch uint32) (OSDReply, error) {
	return w.perform(ctx, epoch, OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: w.cookie, Action: OSDWatchPing, Generation: w.options.RegistrationGeneration}})
}

// Unwatch explicitly submits remote removal. Local delivery ends only after a
// successful server reply; an uncertain result remains visible to the caller.
func (w *OSDWatch) Unwatch(ctx context.Context, epoch uint32) (OSDReply, error) {
	r, err := w.perform(ctx, epoch, OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: w.cookie, Action: OSDWatchUnwatch}})
	if err == nil {
		w.finish(ErrClosed)
	}
	return r, err
}

// Acknowledge sends the user's explicit reply to one notification. Handler
// success is the caller's decision; receiving Next never invokes this method.
func (w *OSDWatch) Acknowledge(ctx context.Context, epoch uint32, event OSDWatchEvent, payload []byte) (OSDReply, error) {
	o := w.connection
	if event.Opcode != OSDWatchNotification || event.Cookie != w.cookie || event.NotifyID == 0 || event.Source != o.target || event.GlobalID != o.globalID || event.SessionGeneration != o.generation {
		return OSDReply{}, osd.ErrRequest
	}
	return w.perform(ctx, epoch, OSDOperation{Code: OSDNotifyAckOperation, Data: payload, Watch: OSDWatchInput{Cookie: w.cookie, NotifyID: event.NotifyID}})
}

func (w *OSDWatchStream) finishLocked(err error) {
	if w.terminal != nil {
		return
	}
	w.terminal = err
	if state := w.connection.watchState; state != nil && state.streams[w.cookie] == w {
		delete(state.streams, w.cookie)
	}
	close(w.changed)
	w.changed = make(chan struct{})
}
func (w *OSDWatchStream) finish(err error) {
	w.connection.client.mu.Lock()
	w.finishLocked(err)
	w.connection.client.mu.Unlock()
}
func (w *OSDWatchStream) Close() error { w.finish(ErrClosed); return nil }

// Next consumes one event at most once, including with concurrent waiters.
// Canceling the local wait consumes nothing and sends no remote operation.
func (w *OSDWatchStream) Next(ctx context.Context) (OSDWatchEvent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return OSDWatchEvent{}, err
		}
		c := w.connection.client
		c.mu.Lock()
		if len(w.queue) > 0 {
			q := w.queue[0]
			w.queue[0] = queuedOSDWatchEvent{}
			w.queue = w.queue[1:]
			w.bytes -= q.bytes
			c.mu.Unlock()
			return q.event, nil
		}
		terminal, changed, s := w.terminal, w.changed, w.connection.session
		c.mu.Unlock()
		if terminal != nil {
			return OSDWatchEvent{}, terminal
		}
		if s != nil {
			if err := s.Err(); err != nil {
				w.finish(err)
				continue
			}
		}
		select {
		case <-changed:
		case <-s.Done():
		case <-ctx.Done():
			return OSDWatchEvent{}, ctx.Err()
		}
	}
}

func (o *OSDConnection) stopWatch(err error) {
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	if o.watchState != nil {
		for _, w := range o.watchState.streams {
			w.finishLocked(err)
		}
	}
}

func (o *OSDConnection) acceptsWatchLocked(source *session.Session) bool {
	c := o.client
	return !c.closed && !o.closed && o.session == source && c.osds[o.target.ID] == o && c.auth.GlobalID == o.globalID
}

func (o *OSDConnection) handleWatch(source *session.Session, m msgr.MessageData) error {
	c := o.client
	c.mu.Lock()
	active := o.acceptsWatchLocked(source)
	enabled := o.watchState != nil
	c.mu.Unlock()
	if !active {
		return nil
	}
	if !enabled {
		return fmt.Errorf("%w: unsolicited watch event without a local consumer", ErrUnsupportedFeatures)
	}
	e, err := osd.DecodeWatchEvent(m, c.options.MaxFrameSize)
	if err != nil {
		return fmt.Errorf("%w: invalid OSD watch event: %w", msgr.ErrFrame, err)
	}
	return o.publishWatchEvent(source, e)
}

func (o *OSDConnection) publishWatchEvent(source *session.Session, e osd.WatchEvent) error {
	c := o.client
	event := OSDWatchEvent{Opcode: e.Opcode, Cookie: e.Cookie, Version: e.Version, NotifyID: e.NotifyID, NotifierGlobalID: e.NotifierGlobalID, ReturnCode: e.ReturnCode, Payload: e.Payload, RawFront: e.RawFront, RawData: e.RawData, Source: o.target, GlobalID: o.globalID, SessionGeneration: o.generation}
	for _, r := range e.Replies {
		event.Replies = append(event.Replies, OSDNotifyReply{OSDNotifyIdentity: OSDNotifyIdentity{GlobalID: r.GlobalID, Cookie: r.Cookie}, Data: r.Data})
	}
	for _, r := range e.Missed {
		event.Missed = append(event.Missed, OSDNotifyIdentity{GlobalID: r.GlobalID, Cookie: r.Cookie})
	}
	size := uint64(len(e.RawFront)) + uint64(len(e.RawData)) + 256 + uint64(len(e.Replies))*48 + uint64(len(e.Missed))*16
	c.mu.Lock()
	defer c.mu.Unlock()
	if !o.acceptsWatchLocked(source) {
		return nil
	}
	w := o.watchState.streams[e.Cookie]
	// Cookies are never reused: valid late events cannot be associated with a
	// newer registration. They are discarded after explicit local termination.
	if w == nil {
		return nil
	}
	if w.notifier != (e.Opcode == osd.WatchEventComplete) {
		return fmt.Errorf("%w: watch event does not match consumer kind", msgr.ErrFrame)
	}
	if w.notifier && (w.notifyIDKnown && w.notifyID != e.NotifyID || w.completionSeen && w.completionID != e.NotifyID) {
		return fmt.Errorf("%w: notify completion ID mismatch", msgr.ErrFrame)
	}
	if w.notifier && w.completionSeen {
		return nil // Duplicate completion for the same notify ID.
	}
	if len(w.queue) >= 64 || size > uint64(w.options.MaxBufferedBytes)-w.bytes {
		w.finishLocked(ErrOSDWatchOverflow)
		return nil
	}
	w.queue = append(w.queue, queuedOSDWatchEvent{event: event, bytes: size})
	w.bytes += size
	close(w.changed)
	w.changed = make(chan struct{})
	if e.Opcode == osd.WatchEventDisconnect {
		w.finishLocked(ErrOSDWatchDisconnected)
	}
	if e.Opcode == osd.WatchEventComplete {
		w.completionSeen, w.completionID = true, e.NotifyID
		if w.notifyIDKnown {
			w.finishLocked(ErrOSDNotifyComplete)
		}
	}
	return nil
}
