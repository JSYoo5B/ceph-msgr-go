package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func reconnectTestClient(t *testing.T) (*Client, <-chan *osdTestPeer, *atomic.Uint32) {
	return reconnectProfileTestClient(t, OSDProfileObjectHash)
}

func reconnectProfileTestClient(t *testing.T, profile OSDProfile) (*Client, <-chan *osdTestPeer, *atomic.Uint32) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableOSD = true
	options.OSDProfile = profile
	options.MaxOSDConnections = 2
	peers := make(chan *osdTestPeer, 8)
	dials := &atomic.Uint32{}
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		if endpoint == "192.0.2.1:6804" || endpoint == "192.0.2.1:6805" {
			id := uint64(0)
			if endpoint == "192.0.2.1:6805" {
				id = 1
			}
			dials.Add(1)
			p := &osdTestPeer{conn: server, requests: make(chan msgr.MessageData, 8), done: make(chan struct{})}
			peers <- p
			go func() {
				defer close(p.done)
				defer p.conn.Close()
				reader, writer, err := mockAuthenticate(server, peerConfig{role: 4, id: id, payload: func(tag msgr.Tag, p []byte) []byte {
					if profile.requestV9() {
						return backoffTransportFeatures(tag, p, 1<<8|1<<9|1<<21|1<<30|1<<49|1<<56|1<<57|1<<58)
					}
					return p
				}})
				if err != nil {
					return
				}
				p.writer = writer
				for {
					f, err := reader.Read()
					if err != nil {
						return
					}
					if f.Tag != msgr.Message {
						continue
					}
					m, err := msgr.DecodeMessage(f)
					if err != nil {
						return
					}
					p.requests <- m
				}
			}()
		} else {
			go mockDaemon(server, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, services: cephx.ServiceAuth | cephx.ServiceOSD | cephx.ServiceMgr, payload: func(tag msgr.Tag, p []byte) []byte {
				if profile.replicatedPG() {
					return backoffTransportFeatures(tag, p, 1<<30)
				}
				return p
			}})
		}
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, peers, dials
}

func reconnectOpen(t *testing.T, c *Client, peers <-chan *osdTestPeer, id int32) (*OSDConnection, *osdTestPeer) {
	t.Helper()
	address := "192.0.2.1:6804"
	if id == 1 {
		address = "192.0.2.1:6805"
	}
	o, err := c.OpenOSD(context.Background(), OSDTarget{ID: id, Address: address})
	if err != nil {
		t.Fatal(err)
	}
	return o, <-peers
}

func confirmedTestWatch(t *testing.T, ctx context.Context, o *OSDConnection, p *osdTestPeer, generation uint32) *OSDWatch {
	t.Helper()
	type result struct {
		w   *OSDWatch
		err error
	}
	done := make(chan result, 1)
	go func() {
		w, _, err := o.WatchObject(ctx, watchTestRoute(), OSDWatchOptions{RegistrationGeneration: generation, TimeoutSeconds: 37})
		done <- result{w, err}
	}()
	req := p.next(t)
	o.client.mu.Lock()
	var cookie uint64
	for cookie = range o.watchState.streams {
	}
	o.client.mu.Unlock()
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: cookie, Action: OSDWatchRegister, Generation: generation, TimeoutSeconds: 37}}
	if err := osdMapsWrite(p, watchSyntheticReply(t, req, op, nil)); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	return got.w
}

func reconnectReply(t *testing.T, p *osdTestPeer, req msgr.MessageData, w *OSDWatchStream, code int32) {
	t.Helper()
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: w.cookie, Action: OSDWatchReconnect, Generation: w.options.RegistrationGeneration}}
	e := wire.Encoder{}
	e.U16(uint16(OSDWatchOperation))
	e.U32(0)
	e.U64(w.cookie)
	e.U64(0)
	e.U8(OSDWatchReconnect)
	e.U32(w.options.RegistrationGeneration)
	e.U32(0)
	e.Raw(make([]byte, 3))
	e.U32(0)
	if !bytes.Contains(req.Front, e.Data) || len(req.Data) != 0 {
		t.Fatal("reconnect changed cookie/action/generation or carried data")
	}
	reply := watchSyntheticReply(t, req, op, nil)
	if code < 0 {
		binary.LittleEndian.PutUint32(reply.Front[38:42], uint32(code))
		binary.LittleEndian.PutUint32(reply.Front[104:108], uint32(code))
	}
	if err := osdMapsWrite(p, reply); err != nil {
		t.Fatal(err)
	}
}

type reconnectResult struct {
	watch *OSDWatch
	reply OSDReply
	err   error
}

func startReconnect(ctx context.Context, o *OSDConnection, previous *OSDWatch, request OSDRequest) <-chan reconnectResult {
	done := make(chan reconnectResult, 1)
	go func() { w, r, e := o.ReconnectWatch(ctx, previous, request); done <- reconnectResult{w, r, e} }()
	return done
}
func pendingReconnect(t *testing.T, o *OSDConnection, cookie uint64) *OSDWatchStream {
	t.Helper()
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	w := o.watchState.streams[cookie]
	if w == nil {
		t.Fatal("reconnect did not preinstall consumer")
	}
	return w
}

func TestOSDWatchReconnectFencesOldSessionAndPreservesAcceptedEvents(t *testing.T) {
	c, peers, dials := reconnectTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, oldPeer := reconnectOpen(t, c, peers, 0)
	next, nextPeer := reconnectOpen(t, c, peers, 1)
	watch := confirmedTestWatch(t, ctx, old, oldPeer, 7)
	if err := old.handleWatch(old.session, watchSyntheticEvent(OSDWatchNotification, watch.Cookie(), 90, []byte("old"), nil)); err != nil {
		t.Fatal(err)
	}
	done := startReconnect(ctx, next, watch, watchTestRoute())
	req := nextPeer.next(t)
	pending := pendingReconnect(t, next, watch.Cookie())
	if pending.options.RegistrationGeneration != 8 || pending.options.MaxBufferedBytes != watch.options.MaxBufferedBytes {
		t.Fatal("registration generation or queue budget changed")
	}
	oldEvent, err := watch.Next(ctx)
	if err != nil || oldEvent.NotifyID != 90 || oldEvent.SessionGeneration != old.generation {
		t.Fatal("handoff discarded accepted old event", oldEvent, err)
	}
	if _, err := watch.Next(ctx); !errors.Is(err, ErrOSDWatchSuperseded) {
		t.Fatal("old stream did not end with handoff cause", err)
	}
	for _, send := range []func() error{
		func() error { _, e := watch.Ping(ctx, 11); return e }, func() error { _, e := watch.Unwatch(ctx, 11); return e }, func() error { _, e := watch.Acknowledge(ctx, 11, oldEvent, nil); return e },
	} {
		if err := send(); !errors.Is(err, ErrOSDWatchSuperseded) {
			t.Fatal("old control escaped ownership fence", err)
		}
	}
	if err := old.handleWatch(old.session, watchSyntheticEvent(OSDWatchNotification, watch.Cookie(), 91, nil, nil)); err != nil {
		t.Fatal(err)
	}
	// Notifications may arrive on the new connection before RECONNECT's reply.
	event := watchSyntheticEvent(OSDWatchNotification, watch.Cookie(), 92, []byte{0, 255}, nil)
	event.Transaction = req.Transaction
	if err := osdMapsWrite(nextPeer, event); err != nil {
		t.Fatal(err)
	}
	reconnectReply(t, nextPeer, req, pending, 0)
	got := <-done
	if got.err != nil || got.watch.Cookie() != watch.Cookie() {
		t.Fatal(got.err)
	}
	newEvent, err := got.watch.Next(ctx)
	if err != nil || newEvent.NotifyID != 92 || newEvent.SessionGeneration != next.generation || newEvent.Source.ID != 1 || !bytes.Equal(newEvent.Payload, []byte{0, 255}) {
		t.Fatal("new-source event identity lost", newEvent, err)
	}
	if _, err := got.watch.Acknowledge(ctx, 11, oldEvent, nil); err == nil {
		t.Fatal("old-generation event acknowledged through new owner")
	}
	if w, r, e := old.ReconnectWatch(ctx, got.watch, watchTestRoute()); w != nil || r.Submission != (OSDSubmission{}) || !errors.Is(e, ErrOSDWatchReconnect) {
		t.Fatal("cookie returned to an already-used physical session", e)
	}
	if w, _, e := next.ReconnectWatch(ctx, watch, watchTestRoute()); w != nil || !errors.Is(e, ErrOSDWatchSuperseded) {
		t.Fatal("superseded receipt forked", e)
	}
	select {
	case <-oldPeer.requests:
		t.Fatal("superseded old control transmitted")
	default:
	}
	if dials.Load() != 2 {
		t.Fatal("handoff opened or retried connections")
	}
}

func TestOSDWatchReconnectAdmissionRetainsOldOwner(t *testing.T) {
	c, peers, _ := reconnectTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, p := reconnectOpen(t, c, peers, 0)
	next, np := reconnectOpen(t, c, peers, 1)
	watch := confirmedTestWatch(t, ctx, old, p, 0)
	for _, tc := range []struct {
		name string
		edit func(*OSDRequest)
	}{
		{"different object", func(r *OSDRequest) { r.Object.Name = "other" }},
		{"empty object", func(r *OSDRequest) { r.Object.Name = "" }},
		{"negative pool", func(r *OSDRequest) { r.Object.Pool = -1 }},
		{"different incarnation", func(r *OSDRequest) { r.ClientIncarnation++ }},
		{"zero epoch", func(r *OSDRequest) { r.MapEpoch = 0 }},
		{"invalid time", func(r *OSDRequest) { r.ModificationTime.Nanoseconds = 1_000_000_000 }},
		{"snapshot read", func(r *OSDRequest) { r.ReadSnapshot = 1 }},
		{"snapshot sequence", func(r *OSDRequest) { r.SnapContext.Sequence = 1 }},
		{"snapshot vector", func(r *OSDRequest) { r.SnapContext.Snapshots = []uint64{1} }},
		{"ordered snapshots", func(r *OSDRequest) { r.OrderSnapshots = true }},
		{"operations", func(r *OSDRequest) { r.Operations = []OSDOperation{{Code: OSDStat}} }},
		{"invalid placement", func(r *OSDRequest) { r.Placement = &OSDPlacement{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := watchTestRoute()
			tc.edit(&route)
			before, cookie := c.osdNext.Load(), watch.Cookie()
			w, r, e := next.ReconnectWatch(ctx, watch, route)
			if w != nil || e == nil || r.Submission != (OSDSubmission{}) || c.osdNext.Load() != before || watch.Cookie() != cookie {
				t.Fatal("invalid handoff reached submission or changed identity", e)
			}
			c.mu.Lock()
			unchanged := watch.registration.owner == watch.OSDWatchStream && watch.registration.generation == 0 && watch.registration.lastSessionGeneration == old.generation && watch.terminal == nil && old.watchState.streams[cookie] == watch.OSDWatchStream && (next.watchState == nil || len(next.watchState.streams) == 0)
			c.mu.Unlock()
			if !unchanged {
				t.Fatal("invalid input consumed registration authority or a stream slot")
			}
			select {
			case <-np.requests:
				t.Fatal("invalid input transmitted reconnect")
			default:
			}
		})
	}
	// The previous receipt still authorizes actual remote controls after every
	// rejected route, rather than merely retaining the same pointer locally.
	pinged := make(chan error, 1)
	go func() { _, e := watch.Ping(ctx, 11); pinged <- e }()
	ping := p.next(t)
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: watch.Cookie(), Action: OSDWatchPing}}
	if err := osdMapsWrite(p, watchSyntheticReply(t, ping, op, nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-pinged; err != nil {
		t.Fatal("rejected route revoked old Ping authority", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if w, _, e := next.ReconnectWatch(canceled, watch, watchTestRoute()); w != nil || !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if w, _, e := next.ReconnectWatch(ctx, &OSDWatch{OSDWatchStream: &OSDWatchStream{}}, watchTestRoute()); w != nil || !errors.Is(e, ErrOSDWatchReconnect) {
		t.Fatal("unconfirmed cookie received reconnect authority", e)
	}
	c.mu.Lock()
	c.auth.GlobalID++
	c.mu.Unlock()
	if w, _, e := next.ReconnectWatch(ctx, watch, watchTestRoute()); w != nil || !errors.Is(e, ErrOSDWatchReconnect) {
		t.Fatal("foreign identity accepted", e)
	}
	c.mu.Lock()
	c.auth.GlobalID--
	watch.registration.generation = math.MaxUint32
	c.mu.Unlock()
	if w, _, e := next.ReconnectWatch(ctx, watch, watchTestRoute()); w != nil || !errors.Is(e, ErrOSDWatchReconnect) {
		t.Fatal("generation wrapped", e)
	}
	c.mu.Lock()
	if watch.registration.owner != watch.OSDWatchStream || watch.terminal != nil {
		t.Fatal("admission failure changed old owner")
	}
	c.mu.Unlock()
	select {
	case <-np.requests:
		t.Fatal("rejected handoff transmitted")
	default:
	}
}

func TestOSDWatchReconnectUnknownOutcomeRetainsNewOwnerWithoutRetry(t *testing.T) {
	c, peers, _ := reconnectTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, p := reconnectOpen(t, c, peers, 0)
	watch := confirmedTestWatch(t, ctx, old, p, 0)
	if err := old.handleWatch(old.session, watchSyntheticEvent(OSDWatchNotification, watch.Cookie(), 90, nil, nil)); err != nil {
		t.Fatal(err)
	}
	old.Close()
	next, np := reconnectOpen(t, c, peers, 0)
	operation, stop := context.WithCancel(ctx)
	done := startReconnect(operation, next, watch, watchTestRoute())
	sent := np.next(t)
	if err := next.handleWatch(next.session, watchSyntheticEvent(OSDWatchNotification, watch.Cookie(), 91, []byte{0, 255, 1}, nil)); err != nil {
		t.Fatal(err)
	}
	stop()
	got := <-done
	var unknown *OutcomeUnknownError
	if got.watch == nil || !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != sent.Transaction {
		t.Fatal("unknown reconnect lost identity/owner", got.err)
	}
	newEvent, err := got.watch.Next(ctx)
	if err != nil || newEvent.NotifyID != 91 || newEvent.SessionGeneration != next.generation || !bytes.Equal(newEvent.Payload, []byte{0, 255, 1}) {
		t.Fatal("reply loss discarded already accepted new-generation event", newEvent, err)
	}
	if _, err := got.watch.Next(ctx); !errors.As(err, &unknown) {
		t.Fatal("reply loss did not retain new stream terminal cause", err)
	}
	if _, err := watch.Next(ctx); err != nil {
		t.Fatal("old accepted event lost", err)
	}
	if _, err := watch.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal("handoff overwrote old first cause", err)
	}
	if _, err := watch.Unwatch(ctx, 11); !errors.Is(err, ErrOSDWatchSuperseded) {
		t.Fatal("old cleanup could delete unknown new registration", err)
	}
	select {
	case <-np.requests:
		t.Fatal("uncertain reconnect replayed or fell back to WATCH")
	default:
	}
	next.Close()
	third, tp := reconnectOpen(t, c, peers, 0)
	retry := startReconnect(ctx, third, got.watch, watchTestRoute())
	req := tp.next(t)
	pending := pendingReconnect(t, third, watch.Cookie())
	if pending.options.RegistrationGeneration != 2 {
		t.Fatal("explicit retry did not advance registration generation")
	}
	reconnectReply(t, tp, req, pending, -107)
	refused := <-retry
	var server *OSDError
	if refused.watch == nil || !errors.As(refused.err, &server) || server.Code != -107 || refused.reply.Code != -107 {
		t.Fatal("signed ENOTCONN was normalized or replaced", refused.err)
	}
	select {
	case <-tp.requests:
		t.Fatal("ENOTCONN automatically registered a new watch")
	default:
	}
}

func TestOSDWatchReconnectCanceledGateAndUncertainUnwatch(t *testing.T) {
	c, peers, _ := reconnectTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, p := reconnectOpen(t, c, peers, 0)
	next, np := reconnectOpen(t, c, peers, 1)
	watch := confirmedTestWatch(t, ctx, old, p, 0)
	unwatchCtx, stopUnwatch := context.WithCancel(ctx)
	unwatched := make(chan error, 1)
	go func() { _, e := watch.Unwatch(unwatchCtx, 11); unwatched <- e }()
	p.next(t)
	waitCtx, stopWait := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stopWait()
	waited, _, waitErr := next.ReconnectWatch(waitCtx, watch, watchTestRoute())
	if waited != nil || !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatal("canceled local gate wait changed ownership", waitErr)
	}
	c.mu.Lock()
	owner := watch.registration.owner
	c.mu.Unlock()
	if owner != watch.OSDWatchStream {
		t.Fatal("canceled gate fenced old watch")
	}
	stopUnwatch()
	var unknown *OutcomeUnknownError
	if err := <-unwatched; !errors.As(err, &unknown) {
		t.Fatal("unwatch did not preserve uncertainty", err)
	}
	if w, _, err := next.ReconnectWatch(ctx, watch, watchTestRoute()); w != nil || !errors.Is(err, ErrOSDWatchReconnect) {
		t.Fatal("unknown unwatch permitted ambiguous registration reuse", err)
	}
	select {
	case <-np.requests:
		t.Fatal("gate/unwatch rejection transmitted reconnect")
	default:
	}
}

func TestOSDWatchReconnectRequiresConfirmedUnremovedReceipt(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		name := "initial-unknown"
		if confirmed {
			name = "successful-unwatch"
		}
		t.Run(name, func(t *testing.T) {
			c, peers, _ := reconnectTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			old, p := reconnectOpen(t, c, peers, 0)
			next, np := reconnectOpen(t, c, peers, 1)
			var watch *OSDWatch
			if confirmed {
				watch = confirmedTestWatch(t, ctx, old, p, 0)
				finished := make(chan error, 1)
				go func() { _, e := watch.Unwatch(ctx, 11); finished <- e }()
				req := p.next(t)
				op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: watch.Cookie(), Action: OSDWatchUnwatch}}
				if err := osdMapsWrite(p, watchSyntheticReply(t, req, op, nil)); err != nil {
					t.Fatal(err)
				}
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
			} else {
				initialCtx, stop := context.WithCancel(ctx)
				done := make(chan reconnectResult, 1)
				go func() {
					w, r, e := old.WatchObject(initialCtx, watchTestRoute(), OSDWatchOptions{})
					done <- reconnectResult{w, r, e}
				}()
				p.next(t)
				stop()
				got := <-done
				var unknown *OutcomeUnknownError
				if got.watch == nil || !errors.As(got.err, &unknown) {
					t.Fatal("initial WATCH did not retain its uncertain handle", got.err)
				}
				watch = got.watch
			}
			before := c.osdNext.Load()
			if w, r, e := next.ReconnectWatch(ctx, watch, watchTestRoute()); w != nil || !errors.Is(e, ErrOSDWatchReconnect) || r.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
				t.Fatal("unconfirmed or removed watch acquired reconnect authority", e)
			}
			select {
			case <-np.requests:
				t.Fatal("unusable receipt transmitted reconnect")
			default:
			}
		})
	}
}
