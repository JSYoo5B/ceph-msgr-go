package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func watchSyntheticEvent(opcode uint8, cookie, id uint64, payload, data []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.U8(1)
	e.U8(opcode)
	e.U64(cookie)
	e.U64(0)
	e.U64(id)
	e.Bytes(payload)
	e.U32(0)
	e.U64(42)
	return msgr.MessageData{Type: 44, Version: 3, CompatVersion: 1, Front: e.Data, Data: data}
}

func watchSyntheticReply(t *testing.T, request msgr.MessageData, op OSDOperation, data []byte) msgr.MessageData {
	m := osdXattrSyntheticReply(t, []OSDOperation{op}, [][]byte{data}, []int32{0}, 0, osd.FlagOnDisk|osd.FlagAck)
	e := wire.Encoder{}
	osd.EncodeWatchUnion(&e, osd.Operation{Code: uint16(op.Code), Watch: watchWireInput(op.Watch)})
	copy(m.Front[68:96], e.Data)
	m.Transaction = request.Transaction
	return m
}

func watchTestRoute() OSDRequest { r := osdValidationRequest(); r.Operations = nil; return r }

func TestOSDWatchDispatchExplicitAckAndLifecycle(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	type opened struct {
		w *OSDWatch
		r OSDReply
		e error
	}
	done := make(chan opened, 1)
	go func() {
		w, r, e := o.WatchObject(ctx, watchTestRoute(), OSDWatchOptions{TimeoutSeconds: 37, RegistrationGeneration: 7})
		done <- opened{w, r, e}
	}()
	req := peer.next(t)
	c.mu.Lock()
	var stream *OSDWatchStream
	for _, s := range o.watchState.streams {
		stream = s
	}
	c.mu.Unlock()
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: stream.cookie, Action: OSDWatchRegister, Generation: 7, TimeoutSeconds: 37}}
	// An event carrying the outstanding request TID cannot complete WATCH.
	event := watchSyntheticEvent(OSDWatchNotification, stream.cookie, 99, []byte{0, 255, 1}, nil)
	event.Transaction = req.Transaction
	if err := osdMapsWrite(peer, event); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("event completed registration call")
	default:
	}
	if err := osdMapsWrite(peer, watchSyntheticReply(t, req, op, nil)); err != nil {
		t.Fatal(err)
	}
	registered := <-done
	if registered.e != nil {
		t.Fatal(registered.e)
	}
	preCanceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := registered.w.Next(preCanceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	next, err := registered.w.Next(ctx)
	if err != nil || next.Cookie != stream.cookie || next.NotifyID != 99 || !bytes.Equal(next.Payload, []byte{0, 255, 1}) || next.Source != o.target || next.GlobalID != o.globalID || next.SessionGeneration != o.generation {
		t.Fatal(next, err)
	}
	select {
	case <-peer.requests:
		t.Fatal("notification automatically acknowledged")
	default:
	}
	ack := make(chan error, 1)
	go func() { _, e := registered.w.Acknowledge(ctx, 11, next, []byte{0, 255}); ack <- e }()
	req = peer.next(t)
	if !bytes.Equal(req.Data, []byte{99, 0, 0, 0, 0, 0, 0, 0, byte(stream.cookie), 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 255}) {
		t.Fatal("explicit ACK bytes differ", req.Data)
	}
	if err := osdMapsWrite(peer, watchSyntheticReply(t, req, OSDOperation{Code: OSDNotifyAckOperation}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	bad := next
	bad.SessionGeneration++
	if _, err := registered.w.Acknowledge(ctx, 11, bad, nil); err == nil {
		t.Fatal("foreign session generation ACK admitted")
	}
	closed := make(chan error, 1)
	go func() { _, e := registered.w.Unwatch(ctx, 11); closed <- e }()
	req = peer.next(t)
	if err := osdMapsWrite(peer, watchSyntheticReply(t, req, OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: stream.cookie, Action: OSDWatchUnwatch}}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := registered.w.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	// Late events for a retired, never-reused cookie are discarded.
	if err := osdMapsWrite(peer, event); err != nil {
		t.Fatal(err)
	}
	if err := o.session.Err(); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatal("watch reconnected")
	}
}

func TestOSDNotifyCompletionBeforeReplyPreservesPartialData(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	type opened struct {
		n *OSDNotify
		e error
	}
	done := make(chan opened, 1)
	go func() {
		n, _, e := o.NotifyObject(ctx, watchTestRoute(), []byte{0, 255}, OSDWatchOptions{TimeoutSeconds: 1})
		done <- opened{n, e}
	}()
	req := peer.next(t)
	c.mu.Lock()
	var cookie uint64
	for k := range o.watchState.streams {
		cookie = k
	}
	c.mu.Unlock()
	d := wire.Encoder{}
	d.U32(1)
	d.U64(51)
	d.U64(81)
	d.Bytes([]byte{0, 255})
	d.U32(1)
	d.U64(52)
	d.U64(82)
	event := watchSyntheticEvent(OSDWatchCompletion, cookie, 99, nil, d.Data)
	binary.LittleEndian.PutUint32(event.Front[len(event.Front)-12:], 0xffffff92)
	event.Transaction = req.Transaction
	if err := osdMapsWrite(peer, event); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("completion event replaced MOSDOpReply")
	default:
	}
	output := make([]byte, 8)
	binary.LittleEndian.PutUint64(output, 99)
	if err := osdMapsWrite(peer, watchSyntheticReply(t, req, OSDOperation{Code: OSDNotifyOperation, Watch: OSDWatchInput{Cookie: cookie}}, output)); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.e != nil {
		t.Fatal(got.e)
	}
	next, err := got.n.Next(ctx)
	if err != nil || next.ReturnCode != -110 || next.NotifyID != 99 || len(next.Replies) != 1 || !bytes.Equal(next.Replies[0].Data, []byte{0, 255}) || len(next.Missed) != 1 {
		t.Fatal(next, err)
	}
	if _, err := got.n.Next(ctx); !errors.Is(err, ErrOSDNotifyComplete) {
		t.Fatal(err)
	}
}

func TestOSDWatchQueuesOverflowAndCloseKeepFirstCause(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	<-peers
	w, err := o.newWatchStream(watchTestRoute(), OSDWatchOptions{MaxBufferedBytes: 640}, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := o.handleWatch(o.session, watchSyntheticEvent(OSDWatchNotification, w.cookie, i, nil, nil)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	for i := uint64(1); i <= 2; i++ {
		event, err := w.Next(ctx)
		if err != nil || event.NotifyID != i {
			t.Fatal(event, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := w.Next(ctx); !errors.Is(err, ErrOSDWatchOverflow) {
			t.Fatal("stable overflow cause lost", err)
		}
	}
	if err := o.session.Err(); err != nil {
		t.Fatal("stream overflow damaged shared session", err)
	}
	w2, err := o.newWatchStream(watchTestRoute(), OSDWatchOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { _, e := w2.Next(ctx); wait <- e }()
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-wait; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if w2.cookie == w.cookie {
		t.Fatal("watch cookie reused")
	}
}

func TestOSDWatchRechecksIdentityBeforePublication(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	<-peers
	w, err := o.newWatchStream(watchTestRoute(), OSDWatchOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	if !o.acceptsWatchLocked(o.session) {
		t.Fatal("authenticated event was ineligible")
	}
	c.mu.Unlock()
	decoded, err := osd.DecodeWatchEvent(watchSyntheticEvent(OSDWatchNotification, w.cookie, 99, []byte{0, 255}, nil), c.options.MaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	// Admission can change while bounded decoding/copying occurs without the
	// client lock. Publication must repeat the authenticated-source check.
	c.mu.Lock()
	c.auth.GlobalID++
	c.mu.Unlock()
	if err := o.publishWatchEvent(o.session, decoded); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	queued := len(w.queue)
	c.mu.Unlock()
	if queued != 0 {
		t.Fatal("old authenticated identity published an event")
	}
}

func TestOSDWatchRawRequestCannotBypassCookieReservation(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	for _, code := range []OSDOpcode{OSDWatchOperation, OSDNotifyOperation, OSDNotifyAckOperation} {
		request := watchTestRoute()
		request.Operations = []OSDOperation{{Code: code, Watch: OSDWatchInput{Cookie: 71}}}
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), request)
		if !errors.Is(err, ErrUnsupportedOSDOperation) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("raw watch operation reached submission", code, reply, err)
		}
		select {
		case <-peer.requests:
			t.Fatal("raw watch operation transmitted")
		default:
		}
	}
}

func TestOSDWatchConcurrentNextAndDisconnect(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	<-peers
	w, err := o.newWatchStream(watchTestRoute(), OSDWatchOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		event OSDWatchEvent
		err   error
	}
	results := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() { event, err := w.Next(ctx); results <- result{event, err} }()
	}
	for id := uint64(1); id <= 8; id++ {
		if err := o.handleWatch(o.session, watchSyntheticEvent(OSDWatchNotification, w.cookie, id, []byte{byte(id)}, nil)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[uint64]bool{}
	for i := 0; i < 8; i++ {
		got := <-results
		if got.err != nil || seen[got.event.NotifyID] {
			t.Fatal("concurrent waiter lost/duplicated event", got)
		}
		seen[got.event.NotifyID] = true
	}
	if err := o.handleWatch(o.session, watchSyntheticEvent(OSDWatchDisconnect, w.cookie, 0, nil, nil)); err != nil {
		t.Fatal(err)
	}
	event, err := w.Next(ctx)
	if err != nil || event.Opcode != OSDWatchDisconnect || event.Cookie != w.cookie {
		t.Fatal(event, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := w.Next(ctx); !errors.Is(err, ErrOSDWatchDisconnected) {
			t.Fatal("disconnect cause lost", err)
		}
	}
	if err := o.session.Err(); err != nil {
		t.Fatal("registration disconnect failed shared session", err)
	}
}

func TestOSDNotifyCompletionIDMismatchFailsWithoutReplay(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	w, err := o.newWatchStream(watchTestRoute(), OSDWatchOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	w.notifyID = 99
	w.notifyIDKnown = true
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		r := watchTestRoute()
		r.Operations = []OSDOperation{{Code: OSDWriteFull, Length: 1, Data: []byte{1}}}
		_, e := o.Request(ctx, r)
		done <- e
	}()
	request := peer.next(t)
	d := wire.Encoder{}
	d.U32(0)
	d.U32(0)
	event := watchSyntheticEvent(OSDWatchCompletion, w.cookie, 100, nil, d.Data)
	event.Transaction = request.Transaction
	if err := osdMapsWrite(peer, event); err != nil {
		t.Fatal(err)
	}
	err = <-done
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("mismatched completion ended sent mutation incorrectly", err)
	}
	if _, err := w.Next(ctx); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("watch terminal cause lost", err)
	}
	if dials.Load() != 1 {
		t.Fatal("event failure reconnected")
	}
	select {
	case <-peer.requests:
		t.Fatal("event failure replayed mutation")
	default:
	}
}

func TestOSDNotifyEarlyCompletionMismatchedReplyIsNeverDelivered(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	type response struct {
		n   *OSDNotify
		err error
	}
	done := make(chan response, 1)
	go func() {
		n, _, err := o.NotifyObject(ctx, watchTestRoute(), nil, OSDWatchOptions{})
		done <- response{n, err}
	}()
	req := peer.next(t)
	c.mu.Lock()
	var cookie uint64
	for cookie = range o.watchState.streams {
	}
	c.mu.Unlock()
	data := wire.Encoder{}
	data.U32(0)
	data.U32(0)
	event := watchSyntheticEvent(OSDWatchCompletion, cookie, 100, nil, data.Data)
	if err := osdMapsWrite(peer, event); err != nil {
		t.Fatal(err)
	}
	output := make([]byte, 8)
	binary.LittleEndian.PutUint64(output, 99)
	if err := osdMapsWrite(peer, watchSyntheticReply(t, req, OSDOperation{Code: OSDNotifyOperation, Watch: OSDWatchInput{Cookie: cookie}}, output)); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if !errors.Is(got.err, msgr.ErrFrame) {
		t.Fatal("mismatched early completion accepted", got.err)
	}
	if _, err := got.n.Next(ctx); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("unverified completion survived normal reply mismatch", err)
	}
}

func TestOSDWatchCanceledRegistrationRetainsCookieForExplicitCleanup(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	registration, stop := context.WithCancel(ctx)
	type result struct {
		watch *OSDWatch
		reply OSDReply
		err   error
	}
	done := make(chan result, 1)
	go func() {
		w, r, e := o.WatchObject(registration, watchTestRoute(), OSDWatchOptions{})
		done <- result{w, r, e}
	}()
	sent := peer.next(t)
	stop()
	got := <-done
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.watch == nil || got.watch.Cookie() == 0 || got.reply.Submission.Transaction != sent.Transaction {
		t.Fatal("canceled registration lost uncertain identity", got)
	}
	if _, err := got.watch.Next(ctx); !errors.As(err, &unknown) {
		t.Fatal("canceled registration implied remote unwatch", err)
	}
	select {
	case <-peer.requests:
		t.Fatal("canceled registration automatically replayed or unwatched")
	default:
	}
	cleaned := make(chan error, 1)
	go func() { _, err := got.watch.Unwatch(ctx, 11); cleaned <- err }()
	cleanup := peer.next(t)
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: got.watch.Cookie(), Action: OSDWatchUnwatch}}
	if err := osdMapsWrite(peer, watchSyntheticReply(t, cleanup, op, nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-cleaned; err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatal("canceled registration reconnected")
	}
}
