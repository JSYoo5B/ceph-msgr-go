package cephmsgr

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic MDS session peers check local protocol and lifetime policy. They
// are not native interoperability evidence.

func mdsSessionHead(op uint32, seq uint64) *wire.Encoder {
	e := &wire.Encoder{}
	e.U32(op)
	e.U64(seq)
	e.U64(0)
	e.U32(0)
	e.U32(0)
	return e
}

func mdsSessionReply(op uint32, seq uint64) msgr.MessageData {
	return msgr.MessageData{Type: mds.ClientSessionMessage, Version: 1, CompatVersion: 1, Front: mdsSessionHead(op, seq).Data}
}

func mdsSessionV7(op uint32, seq uint64, metadata map[string]string, flags uint32, capAuths bool) msgr.MessageData {
	e := mdsSessionHead(op, seq)
	e.U32(uint32(len(metadata)))
	for k, v := range metadata {
		e.String(k)
		e.String(v)
	}
	features := []byte{}
	if op == mds.SessionOpen {
		features = []byte{0xff, 0xff, 0xff, 0, 0, 0, 0, 0}
	}
	e.Bytes(features)
	metric := wire.Encoder{}
	metric.Bytes(nil)
	e.Struct(1, 1, metric.Data)
	e.U32(flags)
	if capAuths {
		e.U32(1)
		match := wire.Encoder{}
		match.U64(^uint64(0))
		match.U32(1)
		match.U32(1000)
		match.String("")
		match.String("cephfs")
		match.U8(0)
		auth := wire.Encoder{}
		auth.Struct(1, 1, match.Data)
		auth.U8(1)
		auth.U8(1)
		e.Struct(1, 1, auth.Data)
	} else {
		e.U32(0)
	}
	e.U64(^uint64(0))
	return msgr.MessageData{Type: mds.ClientSessionMessage, Version: 7, CompatVersion: 1, Front: e.Data}
}

func mdsSessionNext(t *testing.T, peer *mdsTestPeer, op uint32) mds.ClientSession {
	t.Helper()
	m := peer.next(t)
	if m.Type != mds.ClientSessionMessage || m.Priority != 127 || m.CompatVersion != 1 || m.Transaction != 0 {
		t.Fatal("unexpected client session envelope", m.Type, m.Priority, m.CompatVersion, m.Transaction)
	}
	// Decode the client's request with the shared head layout.
	d := wire.NewDecoder(m.Front)
	got := mds.ClientSession{Version: m.Version, Op: d.U32(), Sequence: d.U64()}
	if got.Op != op {
		t.Fatalf("client session op = %d, want %d", got.Op, op)
	}
	return got
}

type mdsSessionOpenResult struct {
	session *MDSSession
	err     error
}

func mdsStartSession(m *MDSConnection, ctx context.Context, options MDSSessionOptions) <-chan mdsSessionOpenResult {
	done := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, err := m.OpenSession(ctx, options)
		done <- mdsSessionOpenResult{s, err}
	}()
	return done
}

func mdsOpenTestSession(t *testing.T, ctx context.Context, m *MDSConnection, peer *mdsTestPeer, interval time.Duration) *MDSSession {
	t.Helper()
	peer.sessions.Store(true)
	pending := mdsStartSession(m, ctx, MDSSessionOptions{Metadata: map[string]string{"entity_id": "test"}, RenewInterval: interval})
	request := peer.next(t)
	if request.Type != mds.ClientSessionMessage || request.Version != 7 {
		t.Fatal("REQUEST_OPEN envelope", request.Type, request.Version)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, map[string]string{}, 0, true))
	got := <-pending
	if got.err != nil {
		t.Fatal(got.err)
	}
	renew := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	if renew.Sequence != 1 || renew.Version != 1 {
		t.Fatal("OPEN did not trigger the immediate first renewal", renew)
	}
	peer.send(t, mdsSessionReply(mds.SessionRenewCaps, 1))
	return got.session
}

func mdsWaitState(t *testing.T, s *MDSSession, state MDSSessionState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for s.Status().State != state {
		if time.Now().After(deadline) {
			t.Fatalf("session state = %d, want %d", s.Status().State, state)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMDSSessionOpenRenewCloseWire(t *testing.T) {
	c, m, peer, ctx, _ := mdsTestOpen(t)
	peer.sessions.Store(true)
	if _, err := m.OpenSession(ctx, MDSSessionOptions{}); err == nil {
		t.Fatal("zero RenewInterval admitted")
	}
	if _, err := m.OpenSession(ctx, MDSSessionOptions{RenewInterval: time.Hour, MaxEvents: 4097}); err == nil {
		t.Fatal("oversized event bound admitted")
	}
	metadata := map[string]string{"root": "/", "entity_id": "test"}
	pending := mdsStartSession(m, ctx, MDSSessionOptions{Metadata: metadata, RenewInterval: time.Hour})
	request := peer.next(t)
	want := mds.EncodeRequestOpen(metadata, mds.ClientFeatures)
	if request.Version != 7 || request.CompatVersion != 1 || hex.EncodeToString(request.Front) != hex.EncodeToString(want.Front) || len(request.Data) != 0 {
		t.Fatal("REQUEST_OPEN wire differs", request.Version, hex.EncodeToString(request.Front))
	}
	metadata["root"] = "/changed"
	if _, err := m.OpenSession(ctx, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrMDSSessionExists) {
		t.Fatal("second session on one connection", err)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, map[string]string{}, 0, true))
	got := <-pending
	if got.err != nil {
		t.Fatal(got.err)
	}
	s := got.session
	status := s.Status()
	if status.State != MDSSessionOpen || status.Generation != 1 || status.GlobalID != 42 || status.Target != m.target || !MDSFeatureBitsetHas(status.Open.SupportedFeatures, 23) || status.Open.Metadata == nil {
		t.Fatal("open status", status)
	}
	if !reflect.DeepEqual(status.Open.CapAuths, []MDSCapAuth{{UID: -1, GIDs: []uint32{1000}, FilesystemName: "cephfs", Readable: true, Writeable: true}}) {
		t.Fatal("cap auths", status.Open.CapAuths)
	}
	status.Open.CapAuths[0].GIDs[0] = 7
	if s.Status().Open.CapAuths[0].GIDs[0] != 1000 {
		t.Fatal("status aliases session state")
	}
	first := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	if first.Sequence != 1 {
		t.Fatal("first renewal", first)
	}
	renewed := make(chan error, 1)
	go func() {
		r, err := s.Renew(ctx)
		if err == nil && (r.Sequence < 2 || r.Requested.IsZero() || r.Confirmed.Before(r.Requested)) {
			err = errors.New("renewal receipt")
		}
		renewed <- err
	}()
	second := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	if second.Sequence != 2 {
		t.Fatal("explicit renewal", second)
	}
	// A later reply covers the older outstanding renewal.
	peer.send(t, mdsSessionReply(mds.SessionRenewCaps, 2))
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	if r := s.Status().LastRenewal; r.Sequence != 2 || s.Status().RenewSequence != 2 {
		t.Fatal("renewal status", r)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	closeRequest := mdsSessionNext(t, peer, mds.SessionRequestClose)
	if closeRequest.Sequence != 0 || closeRequest.Version != 1 {
		t.Fatal("REQUEST_CLOSE", closeRequest)
	}
	mdsWaitState(t, s, MDSSessionClosing)
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if s.Status().State != MDSSessionClosed || !errors.Is(s.Err(), ErrMDSSessionClosed) {
		t.Fatal("confirmed close", s.Status())
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("closed session not done")
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal("repeated close", err)
	}
	// The command transport survives a confirmed CephFS close, and the slot is
	// released for an explicit new session generation.
	command := mdsStartCommand(m, ctx)
	peer.reply(t, peer.next(t), 0, "ok", nil)
	if outcome := mdsAwaitOutcome(t, ctx, command); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	next := mdsOpenTestSession(t, ctx, m, peer, time.Hour)
	if next.Status().Generation != 2 {
		t.Fatal("session generation", next.Status().Generation)
	}
	_ = c
}

func TestMDSSessionRejectAndServerClose(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	peer.sessions.Store(true)
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionV7(mds.SessionReject, 0, map[string]string{"error_string": "blocklisted (blacklisted)"}, mds.SessionFlagBlocklisted, false))
	got := <-pending
	var rejected *MDSSessionRejectedError
	if got.session != nil || !errors.As(got.err, &rejected) || rejected.Reason != "blocklisted (blacklisted)" || !rejected.Blocklisted || rejected.Flags != 1 {
		t.Fatal("reject", got.err)
	}
	pending = mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionReply(mds.SessionReject, 0))
	got = <-pending
	if !errors.As(got.err, &rejected) || rejected.Reason != "" || rejected.Blocklisted {
		t.Fatal("head-only reject", got.err)
	}
	pending = mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	if got = <-pending; got.session != nil || !errors.Is(got.err, ErrMDSSessionClosed) {
		t.Fatal("close before open", got.err)
	}
	// Confirmed refusals leave no unresolved state; the connection stays usable.
	mdsOpenTestSession(t, ctx, m, peer, time.Hour)
}

func TestMDSSessionCanceledOpenKeepsUncertainHandle(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	peer.sessions.Store(true)
	short, cancel := context.WithCancel(ctx)
	pending := mdsStartSession(m, short, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	cancel()
	got := <-pending
	var unknown *OutcomeUnknownError
	if got.session == nil || !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.session.Status().State != MDSSessionOpening {
		t.Fatal("canceled open must return the opening handle", got)
	}
	if _, err := m.OpenSession(ctx, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrMDSSessionExists) {
		t.Fatal(err)
	}
	if _, err := got.session.Renew(ctx); err == nil {
		t.Fatal("renewal before OPEN")
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	if _, err := got.session.WaitOpen(ctx); err != nil {
		t.Fatal(err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	closed := make(chan error, 1)
	go func() { closed <- got.session.Close(ctx) }()
	mdsSessionNext(t, peer, mds.SessionRequestClose)
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestMDSSessionEventsStaleAndFlushAck(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	s := mdsOpenTestSession(t, ctx, m, peer, time.Hour)
	peer.send(t, mdsSessionReply(mds.SessionStale, 0))
	event, err := s.Next(ctx)
	if err != nil || event.Op != MDSSessionOpStale || event.Received.IsZero() {
		t.Fatal("stale event", event, err)
	}
	mdsWaitState(t, s, MDSSessionStale)
	renew := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	if renew.Sequence != 2 {
		t.Fatal("STALE did not trigger an immediate renewal", renew)
	}
	peer.send(t, mdsSessionReply(mds.SessionRenewCaps, 2))
	mdsWaitState(t, s, MDSSessionOpen)
	recall := mdsSessionReply(mds.SessionRecallState, 0)
	head := mdsSessionHead(mds.SessionRecallState, 0)
	head.Data = head.Data[:20]
	head.U32(100)
	head.U32(7)
	recall.Front = head.Data
	peer.send(t, recall)
	peer.send(t, mdsSessionReply(mds.SessionFlushMessage, 9))
	peer.send(t, mdsSessionReply(mds.SessionForceReadOnly, 0))
	peer.send(t, mdsSessionV7(mds.SessionOpen, 3, nil, 0, false))
	for _, want := range []MDSSessionEvent{{Op: MDSSessionOpRecallState, MaxCaps: 100, MaxLeases: 7}, {Op: MDSSessionOpFlushMessage, Sequence: 9}, {Op: MDSSessionOpForceReadOnly}, {Op: MDSSessionOpOpen, Sequence: 3}} {
		event, err := s.Next(ctx)
		if err != nil || event.Op != want.Op || event.Sequence != want.Sequence || event.MaxCaps != want.MaxCaps || event.MaxLeases != want.MaxLeases {
			t.Fatal("event", event, err, want)
		}
	}
	if !s.Status().ReadOnly || s.Status().PushSequence != 3 {
		t.Fatal("read-only or push sequence status", s.Status())
	}
	if err := s.AcknowledgeFlush(ctx, 9); err != nil {
		t.Fatal(err)
	}
	if ack := mdsSessionNext(t, peer, mds.SessionFlushMessageAck); ack.Sequence != 9 {
		t.Fatal("flush ack", ack)
	}
	// REQUEST_CLOSE carries the adopted OPEN push sequence.
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	if request := mdsSessionNext(t, peer, mds.SessionRequestClose); request.Sequence != 3 {
		t.Fatal("close push sequence", request)
	}
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestMDSSessionPeriodicRenewal(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	s := mdsOpenTestSession(t, ctx, m, peer, 20*time.Millisecond)
	for want := uint64(2); want <= 4; want++ {
		renew := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
		if renew.Sequence != want {
			t.Fatal("periodic renewal sequence", renew.Sequence, want)
		}
		peer.send(t, mdsSessionReply(mds.SessionRenewCaps, want))
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.Status().LastRenewal.Sequence < 4 {
		if time.Now().After(deadline) {
			t.Fatal("periodic renewal not acknowledged", s.Status().LastRenewal)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMDSSessionProtocolViolationsAndOverflowAreTerminal(t *testing.T) {
	for name, send := range map[string][]msgr.MessageData{
		"renew before open": {mdsSessionReply(mds.SessionRenewCaps, 1)},
		"stale before open": {mdsSessionReply(mds.SessionStale, 0)},
		"client operation":  {mdsSessionReply(mds.SessionRequestOpen, 0)},
		"unknown operation": {mdsSessionReply(99, 0)},
		"malformed":         {{Type: mds.ClientSessionMessage, Version: 3, CompatVersion: 1, Front: mdsSessionHead(mds.SessionOpen, 0).Data}},
		"capability":        {{Type: 0x310, Version: 1, CompatVersion: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			_, m, peer, ctx, _ := mdsTestOpen(t)
			peer.sessions.Store(true)
			pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
			peer.next(t)
			for _, message := range send {
				peer.send(t, message)
			}
			got := <-pending
			if got.session != nil || got.err == nil {
				t.Fatal("protocol violation accepted", got)
			}
			<-m.Done()
			if _, err := m.client.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"}); !errors.Is(err, ErrMDSSessionActive) {
				t.Fatal("queued REQUEST_OPEN must leave the address unresolved", err)
			}
		})
	}
	_, m, peer, ctx, _ := mdsTestOpen(t)
	peer.sessions.Store(true)
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour, MaxEvents: 1})
	peer.next(t)
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	s := (<-pending).session
	peer.send(t, mdsSessionReply(mds.SessionRecallState, 0))
	peer.send(t, mdsSessionReply(mds.SessionRecallState, 0))
	<-s.Done()
	if s.Status().State != MDSSessionLost || !errors.Is(m.session.Err(), ErrMDSSessionOverflow) {
		t.Fatal("overflow must fail the connection", s.Status(), m.session.Err())
	}
	// The accepted event is still delivered before the terminal cause.
	if event, err := s.Next(ctx); err != nil || event.Op != MDSSessionOpRecallState {
		t.Fatal("accepted event lost", event, err)
	}
	if _, err := s.Next(ctx); !errors.Is(err, ErrMDSSessionOverflow) {
		t.Fatal("terminal cause", err)
	}
	if err := s.Close(ctx); !errors.Is(err, ErrMDSSessionActive) {
		t.Fatal("close after loss must report the unconfirmed remote session", err)
	}
}

func TestMDSSessionAddressGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.MaxMDSConnections = true, 4
	peers := make(chan *mdsTestPeer, 16)
	var dials atomic.Uint32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		clientPipe, server := net.Pipe()
		client := &mdsTestClientConn{Conn: clientPipe}
		if endpoint == "192.0.2.1:6805" {
			dials.Add(1)
			peer := &mdsTestPeer{conn: server, client: client, requests: make(chan msgr.MessageData, 64), done: make(chan struct{}), errors: make(chan error, 1)}
			peers <- peer
			go peer.serve()
		} else {
			go mdsMockMonitor(server)
		}
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	address := "v2:192.0.2.1:6805/17"
	commandOnly, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 1, Rank: 0, Address: address})
	if err != nil {
		t.Fatal(err)
	}
	<-peers
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: address})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	// A connection accepted before the session opened keeps its own closed
	// MDS-side session object, so an existing handle does not block the open.
	s := mdsOpenTestSession(t, ctx, m, peer, time.Hour)
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 8, Rank: 0, Address: address}); !errors.Is(err, ErrMDSSessionActive) {
		t.Fatal("a new connection would take over the remote session", err)
	}
	if _, err := commandOnly.OpenSession(ctx, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrMDSSessionActive) {
		t.Fatal("second session at one MDS address", err)
	}
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 8, Rank: 0, Address: "v2:192.0.2.1:6805/0"}); err != nil {
		t.Fatal("other MDS address blocked", err)
	}
	<-peers
	// Closing the connection under an open session leaves it unresolved.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	<-s.Done()
	if s.Status().State != MDSSessionLost {
		t.Fatal("connection close did not lose the session", s.Status())
	}
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: address}); !errors.Is(err, ErrMDSSessionActive) {
		t.Fatal("unresolved address reopened", err)
	}
	if _, err := commandOnly.OpenSession(ctx, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrMDSSessionActive) {
		t.Fatal("unresolved address reused for a session", err)
	}
	if dials.Load() != 3 {
		t.Fatal("unexpected dials", dials.Load())
	}
}
