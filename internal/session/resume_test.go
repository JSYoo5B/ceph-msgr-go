package session

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Synthetic stateful-resume checks: frame fields follow pinned
// msg/async/frames_v2.h ReconnectFrame and ProtocolV2 handle_reconnect. They
// are not native interoperability evidence.

type reconnectFields struct {
	addresses                                []msgr.Address
	client, server, global, connect, message uint64
}

func decodeReconnect(f msgr.Frame) (reconnectFields, error) {
	if f.Tag != msgr.SessionReconnect || len(f.Segments) != 1 {
		return reconnectFields{}, errors.New("expected SESSION_RECONNECT")
	}
	d := wire.NewDecoder(f.Segments[0])
	r := reconnectFields{addresses: msgr.DecodeAddresses(d), client: d.U64(), server: d.U64(), global: d.U64(), connect: d.U64(), message: d.U64()}
	return r, d.Done()
}

func TestResumeHandshakeFollowsRetriesAndReset(t *testing.T) {
	state := ResumeState{Addresses: []msgr.Address{{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.9:0")}}, ClientCookie: 5, ServerCookie: 17, GlobalSeq: 100, ConnectSeq: 0, InSeq: 9, Features: Features, GlobalID: 42}
	for name, tc := range map[string]struct {
		answers []func(reconnectFields) (msgr.Tag, []byte)
		check   func(*testing.T, []reconnectFields)
		err     error
		peer    uint64
	}{
		"ok after retries": {answers: []func(reconnectFields) (msgr.Tag, []byte){
			func(reconnectFields) (msgr.Tag, []byte) {
				e := wire.Encoder{}
				e.U64(5)
				return msgr.SessionRetry, e.Data
			},
			func(reconnectFields) (msgr.Tag, []byte) {
				e := wire.Encoder{}
				e.U64(500)
				return msgr.SessionRetryGlobal, e.Data
			},
			func(reconnectFields) (msgr.Tag, []byte) {
				e := wire.Encoder{}
				e.U64(3)
				return msgr.SessionReconnectOK, e.Data
			},
		}, peer: 3, check: func(t *testing.T, got []reconnectFields) {
			if len(got) != 3 || !reflect.DeepEqual(got[0].addresses, state.Addresses) || got[0].client != 5 || got[0].server != 17 || got[0].global != 101 || got[0].connect != 1 || got[0].message != 9 {
				t.Fatal("first reconnect fields", got)
			}
			if got[1].connect != 6 || got[1].global != 101 || got[2].global != 501 || got[2].connect != 6 {
				t.Fatal("retry sequences", got)
			}
		}},
		"reset": {answers: []func(reconnectFields) (msgr.Tag, []byte){
			func(reconnectFields) (msgr.Tag, []byte) { return msgr.SessionReset, []byte{1} },
		}, err: ErrSessionReset},
		"wait": {answers: []func(reconnectFields) (msgr.Tag, []byte){
			func(reconnectFields) (msgr.Tag, []byte) { return msgr.Wait, nil },
		}, err: msgr.ErrFrame},
	} {
		t.Run(name, func(t *testing.T) {
			auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, SecureMode)
			if err != nil {
				t.Fatal(err)
			}
			client, peer := net.Pipe()
			t.Cleanup(func() { client.Close(); peer.Close() })
			cfg := freshMDSPeerDefaults()
			var seen []reconnectFields
			cfg.reconnect = func(f msgr.Frame, r *msgr.Reader, w *msgr.Writer) error {
				for i, answer := range tc.answers {
					if i > 0 {
						var err error
						if f, err = r.Read(); err != nil {
							return err
						}
					}
					got, err := decodeReconnect(f)
					if err != nil {
						return err
					}
					seen = append(seen, got)
					tag, payload := answer(got)
					if err := w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{payload}}); err != nil {
						return err
					}
				}
				// Like a real server, keep the stream open until the client
				// finishes admission and closes it.
				r.Read()
				return nil
			}
			done := make(chan error, 1)
			go func() { done <- freshMDSPeer(peer, fixture, SecureMode, cfg) }()
			tr, err := HandshakeResumeCephFS(context.Background(), client, msgr.Address{Type: 2}, 0, mixedFreshMDSAuth{auth}, SecureMode, 4096, time.Second, state)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatal("resume error", err)
				}
				client.Close()
				<-done
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			client.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			tc.check(t, seen)
			if tr.PeerSequence != tc.peer || tr.Resume.ConnectSeq != 6 || tr.Resume.GlobalSeq != 501 || tr.GlobalID != 42 || tr.Features != Features {
				t.Fatal("resumed transport", tr.PeerSequence, tr.Resume)
			}
		})
	}
	if _, err := HandshakeResumeCephFS(context.Background(), &net.TCPConn{}, msgr.Address{}, 0, nil, SecureMode, 4096, time.Second, ResumeState{}); err == nil {
		t.Fatal("incomplete resume state accepted")
	}
}

type resumablePeer struct {
	r *msgr.Reader
	w *msgr.Writer
	c net.Conn
}

func newResumableSession(t *testing.T, resume func(context.Context, ResumeState) (*Transport, error), maxUnacked int) (*Session, resumablePeer) {
	t.Helper()
	client, peer := net.Pipe()
	state := ResumeState{Addresses: []msgr.Address{{Type: 2, Nonce: 1}}, ClientCookie: 1, ServerCookie: 2, GlobalSeq: 3, GlobalID: 42}
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0), Resume: state}, Config{WriteTimeout: time.Second, Resume: resume, ResumeTimeout: 2 * time.Second, MaxUnacked: maxUnacked}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	return s, resumablePeer{r: msgr.NewReader(peer, 0), w: msgr.NewWriter(peer, 0), c: peer}
}

func TestStatefulSessionResumesAndRetransmitsUnacknowledged(t *testing.T) {
	next := make(chan resumablePeer, 1)
	var states []ResumeState
	resume := func(ctx context.Context, state ResumeState) (*Transport, error) {
		states = append(states, state)
		client, peer := net.Pipe()
		next <- resumablePeer{r: msgr.NewReader(peer, 0), w: msgr.NewWriter(peer, 0), c: peer}
		resumed := state
		resumed.ConnectSeq++
		return &Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0), Resume: resumed, PeerSequence: 1}, nil
	}
	s, first := newResumableSession(t, resume, 0)
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		reply, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte{1}})
		if err == nil && reply.Front[0] != 9 {
			err = errors.New("wrong reply")
		}
		result <- err
	}()
	call := readRequest(t, first.r)
	if err := s.Send(ctx, msgr.MessageData{Type: 22, Front: []byte{2}}); err != nil {
		t.Fatal(err)
	}
	send := readRequest(t, first.r)
	if call.Sequence != 1 || send.Sequence != 2 {
		t.Fatal("initial sequences", call.Sequence, send.Sequence)
	}
	// The peer delivers one message to the client, then the TCP stream dies.
	if err := first.w.Write(msgr.MessageData{Sequence: 1, Type: 22, Front: []byte{7}}.Frame()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	first.c.Close()
	second := <-next
	// PeerSequence 1: the call frame arrived; the Send frame is retransmitted
	// with its original sequence before anything new.
	resent := readRequest(t, second.r)
	if resent.Sequence != 2 || resent.Front[0] != 2 || resent.AckSequence != 1 {
		t.Fatal("retransmission", resent)
	}
	if len(states) != 1 || states[0].InSeq != 1 || states[0].ClientCookie != 1 {
		t.Fatal("resume state", states)
	}
	// A duplicate of an already received message is dropped; the reply
	// continues the gapless stream.
	if err := second.w.Write(msgr.MessageData{Sequence: 1, Type: 22, Front: []byte{7}}.Frame()); err != nil {
		t.Fatal(err)
	}
	if err := second.w.Write(msgr.MessageData{Sequence: 2, Type: msgr.MonCommandReplyMessage, Transaction: call.Transaction, Front: []byte{9}}.Frame()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal("call did not survive the resume", err)
	}
	if err := s.Send(ctx, msgr.MessageData{Type: 22, Front: []byte{3}}); err != nil {
		t.Fatal(err)
	}
	if m := readRequest(t, second.r); m.Sequence != 3 {
		t.Fatal("post-resume sequence", m.Sequence)
	}
	if s.Resumes() != 1 || s.Err() != nil {
		t.Fatal("resume accounting", s.Resumes(), s.Err())
	}
	// A sequence gap is terminal for a stateful stream.
	if err := second.w.Write(msgr.MessageData{Sequence: 4, Type: 22}.Frame()); err != nil {
		t.Fatal(err)
	}
	<-s.Done()
	if !errors.Is(s.Err(), msgr.ErrFrame) {
		t.Fatal("gap accepted", s.Err())
	}
}

func TestStatefulSessionTerminalResumeOutcomes(t *testing.T) {
	var attempts atomic.Int32
	reset := func(ctx context.Context, state ResumeState) (*Transport, error) {
		attempts.Add(1)
		return nil, ErrSessionReset
	}
	s, peer := newResumableSession(t, reset, 0)
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
		result <- err
	}()
	readRequest(t, peer.r)
	peer.c.Close()
	var unknown *OutcomeUnknownError
	if err := <-result; !errors.As(err, &unknown) || !errors.Is(err, ErrSessionReset) {
		t.Fatal("reset must end the session with an unknown outcome", err)
	}
	if attempts.Load() != 1 {
		t.Fatal("reset retried", attempts.Load())
	}

	// Transient dial failures retry until the episode deadline.
	var dials atomic.Int32
	refused := func(ctx context.Context, state ResumeState) (*Transport, error) {
		dials.Add(1)
		return nil, errors.New("connection refused")
	}
	s2, peer2 := newResumableSession(t, refused, 0)
	s2.config.ResumeTimeout = 300 * time.Millisecond
	s2.Start()
	peer2.c.Close()
	<-s2.Done()
	if dials.Load() < 2 || !errors.Is(s2.Err(), context.DeadlineExceeded) {
		t.Fatal("bounded retries", dials.Load(), s2.Err())
	}

	// Protocol errors never resume.
	called := atomic.Bool{}
	never := func(ctx context.Context, state ResumeState) (*Transport, error) {
		called.Store(true)
		return nil, errors.New("unexpected")
	}
	s3, peer3 := newResumableSession(t, never, 0)
	s3.Start()
	if err := peer3.w.Write(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{{1}}}); err != nil {
		t.Fatal(err)
	}
	<-s3.Done()
	if called.Load() || !errors.Is(s3.Err(), msgr.ErrFrame) {
		t.Fatal("protocol error resumed", s3.Err())
	}

	// The retained-message bound is enforced.
	s4, peer4 := newResumableSession(t, never, 1)
	s4.Start()
	if err := s4.Send(ctx, msgr.MessageData{Type: 22}); err != nil {
		t.Fatal(err)
	}
	readRequest(t, peer4.r)
	if err := s4.Send(ctx, msgr.MessageData{Type: 22}); err != nil {
		t.Fatal(err)
	}
	<-s4.Done()
	if !errors.Is(s4.Err(), wire.ErrLimit) {
		t.Fatal("unacknowledged bound", s4.Err())
	}
	// Close during a pending resume ends it.
	block := make(chan struct{})
	slow := func(ctx context.Context, state ResumeState) (*Transport, error) {
		close(block)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s5, peer5 := newResumableSession(t, slow, 0)
	s5.Start()
	peer5.c.Close()
	<-block
	s5.Fail(ErrClosed)
	s5.Wait()
	if !errors.Is(s5.Err(), ErrClosed) {
		t.Fatal(s5.Err())
	}
}
