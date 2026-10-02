package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type tellRequest struct {
	role uint8
	data msgr.MessageData
}

type tellFixture struct {
	options Options
	events  chan tellRequest
	calls   atomic.Uint32
	monDial atomic.Uint32
	mgrDial atomic.Uint32
}

func newTellFixture(t *testing.T, fsid [16]byte, reply func(*msgr.MessageData)) *tellFixture {
	t.Helper()
	f := &tellFixture{options: mockOptions(t, 20, fsid), events: make(chan tellRequest, 16)}
	f.options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		role, id := uint8(1), uint64(0)
		if strings.HasSuffix(endpoint, ":6800") {
			role, id = 16, 99
			f.mgrDial.Add(1)
		} else {
			f.monDial.Add(1)
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: fsid, release: 20, role: role, id: id, reply: reply, command: func(m msgr.MessageData) {
			f.calls.Add(1)
			select {
			case f.events <- tellRequest{role: role, data: m}:
			default:
			}
		}})
		return client, nil
	}
	return f
}

func nextTellRequest(t *testing.T, ctx context.Context, f *tellFixture) tellRequest {
	t.Helper()
	select {
	case request := <-f.events:
		return request
	case <-ctx.Done():
		t.Fatal("synthetic daemon did not receive the command", ctx.Err())
		return tellRequest{}
	}
}

func tellCalls(c *Client, mgr bool) (func(context.Context, Command) (Result, error), func(context.Context, Command) (Result, error), uint8, uint16) {
	if mgr {
		return c.MgrTell, c.MgrCommand, 16, msgr.MgrCommandMessage
	}
	return c.MonTell, c.MonCommand, 1, msgr.MonCommandMessage
}

func TestTellSharesNormalSessionsAndPreservesServerErrors(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			fsid := [16]byte{1, 2, 3}
			f := newTellFixture(t, fsid, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			tell, normal, role, normalType := tellCalls(c, mgr)
			input := []byte{0, 255, 1, 0}
			seen := make(map[uint64]bool)
			for _, operation := range []struct {
				call func(context.Context, Command) (Result, error)
				typ  uint16
				json []byte
				code int32
			}{{normal, normalType, []byte(`{"prefix":"status"}`), 0},
				{tell, 97, []byte(" \n{\"prefix\":\"version\"}\t"), 0},
				{tell, 97, []byte(`{"prefix":"deny"}`), -13},
				{normal, normalType, []byte(`{"prefix":"status"}`), 0}} {
				result, err := operation.call(ctx, Command{JSON: operation.json, Input: input})
				var commandError *CommandError
				var auth *AuthenticationError
				var unknown *OutcomeUnknownError
				if result.Code != operation.code || result.Message != "status text" || !bytes.Equal(result.Data, input) || errors.As(err, &auth) || errors.As(err, &unknown) {
					t.Fatal("tell/normal call lost its raw server result", result, err)
				}
				if operation.code == 0 && err != nil || operation.code < 0 && (!errors.As(err, &commandError) || commandError.Code != -13 || commandError.Message != result.Message) {
					t.Fatal("command rejection was confused with authentication or transport", result, err)
				}
				request := nextTellRequest(t, ctx, f)
				m := request.data
				if request.role != role || m.Type != operation.typ || m.Version != 1 || m.CompatVersion != 0 || m.Transaction == 0 || seen[m.Transaction] || !bytes.Equal(m.Data, input) {
					t.Fatal("mixed command routes lost daemon/type/TID/raw input", request)
				}
				seen[m.Transaction] = true
				if operation.typ == 97 {
					// Independent MCommand front oracle: no Paxos prefix, UUID16,
					// one string with its exact JSON bytes, including whitespace.
					want := append([]byte(nil), fsid[:]...)
					want = binary.LittleEndian.AppendUint32(want, 1)
					want = binary.LittleEndian.AppendUint32(want, uint32(len(operation.json)))
					want = append(want, operation.json...)
					if !bytes.Equal(m.Front, want) {
						t.Fatal("tell body disagrees with independent MCommand layout", m.Front)
					}
				}
			}
			state := c.Snapshot()
			wantMgrDials := uint32(0)
			if mgr {
				wantMgrDials = 1
			}
			if f.monDial.Load() != 1 || f.mgrDial.Load() != wantMgrDials || f.calls.Load() != 4 || state.AuthRejection != nil || !state.Monitor.Ready || mgr && !state.Manager.Ready {
				t.Fatal("server -13 disrupted shared sessions or triggered replay/auth rejection", state, f.calls.Load(), f.monDial.Load(), f.mgrDial.Load())
			}
		})
	}
}

func TestMalformedTellReplyPreservesRawOutputWithoutReplay(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		for _, malformed := range []string{"truncated", "compatibility"} {
			name := "MON/" + malformed
			if mgr {
				name = "MGR/" + malformed
			}
			t.Run(name, func(t *testing.T) {
				var corrupt atomic.Bool
				corrupt.Store(true)
				f := newTellFixture(t, [16]byte{1}, func(m *msgr.MessageData) {
					if m.Type == msgr.TellCommandReplyMessage && corrupt.Swap(false) {
						if malformed == "truncated" {
							m.Front = m.Front[:len(m.Front)-1]
						} else {
							m.Version, m.CompatVersion = 2, 2
						}
					}
				})
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				c, err := Dial(ctx, f.options)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				tell, normal, _, _ := tellCalls(c, mgr)
				input := []byte{0, 255, 1, 0}
				result, err := tell(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
				var unknown *OutcomeUnknownError
				var server *CommandError
				cause := error(io.ErrUnexpectedEOF)
				if malformed == "compatibility" {
					cause = wire.ErrVersion
				}
				if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) || !errors.Is(err, cause) || errors.As(err, &server) || !bytes.Equal(result.Data, input) {
					t.Fatal("invalid tell reply lost raw data or unknown outcome", result, err)
				}
				if result, err := normal(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || result.Code != 0 {
					t.Fatal("new normal command did not recover after tell failure", result, err)
				}
				if f.calls.Load() != 2 || c.Snapshot().AuthRejection != nil {
					t.Fatal("uncertain tell was replayed or changed authentication", f.calls.Load(), c.Snapshot())
				}
			})
		}
	}
}

type tellWaitingContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *tellWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestTellCancellationAdmissionAndClosePreserveOutcomes(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			f := newTellFixture(t, [16]byte{1}, nil)
			f.options.MaxInFlight = 1
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			tell, normal, _, _ := tellCalls(c, mgr)
			before, stopBefore := context.WithCancel(ctx)
			stopBefore()
			if _, err := tell(before, Command{}); !errors.Is(err, context.Canceled) || f.calls.Load() != 0 {
				t.Fatal("already canceled tell reached validation or wire", err, f.calls.Load())
			}
			first, stopFirst := context.WithCancel(ctx)
			defer stopFirst()
			finished := make(chan error, 1)
			go func() { _, err := tell(first, Command{JSON: []byte(`{"prefix":"block mutation"}`)}); finished <- err }()
			nextTellRequest(t, ctx, f) // Daemon received the started mutation.
			waiting, stopWaiting := context.WithCancel(ctx)
			defer stopWaiting()
			waiter := &tellWaitingContext{Context: waiting, waiting: make(chan struct{})}
			queued := make(chan error, 1)
			go func() { _, err := tell(waiter, Command{JSON: []byte(`{"prefix":"queued mutation"}`)}); queued <- err }()
			select {
			case <-waiter.waiting:
			case <-ctx.Done():
				t.Fatal("second tell did not reach the occupied admission slot", ctx.Err())
			}
			stopWaiting()
			var unknown *OutcomeUnknownError
			if err := admissionResult(t, ctx, queued); !errors.Is(err, context.Canceled) || errors.As(err, &unknown) || f.calls.Load() != 1 {
				t.Fatal("canceled queued tell was submitted or marked uncertain", err, f.calls.Load())
			}
			stopFirst()
			if err := admissionResult(t, ctx, finished); !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
				t.Fatal("started tell lost its cancellation/unknown outcome", err)
			}
			if _, err := normal(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("tell cancellation disrupted the shared connection", err)
			}
			nextTellRequest(t, ctx, f)
			closing := make(chan error, 1)
			go func() { _, err := tell(ctx, Command{JSON: []byte(`{"prefix":"block close"}`)}); closing <- err }()
			nextTellRequest(t, ctx, f)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if err := admissionResult(t, ctx, closing); !errors.Is(err, ErrClosed) || !errors.As(err, &unknown) || f.calls.Load() != 3 {
				t.Fatal("Close lost the started tell outcome or replayed it", err, f.calls.Load())
			}
			if _, err := tell(ctx, Command{}); !errors.Is(err, ErrClosed) {
				t.Fatal("closed tell validated input before known local rejection", err)
			}
			state := c.Snapshot()
			if !state.Closed || state.Monitor.Ready || state.Manager.Ready || state.AuthRejection != nil {
				t.Fatal("Close did not release tell sessions", state)
			}
		})
	}
}

func TestTellRejectsZeroFSIDBeforeDialOrWireAdmission(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Force the invalid local state while preserving a healthy MON and cold MGR.
	// This proves the tell route cannot silently become a legacy zero-FSID route.
	c.mu.Lock()
	c.fsid = [16]byte{}
	c.mu.Unlock()
	for _, call := range []func(context.Context, Command) (Result, error){c.MonTell, c.MgrTell} {
		result, err := call(ctx, Command{JSON: []byte(`{"prefix":"version"}`)})
		var unknown *OutcomeUnknownError
		var server *CommandError
		var auth *AuthenticationError
		if err == nil || !strings.Contains(err.Error(), "authenticated cluster FSID") || errors.As(err, &unknown) || errors.As(err, &server) || errors.As(err, &auth) || len(result.Data) != 0 || f.calls.Load() != 0 || f.monDial.Load() != 1 || f.mgrDial.Load() != 0 {
			t.Fatal("zero-FSID tell was sent or confused with a remote outcome", result, err, f.calls.Load(), f.monDial.Load(), f.mgrDial.Load())
		}
	}
}
