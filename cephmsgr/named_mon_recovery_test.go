package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestNamedMonitorExactNameAndUnusableAddressesDoNotFallback(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	for _, name := range []string{"unknown", "mon.b", "1", "*", " b", "b ", "B", ""} {
		_, err := f.c.MonTellTo(f.ctx, name, Command{JSON: []byte(`{"prefix":"version"}`)})
		namedTestKnown(t, err)
		if !errors.Is(err, ErrMonitorNotFound) || f.namedDials.Load() != 0 || f.monDials.Load() != 1 {
			t.Fatal("name was normalized or routed to another monitor", name, err)
		}
	}
	namedTestPrimaryHealthy(t, f)
	for _, addresses := range [][]msgr.Address{
		{{Type: 1, Endpoint: netip.MustParseAddrPort("192.0.2.2:6789")}},
		{{Type: 2}},
		{},
	} {
		t.Run("no usable v2", func(t *testing.T) {
			f := namedTestFixtureFor(t, namedTestAddressB(), func(_ *Options, members []namedTestMember) { members[1].addresses = addresses }, nil)
			_, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"version"}`)})
			namedTestKnown(t, err)
			if f.namedDials.Load() != 0 || f.monDials.Load() != 1 {
				t.Fatal("unusable named target silently used another MON", err)
			}
		})
	}
}

func TestNamedMonitorAdmissionRejectsInvalidTargetBeforeTell(t *testing.T) {
	for _, failure := range []string{"FSID", "release", "header", "truncated", "missing", "renamed", "nonce", "flow", "scope", "family"} {
		t.Run(failure, func(t *testing.T) {
			address := msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("[::ffff:192.0.2.2]:3301"), FlowInfo: 0x10203040, ScopeID: 3}
			f := namedTestFixtureFor(t, address, nil, func(p *namedTestPeer) {
				fsid, release, name, changed := [16]byte{1}, byte(20), "b", address
				switch failure {
				case "FSID":
					fsid[0] = 2
				case "release":
					release = 19
				case "header":
					p.mapMessage.Version, p.mapMessage.CompatVersion = 2, 2
				case "missing":
					p.mapMissing = true
				case "renamed":
					name = "renamed"
				case "nonce":
					changed.Nonce++
				case "flow":
					changed.FlowInfo++
				case "scope":
					changed.ScopeID++
				case "family":
					changed.Endpoint = netip.AddrPortFrom(changed.Endpoint.Addr().Unmap(), changed.Endpoint.Port())
				}
				p.mapMessage.Front = namedTestMap(fsid, release, []namedTestMember{{name, []msgr.Address{changed}}})
				if failure == "truncated" {
					p.mapMessage.Front = p.mapMessage.Front[:len(p.mapMessage.Front)-1]
				}
			})
			if err := f.c.WaitMgrReady(f.ctx); err != nil {
				t.Fatal(err)
			}
			before := f.c.Snapshot()
			operation := f.ctx
			stop := func() {}
			if failure == "missing" {
				operation, stop = context.WithTimeout(f.ctx, 300*time.Millisecond)
			}
			_, err := f.c.MonTellTo(operation, "b", Command{JSON: []byte(`{"prefix":"mutation"}`)})
			stop()
			namedTestKnown(t, err)
			want := error(ErrMonitorTargetChanged)
			switch failure {
			case "FSID":
				want = msgr.ErrAuthentication
			case "release":
				want = maps.ErrRelease
			case "header":
				want = wire.ErrVersion
			case "truncated":
				want = io.ErrUnexpectedEOF
			case "missing":
				want = context.DeadlineExceeded
			}
			p := namedTestTarget(t, f)
			p.next(t, f.ctx, 5)
			if !errors.Is(err, want) || p.tellCount.Load() != 0 || f.namedDials.Load() != 1 || !reflect.DeepEqual(before, f.c.Snapshot()) {
				t.Fatal("invalid directed admission changed the primary or transmitted Tell", failure, err, p.tellCount.Load(), before, f.c.Snapshot())
			}
			if (failure == "header" || failure == "truncated") && !errors.Is(err, ErrMalformedMessage) {
				t.Fatal("invalid named map lost message/decode provenance", err)
			}
			namedTestPrimaryHealthy(t, f)
		})
	}
}

func TestNamedMonitorWireTargetRetainsAddressFamilyScopeAndFlow(t *testing.T) {
	for _, address := range []msgr.Address{
		namedTestAddressB(),
		{Type: 2, Nonce: 9, Endpoint: netip.MustParseAddrPort("[::ffff:192.0.2.2]:3301"), FlowInfo: 0x12345678, ScopeID: 3},
		{Type: 2, Nonce: 10, Endpoint: netip.MustParseAddrPort("[fe80::2]:3301"), FlowInfo: 0x87654321, ScopeID: 4},
	} {
		t.Run(address.Endpoint.String(), func(t *testing.T) {
			f := namedTestFixtureFor(t, address, nil, nil)
			if _, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil {
				t.Fatal("exact independent SERVER_IDENT address was rejected", address, err)
			}
			p := namedTestTarget(t, f)
			p.next(t, f.ctx, 5)
			p.next(t, f.ctx, 97)
			if p.tellCount.Load() != 1 || f.monDials.Load() != 1 {
				t.Fatal("wire address was normalized or primary was replaced")
			}
		})
	}
}

func TestNamedMonitorAdmissionSlotPrecedesValidationInputCopyAndDial(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), func(o *Options, _ []namedTestMember) { o.MaxInFlight = 1 }, nil)
	held, cancelHeld := context.WithCancel(f.ctx)
	defer cancelHeld()
	first := make(chan error, 1)
	go func() {
		_, err := f.c.MonCommand(held, Command{JSON: []byte(`{"prefix":"hold primary mutation"}`)})
		first <- err
	}()
	f.primary.next(t, f.ctx, msgr.MonCommandMessage)
	queued, cancelQueued := context.WithCancel(f.ctx)
	waiter := &tellWaitingContext{Context: queued, waiting: make(chan struct{})}
	second := make(chan error, 1)
	go func() { _, err := f.c.MonTellTo(waiter, "b", Command{JSON: []byte(`invalid JSON`)}); second <- err }()
	select {
	case <-waiter.waiting:
	case <-f.ctx.Done():
		t.Fatal("named tell did not wait for shared command slot", f.ctx.Err())
	}
	cancelQueued()
	if err := admissionResult(t, f.ctx, second); !errors.Is(err, context.Canceled) {
		t.Fatal("queued named command was validated before cancellation", err)
	}
	if f.namedDials.Load() != 0 {
		t.Fatal("queued named command opened a socket")
	}
	input := []byte("before")
	copyWaiter := &tellWaitingContext{Context: f.ctx, waiting: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		_, err := f.c.MonTellTo(copyWaiter, "b", Command{JSON: []byte(`{"prefix":"version"}`), Input: input})
		finished <- err
	}()
	select {
	case <-copyWaiter.waiting:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	// Done() signals the actual occupied-slot select. No input copy may occur
	// until that slot is released; changing caller bytes here is synchronized.
	copy(input, "after!")
	cancelHeld()
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, f.ctx, first); !errors.As(err, &unknown) {
		t.Fatal("held mutation lost unknown outcome", err)
	}
	if err := admissionResult(t, f.ctx, finished); err != nil {
		t.Fatal(err)
	}
	p := namedTestTarget(t, f)
	p.next(t, f.ctx, 5)
	if request := p.next(t, f.ctx, 97); !bytes.Equal(request.Data, []byte("after!")) {
		t.Fatal("named command copied input before admission", request.Data)
	}
}

func TestNamedMonitorMalformedRepliesPreserveRawOutputWithoutReplay(t *testing.T) {
	for _, mode := range []string{"truncated", "compatibility"} {
		t.Run(mode, func(t *testing.T) {
			f := namedTestFixtureFor(t, namedTestAddressB(), nil, func(p *namedTestPeer) {
				p.reply = func(reply *msgr.MessageData) {
					if mode == "truncated" {
						reply.Front = reply.Front[:len(reply.Front)-1]
					} else {
						reply.Version, reply.CompatVersion = 2, 2
					}
				}
			})
			input := []byte{0, 255, '\r', '\n'}
			result, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
			var unknown *OutcomeUnknownError
			cause := error(io.ErrUnexpectedEOF)
			if mode == "compatibility" {
				cause = wire.ErrVersion
			}
			if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) || !errors.Is(err, cause) || !bytes.Equal(result.Data, input) {
				t.Fatal("malformed named reply lost raw data or unknown/protocol cause", result, err)
			}
			p := namedTestTarget(t, f)
			p.next(t, f.ctx, 5)
			p.next(t, f.ctx, 97)
			namedTestPrimaryHealthy(t, f)
			if f.namedDials.Load() != 1 || p.tellCount.Load() != 1 {
				t.Fatal("uncertain named mutation was replayed or failed over")
			}
		})
	}
}

func TestNamedMonitorCancellationAndCloseKeepExecutionUncertainty(t *testing.T) {
	for _, mode := range []string{"cancel", "Close", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
			canceled, stopCanceled := context.WithCancel(f.ctx)
			stopCanceled()
			if _, err := f.c.MonTellTo(canceled, "unknown", Command{}); !errors.Is(err, context.Canceled) || f.namedDials.Load() != 0 {
				t.Fatal("pre-canceled named tell reached admission", err)
			}
			operation, cancel := context.WithCancel(f.ctx)
			defer cancel()
			finished := make(chan error, 1)
			command := []byte(`{"prefix":"hold mutation"}`)
			if mode == "disconnect" {
				command = []byte(`{"prefix":"drop mutation"}`)
			}
			go func() {
				_, err := f.c.MonTellTo(operation, "b", Command{JSON: command})
				finished <- err
			}()
			p := namedTestTarget(t, f)
			p.next(t, f.ctx, 5)
			p.next(t, f.ctx, 97)
			want := error(context.Canceled)
			if mode == "Close" {
				want = ErrClosed
				if err := f.c.Close(); err != nil {
					t.Fatal(err)
				}
			} else if mode == "cancel" {
				cancel()
			} else {
				want = io.EOF
			}
			var unknown *OutcomeUnknownError
			if err := admissionResult(t, f.ctx, finished); !(errors.Is(err, want) || mode == "disconnect" && errors.Is(err, io.ErrClosedPipe)) || !errors.As(err, &unknown) {
				t.Fatal("started named mutation lost cancellation/shutdown uncertainty", err)
			}
			select {
			case <-p.done:
			case <-f.ctx.Done():
				t.Fatal("named session outlived canceled operation", f.ctx.Err())
			}
			if f.namedDials.Load() != 1 || p.tellCount.Load() != 1 {
				t.Fatal("canceled uncertain named mutation was replayed")
			}
			if mode != "Close" {
				namedTestPrimaryHealthy(t, f)
			} else {
				if _, err := f.c.MonTellTo(f.ctx, "unknown", Command{JSON: []byte("invalid")}); !errors.Is(err, ErrClosed) {
					t.Fatal("closed named tell validated input/name first", err)
				}
			}
		})
	}
}

func TestNamedMonitorUnrelatedLateReplyCannotCompletePendingTell(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	completed := make(chan struct {
		result Result
		err    error
	}, 1)
	go func() {
		result, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"hold reply"}`)})
		completed <- struct {
			result Result
			err    error
		}{result, err}
	}()
	p := namedTestTarget(t, f)
	p.next(t, f.ctx, 5)
	request := p.next(t, f.ctx, 97)
	front := namedTestString(binary.LittleEndian.AppendUint32(nil, 0), "actual reply")
	// An independently encoded late/unrelated TID arrives first. It must not
	// establish this command's outcome; a following matching reply owns it.
	p.send(t, f.ctx, msgr.MessageData{Type: 98, Version: 1, Transaction: request.Transaction + 17, Front: front, Data: []byte("unrelated")})
	p.send(t, f.ctx, msgr.MessageData{Type: 98, Version: 1, Transaction: request.Transaction, Front: front, Data: []byte{0, 255, 1}})
	select {
	case got := <-completed:
		if got.err != nil || got.result.Code != 0 || got.result.Message != "actual reply" || !bytes.Equal(got.result.Data, []byte{0, 255, 1}) {
			t.Fatal("unrelated named-session reply completed a different TID", got.result, got.err)
		}
	case <-f.ctx.Done():
		t.Fatal("matching directed reply did not complete", f.ctx.Err())
	}
	namedTestPrimaryHealthy(t, f)
}

func TestNamedMonitorUnavailableTargetDoesNotTryAnotherMember(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, func(p *namedTestPeer) { p.dropAuth = true })
	_, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"mutation"}`)})
	namedTestKnown(t, err)
	if f.namedDials.Load() != 1 || f.monDials.Load() != 1 {
		t.Fatal("unavailable named target was replaced with a different MON", err)
	}
	namedTestPrimaryHealthy(t, f)
}

func TestNamedMonitorReplyWaitOutlivesEndpointSetupDeadline(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), func(o *Options, _ []namedTestMember) { o.ConnectTimeout = 250 * time.Millisecond }, nil)
	finished := make(chan error, 1)
	go func() {
		_, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"hold long reply"}`)})
		finished <- err
	}()
	p := namedTestTarget(t, f)
	p.next(t, f.ctx, 5)
	request := p.next(t, f.ctx, 97)
	// Start this clock after transmission: it exceeds every endpoint setup
	// deadline from the already completed authentication/admission attempt.
	delay, stop := context.WithTimeout(f.ctx, 2*f.c.options.ConnectTimeout)
	defer stop()
	select {
	case err := <-finished:
		t.Fatal("endpoint setup timeout canceled an established Tell", err)
	case <-delay.Done():
	}
	front := namedTestString(binary.LittleEndian.AppendUint32(nil, 0), "long reply")
	p.send(t, f.ctx, msgr.MessageData{Type: 98, Version: 1, Transaction: request.Transaction, Front: front})
	if err := admissionResult(t, f.ctx, finished); err != nil {
		t.Fatal("live caller could not wait past setup timeout", err)
	}
	namedTestPrimaryHealthy(t, f)
}

func TestNamedMonitorCloseJoinsIndependentHandshakeCleanup(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	connections := make(chan *managerCleanupConn, 1)
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, func(p *namedTestPeer) {
		p.stallAuth = true
		held := &managerCleanupConn{Conn: p.client, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
		p.client = held
		connections <- held
	})
	t.Cleanup(unblock) // Release before fixture cleanup waits for Client.Close.
	finished := make(chan error, 1)
	go func() {
		_, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"mutation"}`)})
		finished <- err
	}()
	var held *managerCleanupConn
	select {
	case held = <-connections:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- f.c.Close() }()
	select {
	case <-held.closing:
	case <-f.ctx.Done():
		t.Fatal("Close did not interrupt named handshake", f.ctx.Err())
	}
	select {
	case err := <-closed:
		t.Fatal("Close returned before independent connection cleanup", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := admissionResult(t, f.ctx, closed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-held.cleaned:
	default:
		t.Fatal("private handshake connection cleanup outlived Close")
	}
	if err := admissionResult(t, f.ctx, finished); !errors.Is(err, ErrClosed) {
		t.Fatal("unsubmitted directed command lost Close cause", err)
	} else {
		namedTestKnown(t, err)
	}
}
