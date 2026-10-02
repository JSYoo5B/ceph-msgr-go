package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// This is the pinned Linux Ceph server's wire value, not the host OS errno.
const unsupportedConnectionModeCode int32 = -95

type securityModeOffer struct {
	method uint32
	modes  []uint32
}

// Observe only the CRC-only listener's preauthentication writes. Keep numeric
// offers after decoding; never log the temporary bytes containing credentials.
// Seeing every offer also catches fallback on the same or a fresh connection.
type securityModeObserver struct {
	mu      sync.Mutex
	enabled bool
	targets map[string]bool // nil observes every endpoint (CRC-only MON fixture).
	conns   []*securityModeConn
}

type securityModeConn struct {
	net.Conn
	observer *securityModeObserver
	pending  []byte
	banner   bool
	offers   []securityModeOffer
	err      error
}

func (o *securityModeObserver) wrap(conn net.Conn, endpoint string) net.Conn {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.enabled || (o.targets != nil && !o.targets[endpoint]) {
		return conn
	}
	c := &securityModeConn{Conn: conn, observer: o}
	o.conns = append(o.conns, c)
	return c
}

func (c *securityModeConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.observer.mu.Lock()
	defer c.observer.mu.Unlock()
	if c.observer.enabled && n > 0 {
		c.observe(p[:n])
	}
	return n, err
}

func (c *securityModeConn) observe(p []byte) {
	if c.err != nil {
		return
	}
	const limit = 64 << 10
	if len(c.pending)+len(p) > limit {
		c.pending = nil
		c.err = wire.ErrLimit
		return
	}
	c.pending = append(c.pending, p...)
	for len(c.pending) > 0 {
		input := bytes.NewReader(c.pending)
		if !c.banner {
			_, err := msgr.ReadBanner(input, msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1})
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			if err != nil {
				c.err = err
				return
			}
			c.banner = true
		} else {
			frame, err := msgr.NewReader(input, limit).Read()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			if err != nil {
				c.err = err
				return
			}
			switch frame.Tag {
			case msgr.Hello:
			case msgr.AuthRequest:
				if len(frame.Segments) != 1 {
					c.err = msgr.ErrFrame
					return
				}
				d := wire.NewDecoder(frame.Segments[0])
				offer := securityModeOffer{method: d.U32()}
				for n := d.Count(4, 32); n > 0; n-- {
					offer.modes = append(offer.modes, d.U32())
				}
				d.Bytes() // Discard the credential/authorizer payload.
				if err := d.Done(); err != nil {
					c.err = err
					return
				}
				c.offers = append(c.offers, offer)
			default:
				// A mode mismatch is rejected before a CephX challenge or
				// application command. No further client frame is expected.
				c.err = errors.New("unexpected frame after connection mode rejection")
				return
			}
		}
		c.pending = c.pending[len(c.pending)-input.Len():]
	}
	c.pending = nil
}

func (o *securityModeObserver) check() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.conns) == 0 {
		return 0, errors.New("no CRC-only listener connection was observed")
	}
	for _, c := range o.conns {
		if c.err != nil || len(c.pending) != 0 || !c.banner || len(c.offers) != 1 {
			return 0, errors.New("mode rejection did not end after one complete authentication offer")
		}
		offer := c.offers[0]
		if offer.method != 2 || len(offer.modes) != 1 || offer.modes[0] != 2 {
			return 0, errors.New("client offered a method or mode other than CephX secure")
		}
	}
	return len(o.conns), nil
}

func TestCephConnectionModeRejectionIntegration(t *testing.T) {
	mode := os.Getenv("CEPH_MSGR_TEST_MODE_REJECTION")
	if mode == "" {
		t.Skip("set CEPH_MSGR_TEST_MODE_REJECTION=mon or mgr for the isolated mode fixture")
	}
	if mode != "mon" && mode != "mgr" {
		t.Fatal("CEPH_MSGR_TEST_MODE_REJECTION must be mon or mgr")
	}
	options := integrationOptions(t) // The fixture's valid, unchanged credential.
	observer := &securityModeObserver{enabled: mode == "mon"}
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		return observer.wrap(conn, endpoint), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	assertRejection := func(err error) {
		t.Helper()
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) || !matchesAuthenticationRejection(err, 2, unsupportedConnectionModeCode) {
			t.Fatal("CRC-only listener rejection lost its known server method/code", err)
		}
	}
	assertOffers := func() {
		t.Helper()
		connections, err := observer.check()
		if err != nil {
			t.Fatal("CRC fallback observation", err)
		}
		t.Logf("%d rejected connections each offered CephX with secure mode only", connections)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if mode == "mon" {
		if c != nil {
			c.Close()
			t.Fatal("CRC-only MON accepted the secure-only client")
		}
		assertRejection(err)
		assertOffers()
		return
	}
	if err != nil {
		t.Fatal("secure MON authentication", err)
	}
	defer c.Close()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Fatal("MGR mode recovery requires the disposable fixture control directory")
	}
	initial := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Available && s.Manager.Name != "" && len(s.Manager.Endpoints) > 0
	})
	observer.mu.Lock()
	observer.enabled = true
	observer.targets = make(map[string]bool)
	for _, endpoint := range initial.Manager.Endpoints {
		observer.targets[endpoint] = true
	}
	observer.mu.Unlock()
	restored := false
	t.Cleanup(func() {
		if !restored {
			restore, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			if err := testcluster.ControlDaemon(restore, control, "secure", "mgr", initial.Manager.Name); err != nil {
				t.Error("restore secure fixture MGRs", err)
			}
		}
	})
	checkMonitor := func() {
		t.Helper()
		result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatal("MGR-only mode rejection disrupted MON status", result.Code, err)
		}
		state := c.Snapshot()
		if !state.Monitor.Ready || state.Manager.Ready || state.AuthRejection != nil || state.Closed || state.GlobalID != initial.GlobalID {
			t.Fatal("MGR mode rejection changed MON admission or identity", state)
		}
	}
	assertRejection(c.WaitMgrReady(ctx))
	assertOffers()
	checkMonitor()
	result, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)})
	assertRejection(err)
	if result.Code != 0 || result.Message != "" || len(result.Data) != 0 {
		t.Fatal("rejected MGR setup produced an application command reply", result.Code, len(result.Message), len(result.Data))
	}
	assertOffers()
	checkMonitor()
	observer.mu.Lock()
	observer.enabled = false
	observer.mu.Unlock()
	// The fixture restores both MGRs, so a CRC-only standby cannot take over
	// during restart. Its acknowledgement independently checks MGR readiness.
	if err := testcluster.ControlDaemon(ctx, control, "secure", "mgr", initial.Manager.Name); err != nil {
		t.Fatal("restore secure fixture MGRs", err)
	}
	restored = true
	waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.MapEpoch > initial.Manager.MapEpoch && s.Manager.GlobalID != initial.Manager.GlobalID
	})
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("same client did not prepare MGR after secure mode restoration", err)
	}
	result, err = c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)})
	if err != nil || result.Code != 0 || !json.Valid(result.Data) {
		t.Fatal("same client MGR read after secure mode restoration", result.Code, err)
	}
	final := c.Snapshot()
	if !final.Monitor.Ready || !final.Manager.Ready || final.AuthRejection != nil || final.Closed || final.GlobalID != initial.GlobalID {
		t.Fatal("secure restoration did not preserve the same client identity", final)
	}
	t.Log("MGR mode rejection stayed a known setup failure; MON remained healthy and the same client recovered after secure restoration")
}

func TestSecurityModeObserver(t *testing.T) {
	for _, test := range []struct {
		name   string
		offers [][]uint32
		want   bool
	}{
		{"secure", [][]uint32{{2}}, true},
		{"CRC fallback", [][]uint32{{1}}, false},
		{"secure and CRC", [][]uint32{{2, 1}}, false},
		{"same connection fallback", [][]uint32{{2}, {1}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			output.Write((msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}).Encode())
			writer := msgr.NewWriter(&output, 4096)
			if err := writer.Write(msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{{8}}}); err != nil {
				t.Fatal(err)
			}
			for _, modes := range test.offers {
				auth := wire.Encoder{}
				auth.U32(2)
				auth.U32(uint32(len(modes)))
				for _, mode := range modes {
					auth.U32(mode)
				}
				auth.Bytes([]byte("discarded fixture payload"))
				if err := writer.Write(msgr.Frame{Tag: msgr.AuthRequest, Segments: [][]byte{auth.Data}}); err != nil {
					t.Fatal(err)
				}
			}
			observer := &securityModeObserver{}
			conn := &securityModeConn{observer: observer}
			observer.conns = append(observer.conns, conn)
			// Actual Conn.Write boundaries need not coincide with frames.
			for _, b := range output.Bytes() {
				conn.observe([]byte{b})
			}
			if _, err := observer.check(); (err == nil) != test.want {
				t.Fatal("incorrect mode observation", err)
			}
		})
	}
}
