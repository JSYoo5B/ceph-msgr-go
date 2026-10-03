package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Retain only the numeric initial offer on each actual connection. Stop
// recording before the challenge, credentials, and application messages.
type crcOfferObserver struct {
	mu    sync.Mutex
	conns []*crcOfferConn
}

type crcOfferConn struct {
	net.Conn
	observer *crcOfferObserver
	endpoint string
	pending  []byte
	banner   bool
	offer    *securityModeOffer
	err      error
}

func (o *crcOfferObserver) wrap(conn net.Conn, endpoint string) net.Conn {
	o.mu.Lock()
	defer o.mu.Unlock()
	c := &crcOfferConn{Conn: conn, observer: o, endpoint: endpoint}
	o.conns = append(o.conns, c)
	return c
}

func (c *crcOfferConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.observer.mu.Lock()
	defer c.observer.mu.Unlock()
	if n > 0 && c.offer == nil && c.err == nil {
		c.observe(p[:n])
	}
	return n, err
}

func (c *crcOfferConn) observe(p []byte) {
	const limit = 64 << 10
	if len(c.pending)+len(p) > limit {
		clear(c.pending)
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
				offer := &securityModeOffer{method: d.U32()}
				for n := d.Count(4, 32); n > 0; n-- {
					offer.modes = append(offer.modes, d.U32())
				}
				d.Bytes()
				c.err = d.Done()
				c.offer = offer
				clear(frame.Segments[0])
				clear(c.pending)
				c.pending = nil
				return
			default:
				c.err = errors.New("unexpected frame before initial authentication offer")
				return
			}
		}
		consumed := len(c.pending) - input.Len()
		clear(c.pending[:consumed])
		c.pending = c.pending[consumed:]
	}
	c.pending = nil
}

func (o *crcOfferObserver) check(mon, mgr string) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	seenMon, seenMgr := false, false
	completed := 0
	for _, c := range o.conns {
		if c.err != nil {
			return 0, errors.New("malformed initial authentication offer on actual connection")
		}
		// Renewal may have dialed a replacement while the admitted endpoints
		// remain ready. Its not-yet-sent offer cannot establish a wire mode.
		if c.offer == nil {
			continue
		}
		if !c.banner || len(c.pending) != 0 || c.offer.method != 2 || len(c.offer.modes) != 1 || c.offer.modes[0] != 1 {
			return 0, errors.New("actual connection did not offer CephX with CRC alone")
		}
		completed++
		seenMon = seenMon || c.endpoint == mon
		seenMgr = seenMgr || c.endpoint == mgr
	}
	if !seenMon || !seenMgr {
		return 0, errors.New("both admitted MON and MGR offers must be observed")
	}
	return completed, nil
}

func TestCephCRCConnectionModeIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_CONNECTION_MODE") != "crc" {
		t.Skip("requires the explicitly selected CRC-only fixture")
	}
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" {
		t.Skip("requires the disposable two-MGR fixture")
	}
	for _, name := range []string{"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS", "CEPH_MSGR_TEST_MAPPED_IPV6"} {
		if os.Getenv(name) == "1" {
			t.Skip("uses the ordinary CRC authentication fixture")
		}
	}
	options := integrationOptions(t)
	options.ConnectionMode = cephmsgr.CRCMode
	observer := &crcOfferObserver{}
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		return observer.wrap(conn, endpoint), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("explicit CRC MON authentication", err)
	}
	defer c.Close()
	for _, target := range []struct {
		name string
		call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		json string
	}{
		{"MON", c.MonCommand, `{"prefix":"status","format":"json"}`},
		{"MGR", c.MgrCommand, `{"prefix":"pg stat","format":"json"}`},
	} {
		result, err := target.call(ctx, cephmsgr.Command{JSON: []byte(target.json)})
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatal(target.name, "CRC raw JSON reply", result.Code, err)
		}
		result, err = target.call(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_crc_command__"}`)})
		var server *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || result.Code != -22 || result.Message == "" || server.Code != result.Code || server.Message != result.Message {
			t.Fatal(target.name, "CRC raw server rejection", result.Code, err)
		}
	}
	state := c.Snapshot()
	if len(state.Monitor.Members) < 2 {
		t.Fatal("CRC fixture did not publish independent named MON targets")
	}
	versionCommand := cephmsgr.Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	for _, target := range []struct {
		name string
		call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
	}{
		{"MON Tell", c.MonTell},
		{"MGR Tell", c.MgrTell},
		{"named MON Tell", func(ctx context.Context, command cephmsgr.Command) (cephmsgr.Result, error) {
			return c.MonTellTo(ctx, state.Monitor.Members[1].Name, command)
		}},
	} {
		result, err := target.call(ctx, versionCommand)
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatal(target.name, "CRC daemon-local reply", result.Code, err)
		}
	}
	payload := []byte{0, 255, 13, 10, 1, 0}
	key := fmt.Sprintf("native-crc-raw-%d", time.Now().UnixNano())
	command := func(prefix string) cephmsgr.Command {
		encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "key": key})
		return cephmsgr.Command{JSON: encoded}
	}
	write := command("config-key set")
	write.Input = payload
	if _, err := c.MonCommand(ctx, write); err != nil {
		t.Fatal("CRC raw binary input", err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := c.MonCommand(cleanup, command("config-key del")); err != nil {
			t.Error("CRC binary fixture cleanup", err)
		}
	}()
	result, err := c.MonCommand(ctx, command("config-key get"))
	if err != nil || result.Code != 0 || !bytes.Equal(result.Data, payload) {
		t.Fatal("CRC changed raw binary output", result.Code, len(result.Data), err)
	}
	state = c.Snapshot()
	if !state.Monitor.Ready || !state.Manager.Ready || state.AuthRejection != nil {
		t.Fatal("CRC commands lost authenticated MON/MGR readiness", state)
	}
	connections, err := observer.check(state.Monitor.Endpoint, state.Manager.Endpoint)
	if err != nil {
		t.Fatal("actual CRC authentication offer", err)
	}
	// Secure remains the default and cannot fall back against these listeners.
	secure := integrationOptions(t)
	secure.ConnectionMode = cephmsgr.SecureMode
	rejected, err := cephmsgr.Dial(ctx, secure)
	if rejected != nil {
		rejected.Close()
		t.Fatal("secure client silently accepted the CRC-only MON")
	}
	if !matchesAuthenticationRejection(err, 2, unsupportedConnectionModeCode) {
		t.Fatal("secure client did not preserve CRC-only MON rejection", err)
	}
	t.Logf("%d actual MON/MGR connections offered CephX with CRC mode [1] alone; JSON, binary output and server errors preserved; secure selection rejected", connections)
}
