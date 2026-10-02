package integration_test

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephHostnameSeedIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" || os.Getenv("CEPH_MSGR_TEST_PROXY") != "" {
		t.Skip("requires container-local disposable Ceph and the default Go DNS dialer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = []string{"v2:localhost:33300/0"}
	options.DialContext = nil
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate hostname seed with the resolved peer address", err)
	}
	defer c.Close()
	for _, target := range []struct {
		call   func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		prefix string
	}{{c.MonCommand, "status"}, {c.MgrCommand, "pg stat"}} {
		encoded, _ := json.Marshal(map[string]string{"prefix": target.prefix, "format": "json"})
		result, err := target.call(ctx, cephmsgr.Command{JSON: encoded})
		if err != nil || !json.Valid(result.Data) {
			t.Fatal("command after hostname bootstrap", target.prefix, err)
		}
	}
	peer := c.Snapshot().Monitor.Endpoint
	t.Logf("hostname seed resolved to %s; authenticated MON/MGR commands passed", peer)
}

type genericHostnameAddr string

func (a genericHostnameAddr) Network() string { return "tcp" }
func (a genericHostnameAddr) String() string  { return string(a) }

type genericHostnameConn struct {
	net.Conn
	peer net.Addr
}

func (c *genericHostnameConn) RemoteAddr() net.Addr { return c.peer }

func TestCephGenericHostnameSeedIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires the disposable Ceph fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	endpoint, nonce, _ := strings.Cut(strings.TrimPrefix(options.Monitors[0], "v2:"), "/")
	peer, err := netip.ParseAddrPort(endpoint)
	if err != nil {
		t.Fatal("fixture requires a numeric MON seed", err)
	}
	alias := net.JoinHostPort("ceph-msgr-fixture.invalid", strconv.Itoa(int(peer.Port())))
	options.Monitors = []string{"v2:" + alias}
	if nonce != "" {
		options.Monitors[0] += "/" + nonce
	}
	dial := options.DialContext
	var routed atomic.Uint32
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == alias {
			address = endpoint
			routed.Add(1)
		}
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		// The fixture relay supplies the logical Ceph peer, rather than its
		// physical proxy endpoint. Preserve it in the generic net.Addr form.
		return &genericHostnameConn{Conn: conn, peer: genericHostnameAddr(conn.RemoteAddr().String())}, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate hostname seed through generic peer address", err)
	}
	t.Cleanup(func() { c.Close() })
	for _, target := range []struct {
		call   func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		prefix string
	}{{c.MonCommand, "status"}, {c.MgrCommand, "pg stat"}} {
		encoded, _ := json.Marshal(map[string]string{"prefix": target.prefix, "format": "json"})
		result, err := target.call(ctx, cephmsgr.Command{JSON: encoded})
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatalf("%s through generic peer: code=%d dataBytes=%d err=%q", target.prefix, result.Code, len(result.Data), err)
		}
	}
	state := c.Snapshot()
	if routed.Load() == 0 || !state.Monitor.Ready || state.Monitor.Endpoint != peer.String() || state.GlobalID == 0 || state.AuthRejection != nil {
		t.Fatalf("generic hostname did not authenticate the fixture MON: routed=%d endpoint=%q ready=%v globalID=%d rejection=%v", routed.Load(), state.Monitor.Endpoint, state.Monitor.Ready, state.GlobalID, state.AuthRejection)
	}
	if err := c.Close(); err != nil || !c.Snapshot().Closed {
		t.Fatal("close client with generic peer addresses", err)
	}
	t.Logf("custom hostname seed authenticated %s; raw MON/MGR JSON and Close passed", state.Monitor.Endpoint)
}
