package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type monitorPeerAddressConn struct {
	net.Conn
	remote net.Addr
}

func (c monitorPeerAddressConn) RemoteAddr() net.Addr { return c.remote }

type monitorNumericAddress string

func (a monitorNumericAddress) Network() string { return "tcp" }
func (a monitorNumericAddress) String() string  { return string(a) }

func TestMonitorTargetInferenceUsesTCPAddressFamily(t *testing.T) {
	for _, tc := range []struct {
		name, seed, endpoint, target string
		remote                       net.Addr
	}{
		{"four-byte IPv4", "v2:monitor.example:3300/7", "monitor.example:3300", "192.0.2.1:3300", &net.TCPAddr{IP: net.IP{192, 0, 2, 1}, Port: 3300}},
		{"ParseIP IPv4", "v2:monitor.example:3300/7", "monitor.example:3300", "192.0.2.1:3300", &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 3300}},
		{"physical IPv6", "v2:monitor.example:3300/7", "monitor.example:3300", "[2001:db8::1]:3300", &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 3300}},
		{"generic IPv4", "v2:monitor.example:3300/7", "monitor.example:3300", "192.0.2.1:3300", monitorNumericAddress("192.0.2.1:3300")},
		{"generic IPv6", "v2:monitor.example:3300/7", "monitor.example:3300", "[2001:db8::1]:3300", monitorNumericAddress("[2001:db8::1]:3300")},
		{"generic mapped IPv6", "v2:monitor.example:3300/7", "monitor.example:3300", "[::ffff:192.0.2.1]:3300", monitorNumericAddress("[::ffff:192.0.2.1]:3300")},
		{"authoritative mapped literal", "v2:[::ffff:192.0.2.1]:3300/7", "[::ffff:192.0.2.1]:3300", "[::ffff:192.0.2.1]:3300", &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 3300}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort(tc.target)}
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = []string{tc.seed}
			identified := make(chan msgr.Address, 1)
			options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if network != "tcp" || endpoint != tc.endpoint {
					return nil, errors.New("monitor seed routing changed")
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				client, peer := net.Pipe()
				finished := make(chan error, 1)
				t.Cleanup(func() {
					client.Close()
					peer.Close()
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("monitor peer did not stop after cleanup")
					}
				})
				go func() {
					finished <- mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, addresses: []msgr.Address{want}, monMap: mockMonMapAddresses(20, [16]byte{1}, []msgr.Address{want}), ident: func(target msgr.Address) {
						select {
						case identified <- target:
						default:
						}
					}})
				}()
				return monitorPeerAddressConn{Conn: client, remote: tc.remote}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			select {
			case target := <-identified:
				if target != want {
					t.Errorf("CLIENT_IDENT used the wrong monitor family: got %+v, want %+v", target, want)
				}
			default:
				t.Fatal("monitor peer did not receive CLIENT_IDENT", err)
			}
			if err != nil {
				t.Fatal("matching monitor SERVER_IDENT was rejected", err)
			}
			t.Cleanup(func() { c.Close() })
			input := []byte{0, 255, '\r', '\n'}
			result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: input})
			if err != nil || !bytes.Equal(result.Data, input) || result.Message != "status text" {
				t.Fatal("inferred monitor lost its raw command response", result, err)
			}
		})
	}
}

type monitorInferenceCloseProbe struct {
	monitorPeerAddressConn
	reads, writes                      atomic.Int32
	closeStarted, closeDone, closeGate chan struct{}
	closeOnce                          sync.Once
}

func (c *monitorInferenceCloseProbe) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}
func (c *monitorInferenceCloseProbe) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}
func (c *monitorInferenceCloseProbe) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closeStarted)
		<-c.closeGate
		err = c.Conn.Close()
		close(c.closeDone)
	})
	return err
}

func TestMonitorTargetInferenceRejectsUnknownPeerBeforeHandshake(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remote net.Addr
	}{
		{"hostname", monitorNumericAddress("monitor.example:3300")},
		{"opaque", monitorNumericAddress("pipe")},
		{"empty", monitorNumericAddress("")},
		{"unknown", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, peer := net.Pipe()
			probe := &monitorInferenceCloseProbe{monitorPeerAddressConn: monitorPeerAddressConn{Conn: client, remote: tc.remote}, closeStarted: make(chan struct{}), closeDone: make(chan struct{}), closeGate: make(chan struct{})}
			var releaseOnce sync.Once
			releaseClose := func() { releaseOnce.Do(func() { close(probe.closeGate) }) }
			finished := make(chan struct{})
			t.Cleanup(func() {
				releaseClose()
				client.Close()
				peer.Close()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("monitor setup did not stop after cleanup")
				}
			})
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = []string{"monitor.example:3300"}
			var dials atomic.Int32
			options.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return probe, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			returned := make(chan error, 1)
			go func() {
				c, err := Dial(ctx, options)
				if c != nil {
					c.Close()
				}
				returned <- err
				close(finished)
			}()
			select {
			case <-probe.closeStarted:
			case <-ctx.Done():
				t.Fatal("unknown peer address did not close its connection", ctx.Err())
			}
			if probe.reads.Load() != 0 || probe.writes.Load() != 0 {
				t.Fatal("unknown peer address started a Messenger handshake", probe.reads.Load(), probe.writes.Load())
			}
			select {
			case err := <-returned:
				t.Fatal("monitor setup returned before connection Close completed", err)
			default:
			}
			releaseClose()
			select {
			case err := <-returned:
				if err == nil || !strings.Contains(err.Error(), "dialer returned no peer IP address") || retryableSetup(err) || dials.Load() != 1 {
					t.Fatal("unknown peer address lost its local setup failure", err, dials.Load())
				}
			case <-ctx.Done():
				t.Fatal("unknown peer address did not finish setup rejection", ctx.Err())
			}
			select {
			case <-probe.closeDone:
			default:
				t.Fatal("setup rejection did not wait for connection Close")
			}
		})
	}
}
