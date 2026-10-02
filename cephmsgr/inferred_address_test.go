package cephmsgr

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type monitorTCPAddressConn struct {
	net.Conn
	remote *net.TCPAddr
}

func (c monitorTCPAddressConn) RemoteAddr() net.Addr { return c.remote }

func TestMonitorTargetInferenceUsesTCPAddressFamily(t *testing.T) {
	for _, tc := range []struct {
		name, seed, endpoint, target string
		remoteIP                     net.IP
	}{
		{"four-byte IPv4", "v2:monitor.example:3300/7", "monitor.example:3300", "192.0.2.1:3300", net.IP{192, 0, 2, 1}},
		{"ParseIP IPv4", "v2:monitor.example:3300/7", "monitor.example:3300", "192.0.2.1:3300", net.ParseIP("192.0.2.1")},
		{"physical IPv6", "v2:monitor.example:3300/7", "monitor.example:3300", "[2001:db8::1]:3300", net.ParseIP("2001:db8::1")},
		{"authoritative mapped literal", "v2:[::ffff:192.0.2.1]:3300/7", "[::ffff:192.0.2.1]:3300", "[::ffff:192.0.2.1]:3300", net.ParseIP("192.0.2.1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort(tc.target)}
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = []string{tc.seed}
			identified := make(chan msgr.Address, 1)
			options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if network != "tcp" || endpoint != tc.endpoint {
					t.Fatal("monitor seed routing changed", network, endpoint)
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
				return monitorTCPAddressConn{Conn: client, remote: &net.TCPAddr{IP: tc.remoteIP, Port: 3300}}, nil
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
