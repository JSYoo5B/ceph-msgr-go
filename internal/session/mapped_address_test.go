package session

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestHandshakePreservesMappedIPv6TargetFamily(t *testing.T) {
	// Address vectors use independent Linux sockaddr bytes instead of the
	// product address encoder. Family alone distinguishes IPv4 from mapped
	// AF_INET6 in Tentacle's exact CLIENT_IDENT/SERVER_IDENT comparisons.
	const ipv4 = "02010000000101011c00000002000000070000001000000002000ce4c00002010000000000000000"
	const mapped = "02010000000101012800000002000000070000001c0000000a000ce40000000000000000000000000000ffffc000020100000000"
	const mappedMetadata = "02010000000101012800000002000000070000001c0000000a000ce41234567800000000000000000000ffffc000020103000000"
	for _, tc := range []struct {
		name, endpoint, serverWire string
		flow, scope                uint32
		accepted                   bool
	}{
		{"native IPv4", "192.0.2.1:3300", ipv4, 0, 0, true},
		{"mapped IPv6", "[::ffff:192.0.2.1]:3300", mapped, 0, 0, true},
		{"mapped IPv6 flow and scope", "[::ffff:192.0.2.1]:3300", mappedMetadata, 0x12345678, 3, true},
		{"IPv4 server differs from mapped target", "[::ffff:192.0.2.1]:3300", ipv4, 0, 0, false},
		{"mapped server differs from IPv4 target", "192.0.2.1:3300", mapped, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverWire, err := hex.DecodeString(tc.serverWire)
			if err != nil {
				t.Fatal(err)
			}
			target := msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort(tc.endpoint), FlowInfo: tc.flow, ScopeID: tc.scope}
			client, peer := net.Pipe()
			t.Cleanup(func() { client.Close(); peer.Close() })
			auth := fixtureAuthData()
			identified, finished := make(chan msgr.Address, 1), make(chan error, 1)
			go func() {
				finished <- handshakePeer(peer, auth, handshakePeerConfig{mode: 2, serverFlags: 1, serverAddrWire: serverWire, serverAddresses: func(actual msgr.Address) []msgr.Address {
					identified <- actual
					return nil
				}})
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			transport, err := Handshake(ctx, client, target, 1, 0, auth, 4096, time.Second)
			if transport != nil {
				transport.Conn.Close()
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("handshake peer did not stop", ctx.Err())
			}
			select {
			case actual := <-identified:
				if actual != target {
					t.Errorf("CLIENT_IDENT changed target family or metadata: got %+v, want %+v", actual, target)
				}
			default:
				t.Fatal("peer did not receive CLIENT_IDENT", err)
			}
			if tc.accepted {
				if err != nil {
					t.Fatal("matching SERVER_IDENT was rejected", err)
				}
			} else if !errors.Is(err, msgr.ErrAuthentication) {
				t.Fatal("different address family was accepted or misclassified", err)
			}
		})
	}
}
