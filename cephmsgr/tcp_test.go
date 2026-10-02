package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// This exercises the default Go TCP dialer with synthetic authenticated peers.
// The peer shares our codecs; it does not establish real-Ceph interoperability.
func TestDefaultDialerLoopbackTCP(t *testing.T) {
	for _, tc := range []struct {
		name, network, address string
	}{
		{"IPv4", "tcp4", "127.0.0.1:0"},
		{"IPv6", "tcp6", "[::1]:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen(tc.network, tc.address)
			if err != nil {
				if tc.network == "tcp6" {
					t.Skipf("IPv6 loopback listener unavailable: %v", err)
				}
				t.Fatal(err)
			}
			endpoint := listener.Addr().String()
			address := msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort(endpoint)}
			fsid := [16]byte{1, 2, 3}
			accepted := make(chan net.Conn, 1)
			peerDone := make(chan error, 1)
			commands := make(chan uint64, 2)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					peerDone <- err
					return
				}
				accepted <- conn
				peerDone <- mockDaemon(conn, peerConfig{
					fsid: fsid, release: 20, role: 1,
					addresses: []msgr.Address{address},
					monMap:    mockMonMapAddresses(20, fsid, []msgr.Address{address}),
					command: func(m msgr.MessageData) {
						if strings.Contains(string(m.Front), "block") {
							commands <- m.Transaction
						}
					},
				})
			}()
			var peer net.Conn
			peerWaited := false
			t.Cleanup(func() {
				listener.Close()
				if peer == nil {
					select {
					case peer = <-accepted:
					default:
					}
				}
				if peer != nil {
					peer.Close()
				}
				if !peerWaited {
					select {
					case <-peerDone:
					case <-time.After(3 * time.Second):
						t.Error("TCP peer did not stop after connection cleanup")
					}
				}
			})
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			key, _ := mockCredential()
			c, err := Dial(ctx, Options{
				Monitors: []string{endpoint}, Identity: "client.test", Key: key,
				ConnectTimeout: 3 * time.Second,
			}) // DialContext deliberately omitted to use net.Dialer.
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			select {
			case peer = <-accepted:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			state := c.Snapshot()
			if !state.Monitor.Ready || state.Monitor.Endpoint != endpoint || len(state.Monitor.Endpoints) != 1 || state.Monitor.Endpoints[0] != endpoint {
				t.Fatal("TCP endpoint not retained in authenticated monitor state", state)
			}
			raw := []byte(" {\"health\":\"HEALTH_OK\",\"future\":[1,2]}\n")
			command := Command{JSON: []byte(`{"prefix":"status"}`), Input: raw}
			result, err := c.MonCommand(ctx, command)
			if err != nil || result.Code != 0 || result.Message != "status text" || !bytes.Equal(result.Data, raw) {
				t.Fatal("TCP command output was not preserved", result, err)
			}
			result, err = c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"deny"}`), Input: raw})
			var rejected *CommandError
			if result.Code != -13 || result.Message != "status text" || !bytes.Equal(result.Data, raw) || !errors.As(err, &rejected) || rejected.Code != -13 {
				t.Fatal("TCP server rejection was not preserved", result, err)
			}
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			pending := make(chan error, 1)
			go func() {
				_, err := c.MonCommand(call, Command{JSON: []byte(`{"prefix":"block-cancel"}`)})
				pending <- err
			}()
			select {
			case <-commands:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cancel() // The peer has already consumed the complete command frame.
			var unknown *OutcomeUnknownError
			select {
			case err := <-pending:
				if !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
					t.Fatal("transmitted TCP cancellation lost its unknown outcome", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if result, err := c.MonCommand(ctx, command); err != nil || !bytes.Equal(result.Data, raw) {
				t.Fatal("request cancellation affected the shared TCP connection", result, err)
			}
			go func() {
				_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"block-close"}`)})
				pending <- err
			}()
			select {
			case <-commands:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			select {
			case err := <-pending:
				if !errors.Is(err, ErrClosed) || !errors.As(err, &unknown) {
					t.Fatal("Close did not release a transmitted TCP command", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Close did not join the TCP client workers", ctx.Err())
			}
			select {
			case err := <-peerDone:
				peerWaited = true
				// Close can interrupt an in-flight ACK or control frame.
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
					t.Fatal("TCP peer failed before observing client closure", err)
				}
			case <-ctx.Done():
				t.Fatal("Close did not close the peer TCP connection", ctx.Err())
			}
			if state := c.Snapshot(); !state.Closed || state.Monitor.Ready || state.Manager.Ready {
				t.Fatal("closed TCP client retained readiness", state)
			}
			if _, err := c.MonCommand(ctx, command); !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
				t.Fatal("closed TCP client admitted another command", err)
			}
		})
	}
}
