package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// The synthetic peers share our codecs. This verifies native TCP discovery and
// concurrent MON/MGR sessions, not interoperability with independent Ceph code.
func TestDefaultDialerManagerLoopbackTCP(t *testing.T) {
	for _, tc := range []struct {
		name, network, address string
	}{
		{"IPv4", "tcp4", "127.0.0.1:0"},
		{"IPv6", "tcp6", "[::1]:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var listeners [2]net.Listener
			var peers [2]net.Conn
			var addresses [2]msgr.Address
			accepted := [2]chan net.Conn{make(chan net.Conn, 1), make(chan net.Conn, 1)}
			peerDone := [2]chan error{make(chan error, 1), make(chan error, 1)}
			var started, waited [2]bool
			commands := make(chan int, 2)
			release := make(chan struct{})
			releaseReplies := sync.OnceFunc(func() { close(release) })
			t.Cleanup(func() {
				releaseReplies()
				for i, listener := range listeners {
					if listener != nil {
						listener.Close()
					}
					if peers[i] == nil {
						select {
						case peers[i] = <-accepted[i]:
						default:
						}
					}
					if peers[i] != nil {
						peers[i].Close()
					}
					if started[i] && !waited[i] {
						select {
						case <-peerDone[i]:
						case <-time.After(3 * time.Second):
							t.Errorf("TCP peer %d did not stop after cleanup", i)
						}
					}
				}
			})
			for i := range listeners {
				listener, err := net.Listen(tc.network, tc.address)
				if err != nil {
					if tc.network == "tcp6" {
						t.Skipf("IPv6 loopback listener unavailable: %v", err)
					}
					t.Fatal(err)
				}
				listeners[i] = listener
				addresses[i] = msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort(listener.Addr().String())}
			}
			fsid := [16]byte{1, 2, 3}
			mgr := wire.Encoder{}
			mgr.U32(7)
			msgr.EncodeAddresses(&mgr, addresses[1:])
			mgr.U64(99)
			mgr.U8(1)
			mgr.String("a")
			mgr.U32(0) // standby count
			mgrMap := wire.Encoder{}
			mgrMap.Struct(14, 6, mgr.Data)
			for i := range listeners {
				cfg := peerConfig{fsid: fsid, release: 20, role: 1, addresses: addresses[i : i+1]}
				if i == 0 {
					cfg.initialMaps = func(send func(msgr.MessageData) error) error {
						if err := send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAddresses(20, fsid, addresses[:1])}); err != nil {
							return err
						}
						return send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mgrMap.Data})
					}
				} else {
					cfg.role, cfg.id = 16, 99
				}
				cfg.command = func(msgr.MessageData) {
					commands <- i
					<-release
				}
				started[i] = true
				go func(index int, cfg peerConfig) {
					conn, err := listeners[index].Accept()
					if err != nil {
						peerDone[index] <- err
						return
					}
					accepted[index] <- conn
					peerDone[index] <- mockDaemon(conn, cfg)
				}(i, cfg)
			}
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			key, _ := mockCredential()
			c, err := Dial(ctx, Options{
				Monitors: []string{listeners[0].Addr().String()}, Identity: "client.test", Key: key,
				ConnectTimeout: 3 * time.Second,
			}) // DialContext deliberately omitted for both connections.
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseReplies(); c.Close() })
			if err := c.WaitMgrReady(ctx); err != nil {
				t.Fatal("default dialer did not authenticate the discovered MGR", err)
			}
			for i := range peers {
				select {
				case peers[i] = <-accepted[i]:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			state := c.Snapshot()
			if !state.Monitor.Ready || !state.Manager.Ready || state.Monitor.Endpoint != listeners[0].Addr().String() || state.Manager.Endpoint != listeners[1].Addr().String() || state.Manager.MapEpoch != 7 || state.Manager.GlobalID != 99 {
				t.Fatal("discovered TCP sessions did not retain their distinct identities", state)
			}
			type response struct {
				index  int
				result Result
				err    error
			}
			results := make(chan response, 2)
			raw := [2][]byte{[]byte(" {\"health\":\"HEALTH_OK\"}\n"), {0, 0xff, 0x7f}}
			calls := []func(context.Context, Command) (Result, error){c.MonCommand, c.MgrCommand}
			for i, call := range calls {
				go func(index int, call func(context.Context, Command) (Result, error)) {
					result, err := call(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: raw[index]})
					results <- response{index, result, err}
				}(i, call)
			}
			var received [2]bool
			for range received {
				select {
				case i := <-commands:
					if received[i] {
						t.Fatal("both concurrent commands reached the same daemon", i)
					}
					received[i] = true
				case <-ctx.Done():
					t.Fatal("MON and MGR commands did not progress concurrently", ctx.Err())
				}
			}
			releaseReplies() // Both daemons consumed their commands before either replied.
			for range received {
				select {
				case got := <-results:
					if got.err != nil || got.result.Code != 0 || got.result.Message != "status text" || !bytes.Equal(got.result.Data, raw[got.index]) {
						t.Fatal("concurrent TCP response lost its daemon or raw output", got)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Close did not join the MON/MGR TCP workers", ctx.Err())
			}
			for i := range peerDone {
				select {
				case err := <-peerDone[i]:
					waited[i] = true
					// Close can interrupt an in-flight ACK or control frame.
					if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
						t.Fatal("TCP peer failed before observing client closure", i, err)
					}
				case <-ctx.Done():
					t.Fatal("Close did not close both daemon TCP connections", i, ctx.Err())
				}
			}
			if state := c.Snapshot(); !state.Closed || state.Monitor.Ready || state.Manager.Ready {
				t.Fatal("closed TCP client retained daemon readiness", state)
			}
		})
	}
}
