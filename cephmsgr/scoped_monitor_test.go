package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestLearnedMonitorRecoveryPreservesWireAddress(t *testing.T) {
	for _, test := range []struct {
		name      string
		scope     uint32
		flow      uint32
		staleFlow bool
	}{
		{name: "scope", scope: 3},
		{name: "flow", flow: 0x12345678},
		{name: "scope_and_flow", scope: 3, flow: 0x12345678},
		{name: "same_endpoint_distinct_flow", flow: 0x12345678, staleFlow: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Transport routing and wire identity are separate: a custom dialer
			// avoids depending on the test host's interface names or scope IDs.
			address := msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("[fe80::1]:3300"), ScopeID: test.scope, FlowInfo: test.flow}
			learned := []msgr.Address{address}
			if test.staleFlow {
				stale := address
				stale.FlowInfo--
				learned = append([]msgr.Address{stale}, learned...)
			}
			fsid := [16]byte{1}
			options := mockOptions(t, 20, fsid)
			identified := make(chan msgr.Address, len(learned))
			var seedDials, learnedDials atomic.Int32
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				cfg := peerConfig{fsid: fsid, release: 20, role: 1, monMap: mockMonMapAddresses(20, fsid, learned)}
				if endpoint == options.Monitors[0] {
					if seedDials.Add(1) > 1 {
						return nil, errors.New("bootstrap monitor unavailable")
					}
				} else if endpoint == dialAddress(address) {
					learnedDials.Add(1)
					cfg.addresses = []msgr.Address{address}
					cfg.ident = func(target msgr.Address) {
						select {
						case identified <- target:
						default:
						}
					}
				} else {
					return nil, errors.New("unexpected monitor endpoint")
				}
				client, peer := net.Pipe()
				go mockDaemon(peer, cfg)
				return client, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.mu.Lock()
			old := c.mon
			c.mu.Unlock()
			old.Fail(errors.New("bootstrap monitor disconnected"))
			for _, expected := range learned {
				select {
				case target := <-identified:
					if target != expected {
						t.Fatalf("CLIENT_IDENT lost learned wire identity: got %+v, want %+v", target, expected)
					}
				case <-ctx.Done():
					t.Fatal("learned monitor identity was not attempted", ctx.Err())
				}
			}
			if err := c.WaitMonReady(ctx); err != nil {
				t.Fatal("learned monitor identification did not complete", err)
			}
			payload := []byte{0, 255, '\r', '\n'}
			result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: payload})
			if err != nil || !bytes.Equal(result.Data, payload) {
				t.Fatal("recovered monitor did not preserve command output", result, err)
			}
			if seedDials.Load() != 2 || learnedDials.Load() != int32(len(learned)) {
				t.Fatal("recovery did not retain distinct learned identities", seedDials.Load(), learnedDials.Load())
			}
		})
	}
}
