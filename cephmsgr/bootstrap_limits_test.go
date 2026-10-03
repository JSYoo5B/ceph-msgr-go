package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestBootstrapLimitRefusalStopsBeforeAuthentication(t *testing.T) {
	for _, limit := range []uint32{64 << 10, 2 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = []string{"127.0.0.1:3300"}
			options.MaxFrameSize = limit
			options.ConnectTimeout = 2 * time.Second
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			var dials atomic.Uint32
			options.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return client, nil
			}
			type observation struct {
				authSent bool
				err      error
			}
			done := make(chan observation, 1)
			go func() {
				var observed observation
				defer func() { peer.Close(); done <- observed }()
				if observed.err = peer.SetDeadline(time.Now().Add(4 * time.Second)); observed.err != nil {
					return
				}
				banner := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
				if _, observed.err = msgr.ReadBanner(peer, banner); observed.err != nil {
					return
				}
				if observed.err = msgr.WriteFull(peer, banner.Encode()); observed.err != nil {
					return
				}
				r := msgr.NewReader(peer, 2<<20)
				frame, err := r.Read()
				if err != nil || frame.Tag != msgr.Hello {
					observed.err = fmt.Errorf("expected client HELLO, got tag %d: %v", frame.Tag, err)
					return
				}
				hello := wire.Encoder{}
				hello.U8(1)
				msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
				// Valid HELLO fields with an extended address envelope. This
				// exceeds the fixed transcript bound but fits the larger frame
				// bound, so both public error paths have distinct causes.
				extra := bytes.Repeat([]byte{0x47}, 1<<20)
				size := binary.LittleEndian.Uint32(hello.Data[4:8])
				binary.LittleEndian.PutUint32(hello.Data[4:8], size+uint32(len(extra)))
				hello.Raw(extra)
				// Rejection closes the peer while it writes the remaining body.
				_ = msgr.NewWriter(peer, 2<<20).Write(msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{hello.Data}})
				frame, err = r.Read()
				observed.authSent = err == nil && frame.Tag == msgr.AuthRequest
			}()
			c, err := Dial(ctx, options)
			if c != nil {
				c.Close()
				t.Fatal("limit refusal returned a client")
			}
			var unknown *OutcomeUnknownError
			if !errors.Is(err, ErrLimitExceeded) || errors.Is(err, ErrMalformedMessage) || errors.As(err, &unknown) || !strings.Contains(err.Error(), "hello") {
				t.Fatal("bootstrap limit lost its known pre-authentication classification", err)
			}
			select {
			case observed := <-done:
				if observed.err != nil || observed.authSent || dials.Load() != 1 {
					t.Fatal("limit refusal retried or sent authentication", observed.err, observed.authSent, dials.Load())
				}
			case <-ctx.Done():
				t.Fatal("rejected handshake did not release its peer", ctx.Err())
			}
		})
	}
}
