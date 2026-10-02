package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestBootstrapRejectsCompleteMalformedPayloadWithoutRetry(t *testing.T) {
	for _, test := range []struct {
		name   string
		tag    msgr.Tag
		monMap bool
	}{
		{name: "HELLO", tag: msgr.Hello},
		{name: "SERVER_IDENT", tag: msgr.ServerIdent},
		{name: "MonMap", monMap: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			var attempts atomic.Int32
			options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				client, peer := net.Pipe()
				cfg := peerConfig{fsid: [16]byte{1}, release: 20, role: 1}
				if attempts.Add(1) == 1 {
					if test.monMap {
						cfg.monMap = []byte{0} // Complete message; missing the blob length.
					} else {
						cfg.payload = func(tag msgr.Tag, payload []byte) []byte {
							if tag == test.tag {
								return payload[:len(payload)-1]
							}
							return payload
						}
					}
				}
				go mockDaemon(peer, cfg)
				return client, ctx.Err()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if c != nil {
				c.Close()
			}
			if !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) || attempts.Load() != 1 {
				t.Fatal("malformed complete payload was retried as transport loss", attempts.Load(), err)
			}
		})
	}
}

func TestCompleteMalformedMapsRetainProtocolAndDecodeCauses(t *testing.T) {
	for _, typ := range []uint16{msgr.MonMapMessage, msgr.MgrMapMessage} {
		c := &Client{changed: make(chan struct{}), mon: new(session.Session)}
		_, err := c.handleMap(c.mon, msgr.MessageData{Type: typ, Front: []byte{0}})
		if !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("map %d decoder truncation was classified as transport loss: %v", typ, err)
		}
	}
}

func TestUnsupportedMonitorReleaseRemainsPolicyFailure(t *testing.T) {
	c := &Client{changed: make(chan struct{}), mon: new(session.Session)}
	_, err := c.handleMap(c.mon, msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(19, [16]byte{1})})
	if !errors.Is(err, maps.ErrRelease) || errors.Is(err, msgr.ErrFrame) || retryableSetup(err) {
		t.Fatal("valid older-release map was relabeled as corruption or transient loss", err)
	}
}

func TestBootstrapRetriesPartiallyTransmittedHandshakeFrame(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	validDial := options.DialContext
	var attempts atomic.Int32
	peerDone := make(chan error, 1)
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if attempts.Add(1) > 1 {
			return validDial(ctx, network, endpoint)
		}
		client, peer := net.Pipe()
		go func() {
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(2 * time.Second))
			banner := msgr.Banner{Supported: 3, Required: 1}
			if _, err := msgr.ReadBanner(peer, banner); err != nil {
				peerDone <- err
				return
			}
			if err := msgr.WriteFull(peer, banner.Encode()); err != nil {
				peerDone <- err
				return
			}
			if _, err := msgr.NewReader(peer, 0).Read(); err != nil {
				peerDone <- err
				return
			}
			hello := wire.Encoder{}
			hello.U8(1)
			msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
			var frame bytes.Buffer
			if err := msgr.NewWriter(&frame, 0).Write(msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{hello.Data}}); err != nil {
				peerDone <- err
				return
			}
			// Preserve the valid declared length; close before the last byte of
			// the frame arrives. This is a real network read truncation.
			peerDone <- msgr.WriteFull(peer, frame.Bytes()[:frame.Len()-1])
		}()
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("partial network frame was not retried", err)
	}
	defer c.Close()
	if err := <-peerDone; err != nil || attempts.Load() != 2 {
		t.Fatal("partial network loss did not lead to exactly one retry", attempts.Load(), err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("recovered bootstrap session unusable", err)
	}
}
