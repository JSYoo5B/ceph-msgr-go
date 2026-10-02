package cephmsgr

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestRecoveryReadOracleRejectsMalformedReply(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			fsid := [16]byte{1}
			options := mockOptions(t, 20, fsid)
			var commands atomic.Uint32
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				client, peer := net.Pipe()
				role, id := uint8(1), uint64(0)
				if strings.HasSuffix(endpoint, ":6800") {
					role, id = 16, 99
				}
				go mockDaemon(peer, peerConfig{fsid: fsid, release: 20, role: role, id: id, reply: func(m *msgr.MessageData) {
					commands.Add(1)
					m.Front = m.Front[:len(m.Front)-1]
				}})
				return client, ctx.Err()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			read, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			defer stop()
			err = fixtureRecoveryRead(read, c, mgr)
			var unknown *OutcomeUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) || commands.Load() != 1 {
				t.Fatal("recovery oracle hid a malformed response by retrying it", commands.Load(), err)
			}
		})
	}
}
