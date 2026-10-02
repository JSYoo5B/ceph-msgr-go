package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestMalformedCommandReplyPreservesOutputAndUnknownOutcome(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			var corrupt atomic.Bool
			corrupt.Store(true)
			var commands atomic.Int32
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				client, peer := net.Pipe()
				role, id := uint8(1), uint64(0)
				if strings.HasSuffix(endpoint, ":6800") {
					role, id = 16, 99
				}
				go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: role, id: id, reply: func(m *msgr.MessageData) {
					commands.Add(1)
					if corrupt.Swap(false) {
						m.Front = m.Front[:len(m.Front)-1]
					}
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
			call := c.MonCommand
			if mgr {
				call = c.MgrCommand
			}
			input := []byte{0, 255, 1, 0}
			result, err := call(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
			var unknown *OutcomeUnknownError
			var serverError *CommandError
			if !errors.As(err, &unknown) || !errors.Is(err, msgr.ErrFrame) || errors.As(err, &serverError) || !bytes.Equal(result.Data, input) {
				t.Fatal("malformed reply lost outcome or raw output", result, err)
			}
			if _, err := call(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("new call did not recover", err)
			}
			if commands.Load() != 2 {
				t.Fatal("uncertain command was replayed", commands.Load())
			}
		})
	}
}
