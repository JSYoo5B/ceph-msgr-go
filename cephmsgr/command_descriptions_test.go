package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These entries were observed in a native Tentacle MON response. The command
// transport must preserve them as raw bytes without interpreting their schema.
const descriptionExamples = `{
 "cmd947":{"sig":["config","set",{"name":"who","type":"CephString"},{"name":"name","type":"CephString"},{"name":"value","type":"CephString"},{"name":"force","req":"false","type":"CephBool"}],"help":"Set a configuration option for one or more entities","module":"config","perm":"rw","flags":0},
 "cmd352":{"sig":["dashboard","create-self-signed-cert"],"help":"","module":"mgr","perm":"w","flags":8},
 "cmd107":{"sig":["dashboard","create-self-signed-cert"],"help":"Create self signed certificate","module":"mgr","perm":"w","flags":8}
}`

func TestRawCommandCatalogRoutesAndResults(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		t.Run(fmt.Sprint(mgr), func(t *testing.T) {
			var mode atomic.Uint32
			f := newTellFixture(t, [16]byte{1}, func(m *msgr.MessageData) {
				e := wire.Encoder{}
				if m.Type == msgr.MonCommandReplyMessage {
					msgr.Paxos(&e)
				}
				code := int32(0)
				if mode.Load() == 1 {
					code = -13
				}
				e.U32(uint32(code))
				e.String("catalog status")
				if m.Type == msgr.MonCommandReplyMessage {
					e.U32(0)
				}
				m.Front = e.Data
				m.Data = []byte(descriptionExamples)
				if mode.Load() != 0 {
					m.Data = []byte("non-JSON server output\n")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			call, role, typ := c.MonCommand, uint8(1), msgr.MonCommandMessage
			if mgr {
				call, role, typ = c.MgrCommand, 16, msgr.MgrCommandMessage
			}
			command := Command{JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`)}
			for step := uint32(0); step < 4; step++ {
				mode.Store(step % 3)
				response, err := call(ctx, command)
				request := nextTellRequest(t, ctx, f)
				if request.role != role || request.data.Type != typ || len(request.data.Data) != 0 || request.data.Transaction == 0 || !bytes.Contains(request.data.Front, []byte(`{"prefix":"get_command_descriptions","format":"json"}`)) {
					t.Fatal("discovery used wrong route or request", request)
				}
				if response.Message != "catalog status" {
					t.Fatal("raw status lost", response)
				}
				var server *CommandError
				var unknown *OutcomeUnknownError
				if errors.As(err, &unknown) || errors.Is(err, ErrMalformedMessage) {
					t.Fatal("known reply became a protocol failure or uncertain outcome", err)
				}
				switch mode.Load() {
				case 0:
					if err != nil || response.Code != 0 || !bytes.Equal(response.Data, []byte(descriptionExamples)) {
						t.Fatal("successful catalog lost raw result", response, err)
					}
				case 1:
					if !errors.As(err, &server) || server.Code != -13 || response.Code != -13 || !bytes.Equal(response.Data, []byte("non-JSON server output\n")) {
						t.Fatal("server refusal was decoded or changed", response, err)
					}
				case 2:
					if err != nil || response.Code != 0 || !bytes.Equal(response.Data, []byte("non-JSON server output\n")) {
						t.Fatal("transport interpreted successful command output as JSON", response, err)
					}
				}
			}
			mode.Store(0)
			if _, err := call(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("raw catalog replies retired healthy session", err)
			}
			nextTellRequest(t, ctx, f)
			stopped, stop := context.WithCancel(ctx)
			stop()
			response, err := call(stopped, command)
			if !errors.Is(err, context.Canceled) || response.Data != nil || f.calls.Load() != 5 {
				t.Fatal("already canceled discovery sent a command", err, f.calls.Load())
			}
			wantManagers := uint32(0)
			if mgr {
				wantManagers = 1
			}
			if f.monDial.Load() != 1 || f.mgrDial.Load() != wantManagers {
				t.Fatal("raw catalog replies reopened shared connections", f.monDial.Load(), f.mgrDial.Load())
			}
		})
	}
}
