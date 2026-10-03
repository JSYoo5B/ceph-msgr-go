package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const adminDescriptionsExample = `{"version":{"sig":["version"],"help":"daemon version","future_metadata":{"supported":true}}}`

func TestTellDescriptionsRoutesAndAdminMetadata(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		t.Run(fmt.Sprint(mgr), func(t *testing.T) {
			var denied atomic.Bool
			f := newTellFixture(t, [16]byte{1}, func(m *msgr.MessageData) {
				e := wire.Encoder{}
				code := int32(0)
				m.Data = []byte(adminDescriptionsExample)
				if denied.Load() {
					code, m.Data = -13, []byte("raw admin refusal\n")
				}
				e.U32(uint32(code))
				e.String("admin status")
				m.Front = e.Data
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			call, role := c.MonTellDescriptions, uint8(1)
			if mgr {
				call, role = c.MgrTellDescriptions, 16
			}
			for _, reject := range []bool{false, true, false} {
				denied.Store(reject)
				response, err := call(ctx)
				request := nextTellRequest(t, ctx, f)
				if request.role != role || request.data.Type != msgr.TellCommandMessage || len(request.data.Data) != 0 || !bytes.Contains(request.data.Front, []byte(`{"prefix":"get_command_descriptions","format":"json"}`)) {
					t.Fatal("admin schema used management route or extra input", request)
				}
				if response.Result.Message != "admin status" {
					t.Fatal("admin status lost", response.Result)
				}
				if reject {
					var server *CommandError
					if !errors.As(err, &server) || server.Code != -13 || response.Result.Code != -13 || errors.Is(err, ErrInvalidCommandDescriptions) || response.Commands != nil || !bytes.Equal(response.Result.Data, []byte("raw admin refusal\n")) {
						t.Fatal("admin refusal was decoded or lost raw output", response.Result, err)
					}
					continue
				}
				if err != nil || response.Result.Code != 0 || !bytes.Equal(response.Result.Data, []byte(adminDescriptionsExample)) || len(response.Commands) != 1 {
					t.Fatal("admin catalog failed", response.Result, err)
				}
				d := response.Commands[0]
				if d.ID != "version" || d.Prefix != "version" || d.Help != "daemon version" || d.Module != "" || d.Permission != "" || d.Flags != 0 {
					t.Fatal("admin-only metadata did not retain its defaults", d)
				}
				var raw map[string]json.RawMessage
				if err := json.Unmarshal(d.Raw, &raw); err != nil || string(raw["future_metadata"]) != `{"supported":true}` {
					t.Fatal("unknown admin metadata lost", err)
				}
			}
			wantManagers := uint32(0)
			if mgr {
				wantManagers = 1
			}
			if f.calls.Load() != 3 || f.monDial.Load() != 1 || f.mgrDial.Load() != wantManagers {
				t.Fatal("admin lookup cached, repeated or reopened its route", f.calls.Load())
			}
		})
	}
}

func TestNamedMonTellDescriptionsUsesIndependentAdmission(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, func(p *namedTestPeer) {
		p.reply = func(m *msgr.MessageData) { m.Data = []byte(adminDescriptionsExample) }
	})
	before := f.c.snapshotAuth()
	response, err := f.c.MonTellToDescriptions(f.ctx, "b")
	if err != nil || response.Result.Message != "named status" || len(response.Commands) != 1 || response.Commands[0].Prefix != "version" {
		t.Fatal("named admin catalog failed", response.Result, err)
	}
	p := namedTestTarget(t, f)
	p.next(t, f.ctx, msgr.MonGetMapMessage)
	request := p.next(t, f.ctx, msgr.TellCommandMessage)
	if !bytes.Contains(request.Front, []byte(`{"prefix":"get_command_descriptions","format":"json"}`)) || len(request.Data) != 0 || p.tellCount.Load() != 1 || f.namedDials.Load() != 1 || f.monDials.Load() != 1 || f.mgrDials.Load() != 0 {
		t.Fatal("named admin lookup used wrong target or route", request)
	}
	if !reflect.DeepEqual(before, f.c.snapshotAuth()) {
		t.Fatal("named schema lookup changed primary credentials")
	}
	select {
	case <-p.done:
	case <-f.ctx.Done():
		t.Fatal("named schema lookup did not finish connection cleanup", f.ctx.Err())
	}
	namedTestPrimaryHealthy(t, f)
}
