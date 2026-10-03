package cephmsgr

import (
	"bytes"
	"context"
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

func TestRawTellCatalogRoutesAndResults(t *testing.T) {
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
			call, role := c.MonTell, uint8(1)
			if mgr {
				call, role = c.MgrTell, 16
			}
			command := Command{JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`)}
			for _, reject := range []bool{false, true, false} {
				denied.Store(reject)
				response, err := call(ctx, command)
				request := nextTellRequest(t, ctx, f)
				if request.role != role || request.data.Type != msgr.TellCommandMessage || len(request.data.Data) != 0 || !bytes.Contains(request.data.Front, []byte(`{"prefix":"get_command_descriptions","format":"json"}`)) {
					t.Fatal("admin schema used management route or extra input", request)
				}
				if response.Message != "admin status" {
					t.Fatal("admin status lost", response)
				}
				if reject {
					var server *CommandError
					if !errors.As(err, &server) || server.Code != -13 || response.Code != -13 || !bytes.Equal(response.Data, []byte("raw admin refusal\n")) {
						t.Fatal("admin refusal was decoded or lost raw output", response, err)
					}
					continue
				}
				if err != nil || response.Code != 0 || !bytes.Equal(response.Data, []byte(adminDescriptionsExample)) {
					t.Fatal("raw admin catalog failed", response, err)
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

func TestNamedMonTellCatalogUsesIndependentAdmission(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, func(p *namedTestPeer) {
		p.reply = func(m *msgr.MessageData) { m.Data = []byte(adminDescriptionsExample) }
	})
	before := f.c.snapshotAuth()
	response, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`)})
	if err != nil || response.Message != "named status" || !bytes.Equal(response.Data, []byte(adminDescriptionsExample)) {
		t.Fatal("named raw admin catalog failed", response, err)
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
