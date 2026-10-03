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

// These entries were observed in a native Tentacle MON response. The repeated
// dashboard prefix is real; its two opaque IDs must not collapse into one.
const descriptionExamples = `{
 "cmd947":{"sig":["config","set",{"name":"who","type":"CephString"},{"name":"name","type":"CephString"},{"name":"value","type":"CephString"},{"name":"force","req":"false","type":"CephBool"}],"help":"Set a configuration option for one or more entities","module":"config","perm":"rw","flags":0},
 "cmd352":{"sig":["dashboard","create-self-signed-cert"],"help":"","module":"mgr","perm":"w","flags":8},
 "cmd107":{"sig":["dashboard","create-self-signed-cert"],"help":"Create self signed certificate","module":"mgr","perm":"w","flags":8}
}`

func TestCommandDescriptionsNativeExamples(t *testing.T) {
	commands, err := decodeCommandDescriptions([]byte(descriptionExamples))
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 3 || !reflect.DeepEqual([]string{commands[0].ID, commands[1].ID, commands[2].ID}, []string{"cmd107", "cmd352", "cmd947"}) {
		t.Fatal("opaque IDs were reordered or lost", commands)
	}
	if commands[0].Prefix != "dashboard create-self-signed-cert" || commands[1].Prefix != commands[0].Prefix || commands[0].Flags != CommandFlagManager {
		t.Fatal("overload or flags lost", commands)
	}
	config := commands[2]
	if config.Prefix != "config set" || config.Module != "config" || config.Permission != "rw" || len(config.Signature) != 6 {
		t.Fatal("native descriptor lost", config)
	}
	arg := config.Signature[5].Argument
	if arg == nil || arg.Name != "force" || arg.Type != "CephBool" || string(arg.Attributes["req"]) != `"false"` || string(arg.Attributes["name"]) != `"force"` {
		t.Fatal("native argument lost", arg)
	}
}

func TestCommandDescriptionsRawAttributesAndOwnership(t *testing.T) {
	data := []byte(`{"opaque":{"sig":["probe",{"name":"input","type":"FutureType","req":"fasle","n":"N","future":{"v":1}},"after"],"flags":9223372036854775840,"future_metadata":{"enabled":true}}}`)
	commands, err := decodeCommandDescriptions(data)
	if err != nil {
		t.Fatal(err)
	}
	d := commands[0]
	arg := d.Signature[1].Argument
	if d.Prefix != "probe" || d.Signature[2].Literal != "after" || d.Flags != CommandFlagHidden|CommandFlags(1<<63) || string(arg.Attributes["req"]) != `"fasle"` || string(arg.Attributes["future"]) != `{"v":1}` {
		t.Fatal("prefix, unknown flags or raw attributes changed", d)
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(d.Raw, &metadata); err != nil || string(metadata["future_metadata"]) != `{"enabled":true}` {
		t.Fatal("unknown metadata lost", err)
	}
	rawBefore := append([]byte(nil), d.Raw...)
	for i := range data {
		data[i] = 'x'
	}
	if !bytes.Equal(d.Raw, rawBefore) {
		t.Fatal("descriptor borrowed input bytes")
	}
	arg.Attributes["name"][1] = 'X'
	if !bytes.Equal(d.Raw, rawBefore) || arg.Name != "input" {
		t.Fatal("argument attributes alias descriptor or decoded strings")
	}
	d.Raw[0] = '!'
	if string(arg.Attributes["req"]) != `"fasle"` {
		t.Fatal("descriptor mutation changed argument attributes")
	}
	bools, err := decodeCommandDescriptions([]byte(`{"id":{"sig":["probe",{"name":"input","type":"CephString","req":false}]}}`))
	if err != nil || string(bools[0].Signature[1].Argument.Attributes["req"]) != "false" {
		t.Fatal("boolean req was normalized or rejected", err)
	}
}

func TestCommandDescriptionsRejectUnsupportedShapes(t *testing.T) {
	for _, data := range []string{`null`, `[]`, `{"id":{"sig":[true]}}`, `{"id":{"sig":["probe",{"name":7,"type":"CephString"}]}}`} {
		if commands, err := decodeCommandDescriptions([]byte(data)); err == nil || commands != nil {
			t.Fatal("invalid schema returned a partial catalog", data, err)
		}
	}
}

func TestCommandDescriptionsPublicRoutesAndResults(t *testing.T) {
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
			call, ordinary, role, typ := c.MonCommandDescriptions, c.MonCommand, uint8(1), msgr.MonCommandMessage
			if mgr {
				call, ordinary, role, typ = c.MgrCommandDescriptions, c.MgrCommand, 16, msgr.MgrCommandMessage
			}
			for step := uint32(0); step < 4; step++ {
				mode.Store(step % 3)
				response, err := call(ctx)
				request := nextTellRequest(t, ctx, f)
				if request.role != role || request.data.Type != typ || len(request.data.Data) != 0 || request.data.Transaction == 0 || !bytes.Contains(request.data.Front, []byte(`{"prefix":"get_command_descriptions","format":"json"}`)) {
					t.Fatal("discovery used wrong route or request", request)
				}
				if response.Result.Message != "catalog status" {
					t.Fatal("raw status lost", response.Result)
				}
				var server *CommandError
				var unknown *OutcomeUnknownError
				if errors.As(err, &unknown) || errors.Is(err, ErrMalformedMessage) {
					t.Fatal("known reply became a protocol failure or uncertain outcome", err)
				}
				switch mode.Load() {
				case 0:
					if err != nil || response.Result.Code != 0 || !bytes.Equal(response.Result.Data, []byte(descriptionExamples)) || len(response.Commands) != 3 {
						t.Fatal("successful catalog lost raw result", response.Result, err)
					}
				case 1:
					if !errors.As(err, &server) || errors.Is(err, ErrInvalidCommandDescriptions) || server.Code != -13 || response.Result.Code != -13 || !bytes.Equal(response.Result.Data, []byte("non-JSON server output\n")) || response.Commands != nil {
						t.Fatal("server refusal was decoded or changed", response.Result, err)
					}
				case 2:
					if !errors.Is(err, ErrInvalidCommandDescriptions) || errors.As(err, &server) || response.Result.Code != 0 || !bytes.Equal(response.Result.Data, []byte("non-JSON server output\n")) || response.Commands != nil {
						t.Fatal("local JSON failure lost completed raw reply", response.Result, err)
					}
				}
			}
			mode.Store(0)
			if _, err := ordinary(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("schema failure retired healthy session", err)
			}
			nextTellRequest(t, ctx, f)
			stopped, stop := context.WithCancel(ctx)
			stop()
			response, err := call(stopped)
			if !errors.Is(err, context.Canceled) || response.Commands != nil || f.calls.Load() != 5 {
				t.Fatal("already canceled discovery sent a command", err, f.calls.Load())
			}
			wantManagers := uint32(0)
			if mgr {
				wantManagers = 1
			}
			if f.monDial.Load() != 1 || f.mgrDial.Load() != wantManagers {
				t.Fatal("schema failure reopened shared connections", f.monDial.Load(), f.mgrDial.Load())
			}
		})
	}
}
