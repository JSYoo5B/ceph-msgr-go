package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func syntheticReplyV8(result int32, redirect bool) msgr.MessageData {
	e := wire.Encoder{}
	e.String("reply-v8-codec")
	encodePG(&e, PG{Pool: 7, Seed: 0x44})
	e.U64(FlagRead | FlagAck | FlagOnDisk | uint64(1)<<63)
	e.U32(uint32(result))
	e.U64(13)
	e.U32(8)
	e.U32(12)
	e.U32(2)
	for i, code := range []uint16{Stat, Read} {
		e.U16(code)
		e.U32(0)
		if i == 0 {
			e.U64(0)
			e.U64(0)
		} else {
			e.U64(2)
			e.U64(5)
		}
		e.U64(0)
		e.U32(0)
		if result < 0 || redirect {
			e.U32(0)
		} else if i == 0 {
			e.U32(16)
		} else {
			e.U32(5)
		}
	}
	e.U32(0)
	e.U32(uint32(result))
	e.U32(0)
	e.U64(91)
	e.U32(8)
	e.U64(13)
	if redirect {
		e.U8(1)
		body := wire.Encoder{}
		encodeLocator(&body, Locator{Pool: 9, Namespace: "redirect-namespace", Key: "redirect-key", Hash: -1})
		body.String("redirect-object")
		body.Bytes(nil)
		e.Struct(1, 1, body.Data)
	} else {
		e.U8(0)
	}
	e.U64(11)
	e.U64(22)
	e.U64(33)
	var data []byte
	if result >= 0 && !redirect {
		stat := wire.Encoder{}
		stat.U64(7)
		stat.U32(1700000000)
		stat.U32(123456789)
		data = append(stat.Data, 0, 255, 'a', 0, 'b')
	}
	return msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Transaction: 77, Front: e.Data, Data: data}
}

func TestReplyV8RetainsSignedStatusFlagsAndOwnedPayload(t *testing.T) {
	for _, code := range []int32{0, -2, -12345} {
		m := syntheticReplyV8(code, false)
		r, err := DecodeReplyV8(m, 0)
		if err != nil || r.Result != code || r.PG != (PG{Pool: 7, Seed: 0x44}) || r.MapEpoch != 12 || r.RetryAttempt != 0 || r.LegacyVersion != (Version{Version: 13, Epoch: 8}) || r.ReplayVersion != (Version{Version: 91, Epoch: 8}) || r.UserVersion != 13 || r.Trace != [3]uint64{11, 22, 33} || r.Operations[0].Result != code || r.Operations[1].Result != 0 || r.Flags&(uint64(1)<<63) == 0 || !r.Redirect.Empty() {
			t.Fatal("v8 reply fields lost", r, err)
		}
		if code == 0 {
			if len(r.Operations[0].Data) != 16 || !bytes.Equal(r.Operations[1].Data, []byte{0, 255, 'a', 0, 'b'}) {
				t.Fatal("per-op data split differs", r.Operations)
			}
			m.Data[16] = 42
			m.Front[4] = 'x'
			if r.Object != "reply-v8-codec" || r.Operations[1].Data[0] != 0 || r.RawData[16] != 0 {
				t.Fatal("decoder retained input ownership")
			}
		}
	}
}

func TestReplyV8ConditionalRedirectPreservesMetadataWithoutFollowing(t *testing.T) {
	r, err := DecodeReplyV8(syntheticReplyV8(0, true), 4096)
	if !errors.Is(err, ErrRedirect) || r.Redirect.Locator != (Locator{Pool: 9, Namespace: "redirect-namespace", Key: "redirect-key", Hash: -1}) || r.Redirect.Object != "redirect-object" || r.Result != 0 || len(r.RawFront) == 0 {
		t.Fatal(r, err)
	}
	// There is no fallback to an unconditional v6 redirect or intermediate
	// header versions: negotiating v8 must select exactly this codec branch.
	for _, version := range []uint16{6, 7, 9} {
		m := syntheticReplyV8(0, false)
		m.Version = version
		if _, err := DecodeReplyV8(m, 4096); !errors.Is(err, wire.ErrVersion) {
			t.Fatal("wrong reply version accepted", version, err)
		}
	}
}

func TestReplyV8RejectsMalformedBoundsBooleanAndData(t *testing.T) {
	m := syntheticReplyV8(0, false)
	for n := 0; n < len(m.Front); n++ {
		bad := m
		bad.Front = m.Front[:n]
		if _, err := DecodeReplyV8(bad, 4096); err == nil {
			t.Fatal("truncated front accepted", n)
		}
	}
	for n := 0; n < len(m.Data); n++ {
		bad := m
		bad.Data = m.Data[:n]
		if _, err := DecodeReplyV8(bad, 4096); err == nil {
			t.Fatal("truncated data accepted", n)
		}
	}
	for _, edit := range []func(*msgr.MessageData){
		func(m *msgr.MessageData) { m.Type = 42 }, func(m *msgr.MessageData) { m.CompatVersion = 3 },
		func(m *msgr.MessageData) { m.Middle = []byte{0} }, func(m *msgr.MessageData) { m.Front = append(m.Front, 0) },
		func(m *msgr.MessageData) { m.Data = append(m.Data, 0) }, func(m *msgr.MessageData) { m.Front[len(m.Front)-25] = 2 },
		func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[63:67], ^uint32(0)) },
		func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[63:67], 0) },
		func(m *msgr.MessageData) { m.Front[18] = 2 },                                  // Unsupported pg_t version.
		func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[31:35], 0) }, // Retired preferred OSD.
	} {
		bad := m
		bad.Front = bytes.Clone(m.Front)
		bad.Data = bytes.Clone(m.Data)
		edit(&bad)
		if _, err := DecodeReplyV8(bad, 4096); err == nil {
			t.Fatal("malformed v8 reply accepted")
		}
	}
	if _, err := DecodeReplyV8(m, uint32(40+len(m.Front)+len(m.Data))); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate envelope budget ignored", err)
	}
	// A native v8 producer derives its redirect flag from non-empty redirect
	// metadata. An asserted flag followed by an empty structure is inconsistent.
	empty := wire.Encoder{}
	encodeLocator(&empty, Locator{Pool: -1, Hash: -1})
	empty.String("")
	empty.Bytes(nil)
	redirect := wire.Encoder{}
	redirect.U8(1)
	redirect.Struct(1, 1, empty.Data)
	bad := m
	start := len(m.Front) - 25
	bad.Front = append(bytes.Clone(m.Front[:start]), redirect.Data...)
	bad.Front = append(bad.Front, m.Front[start+1:]...)
	if _, err := DecodeReplyV8(bad, 4096); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("asserted empty redirect accepted", err)
	}
}

func TestReplyV8MatchesFreshNativeMessages(t *testing.T) {
	for _, tc := range []struct {
		name             string
		result           int32
		redirect, ondisk bool
		ops              int
	}{
		{name: "success", ondisk: true, ops: 2},
		{name: "enoent", result: -2, ondisk: true, ops: 2},
		{name: "unknown-error", result: -12345, ondisk: true, ops: 2},
		{name: "mutation-ack", ops: 1},
		{name: "redirect", redirect: true, ondisk: true, ops: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "reply-v8-native-" + tc.name
			header := oracleBytes(t, name+".header")
			m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data")}})
			if err != nil {
				t.Fatal(err)
			}
			if m.Type != ReplyMessage || m.Version != 8 || m.CompatVersion != 2 || m.Transaction != 77 || m.Sequence != 1 || m.AckSequence != 3 || !bytes.Equal(m.Frame().Segments[0], header) {
				t.Fatal("native reply/header mismatch", m)
			}
			r, err := DecodeReplyV8(m, wire.DefaultLimit)
			if tc.redirect && !errors.Is(err, ErrRedirect) || !tc.redirect && err != nil {
				t.Fatal(err)
			}
			if r.Object != "reply-v8-codec" || r.PG != (PG{Pool: 7, Seed: 0x44}) || r.MapEpoch != 12 || r.Result != tc.result || r.OnDisk() != tc.ondisk || r.RetryAttempt != 0 || r.ReplayVersion != (Version{Version: 91, Epoch: 8}) || r.LegacyVersion != (Version{Version: 13, Epoch: 8}) || r.UserVersion != 13 || len(r.Operations) != tc.ops || r.Trace != [3]uint64{} {
				t.Fatal("native v8 metadata lost", r)
			}
			if tc.name == "success" && (len(r.Operations[0].Data) != 16 || !bytes.Equal(r.Operations[1].Data, []byte{0, 255, 'a', 0, 'b'})) {
				t.Fatal("native per-op outputs lost")
			}
			if tc.result < 0 && (r.Operations[0].Result != tc.result || r.Operations[1].Result != 0) {
				t.Fatal("native signed/unexecuted operation result lost")
			}
			if tc.redirect && (r.Redirect.Object != "redirect-object" || r.Redirect.Locator.Pool != 9 || r.Redirect.Locator.Key != "redirect-key" || r.Redirect.Locator.Namespace != "redirect-namespace") {
				t.Fatal("native redirect metadata lost", r.Redirect)
			}
		})
	}
}

func FuzzDecodeReplyV8(f *testing.F) {
	m := syntheticReplyV8(0, false)
	f.Add(m.Front, m.Data)
	f.Fuzz(func(t *testing.T, front, data []byte) {
		if len(front)+len(data) > 65536 {
			t.Skip()
		}
		DecodeReplyV8(msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Front: front, Data: data}, 65536)
	})
}
