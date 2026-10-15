package osd

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func watchEventInput(opcode uint8, payload, data []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.U8(1)
	e.U8(opcode)
	e.U64(71)
	e.U64(0)
	e.U64(99)
	e.Bytes(payload)
	e.U32(0)
	e.U64(42)
	return msgr.MessageData{Type: WatchNotifyMessage, Version: 3, CompatVersion: 1, Front: e.Data, Data: data}
}

func TestWatchEventStrictEnvelopeAndOwnership(t *testing.T) {
	m := watchEventInput(WatchEventNotify, []byte{0, 1, 255}, nil)
	e, err := DecodeWatchEvent(m, 1024)
	if err != nil || e.Cookie != 71 || e.NotifyID != 99 || e.NotifierGlobalID != 42 || !bytes.Equal(e.Payload, []byte{0, 1, 255}) {
		t.Fatal(e, err)
	}
	m.Front[30] = 20
	if !bytes.Equal(e.Payload, []byte{0, 1, 255}) {
		t.Fatal("decoder retained source bytes")
	}
	m = watchEventInput(WatchEventNotify, []byte{0, 1, 255}, nil)
	for n := 0; n < len(m.Front); n++ {
		invalid := m
		invalid.Front = m.Front[:n]
		if _, err := DecodeWatchEvent(invalid, 1024); err == nil {
			t.Fatal("truncation accepted", n)
		}
	}
	for _, edit := range []func(*msgr.MessageData){
		func(m *msgr.MessageData) { m.Version = 2 }, func(m *msgr.MessageData) { m.CompatVersion = 2 }, func(m *msgr.MessageData) { m.Type = 43 },
		func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }, func(m *msgr.MessageData) { m.Data = []byte{0} }, func(m *msgr.MessageData) { m.Middle = []byte{0} },
		func(m *msgr.MessageData) { m.Front[0] = 2 }, func(m *msgr.MessageData) { m.Front[1] = 8 },
	} {
		invalid := m
		invalid.Front = bytes.Clone(m.Front)
		edit(&invalid)
		if _, err := DecodeWatchEvent(invalid, 1024); err == nil {
			t.Fatal("invalid event accepted")
		}
	}
}

func TestWatchCompletionCollectionBounds(t *testing.T) {
	d := wire.Encoder{}
	d.U32(2)
	d.U64(51)
	d.U64(81)
	d.Bytes([]byte{0, 255})
	d.U64(52)
	d.U64(82)
	d.Bytes(nil)
	d.U32(1)
	d.U64(53)
	d.U64(83)
	m := watchEventInput(WatchEventComplete, nil, d.Data)
	binary.LittleEndian.PutUint32(m.Front[len(m.Front)-12:], uint32(0xffffff92))
	e, err := DecodeWatchEvent(m, 1024)
	if err != nil || e.ReturnCode != -110 || len(e.Replies) != 2 || !bytes.Equal(e.Replies[0].Data, []byte{0, 255}) || len(e.Missed) != 1 || e.Missed[0] != (NotifyIdentity{GlobalID: 53, Cookie: 83}) {
		t.Fatal(e, err)
	}
	for n := 0; n < len(m.Data); n++ {
		invalid := m
		invalid.Data = m.Data[:n]
		if _, err := DecodeWatchEvent(invalid, 1024); err == nil {
			t.Fatal("truncated completion accepted", n)
		}
	}
	bad := m
	bad.Data = bytes.Clone(m.Data)
	binary.LittleEndian.PutUint32(bad.Data, ^uint32(0))
	if _, err := DecodeWatchEvent(bad, 1024); err == nil {
		t.Fatal("unbounded reply count")
	}
	bad = m
	bad.Data = bytes.Clone(m.Data)
	binary.LittleEndian.PutUint64(bad.Data[26:], 50)
	if _, err := DecodeWatchEvent(bad, 1024); err == nil {
		t.Fatal("unordered/duplicate map identity")
	}
}

func watchNativeOperation(name string) Operation {
	op := Operation{Code: Watch, Watch: WatchInput{Cookie: 71, Generation: 7, Action: WatchRegister}}
	switch name {
	case "register":
		op.Watch.TimeoutSeconds = 37
	case "unwatch":
		op.Watch.Action = WatchUnwatch
		op.Watch.Generation = 0
	case "ping":
		op.Watch.Action = WatchPing
	case "reconnect":
		op.Watch.Action = WatchReconnect
	case "notify":
		op = Operation{Code: Notify, Watch: WatchInput{Cookie: 71, ProtocolVersion: 1, TimeoutSeconds: 3}, Data: []byte{0, 'h', 'e', 'l', 'l', 'o', 255}}
	case "ack":
		op = Operation{Code: NotifyAck, Watch: WatchInput{Cookie: 71, NotifyID: 99}, Data: []byte{0, 'a', 'c', 'k', 255}}
	}
	return op
}

func TestWatchOperationsMatchFreshNative(t *testing.T) {
	for _, name := range []string{"register", "unwatch", "ping", "reconnect", "notify", "ack"} {
		t.Run(name, func(t *testing.T) {
			op := watchNativeOperation(name)
			r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "watch-notify-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{op}}
			got, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			native := xattrNativeMessage(t, "watch-notify-native-request-v6-"+name)
			got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
			if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
				t.Fatal("watch wire differs from fresh native encoder")
			}
			reply, err := DecodeReply(xattrNativeMessage(t, "watch-notify-native-reply-v6-"+name), wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateWatchReply(op, reply.Operations[0], true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWatchEventsMatchFreshNative(t *testing.T) {
	for _, name := range []string{"notify", "complete", "timeout", "disconnect"} {
		t.Run(name, func(t *testing.T) {
			name = "watch-notify-native-event-v3-" + name
			m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{oracleBytes(t, name+".header"), oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data")}})
			if err != nil {
				t.Fatal(err)
			}
			e, err := DecodeWatchEvent(m, wire.DefaultLimit)
			if err != nil || e.Cookie != 71 || e.NotifyID != 99 || e.NotifierGlobalID != 42 {
				t.Fatal(e, err)
			}
			if name == "watch-notify-native-event-v3-notify" && !bytes.Equal(e.Payload, []byte{0, 'h', 'e', 'l', 'l', 'o', 255}) {
				t.Fatal("binary payload lost")
			}
			if name == "watch-notify-native-event-v3-complete" || name == "watch-notify-native-event-v3-timeout" {
				if len(e.Replies) != 2 || !bytes.Equal(e.Replies[0].Data, []byte{0, 'a', 'c', 'k', 255}) {
					t.Fatal(e)
				}
			}
			if name == "watch-notify-native-event-v3-timeout" && (e.ReturnCode != -110 || len(e.Missed) != 1) {
				t.Fatal(e)
			}
		})
	}
}

func FuzzDecodeWatchEvent(f *testing.F) {
	f.Add(watchEventInput(WatchEventNotify, []byte{0, 255}, nil).Front, []byte(nil))
	f.Fuzz(func(t *testing.T, front, data []byte) {
		if len(front)+len(data) > 65536 {
			t.Skip()
		}
		DecodeWatchEvent(msgr.MessageData{Type: 44, Version: 3, CompatVersion: 1, Front: front, Data: data}, 65536)
	})
}
