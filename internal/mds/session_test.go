package mds

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func sessionHead(op uint32, seq uint64, maxCaps, maxLeases uint32) *wire.Encoder {
	e := &wire.Encoder{}
	e.U32(op)
	e.U64(seq)
	e.U32(1700000000)
	e.U32(5)
	e.U32(maxCaps)
	e.U32(maxLeases)
	return e
}

func serverOpen(t *testing.T) []byte {
	t.Helper()
	e := sessionHead(SessionOpen, 0, 0, 0)
	e.U32(1)
	e.String("error_string")
	e.String("")
	e.Bytes([]byte{0xff, 0xff, 0xff, 0, 0, 0, 0, 0})
	metric := wire.Encoder{}
	metric.Bytes([]byte{0xfe, 0xff, 0, 0, 0, 0, 0, 0})
	e.Struct(1, 1, metric.Data)
	e.U32(SessionFlagBlocklisted)
	e.U32(1)
	match := wire.Encoder{}
	match.U64(^uint64(0))
	match.U32(2)
	match.U32(1000)
	match.U32(1001)
	match.String("volumes/a")
	match.String("cephfs")
	match.U8(1)
	auth := wire.Encoder{}
	auth.Struct(1, 1, match.Data)
	auth.U8(1)
	auth.U8(0)
	e.Struct(1, 1, auth.Data)
	e.U64(42)
	return e.Data
}

func TestEncodeRequestOpenNativeLayout(t *testing.T) {
	m := EncodeRequestOpen(map[string]string{"root": "/", "entity_id": "admin"})
	if m.Type != ClientSessionMessage || m.Version != 7 || m.CompatVersion != 1 || m.Priority != 127 || len(m.Middle) != 0 || len(m.Data) != 0 {
		t.Fatalf("header = %+v", m)
	}
	want := "00000000" + "0000000000000000" + "0000000000000000" + "00000000" + "00000000" +
		"02000000" + "09000000" + hex.EncodeToString([]byte("entity_id")) + "05000000" + hex.EncodeToString([]byte("admin")) +
		"04000000" + hex.EncodeToString([]byte("root")) + "01000000" + "2f" +
		"08000000" + "ff01000000000000" +
		"0101" + "04000000" + "00000000" +
		"00000000" + "00000000" + "ffffffffffffffff"
	if got := hex.EncodeToString(m.Front); got != want {
		t.Fatalf("front\n got %s\nwant %s", got, want)
	}
	if !FeatureBitsetHas(ClientFeatures, 8) || FeatureBitsetHas(ClientFeatures, 9) || FeatureBitsetHas(ClientFeatures, 16) {
		t.Fatal("declared feature set is not exactly bits 0-8")
	}
}

func TestEncodeSessionControlHeadOnly(t *testing.T) {
	for _, op := range []uint32{SessionRequestRenewCaps, SessionRequestClose, SessionFlushMessageAck} {
		m, err := EncodeSessionControl(op, 0x0102030405060708)
		if err != nil || m.Version != 1 || m.CompatVersion != 1 || len(m.Front) != SessionHeadBytes {
			t.Fatalf("op %d: %+v %v", op, m, err)
		}
		if !bytes.Equal(m.Front[4:12], []byte{8, 7, 6, 5, 4, 3, 2, 1}) || !bytes.Equal(m.Front[12:], make([]byte, 16)) {
			t.Fatalf("op %d head = %x", op, m.Front)
		}
	}
	for _, op := range []uint32{SessionRequestOpen, SessionOpen, SessionReject, 99} {
		if _, err := EncodeSessionControl(op, 0); !errors.Is(err, ErrSession) {
			t.Fatalf("op %d accepted", op)
		}
	}
}

func TestDecodeClientSessionVersions(t *testing.T) {
	head := sessionHead(SessionRecallState, 3, 100, 7)
	got, err := DecodeClientSession(msgr.MessageData{Type: ClientSessionMessage, Version: 1, CompatVersion: 1, Front: head.Data}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := ClientSession{Version: 1, Op: SessionRecallState, Sequence: 3, Stamp: Time{1700000000, 5}, MaxCaps: 100, MaxLeases: 7, OldestClientTID: NoOldestClientTID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("v1 = %+v", got)
	}
	got, err = DecodeClientSession(msgr.MessageData{Type: ClientSessionMessage, Version: 7, CompatVersion: 1, Front: serverOpen(t)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != SessionOpen || got.Metadata["error_string"] != "" || len(got.Metadata) != 1 || !FeatureBitsetHas(got.SupportedFeatures, 23) || FeatureBitsetHas(got.MetricFlags, 0) || !FeatureBitsetHas(got.MetricFlags, 1) || got.Flags != SessionFlagBlocklisted || got.OldestClientTID != 42 {
		t.Fatalf("v7 = %+v", got)
	}
	if !reflect.DeepEqual(got.CapAuths, []CapAuth{{UID: -1, GIDs: []uint32{1000, 1001}, Path: "volumes/a", FilesystemName: "cephfs", RootSquash: true, Readable: true}}) {
		t.Fatalf("cap auths = %+v", got.CapAuths)
	}
}

func TestDecodeClientSessionRefusesUnpinnedShapes(t *testing.T) {
	open := serverOpen(t)
	head := sessionHead(SessionStale, 0, 0, 0).Data
	cases := map[string]struct {
		m    msgr.MessageData
		want error
	}{
		"version 2":     {msgr.MessageData{Type: 22, Version: 2, CompatVersion: 1, Front: head}, wire.ErrVersion},
		"version 8":     {msgr.MessageData{Type: 22, Version: 8, CompatVersion: 1, Front: open}, wire.ErrVersion},
		"compat 0":      {msgr.MessageData{Type: 22, Version: 1, CompatVersion: 0, Front: head}, wire.ErrVersion},
		"wrong type":    {msgr.MessageData{Type: 21, Version: 1, CompatVersion: 1, Front: head}, msgr.ErrFrame},
		"data segment":  {msgr.MessageData{Type: 22, Version: 1, CompatVersion: 1, Front: head, Data: []byte{1}}, msgr.ErrFrame},
		"v1 trailing":   {msgr.MessageData{Type: 22, Version: 1, CompatVersion: 1, Front: append(bytes.Clone(head), 0)}, nil},
		"v7 trailing":   {msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: append(bytes.Clone(open), 0)}, nil},
		"v1 truncated":  {msgr.MessageData{Type: 22, Version: 1, CompatVersion: 1, Front: head[:27]}, nil},
		"v7 truncated":  {msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: open[:len(open)-1]}, nil},
		"v7 as v1 head": {msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: head}, nil},
		"frame limit":   {msgr.MessageData{Type: 22, Version: 1, CompatVersion: 1, Front: head}, wire.ErrLimit},
	}
	for name, c := range cases {
		limit := uint32(0)
		if name == "frame limit" {
			limit = 60
		}
		_, err := DecodeClientSession(c.m, limit)
		if err == nil || c.want != nil && !errors.Is(err, c.want) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	duplicate := sessionHead(SessionReject, 0, 0, 0)
	duplicate.U32(2)
	for range 2 {
		duplicate.String("k")
		duplicate.String("v")
	}
	if _, err := DecodeClientSession(msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: duplicate.Data}, 0); !errors.Is(err, ErrSession) {
		t.Fatalf("duplicate metadata key: %v", err)
	}
	huge := sessionHead(SessionReject, 0, 0, 0)
	huge.U32(0)
	huge.U32(MaxClientFeatureBytes + 1)
	if _, err := DecodeClientSession(msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: huge.Data}, 0); !errors.Is(err, wire.ErrLimit) {
		t.Fatalf("oversized bitset: %v", err)
	}
	many := sessionHead(SessionReject, 0, 0, 0)
	many.U32(MaxSessionMetadata + 1)
	many.Raw(make([]byte, 8*(MaxSessionMetadata+1)))
	if _, err := DecodeClientSession(msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: many.Data}, 0); !errors.Is(err, wire.ErrLimit) {
		t.Fatalf("metadata count: %v", err)
	}
}

func TestDecodeClientSessionOwnsInput(t *testing.T) {
	front := serverOpen(t)
	got, err := DecodeClientSession(msgr.MessageData{Type: 22, Version: 7, CompatVersion: 1, Front: front}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range front {
		front[i] = 0
	}
	if !FeatureBitsetHas(got.SupportedFeatures, 0) || got.CapAuths[0].Path != "volumes/a" {
		t.Fatal("decoded session aliases caller input")
	}
}

func FuzzDecodeClientSession(f *testing.F) {
	f.Add(uint16(7), serverOpen(&testing.T{}))
	f.Add(uint16(1), sessionHead(SessionStale, 1, 0, 0).Data)
	f.Fuzz(func(t *testing.T, version uint16, front []byte) {
		got, err := DecodeClientSession(msgr.MessageData{Type: 22, Version: version, CompatVersion: 1, Front: front}, 1<<20)
		if err == nil && got.Version != version {
			t.Fatal("version not preserved")
		}
	})
}
