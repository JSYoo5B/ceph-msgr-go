package osd

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestRequestV9AdmissionIdentityAndEveryTruncation(t *testing.T) {
	for i, m := range unitV9AdmissionMessages(t) {
		got, err := DecodeRequestV9AdmissionTarget(m, 4096)
		want := RequestAdmissionTarget{PG: PG{Pool: 7, Seed: 0x44}, Shard: -1, Object: PGCursor{Pool: 7, Hash: 0x11223344, Key: "route-key", Namespace: "tenant", Object: "v8-codec", Snap: NoSnap}}
		if i == 4 {
			want.Object = PGCursor{Pool: 7, Hash: 0x44, Namespace: "tenant", Snap: NoSnap}
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("v9 backoff target differs", i, got, want, err)
		}
		for n := 0; n < len(m.Front); n++ {
			bad := m
			bad.Front = m.Front[:n]
			if got, err := DecodeRequestV9AdmissionTarget(bad, 4096); err == nil || !reflect.DeepEqual(got, RequestAdmissionTarget{}) {
				t.Fatal("truncation bypassed v9 gate", i, n, got, err)
			}
		}
		if _, err := DecodeRequestV8AdmissionTarget(m, 4096); err == nil {
			t.Fatal("v8 admission accepted v9")
		}
	}
}

func TestRequestV9AdmissionRejectsNonemptyMalformedAndWrongProfile(t *testing.T) {
	m := unitV9AdmissionMessages(t)[0]
	for _, tc := range []struct {
		name string
		edit func(*msgr.MessageData)
	}{
		{"wrong-type", func(m *msgr.MessageData) { m.Type = ReplyMessage }},
		{"wrong-version", func(m *msgr.MessageData) { m.Version = 8 }},
		{"wrong-compat", func(m *msgr.MessageData) { m.CompatVersion = 2 }},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{0} }},
		{"trace-version", func(m *msgr.MessageData) { m.Front[unitOTELStart] = 2 }},
		{"trace-compat-zero", func(m *msgr.MessageData) { m.Front[unitOTELStart+1] = 0 }},
		{"trace-compat-two", func(m *msgr.MessageData) { m.Front[unitOTELStart+1] = 2 }},
		{"trace-length-zero", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[unitOTELStart+2:], 0) }},
		{"trace-length-two", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[unitOTELStart+2:], 2) }},
		{"trace-length-overflow", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[unitOTELStart+2:], ^uint32(0)) }},
		{"trace-valid-flag", func(m *msgr.MessageData) { m.Front[unitOTELStart+6] = 1 }},
		{"trace-noncanonical-flag", func(m *msgr.MessageData) { m.Front[unitOTELStart+6] = 2 }},
		{"real-nonempty-context-shape", func(m *msgr.MessageData) {
			body := append([]byte{1, 1, 26, 0, 0, 0, 1}, bytes.Repeat([]byte{1}, 25)...)
			m.Front = append(append(append([]byte(nil), m.Front[:unitOTELStart]...), body...), m.Front[unitOTELStart+7:]...)
		}},
		{"ec-shard", func(m *msgr.MessageData) { m.Front[23] = 0 }},
		{"trailing-front", func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := m
			bad.Front = bytes.Clone(m.Front)
			tc.edit(&bad)
			if got, err := DecodeRequestV9AdmissionTarget(bad, 4096); err == nil || !reflect.DeepEqual(got, RequestAdmissionTarget{}) {
				t.Fatal("unsupported trace/profile escaped gate", got, err)
			}
		})
	}
	if got, err := DecodeRequestV9AdmissionTarget(m, uint32(40+len(m.Front))); err == nil || !reflect.DeepEqual(got, RequestAdmissionTarget{}) {
		t.Fatal("frame limit bypass", got, err)
	}
}
