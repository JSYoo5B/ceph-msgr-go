package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func nativeAdmissionMessage(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{
		oracleBytes(t, name+".header"), oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// These retained fronts/headers/data were produced by the independent pinned
// C++ two-stage encoder, not the Go request encoder under test. See the existing
// request-v8-native and pg-list-v8-native provenance receipts in testdata.
func TestRequestAdmissionTargetMatchesIndependentNativeRequests(t *testing.T) {
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum", "shard", "pg-list"} {
		t.Run(kind, func(t *testing.T) {
			name := "request-v8-native-" + kind
			want := RequestAdmissionTarget{PG: PG{Pool: 7, Seed: 0x44}, Shard: -1,
				Object: PGCursor{Pool: 7, Hash: 0x11223344, Key: "route-key", Namespace: "tenant", Object: "v8-codec", Snap: NoSnap}}
			if kind == "shard" {
				want.PG.Seed, want.Shard, want.Object.Hash = 0x55, 3, 0xabcdef01
			}
			if kind == "pg-list" {
				name = "pg-list-v8-native-request-v8-list"
				want.Object = PGCursor{Pool: 7, Hash: 0x44, Namespace: "tenant", Snap: NoSnap}
			}
			m := nativeAdmissionMessage(t, name)
			got, err := DecodeRequestV8AdmissionTarget(m, wire.DefaultLimit)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("native target projection differs", got, want, err)
			}
			if got.Object.Snap != NoSnap {
				t.Fatal("actual read snapshot leaked into head-object backoff identity")
			}
		})
	}
}

func TestRequestAdmissionTargetRejectsEveryTruncatedNativeFront(t *testing.T) {
	for _, name := range []string{"request-v8-native-read", "pg-list-v8-native-request-v8-list"} {
		m := nativeAdmissionMessage(t, name)
		for n := 0; n < len(m.Front); n++ {
			bad := m
			bad.Front = m.Front[:n]
			if got, err := DecodeRequestV8AdmissionTarget(bad, wire.DefaultLimit); err == nil || !reflect.DeepEqual(got, RequestAdmissionTarget{}) {
				t.Fatal("truncated native front produced an admission identity", name, n, got, err)
			}
		}
	}
}

func TestRequestAdmissionTargetRejectsUnsupportedAndInconsistentIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*msgr.MessageData)
	}{
		{"wrong message", func(m *msgr.MessageData) { m.Type = ReplyMessage }},
		{"v6", func(m *msgr.MessageData) { m.Version = 6 }},
		{"v9", func(m *msgr.MessageData) { m.Version = 9 }},
		{"wrong compat", func(m *msgr.MessageData) { m.CompatVersion = 2 }},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{0} }},
		{"spg version", func(m *msgr.MessageData) { m.Front[0] = 2 }},
		{"spg compat", func(m *msgr.MessageData) { m.Front[1] = 0 }},
		{"pg version", func(m *msgr.MessageData) { m.Front[6] = 2 }},
		{"shard", func(m *msgr.MessageData) { m.Front[23] = 254 }},
		{"epoch", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[28:32], 0) }},
		{"reqid version", func(m *msgr.MessageData) { m.Front[36] = 3 }},
		{"reqid compat", func(m *msgr.MessageData) { m.Front[37] = 1 }},
		{"reqid size", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[38:42], 20) }},
		{"locator version", func(m *msgr.MessageData) { m.Front[99] = 7 }},
		{"locator compat", func(m *msgr.MessageData) { m.Front[100] = 4 }},
		{"locator pool", func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[105:113], 8) }},
		{"signed pool", func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint64(m.Front[7:15], ^uint64(0))
			binary.LittleEndian.PutUint64(m.Front[105:113], ^uint64(0))
		}},
		{"oversized key", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[117:121], ^uint32(0)) }},
		{"trailing front", func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeAdmissionMessage(t, "request-v8-native-read")
			m.Front = bytes.Clone(m.Front)
			tc.edit(&m)
			if got, err := DecodeRequestV8AdmissionTarget(m, wire.DefaultLimit); err == nil || !reflect.DeepEqual(got, RequestAdmissionTarget{}) {
				t.Fatal("invalid identity produced an admission target", got, err)
			}
		})
	}
	m := nativeAdmissionMessage(t, "request-v8-native-read")
	limit := uint32(41 + len(m.Front) + len(m.Data))
	if _, err := DecodeRequestV8AdmissionTarget(m, limit); err != nil {
		t.Fatal("exact frame limit rejected", err)
	}
	if _, err := DecodeRequestV8AdmissionTarget(m, limit-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("whole frame escaped limit", err)
	}
	m = nativeAdmissionMessage(t, "pg-list-v8-native-request-v8-list")
	binary.LittleEndian.PutUint32(m.Front[32:36], uint32(FlagRead))
	if _, err := DecodeRequestV8AdmissionTarget(m, wire.DefaultLimit); err == nil {
		t.Fatal("empty ordinary object was accepted as PGNLS")
	}
}
