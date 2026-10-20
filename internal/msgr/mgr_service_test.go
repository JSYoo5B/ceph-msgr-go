package msgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The expected bytes below are written out from the pinned MMgrOpen.h,
// MMgrUpdate.h, MMgrReport.h, MMgrClose.h and MMgrConfigure.h encode_payload
// order, independently of the encoder under test. They are source-derived, not
// native daemon output; see cephmsgr/MGR_SERVICE_CONTRACT.md for the evidence.

func le32(v uint32) []byte { return binary.LittleEndian.AppendUint32(nil, v) }

func str(s string) []byte { return append(le32(uint32(len(s))), s...) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func strMap(pairs ...string) []byte {
	out := le32(uint32(len(pairs) / 2))
	for _, p := range pairs {
		out = append(out, str(p)...)
	}
	return out
}

func TestMgrOpenEncoding(t *testing.T) {
	m, err := MgrOpen("svc", "d1", map[string]string{"b": "2", "a": "1"}, map[string]string{"s": "ok"})
	if err != nil {
		t.Fatal(err)
	}
	want := cat(str("d1"), str("svc"), []byte{1}, strMap("a", "1", "b", "2"), strMap("s", "ok"), le32(0), le32(0))
	if m.Type != 0x700 || m.Version != 3 || m.CompatVersion != 1 || !bytes.Equal(m.Front, want) || len(m.Middle)+len(m.Data) != 0 {
		t.Fatalf("MgrOpen = type %#x v%d/%d front %x, want %x", m.Type, m.Version, m.CompatVersion, m.Front, want)
	}
	empty, err := MgrOpen("svc", "d1", nil, nil)
	if err != nil || !bytes.Equal(empty.Front, cat(str("d1"), str("svc"), []byte{1}, le32(0), le32(0), le32(0), le32(0))) {
		t.Fatal("empty maps must encode as zero counts", err)
	}
}

func TestMgrUpdateEncoding(t *testing.T) {
	m, err := MgrUpdate("svc", "d1", map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	want := cat(str("d1"), str("svc"), []byte{1}, strMap("k", "v"), le32(0))
	if m.Type != 0x70b || m.Version != 2 || m.CompatVersion != 1 || !bytes.Equal(m.Front, want) {
		t.Fatalf("MgrUpdate front %x, want %x", m.Front, want)
	}
	if _, err := MgrUpdate("svc", "d1", nil); !errors.Is(err, ErrServiceRegistration) {
		t.Fatal("an update the MGR would ignore was accepted", err)
	}
}

func TestMgrReportEncoding(t *testing.T) {
	packed := []byte{1, 1, 0, 0, 0, 0}
	base := func(status, task []byte) []byte {
		return cat(str("d1"), le32(0), le32(uint32(len(packed))), packed, le32(0), str("svc"), status,
			le32(0), le32(0), le32(0), task, []byte{0})
	}
	for name, tc := range map[string]struct {
		status, task map[string]string
		want         []byte
	}{
		"beacon":       {nil, nil, base([]byte{0}, []byte{0})},
		"status":       {map[string]string{"a": "b"}, nil, base(cat([]byte{1}, strMap("a", "b")), []byte{0})},
		"task":         {nil, map[string]string{"t": "u"}, base([]byte{0}, cat([]byte{1}, strMap("t", "u")))},
		"empty status": {map[string]string{}, nil, base(cat([]byte{1}, le32(0)), []byte{0})},
	} {
		m, err := MgrReport("svc", "d1", tc.status, tc.task)
		if err != nil || m.Type != 0x702 || m.Version != 9 || m.CompatVersion != 1 || !bytes.Equal(m.Front, tc.want) {
			t.Fatalf("%s: front %x, want %x (%v)", name, m.Front, tc.want, err)
		}
	}
}

func TestMgrCloseEncodingAndEchoDecoding(t *testing.T) {
	m, err := MgrClose("svc", "d1")
	if err != nil || m.Type != 0x708 || m.Version != 1 || m.CompatVersion != 1 || !bytes.Equal(m.Front, cat(str("d1"), str("svc"))) {
		t.Fatalf("MgrClose = %#v (%v)", m, err)
	}
	service, daemon, err := DecodeMgrClose(m, wire.DefaultLimit)
	if err != nil || service != "svc" || daemon != "d1" {
		t.Fatal("echo not decoded", service, daemon, err)
	}
	for name, mutate := range map[string]func(*MessageData){
		"type":     func(m *MessageData) { m.Type = MgrOpenMessage },
		"version":  func(m *MessageData) { m.Version = 2 },
		"compat":   func(m *MessageData) { m.CompatVersion = 0 },
		"data":     func(m *MessageData) { m.Data = []byte{1} },
		"middle":   func(m *MessageData) { m.Middle = []byte{1} },
		"trailing": func(m *MessageData) { m.Front = append(append([]byte{}, m.Front...), 0) },
		"short":    func(m *MessageData) { m.Front = m.Front[:len(m.Front)-1] },
	} {
		bad := m
		mutate(&bad)
		if _, _, err := DecodeMgrClose(bad, wire.DefaultLimit); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	if _, _, err := DecodeMgrClose(m, 10); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("frame limit ignored", err)
	}
}

func TestMgrConfigureDecoding(t *testing.T) {
	v := func(version uint16, front []byte) MessageData {
		return MessageData{Type: MgrConfigureMessage, Version: version, CompatVersion: 1, Front: front}
	}
	c, err := DecodeMgrConfigure(v(4, cat(le32(5), le32(2), le32(0), []byte{0})), wire.DefaultLimit)
	if err != nil || c.Version != 4 || c.StatsPeriod != 5 || c.StatsThreshold != 2 || c.Extension != nil {
		t.Fatalf("client-peer configure = %+v (%v)", c, err)
	}
	if c, err = DecodeMgrConfigure(v(1, le32(7)), wire.DefaultLimit); err != nil || c.StatsPeriod != 7 || c.Version != 1 {
		t.Fatal("v1", c, err)
	}
	if c, err = DecodeMgrConfigure(v(2, cat(le32(7), le32(9))), wire.DefaultLimit); err != nil || c.StatsThreshold != 9 {
		t.Fatal("v2", c, err)
	}
	if c, err = DecodeMgrConfigure(v(3, cat(le32(7), le32(9), le32(0))), wire.DefaultLimit); err != nil || c.Extension != nil {
		t.Fatal("v3", c, err)
	}
	queries := cat(le32(1), []byte{0xde, 0xad})
	if c, err = DecodeMgrConfigure(v(4, cat(le32(5), le32(2), queries)), wire.DefaultLimit); err != nil || !bytes.Equal(c.Extension, queries) {
		t.Fatal("OSD queries must be preserved raw", c, err)
	}
	metric := []byte{1, 0xbe, 0xef}
	if c, err = DecodeMgrConfigure(v(4, cat(le32(5), le32(2), le32(0), metric)), wire.DefaultLimit); err != nil || !bytes.Equal(c.Extension, metric) {
		t.Fatal("metric configuration must be preserved raw", c, err)
	}
	for name, m := range map[string]MessageData{
		"version 0":    v(0, le32(1)),
		"version 5":    v(5, cat(le32(5), le32(2), le32(0), []byte{0})),
		"compat":       {Type: MgrConfigureMessage, Version: 4, CompatVersion: 2, Front: cat(le32(5), le32(2), le32(0), []byte{0})},
		"type":         {Type: MgrOpenMessage, Version: 4, CompatVersion: 1},
		"truncated":    v(4, cat(le32(5), le32(2), le32(0))),
		"trailing":     v(4, cat(le32(5), le32(2), le32(0), []byte{0, 0})),
		"bad optional": v(4, cat(le32(5), le32(2), le32(0), []byte{2})),
		"data segment": {Type: MgrConfigureMessage, Version: 1, CompatVersion: 1, Front: le32(1), Data: []byte{1}},
		"short period": v(1, []byte{1, 2}),
	} {
		if _, err := DecodeMgrConfigure(m, wire.DefaultLimit); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	if _, err := DecodeMgrConfigure(v(1, le32(1)), 10); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("frame limit ignored", err)
	}
}

func TestServiceRegistrationBounds(t *testing.T) {
	big := strings.Repeat("x", MaxServiceStringBytes+1)
	many := map[string]string{}
	for i := 0; i <= MaxServiceMapEntries; i++ {
		many[string(rune('a'+i%26))+strings.Repeat("k", i/26)] = "v"
	}
	for name, err := range map[string]error{
		"empty service":   second(MgrOpen("", "d", nil, nil)),
		"empty daemon":    second(MgrOpen("s", "", nil, nil)),
		"NUL in daemon":   second(MgrOpen("s", "d\x00", nil, nil)),
		"long service":    second(MgrClose(strings.Repeat("s", MaxServiceNameBytes+1), "d")),
		"large value":     second(MgrOpen("s", "d", map[string]string{"k": big}, nil)),
		"large key":       second(MgrReport("s", "d", map[string]string{big: "v"}, nil)),
		"too many":        second(MgrOpen("s", "d", many, nil)),
		"message too big": second(MgrOpen("s", "d", bigMessage(), bigMessage())),
	} {
		if !errors.Is(err, ErrServiceRegistration) {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
}

func second(_ MessageData, err error) error { return err }

// bigMessage stays inside every per-entry bound but exceeds the message bound
// together with the status map.
func bigMessage() map[string]string {
	m := map[string]string{}
	for i := 0; i < 12; i++ {
		m[strings.Repeat("k", i+1)] = strings.Repeat("v", MaxServiceStringBytes)
	}
	return m
}
