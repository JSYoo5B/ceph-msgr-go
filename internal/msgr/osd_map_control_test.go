package msgr

import (
	"bytes"
	"testing"
)

// Literal payloads follow the fixed Tentacle C++ fields, without the product
// encoder/decoder constructing expected bytes. Type 6 has a default v1/compat0
// header and an 18-byte Paxos prefix followed by four inclusive uint32 bounds.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonGetOSDMap.h#L75
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/PaxosServiceMessage.h#L36
func TestGetOSDMapsIndependentVectors(t *testing.T) {
	for _, test := range []struct {
		name    string
		bounds  [4]uint32
		literal string
	}{
		{"ranges", [4]uint32{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00}, `44332211 88776655 ccbbaa99 00ffeedd`},
		{"full only", [4]uint32{7, 7, 0, 0}, `07000000 07000000 00000000 00000000`},
		{"incremental only", [4]uint32{0, 0, 8, 9}, `00000000 00000000 08000000 09000000`},
		{"empty ranges", [4]uint32{}, `00000000 00000000 00000000 00000000`},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := GetOSDMaps(test.bounds[0], test.bounds[1], test.bounds[2], test.bounds[3])
			want := configLiteral(t, `0000000000000000 ffff 0000000000000000 `+test.literal)
			if !bytes.Equal(m.Front, want) || len(m.Front) != 34 {
				t.Fatal("OSD map ranges changed their Paxos prefix or inclusive field order", m.Front)
			}
			checkOSDMapControlHeader(t, m, 6, 1, 0, `0000000000000000 0000000000000000 0600 7f00 0100 00000000 0000 0000000000000000 01 0000 0000`)
		})
	}
}

// MMonSubscribe v3 stores a map of key -> packed uint64 start/uint8 flags,
// followed by the unmodified hostname. Only osdmap is encoded: MON updates the
// provided key without replacing existing subscriptions. ONETIME is bit 0.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonSubscribe.h#L103
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/Monitor.cc#L5439
func TestOSDMapSubscribeIndependentVectors(t *testing.T) {
	for _, test := range []struct {
		name     string
		next     uint64
		onetime  bool
		hostname string
		literal  string
	}{
		{"latest full", 0, true, "", `0000000000000000 01 00000000`},
		{"continuous latest", 0, false, "", `0000000000000000 00 00000000`},
		{"continuous full cursor", 0x1122334455667788, false, " Host.雪\x00\xff ", `8877665544332211 00 0c000000 20486f73742ee99baa00ff20`},
		{"one publication cycle", 0xffffffffffffffff, true, "", `ffffffffffffffff 01 00000000`},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := OSDMapSubscribe(test.next, test.onetime, test.hostname)
			want := configLiteral(t, `01000000 06000000 6f73646d6170 `+test.literal)
			if !bytes.Equal(m.Front, want) {
				t.Fatal("OSD map subscription changed its only key, full cursor, flags or raw hostname", m.Front)
			}
			checkOSDMapControlHeader(t, m, 15, 3, 1, `0000000000000000 0000000000000000 0f00 7f00 0300 00000000 0000 0000000000000000 01 0100 0000`)
		})
	}
}

func checkOSDMapControlHeader(t *testing.T, m MessageData, typ, version, compat uint16, literal string) {
	t.Helper()
	f := m.Frame()
	if m.Type != typ || m.Version != version || m.CompatVersion != compat || m.Priority != 127 || m.Transaction != 0 || m.Sequence != 0 || m.AckSequence != 0 || len(m.Middle) != 0 || len(m.Data) != 0 || f.Tag != Message || len(f.Segments) != 4 || !bytes.Equal(f.Segments[0], configLiteral(t, literal)) {
		t.Fatal("OSD map control acquired an application transaction or changed its wire header", m)
	}
}

func TestOSDMapControlKeepsExistingSubscriptionsExplicit(t *testing.T) {
	// The new explicit request must not insert osdmap into existing map/log/
	// config/digest requests. Expected vectors remain source-derived literals.
	for _, test := range []struct {
		name    string
		message MessageData
		literal string
	}{
		{"maps", Subscribe(0, 0, ""), `02000000 06000000 6d67726d6170 0000000000000000 00 06000000 6d6f6e6d6170 0000000000000000 00 00000000`},
		{"logs", LogSubscribe("info", 0, ""), `03000000 08000000 6c6f672d696e666f 0000000000000000 00 06000000 6d67726d6170 0000000000000000 00 06000000 6d6f6e6d6170 0000000000000000 00 00000000`},
		{"config", ConfigSubscribe(""), `03000000 06000000 636f6e666967 0000000000000000 00 06000000 6d67726d6170 0000000000000000 00 06000000 6d6f6e6d6170 0000000000000000 00 00000000`},
		{"digest", DigestSubscribe(""), `03000000 09000000 6d6772646967657374 0000000000000000 00 06000000 6d67726d6170 0000000000000000 00 06000000 6d6f6e6d6170 0000000000000000 00 00000000`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !bytes.Equal(test.message.Front, configLiteral(t, test.literal)) {
				t.Fatal("OSD map support changed an existing subscription request", test.message.Front)
			}
		})
	}
}
