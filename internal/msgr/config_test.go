package msgr

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Pinned MConfig layout, assembled without the product encoder: uint32 count,
// then key/value length-prefixed strings. The full map contains an empty key,
// empty value, embedded NUL, invalid UTF-8 and whitespace intentionally.
const configVector = `
03000000
00000000 03000000 ff000a
03000000 612e62 00000000
04000000 7a0001ff 05000000 76310a2020
`

func configLiteral(t testing.TB, text string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.Join(strings.Fields(text), ""))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestConfigRequestsIndependentVectors(t *testing.T) {
	sub := ConfigSubscribe("")
	want := configLiteral(t, `
03000000
06000000 636f6e666967 0000000000000000 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00
00000000
`)
	if sub.Type != 15 || sub.Version != 3 || sub.CompatVersion != 1 || sub.Priority != 127 || sub.Transaction != 0 || !reflect.DeepEqual(sub.Front, want) || len(sub.Middle) != 0 || len(sub.Data) != 0 {
		t.Fatal("config subscription changed its continuous start, map subscriptions, hostname or header")
	}
	get := GetConfig("client.unit.scope", "")
	want = configLiteral(t, `08000000 0a000000 756e69742e73636f7065 00000000 00000000`)
	if get.Type != 63 || get.Version != 1 || get.CompatVersion != 1 || get.Priority != 127 || get.Transaction != 0 || !reflect.DeepEqual(get.Front, want) || len(get.Middle) != 0 || len(get.Data) != 0 {
		t.Fatal("getconfig changed its client EntityName, empty masks or header")
	}
	header := configLiteral(t, `000000000000000000000000000000003f007f00010000000000000000000000000000000101000000`)
	if !reflect.DeepEqual(get.Frame().Segments[0], header) {
		t.Fatal("getconfig acquired an application transaction or changed its compatibility header")
	}
	// The authenticated client ID may itself contain the word client; only the
	// entity prefix is removed, and no numeric global ID is encoded in its place.
	get = GetConfig("client.client.scope", "")
	if !reflect.DeepEqual(get.Front, configLiteral(t, `08000000 0c000000 636c69656e742e73636f7065 00000000 00000000`)) {
		t.Fatal("getconfig rewrote the authenticated bare client name")
	}
}

func TestDecodeConfigIndependentVectorOwnsRawStrings(t *testing.T) {
	front := configLiteral(t, configVector)
	want := map[string]string{"": "\xff\x00\n", "a.b": "", "z\x00\x01\xff": "v1\n  "}
	decoded, err := DecodeConfig(MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: front}, 4096)
	if err != nil || !reflect.DeepEqual(decoded, want) {
		t.Fatal("config strings were narrowed or normalized", err)
	}
	for i := range front {
		front[i] = 0
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatal("config strings retained the receive buffer")
	}
	decoded["a.b"] = "caller mutation"
	next, err := DecodeConfig(MessageData{Type: 62, Version: 2, CompatVersion: 1, Front: configLiteral(t, configVector)}, 4096)
	if err != nil || !reflect.DeepEqual(next, want) {
		t.Fatal("compatible header or independent map ownership was lost", err)
	}
	// Each message is a full map, not a delta merged with previous keys. An
	// empty full map is valid and distinguishable from a failed decode.
	empty, err := DecodeConfig(MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: configLiteral(t, `00000000`)}, 45)
	if err != nil || empty == nil || len(empty) != 0 || !reflect.DeepEqual(next, want) {
		t.Fatal("empty full replacement was rejected or altered an earlier map", err)
	}
}

func TestDecodeConfigMalformedInputPublishesNothing(t *testing.T) {
	for _, test := range []struct {
		name   string
		modify func(*MessageData)
		cause  error
	}{
		{"wrong message", func(m *MessageData) { m.Type = GetConfigMessage }, nil},
		{"zero message version", func(m *MessageData) { m.Version, m.CompatVersion = 0, 0 }, wire.ErrVersion},
		{"unsupported compatibility", func(m *MessageData) { m.Version, m.CompatVersion = 2, 2 }, wire.ErrVersion},
		{"compatibility exceeds version", func(m *MessageData) { m.Version, m.CompatVersion = 0, 1 }, wire.ErrVersion},
		{"truncated count", func(m *MessageData) { m.Front = m.Front[:3] }, io.ErrUnexpectedEOF},
		{"truncated final value", func(m *MessageData) { m.Front = m.Front[:len(m.Front)-1] }, io.ErrUnexpectedEOF},
		{"count overflow", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front, ^uint32(0)) }, wire.ErrLimit},
		{"count exceeds remaining", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front, 6) }, wire.ErrLimit},
		{"oversized key", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[4:], 4097) }, wire.ErrLimit},
		{"oversized value", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[8:], 4097) }, wire.ErrLimit},
		{"outer trailing bytes", func(m *MessageData) { m.Front = append(m.Front, 0) }, nil},
		{"unexpected middle", func(m *MessageData) { m.Middle = []byte{0} }, nil},
		{"unexpected data", func(m *MessageData) { m.Data = []byte{0} }, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := MessageData{Type: ConfigMessage, Version: 1, CompatVersion: 1, Front: configLiteral(t, configVector)}
			test.modify(&m)
			config, err := DecodeConfig(m, 4096)
			if config != nil || !errors.Is(err, ErrFrame) || (test.cause != nil && !errors.Is(err, test.cause)) {
				t.Fatal("malformed config published a partial map or lost its protocol cause", err)
			}
		})
	}
	duplicate := configLiteral(t, `02000000 01000000 61 01000000 31 01000000 61 01000000 32`)
	if config, err := DecodeConfig(MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: duplicate}, 4096); config != nil || !errors.Is(err, ErrFrame) {
		t.Fatal("duplicate key published an ambiguous replacement map", err)
	}
}

func TestDecodeConfigIndependentCountAndFrameBounds(t *testing.T) {
	// Raw unique four-byte keys isolate the exact entry-count boundary. A
	// separately assembled over-cap map has enough physical bytes, so it fails
	// the independent count cap rather than its minimum remaining-byte bound.
	front := binary.LittleEndian.AppendUint32(nil, 65536)
	for i := uint32(0); i < 65536; i++ {
		front = binary.LittleEndian.AppendUint32(front, 4)
		front = binary.LittleEndian.AppendUint32(front, i)
		front = binary.LittleEndian.AppendUint32(front, 0)
	}
	config, err := DecodeConfig(MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: front}, uint32(len(front)+41))
	if err != nil || len(config) != 65536 {
		t.Fatal("exact count or logical frame limit was rejected", err)
	}
	tooMany := make([]byte, 4+65537*8)
	binary.LittleEndian.PutUint32(tooMany, 65537)
	if config, err := DecodeConfig(MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: tooMany}, uint32(len(tooMany)+41)); config != nil || !errors.Is(err, wire.ErrLimit) || !errors.Is(err, ErrFrame) {
		t.Fatal("config collection exceeded its independent count cap", err)
	}
	m := MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: configLiteral(t, configVector)}
	for _, limit := range []uint32{0, 40, uint32(len(m.Front)), uint32(len(m.Front) + 40)} {
		if config, err := DecodeConfig(m, limit); config != nil || !errors.Is(err, wire.ErrLimit) || !errors.Is(err, ErrFrame) {
			t.Fatal("logical frame bound ignored the message header", limit, err)
		}
	}
	if _, err := DecodeConfig(m, uint32(len(m.Front)+41)); err != nil {
		t.Fatal("exact logical frame bound failed", err)
	}
}

func FuzzDecodeConfig(f *testing.F) {
	front := configLiteral(f, configVector)
	f.Add(front, uint8(1), uint8(1), uint16(4096))
	f.Add([]byte{}, uint8(1), uint8(1), uint16(4096))
	f.Add(configLiteral(f, `00000000`), uint8(1), uint8(1), uint16(45))
	f.Add(configLiteral(f, `02000000 01000000 61 00000000 01000000 61 00000000`), uint8(1), uint8(1), uint16(4096))
	f.Add(front, uint8(2), uint8(2), uint16(64))
	f.Add(configLiteral(f, `ffffffff`), uint8(1), uint8(1), uint16(4096))
	f.Fuzz(func(t *testing.T, p []byte, version, compat uint8, limit uint16) {
		config, err := DecodeConfig(MessageData{Type: ConfigMessage, Version: uint16(version), CompatVersion: uint16(compat), Front: p}, uint32(limit))
		if err != nil && (config != nil || !errors.Is(err, ErrFrame)) {
			t.Fatal("malformed config exposed a partial map or lost protocol provenance", err)
		}
	})
}
