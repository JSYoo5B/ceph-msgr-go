package msgr

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independent pinned-layout bytes: Paxos service cursor, FSID, deque count,
// LogEntry v5/compat5, EntityName, signed rank, two MSG_ADDR2 addresses, utime,
// entry sequence, raw priority, binary text and channel. No product encoder
// constructs this vector. Its entry sequence deliberately differs from cursor.
const logVector = `
8877665544332211 ffff 0000000000000000
000102030405060708090a0b0c0d0e0f 01000000
05 05 90000000
01000000 01000000 61
01 ffffffffffffffff
02 02000000
01 01 01 1c000000 02000000 09000000 10000000
02008214c00002010000000000000000
01 01 01 28000000 02000000 0a000000 1c000000
0a008fc01234567800000000000000000000ffff7f00000117000000
00f15365 15cd5b07 0900000000000000 ffff
08000000 ff0068656c6c6f0a 05000000 6175646974
`

func logLiteral(t *testing.T, text string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.Join(strings.Fields(text), ""))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDecodeLogIndependentVectorPreservesMetadata(t *testing.T) {
	front := logLiteral(t, logVector)
	batch, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	want := LogBatch{
		Version: 0x1122334455667788,
		FSID:    [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Entries: []LogEntry{{
			NameType: 1, NameID: "a", RankType: 1, RankNumber: -1,
			Addresses: []Address{
				{Type: 2, Nonce: 9, Endpoint: netip.MustParseAddrPort("192.0.2.1:33300")},
				{Type: 2, Nonce: 10, Endpoint: netip.MustParseAddrPort("[::ffff:127.0.0.1]:36800"), FlowInfo: 0x12345678, ScopeID: 23},
			},
			Seconds: 1700000000, Nanoseconds: 123456789, Sequence: 9,
			Priority: 65535, Message: "\xff\x00hello\n", Channel: "audit",
		}},
	}
	if !reflect.DeepEqual(batch, want) || !batch.Entries[0].Addresses[1].Endpoint.Addr().Is4In6() {
		t.Fatalf("log metadata was narrowed or normalized: got=%+v want=%+v", batch, want)
	}
	// Decoder results must remain owned values after the received frame is
	// released/reused. Strings include NUL and invalid UTF-8 intentionally.
	for i := range front {
		front[i] = 0
	}
	if !reflect.DeepEqual(batch, want) {
		t.Fatal("decoded metadata retained the receive buffer")
	}
}

func TestDecodeLogBoundsAndMalformedInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		modify func(*MessageData)
		cause  error
	}{
		{"wrong message", func(m *MessageData) { m.Type = TellCommandReplyMessage }, nil},
		{"unsupported message compatibility", func(m *MessageData) { m.Version, m.CompatVersion = 2, 2 }, wire.ErrVersion},
		{"truncated Paxos", func(m *MessageData) { m.Front = m.Front[:17] }, io.ErrUnexpectedEOF},
		{"truncated FSID", func(m *MessageData) { m.Front = m.Front[:33] }, io.ErrUnexpectedEOF},
		{"truncated entry", func(m *MessageData) { m.Front = m.Front[:len(m.Front)-1] }, io.ErrUnexpectedEOF},
		{"count overflow", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[34:], ^uint32(0)) }, wire.ErrLimit},
		{"count exceeds remaining", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[34:], 3) }, wire.ErrLimit},
		{"unimplemented older entry", func(m *MessageData) { m.Front[38], m.Front[39] = 4, 4 }, wire.ErrVersion},
		{"zero entry version", func(m *MessageData) { m.Front[38], m.Front[39] = 0, 0 }, wire.ErrVersion},
		{"unsupported entry compatibility", func(m *MessageData) { m.Front[38], m.Front[39] = 6, 6 }, wire.ErrVersion},
		{"oversized entry envelope", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[40:], 4097) }, wire.ErrLimit},
		{"oversized name", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[48:], 4097) }, wire.ErrLimit},
		{"unsupported address marker", func(m *MessageData) { m.Front[62] = 3 }, wire.ErrVersion},
		{"oversized address envelope", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[70:], 4097) }, wire.ErrLimit},
		{"unsupported socket family", func(m *MessageData) { binary.LittleEndian.PutUint16(m.Front[86:], 99) }, nil},
		{"oversized text", func(m *MessageData) { binary.LittleEndian.PutUint32(m.Front[167:], 4097) }, wire.ErrLimit},
		{"outer trailing bytes", func(m *MessageData) { m.Front = append(m.Front, 0) }, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := MessageData{Type: LogMessage, Version: 1, Front: logLiteral(t, logVector)}
			test.modify(&m)
			batch, err := DecodeLog(m, 4096)
			if !errors.Is(err, ErrFrame) || (test.cause != nil && !errors.Is(err, test.cause)) || !reflect.DeepEqual(batch, LogBatch{}) {
				t.Fatalf("malformed complete log lost protocol/cause or published a partial batch: batch=%+v err=%v", batch, err)
			}
		})
	}
	front := logLiteral(t, logVector)
	if _, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, uint32(len(front)-1)); !errors.Is(err, ErrFrame) || !errors.Is(err, wire.ErrLimit) {
		t.Fatal("configured front limit was ignored", err)
	}
	if _, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, uint32(len(front))); err != nil {
		t.Fatal("equality with configured front limit should succeed", err)
	}
}

func TestDecodeLogMalformedSecondEntryPublishesNothing(t *testing.T) {
	front := logLiteral(t, logVector)
	front = append(front, front[38:len(front)-1]...)
	binary.LittleEndian.PutUint32(front[34:], 2)
	batch, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, 4096)
	if !errors.Is(err, ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) || !reflect.DeepEqual(batch, LogBatch{}) {
		t.Fatal("valid first entry escaped a malformed atomic batch", batch, err)
	}
}

func TestDecodeLogCompatibleExtensionsAndRawTimestamp(t *testing.T) {
	front := logLiteral(t, logVector)
	front[38] = 6 // Future entry, compatible with v5.
	binary.LittleEndian.PutUint32(front[40:], 146)
	front = append(front, 0xaa, 0xbb)
	binary.LittleEndian.PutUint32(front[153:], ^uint32(0))
	batch, err := DecodeLog(MessageData{Type: LogMessage, Version: 2, CompatVersion: 1, Front: front}, 4096)
	if err != nil || len(batch.Entries) != 1 || batch.Entries[0].Message != "\xff\x00hello\n" || batch.Entries[0].Nanoseconds != ^uint32(0) {
		t.Fatal("compatible envelope extension or raw timestamp was normalized/rejected", batch, err)
	}
}

func TestDecodeLogEmptyAndDefaultSourceEntries(t *testing.T) {
	front := logLiteral(t, `0100000000000000 ffff 0000000000000000 000102030405060708090a0b0c0d0e0f 00000000`)
	batch, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, 4096)
	if err != nil || batch.Version != 1 || len(batch.Entries) != 0 {
		t.Fatal("empty MLog was rejected", batch, err)
	}
	// LogMonitor's trimmed-history warning has default name/rank/address/channel;
	// default identity metadata is not evidence of a malformed entry.
	entry := logLiteral(t, `
05 05 30000000
00000000 00000000 00 0000000000000000 02 00000000
01000000 02000000 0000000000000000 0300 00000000 00000000
`)
	front = append(front, entry...)
	binary.LittleEndian.PutUint32(front[34:], 1)
	batch, err = DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: front}, 4096)
	if err != nil || len(batch.Entries) != 1 || batch.Entries[0].NameID != "" || len(batch.Entries[0].Addresses) != 0 || batch.Entries[0].Priority != 3 || batch.Entries[0].Channel != "" {
		t.Fatal("default source metadata lost the MON trim warning", batch, err)
	}
	// Even a count with sufficient physical bytes cannot allocate an unbounded
	// collection. The repeated minimum entries isolate the count cap from EOF.
	large := append([]byte(nil), front[:38]...)
	binary.LittleEndian.PutUint32(large[34:], 65537)
	large = append(large, []byte(strings.Repeat(string(entry), 65537))...)
	if batch, err := DecodeLog(MessageData{Type: LogMessage, Version: 1, Front: large}, wire.DefaultLimit); !errors.Is(err, wire.ErrLimit) || !errors.Is(err, ErrFrame) || !reflect.DeepEqual(batch, LogBatch{}) {
		t.Fatal("oversized entry collection bypassed its independent count bound", err)
	}
}

func TestLogSubscribeIndependentVector(t *testing.T) {
	m := LogSubscribe("info", 0x1122334455667788)
	want := logLiteral(t, `
03000000
08000000 6c6f672d696e666f 8877665544332211 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00
00000000
`)
	if m.Type != SubscribeMessage || m.Version != 3 || m.CompatVersion != 1 || m.Priority != 127 || m.Transaction != 0 || !reflect.DeepEqual(m.Front, want) || len(m.Middle) != 0 || len(m.Data) != 0 {
		t.Fatal("continuous log subscription lost lexical order, full cursor or map subscriptions", m)
	}
	plain := Subscribe(0x11223344, 0x55667788)
	want = logLiteral(t, `
02000000
06000000 6d67726d6170 8877665500000000 00
06000000 6d6f6e6d6170 4433221100000000 00
00000000
`)
	if !reflect.DeepEqual(plain.Front, want) || plain.Version != 3 || plain.CompatVersion != 1 {
		t.Fatal("adding logs changed existing map-only subscriptions", plain)
	}
}
