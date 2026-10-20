package osd

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// dirtyEnvelope encodes a single standalone op and returns the packed 38-byte
// per-op envelope and the ordered data segment. start=113 is the native
// request-v6 front length for oracleRequest's locator/object (see
// mutations_test.go).
func dirtyEnvelope(t *testing.T, op Operation) (modes uint32, env, data []byte) {
	t.Helper()
	r := oracleRequest()
	r.Operations = []Operation{op}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	const start = 113
	if binary.LittleEndian.Uint16(m.Front[start-2:start]) != 1 {
		t.Fatal("op count")
	}
	return binary.LittleEndian.Uint32(m.Front[8:12]), m.Front[start : start+opWireSize], m.Data
}

// ISDIRTY and UNDIRTY encode as inputless ops: zero union, zero payload and no
// data. ISDIRTY carries only the read mode bit, UNDIRTY only write.
func TestDirtyOperationsEncodeInputless(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   uint16
		mode   uint64
		opcode uint16
	}{
		{"isdirty", Isdirty, FlagRead, 0x121d},
		{"undirty", Undirty, FlagWrite, 0x221c},
	} {
		t.Run(tc.name, func(t *testing.T) {
			modes, env, data := dirtyEnvelope(t, Operation{Code: tc.code})
			if modes != uint32(tc.mode|FlagOnDisk|FlagReturnVec) {
				t.Fatalf("request mode %#x", modes)
			}
			if binary.LittleEndian.Uint16(env[0:2]) != tc.opcode {
				t.Fatal("opcode")
			}
			if binary.LittleEndian.Uint32(env[2:6]) != 0 {
				t.Fatal("op flags must be zero")
			}
			// Union: offset u64, length u64, truncate size u64, truncate seq u32.
			for off, width := range map[int]int{6: 8, 14: 8, 22: 8, 30: 4} {
				if !allZero(env[off : off+width]) {
					t.Fatalf("union bytes at %d must be zero", off)
				}
			}
			if binary.LittleEndian.Uint32(env[34:38]) != 0 {
				t.Fatal("payload length must be zero")
			}
			if len(data) != 0 {
				t.Fatal("inputless op must contribute no data")
			}
		})
	}
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func TestDirtyOperationsValidationModes(t *testing.T) {
	for _, tc := range []struct {
		code uint16
		mode uint64
	}{{Isdirty, FlagRead}, {Undirty, FlagWrite}} {
		mode, total, err := ValidateOperations([]Operation{{Code: tc.code}}, wire.DefaultLimit)
		if err != nil || mode != tc.mode || total != 0 {
			t.Fatalf("standalone %#x: mode=%#x total=%d err=%v", tc.code, mode, total, err)
		}
	}
}

// Every input-bearing or compounded shape is refused before transmission.
func TestDirtyOperationsRejectExtras(t *testing.T) {
	for _, code := range []uint16{Isdirty, Undirty} {
		for name, ops := range map[string][]Operation{
			"compound-after":  {{Code: code}, {Code: Stat}},
			"compound-before": {{Code: Stat}, {Code: code}},
			"flags":           {{Code: code, Flags: OpFailOK}},
			"excl":            {{Code: code, Flags: OpExclusive}},
			"offset":          {{Code: code, Offset: 1}},
			"length":          {{Code: code, Length: 1}},
			"data":            {{Code: code, Data: []byte{1}}},
			"version":         {{Code: code, Version: 1}},
			"truncate":        {{Code: code, TruncateSize: 1, TruncateSequence: 2}},
			"xattr":           {{Code: code, Xattr: XattrInput{Name: "k"}}},
		} {
			if _, _, err := ValidateOperations(ops, wire.DefaultLimit); !errors.Is(err, ErrOperation) && !errors.Is(err, ErrRequest) {
				t.Fatalf("%#x %s must be refused, got %v", code, name, err)
			}
		}
	}
}
