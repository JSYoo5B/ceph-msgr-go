package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// A filtered listing differs from PGNLS only in the opcode, the indata length
// and the indata, which carries "pg", "filter", the filter name and its raw
// input before the cursor (Objecter add_pgls_filter).
func TestPGListFilterEncodesObjecterLayout(t *testing.T) {
	input := []byte{3, 0, 0, 0, '_', 'k', 'x', 1, 0, 0, 0, 'v'}
	for name, encode := range map[string]func(PGListRequest) (msgr.MessageData, error){
		"v6": func(r PGListRequest) (msgr.MessageData, error) {
			return EncodePGListRequest(r, oracleFeatures, wire.DefaultLimit)
		},
		"v8": func(r PGListRequest) (msgr.MessageData, error) {
			return EncodePGListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
		},
	} {
		plain := pgListNativeRequest()
		filtered := plain
		filtered.Filter = &PGListFilter{Name: "plain", Input: input}
		a, err := encode(plain)
		if err != nil {
			t.Fatal(name, err)
		}
		b, err := encode(filtered)
		if err != nil {
			t.Fatal(name, err)
		}
		want := wire.Encoder{}
		want.String("pg")
		want.String("filter")
		want.String("plain")
		want.Raw(input)
		want.Raw(initialPGCursor())
		if !bytes.Equal(b.Data, want.Data) || !bytes.Equal(a.Data, initialPGCursor()) {
			t.Fatalf("%s: indata %x", name, b.Data)
		}
		head := binary.LittleEndian.AppendUint16(nil, PGNLS)
		head = binary.LittleEndian.AppendUint32(head, 0)
		head = binary.LittleEndian.AppendUint64(head, plain.Count)
		at := bytes.Index(a.Front, head)
		if at < 0 || len(a.Front) != len(b.Front) {
			t.Fatal(name, "operation not found")
		}
		expected := bytes.Clone(a.Front)
		binary.LittleEndian.PutUint16(expected[at:], PGNLSFilter)
		binary.LittleEndian.PutUint32(expected[at+34:], uint32(len(want.Data)))
		if !bytes.Equal(expected, b.Front) {
			t.Fatalf("%s: front differs beyond opcode and length", name)
		}
	}
}

func TestPGListFilterRefusesInvalidNamesAndForeignReplies(t *testing.T) {
	r := pgListNativeRequest()
	for _, bad := range []string{"", strings.Repeat("x", MaxPGListFilterName+1), "pl\x00ain"} {
		r.Filter = &PGListFilter{Name: bad}
		if _, err := EncodePGListRequest(r, oracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
			t.Fatalf("filter name %q admitted: %v", bad, err)
		}
	}
	r.Filter = &PGListFilter{Name: "plain"}
	op := OperationResult{Code: PGNLS, Offset: r.Count, Length: uint64(r.StartEpoch)}
	reply := Reply{PG: r.PG, Result: -22, Operations: []OperationResult{op}}
	if err := ValidatePGListReply(r, reply, false); err == nil {
		t.Fatal("a PGNLS reply was accepted for a filtered request")
	}
	reply.Operations[0].Code = PGNLSFilter
	if err := ValidatePGListReply(r, reply, false); err != nil {
		t.Fatal("filtered refusal", err)
	}
	if _, _, err := ValidateOperations([]Operation{{Code: PGNLSFilter}}, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
		t.Fatal("PGNLS_FILTER admitted in an object vector", err)
	}
}
