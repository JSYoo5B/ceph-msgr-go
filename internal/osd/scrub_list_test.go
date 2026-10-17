package osd

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestScrubListArgumentAndEnvelope(t *testing.T) {
	r := ScrubListRequest{ClientInc: 1, MapEpoch: 11, PG: PG{Pool: 7, Seed: 3}, Interval: 9, Snapsets: true, StartAfter: ScrubObject{Name: "o", Namespace: "n", Snap: 4}, MaxReturn: 5, MaxOutputBytes: 1024}
	want := []byte{1, 1, 34, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 'o', 1, 0, 0, 0, 'n', 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0}
	if got := scrubListArg(r); !bytes.Equal(got, want) {
		t.Fatalf("scrub_ls_arg_t %x", got)
	}
	m, err := EncodeScrubListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(m.Data, want) {
		t.Fatal("SCRUBLS indata", err)
	}
	// The single operation is SCRUBLS with a zero union and the argument length.
	plain := PGListRequest{ClientInc: 1, MapEpoch: 11, PG: r.PG, Count: 1, MaxOutputBytes: 1024}
	listing, err := EncodePGListRequestV8(plain, v8OracleFeatures, wire.DefaultLimit)
	if err != nil || len(listing.Front) != len(m.Front) {
		t.Fatal(err)
	}
	if bytes.Contains(m.Front, []byte{0x05, 0x15}) || !bytes.Contains(m.Front, []byte{0x07, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("SCRUBLS operation code or zero union missing")
	}
	for _, edit := range []func(*ScrubListRequest){func(r *ScrubListRequest) { r.MaxReturn = 0 }, func(r *ScrubListRequest) { r.MaxReturn = 1 << 32 }} {
		bad := r
		edit(&bad)
		if _, err := EncodeScrubListRequest(bad, oracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
			t.Fatal("invalid scrub listing admitted", err)
		}
	}
	if _, _, err := ValidateOperations([]Operation{{Code: ScrubList}}, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
		t.Fatal("SCRUBLS admitted in an object vector", err)
	}
}

func TestScrubListResultDecodingAndReply(t *testing.T) {
	b := wire.Encoder{}
	b.U32(42)
	b.U32(2)
	b.Bytes([]byte{1, 2, 3})
	b.Bytes(nil)
	e := wire.Encoder{}
	e.Struct(1, 1, b.Data)
	page, err := DecodeScrubList(e.Data, 2)
	if err != nil || page.Interval != 42 || len(page.Values) != 2 || !bytes.Equal(page.Values[0], []byte{1, 2, 3}) || len(page.Values[1]) != 0 {
		t.Fatal("scrub_ls_result_t", page, err)
	}
	if _, err := DecodeScrubList(e.Data, 1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("value count above MaxReturn admitted", err)
	}
	r := ScrubListRequest{MapEpoch: 1, PG: PG{Pool: 7, Seed: 3}, MaxReturn: 2, MaxOutputBytes: 1024}
	op := OperationResult{Code: ScrubList, PayloadLength: uint32(len(e.Data)), Data: e.Data}
	if err := ValidateScrubListReply(r, Reply{PG: r.PG, Operations: []OperationResult{op}}, false); err != nil {
		t.Fatal(err)
	}
	refused := Reply{PG: r.PG, Result: -2, Operations: []OperationResult{{Code: ScrubList}}}
	if err := ValidateScrubListReply(r, refused, false); err != nil {
		t.Fatal("ENOENT refusal", err)
	}
	refused.Operations[0].Code = PGNLS
	if err := ValidateScrubListReply(r, refused, false); err == nil {
		t.Fatal("a foreign opcode was accepted")
	}
}
