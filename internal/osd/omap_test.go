package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived literals are synthetic codec evidence. Fresh native encoder
// and actual OSD interoperability evidence must be recorded separately.
func TestOmapSourceLiteralEncodingAndCallerKeyOwnership(t *testing.T) {
	keys := []string{"z", "\x00", "", "\xff"}
	original := append([]string(nil), keys...)
	r := oracleRequest()
	r.Operations = []Operation{
		{Code: OmapGetKeys, Omap: OmapInput{StartAfter: "\x00\xff", MaxEntries: math.MaxUint64, MaxOutputBytes: 64}},
		{Code: OmapGetValues, Flags: OpFailOK, Omap: OmapInput{StartAfter: "a\x00", Prefix: "b\xff", MaxEntries: 2, MaxOutputBytes: 64}},
		{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: keys, MaxOutputBytes: 64}},
	}
	m, err := EncodeRequest(r, oracleFeatures, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("OMAP page changed mode/completion flags")
	}
	reserved := "000000000000000000000000"
	for i, literal := range []string{
		"1112 00000000 0000000000000000 0e00000000000000 " + reserved + " 0e000000",
		"1212 02000000 0000000000000000 1400000000000000 " + reserved + " 14000000",
		"1412 00000000 0000000000000000 1700000000000000 " + reserved + " 17000000",
	} {
		if !bytes.Equal(m.Front[113+i*38:113+(i+1)*38], xattrLiteral(t, literal)) {
			t.Fatal("native-source envelope length/union differs", i)
		}
	}
	want := xattrLiteral(t, "02000000 00ff ffffffffffffffff 02000000 6100 0200000000000000 02000000 62ff 04000000 00000000 01000000 00 01000000 7a 01000000 ff")
	if !bytes.Equal(m.Data, want) || !reflect.DeepEqual(keys, original) {
		t.Fatal("input bytes/order changed or caller keys sorted in place")
	}
	keys[0] = "changed"
	if !bytes.Equal(m.Data, want) {
		t.Fatal("encoded OMAP request retained caller slice")
	}
}

func TestOmapAdmissionBoundsAndVariantIsolation(t *testing.T) {
	base := Operation{Code: OmapGetValues, Omap: OmapInput{MaxEntries: 1, MaxOutputBytes: 64}}
	for _, tc := range []struct {
		name string
		edit func(*Operation)
		want error
	}{
		{"zero budget", func(op *Operation) { op.Omap.MaxOutputBytes = 0 }, ErrRequest},
		{"missing more budget", func(op *Operation) { op.Omap.MaxOutputBytes = 4 }, ErrRequest},
		{"frame budget", func(op *Operation) { op.Omap.MaxOutputBytes = 1025 }, wire.ErrLimit},
		{"caller offset", func(op *Operation) { op.Offset = 1 }, ErrRequest},
		{"caller inputlength", func(op *Operation) { op.Length = 16 }, ErrRequest},
		{"version variant", func(op *Operation) { op.Version = 1 }, ErrRequest},
		{"caller data", func(op *Operation) { op.Data = []byte{0} }, ErrRequest},
		{"xattr variant", func(op *Operation) { op.Xattr.Name = "x" }, ErrRequest},
		{"keys on page", func(op *Operation) { op.Omap.Keys = []string{} }, ErrRequest},
		{"prefix on keys page", func(op *Operation) { op.Code = OmapGetKeys; op.Omap.Prefix = "p" }, ErrRequest},
		{"duplicate keys", func(op *Operation) {
			op.Code = OmapGetValuesByKeys
			op.Omap.MaxEntries = 0
			op.Omap.Keys = []string{"a", "b", "a"}
		}, ErrRequest},
		{"bykeys cursor", func(op *Operation) { op.Code = OmapGetValuesByKeys; op.Omap.MaxEntries = 0; op.Omap.StartAfter = "a" }, ErrRequest},
		{"bykeys count", func(op *Operation) { op.Code = OmapGetValuesByKeys }, ErrRequest},
		{"bykeys prefix", func(op *Operation) { op.Code = OmapGetValuesByKeys; op.Omap.MaxEntries = 0; op.Omap.Prefix = "p" }, ErrRequest},
		{"EXCL", func(op *Operation) { op.Flags = OpExclusive }, ErrOperation},
		{"unknown flags", func(op *Operation) { op.Flags = 4 }, ErrOperation},
		{"unsupported tmap conversion", func(op *Operation) { op.Code = 0x3222 }, ErrOperation},
		{"unsupported opcode", func(op *Operation) { op.Code = 0xffff }, ErrOperation},
		{"large cursor", func(op *Operation) { op.Omap.StartAfter = strings.Repeat("x", 1024) }, wire.ErrLimit},
		{"large keyset", func(op *Operation) {
			op.Code = OmapGetValuesByKeys
			op.Omap.MaxEntries = 0
			op.Omap.Keys = make([]string, 256)
		}, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := base
			tc.edit(&op)
			r := oracleRequest()
			r.Operations = []Operation{op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid input admitted", err)
			}
		})
	}
	for _, code := range []uint16{Read, Stat, SetXattr, GetXattrs} {
		op := Operation{Code: code, Omap: OmapInput{Keys: []string{}}}
		if _, _, err := ValidateOperations([]Operation{op}, 1024); !errors.Is(err, ErrRequest) {
			t.Fatal("old operation admitted nonzero OMAP variant", code, err)
		}
	}
	for _, code := range []uint16{OmapGetKeys, OmapGetValues, OmapGetValuesByKeys} {
		r := oracleRequest()
		r.Operations = []Operation{{Code: code, Omap: OmapInput{MaxOutputBytes: 5}}}
		if _, err := EncodeRequest(r, oracleFeatures, 1024); err != nil {
			t.Fatal("zero requested count/empty keys rejected", code, err)
		}
	}
	r := oracleRequest()
	r.Operations = []Operation{base, base}
	r.Operations[0].Omap.MaxOutputBytes = 200
	r.Operations[1].Omap.MaxOutputBytes = 200
	if _, err := EncodeRequest(r, oracleFeatures, 600); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate reply budgets escaped frame limit", err)
	}
}

func syntheticOmapReply(t testing.TB, request Operation, data []byte) OperationResult {
	t.Helper()
	size, err := omapInputBytes(request, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	return OperationResult{Code: request.Code, Length: size, PayloadLength: uint32(len(data)), Data: data}
}

func TestOmapSyntheticSuccessfulCollectionValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   Operation
		hex  string
	}{
		{"keys binary ordered more", Operation{Code: OmapGetKeys, Omap: OmapInput{MaxEntries: 3, MaxOutputBytes: 64}}, "03000000 01000000 00 02000000 6100 01000000 ff 01"},
		{"values prefix", Operation{Code: OmapGetValues, Omap: OmapInput{StartAfter: "a", Prefix: "b", MaxEntries: 2, MaxOutputBytes: 64}}, "02000000 01000000 62 00000000 02000000 6200 02000000 00ff 00"},
		{"bykeys empty missing and binary", Operation{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: []string{"missing", "\xff", "", "\x00"}, MaxOutputBytes: 64}}, "03000000 00000000 00000000 01000000 00 02000000 00ff 01000000 ff 00000000"},
		{"zero keys page retains true more", Operation{Code: OmapGetKeys, Omap: OmapInput{MaxOutputBytes: 5}}, "00000000 01"},
		{"zero values page retains false more", Operation{Code: OmapGetValues, Omap: OmapInput{MaxOutputBytes: 5}}, "00000000 00"},
		{"empty bykeys", Operation{Code: OmapGetValuesByKeys, Omap: OmapInput{MaxOutputBytes: 4}}, "00000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := xattrLiteral(t, tc.hex)
			op := syntheticOmapReply(t, tc.op, data)
			before := bytes.Clone(data)
			if err := ValidateOmapReply(tc.op, op, true); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, before) {
				t.Fatal("validation changed raw collection output")
			}
		})
	}
}

func TestOmapSyntheticMalformedOutputAndNegativeReply(t *testing.T) {
	request := Operation{Code: OmapGetKeys, Omap: OmapInput{MaxEntries: 2, MaxOutputBytes: 64}}
	for _, literal := range []string{
		"00000000", // Tentacle requires more suffix, never older compatibility fallback.
		"00000000 02", "00000000 00 00", "01000000 ffffffff 00",
		"02000000 01000000 62 01000000 61 00",             // descending
		"02000000 01000000 61 01000000 61 00",             // duplicate
		"01000000 00000000 00",                            // empty key excluded by empty cursor
		"03000000 01000000 61 01000000 62 01000000 63 00", // exceeds requested count
	} {
		if err := ValidateOmapReply(request, syntheticOmapReply(t, request, xattrLiteral(t, literal)), true); err == nil {
			t.Fatal("malformed keys output admitted", literal)
		}
	}
	values := Operation{Code: OmapGetValues, Omap: OmapInput{StartAfter: "a", Prefix: "b", MaxEntries: 1, MaxOutputBytes: 64}}
	for _, literal := range []string{"01000000 01000000 61 00000000 00", "01000000 01000000 63 00000000 00", "01000000 01000000 62 ffffffff 00", "01000000 01000000 62 00000000"} {
		if err := ValidateOmapReply(values, syntheticOmapReply(t, values, xattrLiteral(t, literal)), true); err == nil {
			t.Fatal("malformed filtered map admitted", literal)
		}
	}
	bykeys := Operation{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: []string{"a"}, MaxOutputBytes: 64}}
	for _, literal := range []string{"00000000 00", "01000000 01000000 62 00000000", "02000000 01000000 61 00000000 01000000 61 00000000"} {
		if err := ValidateOmapReply(bykeys, syntheticOmapReply(t, bykeys, xattrLiteral(t, literal)), true); err == nil {
			t.Fatal("malformed bykeys map admitted", literal)
		}
	}
	valid := syntheticOmapReply(t, request, xattrLiteral(t, "00000000 00"))
	for _, edit := range []func(*OperationResult){func(r *OperationResult) { r.Offset = 1 }, func(r *OperationResult) { r.Length++ }, func(r *OperationResult) { r.TruncateSize = 1 }, func(r *OperationResult) { r.TruncateSequence = 1 }, func(r *OperationResult) { r.Xattr.NameLength = 1 }, func(r *OperationResult) { r.Code = OmapGetValues }, func(r *OperationResult) { r.PayloadLength++ }} {
		result := valid
		edit(&result)
		if err := ValidateOmapReply(request, result, true); err == nil {
			t.Fatal("contradictory union/payload accepted")
		}
	}
	negative := syntheticOmapReply(t, request, nil)
	negative.Result = -2
	if err := ValidateOmapReply(request, negative, false); err != nil || negative.Result != -2 {
		t.Fatal("known failure acquired invented collection requirement", err)
	}
	negative.Length = 0
	if err := ValidateOmapReply(request, negative, false); err == nil {
		t.Fatal("known failure lost union validation")
	}
	budget := request
	budget.Omap.MaxOutputBytes = 5
	if err := ValidateOmapReply(budget, syntheticOmapReply(t, budget, xattrLiteral(t, "01000000 01000000 61 00")), true); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("local output cap was ignored", err)
	}
}

// Seeds here are source-derived synthetic collection bytes. Native seeds are a
// separate follow-up; this fuzz target verifies bounded rejection, not Ceph
// interoperability or remote pagination behavior.
func FuzzOmapReplyBounds(f *testing.F) {
	for _, data := range [][]byte{
		{0, 0, 0, 0, 0}, {0, 0, 0, 0, 1}, {0, 0, 0, 0},
		{1, 0, 0, 0, 1, 0, 0, 0, 'a', 0},
		{1, 0, 0, 0, 1, 0, 0, 0, 'a', 0, 0, 0, 0},
		{255, 255, 255, 255},
	} {
		f.Add(uint8(0), data)
		f.Add(uint8(1), data)
		f.Add(uint8(2), data)
		f.Add(uint8(3), data)
	}
	f.Fuzz(func(t *testing.T, variant uint8, data []byte) {
		request := Operation{Omap: OmapInput{MaxOutputBytes: 64}}
		switch variant % 4 {
		case 0:
			request.Code, request.Omap.MaxEntries = OmapGetKeys, 4
		case 1:
			request.Code, request.Omap.MaxEntries = OmapGetValues, 4
		case 2:
			request.Code, request.Omap.Keys = OmapGetValuesByKeys, []string{"", "\x00", "a", "\xff"}
		case 3:
			request.Code = OmapGetHeader
		}
		result := syntheticOmapReply(t, request, data)
		_ = ValidateOmapReply(request, result, true)
	})
}
