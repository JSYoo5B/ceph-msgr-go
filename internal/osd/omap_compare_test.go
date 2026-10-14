package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived literals follow the pinned Objecter.h omap_cmp builder and
// denc map/pair encoding. Fresh native encoder vectors are separate evidence.
func TestOmapCompareSourceLiteralEncodingAndCallerOwnership(t *testing.T) {
	assertions := []OmapAssertion{{Key: "z", Value: []byte("Z"), Comparison: OmapCompareEqual}, {Key: "", Comparison: OmapCompareEqual}, {Key: "\x00", Value: []byte{0x80}, Comparison: OmapCompareLess}}
	original := append([]OmapAssertion(nil), assertions...)
	r := oracleRequest()
	r.Operations = []Operation{{Code: OmapCompare, Omap: OmapInput{Assertions: assertions}}}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("OMAP comparison changed mode/completion flags")
	}
	if !bytes.Equal(m.Front[113:151], xattrLiteral(t, "1912 00000000 0000000000000000 2c00000000000000 000000000000000000000000 2c000000")) {
		t.Fatal("native-source OMAP comparison envelope differs")
	}
	want := xattrLiteral(t, "03000000 00000000 00000000 01000000 01000000 00 01000000 80 05000000 01000000 7a 01000000 5a 01000000")
	if !bytes.Equal(m.Data, want) || !reflect.DeepEqual(assertions, original) {
		t.Fatal("comparison input bytes/order changed or caller assertions sorted in place")
	}
	assertions[0].Value[0] = 'X'
	if !bytes.Equal(m.Data, want) {
		t.Fatal("encoded OMAP comparison retained caller memory")
	}
	r.Operations = []Operation{{Code: OmapCompare}}
	if m, err = EncodeRequest(r, oracleFeatures, 1024); err != nil || !bytes.Equal(m.Data, xattrLiteral(t, "00000000")) {
		t.Fatal("empty existence-only assertion set was not transmitted", err)
	}
}

func TestOmapCompareAdmissionBoundsAndVariantIsolation(t *testing.T) {
	valid := OmapAssertion{Key: "k", Value: []byte{1}, Comparison: OmapCompareGreater}
	for _, tc := range []struct {
		name string
		op   Operation
		want error
	}{
		{"output budget", Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{valid}, MaxOutputBytes: 1}}, ErrRequest},
		{"keys variant", Operation{Code: OmapCompare, Omap: OmapInput{Keys: []string{}}}, ErrRequest},
		{"entries variant", Operation{Code: OmapCompare, Omap: OmapInput{Entries: []OmapEntry{}}}, ErrRequest},
		{"header variant", Operation{Code: OmapCompare, Omap: OmapInput{Header: []byte{}}}, ErrRequest},
		{"cursor variant", Operation{Code: OmapCompare, Omap: OmapInput{StartAfter: "a"}}, ErrRequest},
		{"caller data", Operation{Code: OmapCompare, Data: []byte{1}}, ErrRequest},
		{"caller extent", Operation{Code: OmapCompare, Length: 16}, ErrRequest},
		{"assertions on page", Operation{Code: OmapGetKeys, Omap: OmapInput{Assertions: []OmapAssertion{}, MaxOutputBytes: 8}}, ErrRequest},
		{"assertions on set", Operation{Code: OmapSetValues, Omap: OmapInput{Assertions: []OmapAssertion{}}}, ErrRequest},
		{"duplicate keys", Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{valid, {Key: "k", Comparison: OmapCompareEqual}}}}, ErrRequest},
		{"exclusive flag", Operation{Code: OmapCompare, Flags: OpExclusive}, ErrOperation},
		{"large value", Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "k", Value: make([]byte, 1024), Comparison: OmapCompareEqual}}}}, wire.ErrLimit},
		{"large count", Operation{Code: OmapCompare, Omap: OmapInput{Assertions: func() []OmapAssertion {
			// Unique valid assertions, so only the encoded size is invalid.
			assertions := make([]OmapAssertion, 90)
			for i := range assertions {
				assertions[i] = OmapAssertion{Key: string([]byte{byte(i)}), Comparison: OmapCompareEqual}
			}
			return assertions
		}()}}, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := oracleRequest()
			r.Operations = []Operation{tc.op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid OMAP comparison admitted", err)
			}
		})
	}
	// NE/GTE/LTE exist for xattr but reach EINVAL in the OMAP handler.
	for _, id := range []int32{0, 2, 4, 6, 7, -1, 1 << 30} {
		op := Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{valid, {Key: "z", Comparison: id}}}}
		if _, _, err := ValidateOperations([]Operation{op}, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("unsupported OMAP comparison identifier admitted", id, err)
		}
	}
	for _, id := range []int32{OmapCompareEqual, OmapCompareGreater, OmapCompareLess} {
		op := Operation{Code: OmapCompare, Flags: OpFailOK, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "", Comparison: id}}}}
		if flags, _, err := ValidateOperations([]Operation{op}, 1024); err != nil || flags != FlagRead {
			t.Fatal("supported OMAP comparison rejected or changed mode", id, err)
		}
	}
}

func TestOmapCompareOrderingAgainstStagedWrites(t *testing.T) {
	compare := Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "k", Comparison: OmapCompareEqual}}}}
	set := Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k"}}}}
	clear := Operation{Code: OmapClear, Flags: OpFailOK}
	for _, ops := range [][]Operation{{set, compare}, {clear, compare}, {{Code: Delete}, compare}, {set, {Code: Stat}, compare}} {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("OMAP comparison after a staged OMAP write/DELETE admitted", err)
		}
	}
	for _, ops := range [][]Operation{{compare, set}, {compare, clear}, {compare, {Code: Delete}}, {compare, {Code: Write, Length: 1, Data: []byte{1}}}} {
		if flags, _, err := ValidateOperations(ops, 1024); err != nil || flags != FlagRead|FlagWrite {
			t.Fatal("compare-before-write guard rejected", err)
		}
	}
}

func TestOmapCompareReplyValidation(t *testing.T) {
	op := Operation{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "k", Value: []byte{1}, Comparison: OmapCompareLess}}}}
	valid := syntheticOmapReply(t, op, nil)
	if valid.Length != 18 {
		t.Fatal("comparison input length differs from encoded map", valid.Length)
	}
	if err := ValidateOmapReply(op, valid, true); err != nil {
		t.Fatal("successful comparison rejected", err)
	}
	for _, code := range []int32{-125, -2, -22} {
		failed := valid
		failed.Result = code
		if err := ValidateOmapReply(op, failed, false); err != nil {
			t.Fatal("known comparison result rejected", code, err)
		}
	}
	for _, edit := range []func(*OperationResult){
		func(r *OperationResult) { r.PayloadLength, r.Data = 1, []byte{1} },
		func(r *OperationResult) { r.Length = 0 },
		func(r *OperationResult) { r.Offset = 1 },
		func(r *OperationResult) { r.Code = OmapGetValuesByKeys },
	} {
		result := valid
		edit(&result)
		if err := ValidateOmapReply(op, result, true); err == nil {
			t.Fatal("contradictory comparison reply accepted")
		}
	}
}
