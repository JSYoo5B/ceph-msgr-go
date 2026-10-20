package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Source-derived literals follow pinned Objecter.h builders and rados.h fields.
// They are synthetic codec evidence; fresh native encoder vectors are separate.
func TestOmapWriteSourceLiteralEncodingAndCallerOwnership(t *testing.T) {
	entries := []OmapEntry{{Key: "z", Value: []byte("Z")}, {Key: ""}, {Key: "\xff", Value: []byte{0}}, {Key: "\x00", Value: []byte{0xff, 0x80}}}
	original := append([]OmapEntry(nil), entries...)
	keys := []string{"b", "\x00", "a"}
	header := []byte{0, 0xff, 'H'}
	r := oracleRequest()
	r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 5
	r.Operations = []Operation{
		{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 64}},
		{Code: OmapSetValues, Omap: OmapInput{Entries: entries}},
		{Code: OmapSetHeader, Omap: OmapInput{Header: header}},
		{Code: OmapRemoveKeys, Omap: OmapInput{Keys: keys}},
		{Code: OmapClear, Flags: OpFailOK},
	}
	m, err := EncodeRequest(r, oracleFeatures, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagWrite|FlagOnDisk|FlagReturnVec) {
		t.Fatal("OMAP header/write vector changed mode/completion flags")
	}
	reserved := "000000000000000000000000"
	for i, literal := range []string{
		"1312 00000000 0000000000000000 0000000000000000 " + reserved + " 00000000",
		"1522 00000000 0000000000000000 2b00000000000000 " + reserved + " 2b000000",
		"1622 00000000 0000000000000000 0300000000000000 " + reserved + " 03000000",
		"1822 00000000 0000000000000000 1300000000000000 " + reserved + " 13000000",
		"1722 02000000 0000000000000000 0000000000000000 " + reserved + " 00000000",
	} {
		if !bytes.Equal(m.Front[113+i*38:113+(i+1)*38], xattrLiteral(t, literal)) {
			t.Fatal("native-source OMAP header/write envelope differs", i)
		}
	}
	want := xattrLiteral(t, "04000000 00000000 00000000 01000000 00 02000000 ff80 01000000 7a 01000000 5a 01000000 ff 01000000 00"+
		" 00ff48"+
		" 03000000 01000000 00 01000000 61 01000000 62")
	if !bytes.Equal(m.Data, want) || !reflect.DeepEqual(entries, original) || !reflect.DeepEqual(keys, []string{"b", "\x00", "a"}) {
		t.Fatal("OMAP mutation input bytes/order changed or caller slices sorted in place")
	}
	entries[0].Value[0], header[0], keys[0] = 'X', 'X', "changed"
	if !bytes.Equal(m.Data, want) {
		t.Fatal("encoded OMAP mutation retained caller memory")
	}
}

func TestOmapWriteEmptyVariantsRemainExplicitInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   Operation
		data string
	}{
		{"nil SETVALS map", Operation{Code: OmapSetValues}, "00000000"},
		{"empty SETVALS map", Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{}}}, "00000000"},
		{"empty value is not removal", Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k"}}}}, "01000000 01000000 6b 00000000"},
		{"nil SETHEADER", Operation{Code: OmapSetHeader}, ""},
		{"empty SETHEADER is not CLEAR", Operation{Code: OmapSetHeader, Omap: OmapInput{Header: []byte{}}}, ""},
		{"nil RMKEYS set", Operation{Code: OmapRemoveKeys}, "00000000"},
		{"CLEAR", Operation{Code: OmapClear}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := oracleRequest()
			r.Operations = []Operation{tc.op}
			m, err := EncodeRequest(r, oracleFeatures, 1024)
			if err != nil {
				t.Fatal(err)
			}
			want := xattrLiteral(t, tc.data)
			if !bytes.Equal(m.Data, want) || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec) {
				t.Fatal("empty OMAP mutation input/mode differs")
			}
			if got := binary.LittleEndian.Uint64(m.Front[113+14 : 113+22]); got != uint64(len(want)) {
				t.Fatal("empty OMAP mutation union length differs", got)
			}
		})
	}
}

func TestOmapWriteAdmissionBoundsAndVariantIsolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   Operation
		want error
	}{
		{"header zero budget", Operation{Code: OmapGetHeader}, ErrRequest},
		{"header frame budget", Operation{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 1025}}, wire.ErrLimit},
		{"header cursor", Operation{Code: OmapGetHeader, Omap: OmapInput{StartAfter: "a", MaxOutputBytes: 8}}, ErrRequest},
		{"header count", Operation{Code: OmapGetHeader, Omap: OmapInput{MaxEntries: 1, MaxOutputBytes: 8}}, ErrRequest},
		{"header keys", Operation{Code: OmapGetHeader, Omap: OmapInput{Keys: []string{}, MaxOutputBytes: 8}}, ErrRequest},
		{"header input", Operation{Code: OmapGetHeader, Omap: OmapInput{Header: []byte{}, MaxOutputBytes: 8}}, ErrRequest},
		{"write output budget", Operation{Code: OmapSetValues, Omap: OmapInput{MaxOutputBytes: 4}}, ErrRequest},
		{"clear output budget", Operation{Code: OmapClear, Omap: OmapInput{MaxOutputBytes: 1}}, ErrRequest},
		{"setvals keys", Operation{Code: OmapSetValues, Omap: OmapInput{Keys: []string{"a"}}}, ErrRequest},
		{"setvals header", Operation{Code: OmapSetValues, Omap: OmapInput{Header: []byte{1}}}, ErrRequest},
		{"setvals cursor", Operation{Code: OmapSetValues, Omap: OmapInput{StartAfter: "a"}}, ErrRequest},
		{"setheader entries", Operation{Code: OmapSetHeader, Omap: OmapInput{Entries: []OmapEntry{}}}, ErrRequest},
		{"rmkeys entries", Operation{Code: OmapRemoveKeys, Omap: OmapInput{Entries: []OmapEntry{}}}, ErrRequest},
		{"rmkeys count", Operation{Code: OmapRemoveKeys, Omap: OmapInput{MaxEntries: 1}}, ErrRequest},
		{"clear header", Operation{Code: OmapClear, Omap: OmapInput{Header: []byte{}}}, ErrRequest},
		{"clear keys", Operation{Code: OmapClear, Omap: OmapInput{Keys: []string{}}}, ErrRequest},
		{"setvals duplicate", Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "a"}, {Key: "b"}, {Key: "a", Value: []byte{1}}}}}, ErrRequest},
		{"rmkeys duplicate", Operation{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{"", "a", ""}}}, ErrRequest},
		{"setheader caller data", Operation{Code: OmapSetHeader, Data: []byte{1}}, ErrRequest},
		{"setheader caller extent", Operation{Code: OmapSetHeader, Length: 3, Omap: OmapInput{Header: []byte{1, 2, 3}}}, ErrRequest},
		{"setvals xattr variant", Operation{Code: OmapSetValues, Xattr: XattrInput{Name: "x"}}, ErrRequest},
		{"setvals version", Operation{Code: OmapSetValues, Version: 1}, ErrRequest},
		{"exclusive write", Operation{Code: OmapSetHeader, Flags: OpExclusive}, ErrOperation},
		{"unknown write flag", Operation{Code: OmapClear, Flags: 8}, ErrOperation},
		{"large header", Operation{Code: OmapSetHeader, Omap: OmapInput{Header: make([]byte, 1024)}}, wire.ErrLimit},
		{"large header beyond limit", Operation{Code: OmapSetHeader, Omap: OmapInput{Header: make([]byte, 1025)}}, wire.ErrLimit},
		{"large entry count", Operation{Code: OmapSetValues, Omap: OmapInput{Entries: func() []OmapEntry {
			entries := make([]OmapEntry, 200)
			for i := range entries {
				entries[i].Key = string(rune('a'+i%26)) + strings.Repeat("k", i/26)
			}
			return entries
		}()}}, wire.ErrLimit},
		{"large entry value", Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k", Value: make([]byte, 1024)}}}}, wire.ErrLimit},
		{"large removal key", Operation{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{strings.Repeat("k", 1024)}}}, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := oracleRequest()
			r.Operations = []Operation{tc.op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid OMAP header/write input admitted", err)
			}
		})
	}
	// Each base operation is otherwise valid, so only the OMAP variant fails.
	for _, base := range []Operation{{Code: Stat}, {Code: Write, Length: 1, Data: []byte{1}}, {Code: Create}, {Code: Delete}, {Code: Truncate, Offset: 1}} {
		if _, _, err := ValidateOperations([]Operation{base}, 1024); err != nil {
			t.Fatal("valid base operation rejected", base.Code, err)
		}
		for _, input := range []OmapInput{{Entries: []OmapEntry{}}, {Header: []byte{}}} {
			op := base
			op.Omap = input
			if _, _, err := ValidateOperations([]Operation{op}, 1024); !errors.Is(err, ErrRequest) {
				t.Fatal("non-OMAP operation admitted an OMAP mutation variant", base.Code, err)
			}
		}
	}
	r := oracleRequest()
	r.MTimeSeconds = 1
	r.Operations = []Operation{{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 8}}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrRequest) {
		t.Fatal("read-only header request admitted a mutation mtime", err)
	}
}

func TestOmapReadsAfterStagedOmapWritesAreRejected(t *testing.T) {
	header := Operation{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 8}}
	keys := Operation{Code: OmapGetKeys, Omap: OmapInput{MaxEntries: 1, MaxOutputBytes: 64}}
	values := Operation{Code: OmapGetValues, Omap: OmapInput{MaxEntries: 1, MaxOutputBytes: 64}}
	bykeys := Operation{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: []string{"a"}, MaxOutputBytes: 64}}
	set := Operation{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "a"}}}}
	setHeader := Operation{Code: OmapSetHeader, Omap: OmapInput{Header: []byte{1}}}
	remove := Operation{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{"a"}}}
	clear := Operation{Code: OmapClear}
	failOK := setHeader
	failOK.Flags = OpFailOK
	for _, ops := range [][]Operation{
		{set, header}, {setHeader, header}, {failOK, keys}, {clear, bykeys},
		{remove, {Code: Stat}, values}, {set, {Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}}, keys},
	} {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("OMAP read after a staged OMAP write was admitted", err)
		}
	}
	for _, ops := range [][]Operation{
		{{Code: Delete}, header}, {{Code: Delete}, {Code: Create}, keys},
		{{Code: Delete}, {Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 8}}},
	} {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("metadata read after a staged DELETE was admitted", err)
		}
	}
	for _, ops := range [][]Operation{
		{header, keys, values, bykeys, set, setHeader, remove, clear},
		{header, {Code: Delete}, {Code: Stat}},
		{clear, set, setHeader},
		{set, {Code: Stat}, {Code: Read, Length: 4}, {Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}}},
		{bykeys, {Code: Write, Offset: 4, Length: 1, Data: []byte{1}}, remove},
	} {
		flags, _, err := ValidateOperations(ops, 1024)
		if err != nil || flags&FlagWrite == 0 {
			t.Fatal("read-before-write or independent compound rejected", err)
		}
	}
}

func TestOmapHeaderAndWriteReplyValidation(t *testing.T) {
	header := Operation{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 3}}
	for _, data := range [][]byte{nil, {0}, {0, 0xff, 'H'}} {
		if err := ValidateOmapReply(header, syntheticOmapReply(t, header, data), true); err != nil {
			t.Fatal("raw header output rejected", data, err)
		}
	}
	if err := ValidateOmapReply(header, syntheticOmapReply(t, header, []byte{1, 2, 3, 4}), true); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("header local budget was ignored", err)
	}
	headerUnion := syntheticOmapReply(t, header, nil)
	headerUnion.Length = 1
	if err := ValidateOmapReply(header, headerUnion, true); err == nil {
		t.Fatal("header reply invented an input length")
	}
	for _, op := range []Operation{
		{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "a", Value: []byte{1}}}}},
		{Code: OmapSetHeader, Omap: OmapInput{Header: []byte{1, 2}}},
		{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{"a", "b"}}},
		{Code: OmapClear},
	} {
		valid := syntheticOmapReply(t, op, nil)
		if err := ValidateOmapReply(op, valid, true); err != nil {
			t.Fatal("successful OMAP mutation reply rejected", op.Code, err)
		}
		refused := valid
		refused.Result = -2
		if err := ValidateOmapReply(op, refused, false); err != nil {
			t.Fatal("known OMAP mutation refusal rejected", op.Code, err)
		}
		for _, edit := range []func(*OperationResult){
			func(r *OperationResult) { r.PayloadLength, r.Data = 1, []byte{0} },
			func(r *OperationResult) { r.Length++ },
			func(r *OperationResult) { r.Offset = 1 },
			func(r *OperationResult) { r.Code ^= 1 },
			func(r *OperationResult) { r.Xattr.ValueLength = 1 },
		} {
			result := valid
			edit(&result)
			if err := ValidateOmapReply(op, result, false); err == nil {
				t.Fatal("contradictory OMAP mutation reply accepted", op.Code)
			}
		}
	}
}

// Source-derived literal for Objecter.h omap_rm_range (two encoded strings).
func TestOmapRemoveKeyRangeEncodingAndAdmission(t *testing.T) {
	r := oracleRequest()
	r.MTimeSeconds = 1
	r.Operations = []Operation{{Code: OmapRemoveKeyRange, Flags: OpFailOK, Omap: OmapInput{RangeBegin: "b\x00", RangeEnd: "c\xff"}}}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec) ||
		!bytes.Equal(m.Front[113:151], xattrLiteral(t, "2c22 02000000 0000000000000000 0c00000000000000 000000000000000000000000 0c000000")) ||
		!bytes.Equal(m.Data, xattrLiteral(t, "02000000 6200 02000000 63ff")) {
		t.Fatal("range removal union/input differs from the native builder layout")
	}
	for _, input := range []OmapInput{{}, {RangeBegin: "a", RangeEnd: "a"}, {RangeEnd: "\xff"}} {
		r.Operations = []Operation{{Code: OmapRemoveKeyRange, Omap: input}}
		if _, err := EncodeRequest(r, oracleFeatures, 1024); err != nil {
			t.Fatal("empty or forward range rejected", input, err)
		}
	}
	for _, tc := range []struct {
		name string
		op   Operation
		want error
	}{
		{"reversed range", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeBegin: "b", RangeEnd: "a"}}, ErrRequest},
		{"reversed by prefix", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeBegin: "a\x00", RangeEnd: "a"}}, ErrRequest},
		{"output budget", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{MaxOutputBytes: 1}}, ErrRequest},
		{"keys variant", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{Keys: []string{}}}, ErrRequest},
		{"cursor variant", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{StartAfter: "a"}}, ErrRequest},
		{"range on another opcode", Operation{Code: OmapRemoveKeys, Omap: OmapInput{RangeBegin: "a"}}, ErrRequest},
		{"range end on a read", Operation{Code: OmapGetKeys, Omap: OmapInput{RangeEnd: "z", MaxOutputBytes: 8}}, ErrRequest},
		{"range on stat", Operation{Code: Stat, Omap: OmapInput{RangeBegin: "a"}}, ErrRequest},
		{"large range key", Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeEnd: strings.Repeat("z", 1024)}}, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.Operations = []Operation{tc.op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid range removal admitted", err)
			}
		})
	}
	if _, _, err := ValidateOperations([]Operation{{Code: OmapRemoveKeyRange}, {Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 8}}}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("OMAP read after a staged range removal admitted", err)
	}
	op := Operation{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeBegin: "a", RangeEnd: "b"}}
	valid := syntheticOmapReply(t, op, nil)
	if valid.Length != 10 || ValidateOmapReply(op, valid, true) != nil {
		t.Fatal("range removal reply union rejected", valid.Length)
	}
	valid.PayloadLength, valid.Data = 1, []byte{0}
	if ValidateOmapReply(op, valid, true) == nil {
		t.Fatal("range removal reply returned data")
	}
}
