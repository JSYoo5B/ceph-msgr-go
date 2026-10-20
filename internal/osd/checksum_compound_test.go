package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func compoundChecksumOperation() Operation {
	return Operation{Code: Checksum, Length: 8, Checksum: ChecksumInput{Type: ChecksumCRC32C, ChunkSize: 4, Seed: 0x12345678}}
}

func compoundChecksumData(algorithm uint8, values ...uint64) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(values)))
	for _, value := range values {
		if checksumWidth(algorithm) == 8 {
			e.U64(value)
		} else {
			e.U32(uint32(value))
		}
	}
	return e.Data
}

func compoundChecksumReply(ops []Operation) Reply {
	r := Reply{Operations: make([]OperationResult, len(ops))}
	for i, op := range ops {
		got := OperationResult{Code: op.Code, Flags: op.Flags, Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence}
		switch op.Code {
		case Checksum:
			got.Checksum = ChecksumResult{Type: op.Checksum.Type, ChunkSize: op.Checksum.ChunkSize}
			got.Data = compoundChecksumData(op.Checksum.Type, 11, 22)
		case Stat:
			got.Data = make([]byte, 16)
		case Read:
			got.Data = bytes.Repeat([]byte{0x80}, int(op.Length))
		case AssertVersion:
			got.Length = op.Version
		}
		got.PayloadLength = uint32(len(got.Data))
		r.Operations[i] = got
	}
	return r
}

func TestChecksumCompoundShapeAndAggregateWorkInput(t *testing.T) {
	op := compoundChecksumOperation()
	ops := []Operation{op, {Code: Stat}, {Code: Read, Length: 4}, {Code: AssertVersion, Version: 7}, {Code: CompareExtent, Length: 4, Data: []byte{0, 255, 128, 1}}}
	if flags, input, err := ValidateOperations(ops, 1024); err != nil || flags != FlagRead || input != 8 {
		t.Fatal("closed read-only vector refused", flags, input, err)
	}
	for _, extra := range []Operation{{Code: WriteFull, Length: 1, Data: []byte{1}}, {Code: Call, Class: ClassInput{Mode: ClassRead}}, {Code: GetXattrs}, {Code: MapExt}, {Code: SparseRead}, {Code: Watch}, {Code: ListSnaps}} {
		for _, vector := range [][]Operation{{op, extra}, {extra, op}} {
			if _, _, err := ValidateOperations(vector, 1024); !errors.Is(err, ErrOperation) {
				t.Fatal("unsupported compound member admitted", extra.Code, err)
			}
		}
	}
	for _, index := range []int{0, 1} {
		bad := []Operation{op, {Code: Stat}}
		bad[index].Flags |= OpFailOK
		if _, _, err := ValidateOperations(bad, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("FAILOK admitted", index, err)
		}
	}
	long := op
	long.Length, long.Checksum.ChunkSize = 90, 0
	if _, _, err := ValidateOperations([]Operation{long, {Code: Read, Length: 10}}, 100); err != nil {
		t.Fatal("exact logical work bound refused", err)
	}
	if _, _, err := ValidateOperations([]Operation{long, {Code: Read, Length: 11}}, 100); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate logical work bound ignored", err)
	}
	many := make([]Operation, 16)
	for i := range many {
		many[i] = Operation{Code: Checksum, Length: 1, Checksum: ChecksumInput{Type: ChecksumXXHash64}}
	}
	if _, _, err := ValidateOperations(many, 127); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate seed input bound ignored", err)
	}
	bad := op
	bad.Length = math.MaxUint64
	if _, _, err := ValidateOperations([]Operation{bad, op}, math.MaxUint32); err == nil {
		t.Fatal("overflowing work admitted")
	}
}

func TestChecksumCompoundRequestScopeAndStandaloneUnchanged(t *testing.T) {
	base := Request{Operations: []Operation{compoundChecksumOperation(), {Code: Stat}}}
	for _, edit := range []func(*Request){
		func(r *Request) { r.ReadSnap = 3 }, func(r *Request) { r.BalanceReads = true },
		func(r *Request) { r.LocalizeReads = true }, func(r *Request) { r.RetryAttempt = 1 },
	} {
		bad := base
		edit(&bad)
		if err := ValidateChecksumCompoundRequest(bad); !errors.Is(err, ErrRequest) {
			t.Fatal("unsupported request scope admitted", bad, err)
		}
		bad.Operations = base.Operations[:1]
		if err := ValidateChecksumCompoundRequest(bad); err != nil {
			t.Fatal("standalone policy changed", err)
		}
	}
	base.ReadSnap = NoSnap
	base.IgnoreRedirect, base.OrderReadsWrites, base.SkipRWLocks = true, true, true
	if err := ValidateChecksumCompoundRequest(base); err != nil {
		t.Fatal("existing read-only ordering policy overridden", err)
	}
}

func TestChecksumCompoundEncoderSeedsVersionsAndReplyBudget(t *testing.T) {
	ops := []Operation{{Code: Checksum, Length: 4, Checksum: ChecksumInput{Type: ChecksumCRC32C, Seed: 0x12345678}}, {Code: CompareExtent, Length: 3, Data: []byte{0, 255, 128}}, {Code: Checksum, Length: 4, Checksum: ChecksumInput{Type: ChecksumXXHash64, Seed: 0xfedcba9876543210}}}
	base := Request{MapEpoch: 11, Locator: Locator{Pool: 7, Hash: -1}, Object: "object", Operations: ops}
	v6 := featureObjectLocator | featurePGID64
	v8 := v6 | featureNewOSDOp | featureResendOnSplitMask
	encoders := []struct {
		name   string
		encode func(Request, uint32) (msgr.MessageData, error)
	}{
		{"v6", func(r Request, n uint32) (msgr.MessageData, error) { return EncodeRequest(r, v6, n) }},
		{"v8", func(r Request, n uint32) (msgr.MessageData, error) {
			return EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 3}, Shard: -1}, v8, n)
		}},
		{"v9", func(r Request, n uint32) (msgr.MessageData, error) {
			return EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 3}, Shard: -1}, v8|featureServerSquidMask, n)
		}},
	}
	for _, tc := range encoders {
		t.Run(tc.name, func(t *testing.T) {
			m, err := tc.encode(base, 4096)
			want := make([]byte, 15)
			binary.LittleEndian.PutUint32(want, 0x12345678)
			copy(want[4:], []byte{0, 255, 128})
			binary.LittleEndian.PutUint64(want[7:], 0xfedcba9876543210)
			if err != nil || !bytes.Equal(m.Data, want) {
				t.Fatal("per-op seeds/guard input were not independently concatenated", m.Data, err)
			}
			bad := base
			bad.ReadSnap = 3
			if _, err := tc.encode(bad, 4096); !errors.Is(err, ErrRequest) {
				t.Fatal("compound snapshot admitted", err)
			}
			bad.Operations = base.Operations[:1]
			if _, err := tc.encode(bad, 4096); err != nil {
				t.Fatal("standalone snapshot checksum changed", err)
			}
			budget := base
			budget.Operations = make([]Operation, 16)
			for i := range budget.Operations {
				budget.Operations[i] = Operation{Code: Checksum, Length: 1, Checksum: ChecksumInput{Type: ChecksumCRC32C}}
			}
			if _, _, err := ValidateOperations(budget.Operations, 950); err != nil {
				t.Fatal("reply-only bound failed request validation", err)
			}
			if _, err := tc.encode(budget, 950); !errors.Is(err, wire.ErrLimit) {
				t.Fatal("aggregate reply budget ignored", err)
			}
		})
	}
	if _, err := EncodeRequestV8(V8Request{Request: base, PG: PG{Pool: 7, Seed: 3}, Shard: 0}, v8, 4096); !errors.Is(err, ErrRequest) {
		t.Fatal("compound EC shard admitted", err)
	}
}

func TestChecksumCompoundReplyPrefixTerminalSuffixAndAmbiguity(t *testing.T) {
	ops := []Operation{compoundChecksumOperation(), {Code: AssertVersion, Version: 9}, {Code: Read, Length: 4}, compoundChecksumOperation()}
	valid := compoundChecksumReply(ops)
	valid.Result, valid.Operations[1].Result = -75, -75
	for i := 2; i < len(valid.Operations); i++ {
		valid.Operations[i].Data, valid.Operations[i].PayloadLength = nil, 0
	}
	if err := ValidateChecksumCompoundReply(ops, valid); err != nil || ChecksumCompoundPrefix(valid) != 1 {
		t.Fatal("successful prefix preceding signed error refused", err)
	}
	copyReply := func() Reply { r := valid; r.Operations = append([]OperationResult(nil), valid.Operations...); return r }
	for name, edit := range map[string]func(*Reply){
		"aggregate":                 func(r *Reply) { r.Result = -34 },
		"success-negative":          func(r *Reply) { r.Result = 0 },
		"second-negative":           func(r *Reply) { r.Operations[2].Result = -2 },
		"suffix-output":             func(r *Reply) { r.Operations[2].Data, r.Operations[2].PayloadLength = []byte{1}, 1 },
		"suffix-shortened":          func(r *Reply) { r.Operations[2].Length = 0 },
		"suffix-checksum-shortened": func(r *Reply) { r.Operations[3].Length = 4 },
		"prefix-count": func(r *Reply) {
			r.Operations[0].Data = compoundChecksumData(ChecksumCRC32C, 11)
			r.Operations[0].PayloadLength = 8
		},
		"prefix-width":       func(r *Reply) { r.Operations[0].Data = []byte{2, 0, 0, 0}; r.Operations[0].PayloadLength = 4 },
		"prefix-zero-length": func(r *Reply) { r.Operations[0].Length = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := copyReply()
			edit(&bad)
			if err := ValidateChecksumCompoundReply(ops, bad); err == nil {
				t.Fatal("contradictory compound accepted")
			}
		})
	}
	// An empty checksum prefix can be a successful EOF. An all-zero early
	// aggregate error is kept ambiguous, never converted into completed ops.
	eof := copyReply()
	eof.Operations[0].Data, eof.Operations[0].PayloadLength = nil, 0
	if err := ValidateChecksumCompoundReply(ops, eof); err != nil {
		t.Fatal("EOF checksum before a later guard error rejected", err)
	}
	ambiguous := eof
	ambiguous.Operations = append([]OperationResult(nil), eof.Operations...)
	ambiguous.Result, ambiguous.Operations[1].Result = -2, 0
	if err := ValidateChecksumCompoundReply(ops, ambiguous); err != nil || ChecksumCompoundPrefix(ambiguous) != 0 {
		t.Fatal("early aggregate ambiguity lost", err)
	}
	ambiguous.Operations[0].Data, ambiguous.Operations[0].PayloadLength = valid.Operations[0].Data, valid.Operations[0].PayloadLength
	if err := ValidateChecksumCompoundReply(ops, ambiguous); err == nil {
		t.Fatal("error without terminal operation claimed processed output")
	}
}

func TestChecksumCompoundTrimmedFailureAndFailingReadRawOutput(t *testing.T) {
	checksum := compoundChecksumOperation()
	checksum.Checksum.ChunkSize = 8
	ops := []Operation{{Code: Read, Length: 4}, checksum, {Code: Stat}}
	r := compoundChecksumReply(ops)
	r.Result, r.Operations[1].Result, r.Operations[1].Length = -22, -22, 4
	r.Operations[1].Data, r.Operations[1].PayloadLength = nil, 0
	r.Operations[2].Data, r.Operations[2].PayloadLength = nil, 0
	if err := ValidateChecksumCompoundReply(ops, r); err != nil || len(r.Operations[0].Data) != 4 {
		t.Fatal("unaligned trimmed error lost earlier read", err)
	}
	ops = []Operation{compoundChecksumOperation(), {Code: Read, Length: 4}, {Code: Stat}}
	r = compoundChecksumReply(ops)
	r.Result, r.Operations[1].Result, r.Operations[1].Length = -5, -5, 0
	r.Operations[2].Data, r.Operations[2].PayloadLength = nil, 0
	if err := ValidateChecksumCompoundReply(ops, r); err != nil {
		t.Fatal("bounded raw failing-read output rejected", err)
	}
}
