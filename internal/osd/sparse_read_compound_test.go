package osd

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func compoundSparse(offset, length uint64) Operation {
	return Operation{Code: SparseRead, Offset: offset, Length: length, SparseRead: SparseReadInput{MaxOutputBytes: 4096}}
}

func compoundRead(offset, length uint64) Operation {
	return Operation{Code: Read, Offset: offset, Length: length}
}

// The admitted inventory mirrors the native request layouts; the encoder
// tests below compare them byte for byte with the fresh native vectors.
func sparseReadCompoundAdmitted() map[string][]Operation {
	hinted := func(op Operation, flags uint32) Operation { op.Flags = flags; return op }
	maximum := make([]Operation, MaxOperations)
	for i := range maximum {
		maximum[i] = compoundSparse(uint64(i)*16, 16)
		if i%2 == 1 {
			maximum[i] = compoundRead(uint64(i)*16, 16)
		}
	}
	return map[string][]Operation{
		"librbd-mixed": {compoundSparse(0, 0x10000), compoundRead(0x10000, 0x1000), hinted(compoundSparse(0x11000, 0x20000), OpFadviseSequential)},
		"read-first":   {compoundRead(0, 512), compoundSparse(4096, 8192)},
		"sparse-pair":  {compoundSparse(0, 4096), compoundSparse(8192, 4096)},
		"fadvise-each": {hinted(compoundSparse(0, 4096), OpFadviseRandom), hinted(compoundRead(4096, 4096), OpFadviseNoCache), hinted(compoundSparse(8192, 4096), OpFadviseDontNeed), hinted(compoundRead(12288, 16), OpFadviseWillNeed)},
		"eof-layout":   {compoundSparse(0x100000, 4096), compoundRead(0, 16), compoundSparse(16, 0x100000)},
		"overlapping":  {compoundSparse(0, 64), compoundSparse(0, 64), compoundRead(0, 64)},
		"maximum":      maximum,
	}
}

func sparseReadCompoundRequest(ops []Operation) Request {
	return Request{MapEpoch: 11, Locator: Locator{Pool: 7, Hash: -1}, Object: "object", Operations: ops}
}

func TestSparseReadCompoundAdmittedAcrossEncoders(t *testing.T) {
	for name, ops := range sparseReadCompoundAdmitted() {
		flags, input, err := ValidateOperations(ops, wire.DefaultLimit)
		if err != nil || flags != FlagRead || input != 0 {
			t.Fatal("reviewed SPARSE_READ/READ compound refused", name, flags, input, err)
		}
		for version, encode := range inactiveCopyFromEncoders() {
			m, err := encode(sparseReadCompoundRequest(ops))
			if err != nil || m.Type != RequestMessage || len(m.Front) == 0 || len(m.Data) != 0 || len(m.Middle) != 0 {
				t.Fatal("reviewed compound did not encode", name, version, err)
			}
		}
	}
}

func TestSparseReadCompoundStillRefusedShapes(t *testing.T) {
	sparse := compoundSparse(0, 64)
	others := map[string]Operation{
		"stat":          {Code: Stat},
		"assert":        {Code: AssertVersion, Version: 9},
		"cmpext":        {Code: CompareExtent, Length: 1, Data: []byte{1}},
		"getxattr":      {Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 64}},
		"cmpxattr":      {Code: CompareXattr, Data: []byte{1}, Xattr: XattrInput{Name: "a", ComparisonOp: 1, ComparisonMode: 1}},
		"omap-header":   {Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 64}},
		"checksum":      {Code: Checksum, Length: 8, Checksum: ChecksumInput{Type: ChecksumCRC32C}},
		"mapext":        {Code: MapExt, Length: 8, MapExt: MapExtInput{MaxOutputBytes: 64}},
		"write":         {Code: Write, Length: 1, Data: []byte{1}},
		"writefull":     {Code: WriteFull, Length: 1, Data: []byte{1}},
		"zero":          {Code: Zero, Length: 1},
		"truncate":      {Code: Truncate},
		"setxattr":      {Code: SetXattr, Data: []byte{1}, Xattr: XattrInput{Name: "a"}},
		"call-read":     {Code: Call, Class: ClassInput{Class: "lock", Method: "get_info", Mode: ClassRead, MaxOutputBytes: 64}},
		"isdirty":       {Code: Isdirty},
		"list-watchers": {Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 64}},
	}
	for name, other := range others {
		for _, ops := range [][]Operation{{sparse, other}, {other, sparse}, {compoundRead(0, 8), sparse, other}} {
			if _, _, err := ValidateOperations(ops, wire.DefaultLimit); err == nil {
				t.Fatal("unreviewed SPARSE_READ companion admitted", name)
			}
			for version, encode := range inactiveCopyFromEncoders() {
				m, err := encode(sparseReadCompoundRequest(ops))
				if err == nil || m.Type != 0 || len(m.Front) != 0 || len(m.Data) != 0 {
					t.Fatal("unreviewed SPARSE_READ companion encoded", name, version, err)
				}
			}
		}
	}
	// MAPEXT has no native compound caller and stays standalone.
	mapext := others["mapext"]
	for _, ops := range [][]Operation{{mapext, compoundRead(0, 8)}, {compoundRead(0, 8), mapext}, {mapext, mapext}} {
		if _, _, err := ValidateOperations(ops, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
			t.Fatal("MAPEXT compound admitted", err)
		}
	}
	refused := map[string]struct {
		ops  []Operation
		want error
	}{
		"failok-sparse":   {[]Operation{{Code: SparseRead, Length: 8, Flags: OpFailOK, SparseRead: SparseReadInput{MaxOutputBytes: 64}}, compoundRead(0, 8)}, ErrOperation},
		"failok-read":     {[]Operation{sparse, {Code: Read, Length: 8, Flags: OpFailOK}}, ErrOperation},
		"unknown-flag":    {[]Operation{sparse, {Code: Read, Length: 8, Flags: 0x80}}, ErrOperation},
		"exclusive":       {[]Operation{sparse, {Code: Read, Length: 8, Flags: OpExclusive}}, ErrOperation},
		"read-truncate":   {[]Operation{sparse, {Code: Read, Length: 8, TruncateSize: 4, TruncateSequence: 2}}, ErrRequest},
		"sparse-truncate": {[]Operation{{Code: SparseRead, Length: 8, TruncateSize: 4, TruncateSequence: 2, SparseRead: SparseReadInput{MaxOutputBytes: 64}}, compoundRead(0, 8)}, ErrRequest},
		"zero-length":     {[]Operation{{Code: SparseRead, SparseRead: SparseReadInput{MaxOutputBytes: 64}}, compoundRead(0, 8)}, ErrRequest},
		"small-budget":    {[]Operation{{Code: SparseRead, Length: 8, SparseRead: SparseReadInput{MaxOutputBytes: 7}}, compoundRead(0, 8)}, wire.ErrLimit},
		"overflow":        {[]Operation{{Code: SparseRead, Offset: math.MaxUint64, Length: 8, SparseRead: SparseReadInput{MaxOutputBytes: 64}}, compoundRead(0, 8)}, ErrRequest},
		"read-input":      {[]Operation{sparse, {Code: Read, Length: 1, Data: []byte{1}}}, ErrRequest},
		"over-maximum":    {append(make([]Operation, 0, MaxOperations+1), sparseReadCompoundAdmitted()["maximum"]...), ErrRequest},
	}
	refused["over-maximum"] = struct {
		ops  []Operation
		want error
	}{append(refused["over-maximum"].ops, sparse), ErrRequest}
	for name, tc := range refused {
		if _, _, err := ValidateOperations(tc.ops, wire.DefaultLimit); !errors.Is(err, tc.want) {
			t.Fatal("refused compound shape changed", name, err)
		}
		for version, encode := range inactiveCopyFromEncoders() {
			if m, err := encode(sparseReadCompoundRequest(tc.ops)); err == nil || len(m.Front) != 0 {
				t.Fatal("refused compound shape encoded", name, version, err)
			}
		}
	}
}

func TestSparseReadCompoundAggregateBudgets(t *testing.T) {
	ops := []Operation{{Code: SparseRead, Length: 60, SparseRead: SparseReadInput{MaxOutputBytes: 30}}, compoundRead(0, 40)}
	if _, _, err := ValidateOperations(ops, 100); err != nil {
		t.Fatal("exact logical work bound refused", err)
	}
	ops[1].Length = 41
	if _, _, err := ValidateOperations(ops, 100); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate logical work bound ignored", err)
	}
	ops = []Operation{{Code: SparseRead, Length: 8, SparseRead: SparseReadInput{MaxOutputBytes: 70}}, {Code: SparseRead, Length: 8, SparseRead: SparseReadInput{MaxOutputBytes: 30}}}
	if _, _, err := ValidateOperations(ops, 100); err != nil {
		t.Fatal("exact output budget refused", err)
	}
	ops[1].SparseRead.MaxOutputBytes = 31
	if _, _, err := ValidateOperations(ops, 100); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate output budget ignored", err)
	}
	// Each output budget also enters the enclosing reply-frame budget.
	many := make([]Operation, 16)
	for i := range many {
		many[i] = Operation{Code: SparseRead, Length: 1, SparseRead: SparseReadInput{MaxOutputBytes: 56}}
	}
	if _, _, err := ValidateOperations(many, 1000); err != nil {
		t.Fatal("request-side bound refused", err)
	}
	for version, encode := range map[string]func(Request, uint32) (msgr.MessageData, error){
		"v6": func(r Request, n uint32) (msgr.MessageData, error) { return EncodeRequest(r, oracleFeatures, n) },
		"v8": func(r Request, n uint32) (msgr.MessageData, error) {
			return EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 1}, Shard: -1}, v8OracleFeatures, n)
		},
	} {
		if _, err := encode(sparseReadCompoundRequest(many), 1000); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("aggregate reply budget ignored", version, err)
		}
	}
}

func TestSparseReadCompoundRequestScope(t *testing.T) {
	ops := sparseReadCompoundAdmitted()["read-first"]
	for name, edit := range map[string]func(*Request){
		"snapshot":  func(r *Request) { r.ReadSnap = 3 },
		"balance":   func(r *Request) { r.BalanceReads = true },
		"localize":  func(r *Request) { r.LocalizeReads = true },
		"resubmit":  func(r *Request) { r.RetryAttempt = 1 },
		"enforce":   func(r *Request) { r.EnforceSnapContext = true },
		"ignorecch": func(r *Request) { r.IgnoreCache = true },
	} {
		r := sparseReadCompoundRequest(ops)
		edit(&r)
		for version, encode := range inactiveCopyFromEncoders() {
			if m, err := encode(r); err == nil || len(m.Front) != 0 {
				t.Fatal("compound scope extension admitted", name, version)
			}
		}
		// Standalone SPARSE_READ keeps its own snapshot policy.
		if name == "snapshot" {
			r.Operations = ops[1:]
			for version, encode := range inactiveCopyFromEncoders() {
				if _, err := encode(r); err != nil {
					t.Fatal("standalone snapshot SPARSE_READ changed", version, err)
				}
			}
		}
	}
	head := sparseReadCompoundRequest(ops)
	head.ReadSnap = NoSnap
	if _, err := EncodeRequest(head, oracleFeatures, wire.DefaultLimit); err != nil {
		t.Fatal("explicit head refused", err)
	}
	if _, err := EncodeRequestV8(V8Request{Request: sparseReadCompoundRequest(ops), PG: PG{Pool: 7, Seed: 1}, Shard: 0}, v8OracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("compound EC shard admitted", err)
	}
	if _, err := EncodeRequestV9(V9Request{Request: sparseReadCompoundRequest(ops), PG: PG{Pool: 7, Seed: 1}, Shard: 2}, v9UnitFeatures, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("v9 compound EC shard admitted", err)
	}
}

func sparseReadCompoundData(extents []Extent, data []byte) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(extents)))
	for _, x := range extents {
		e.U64(x.Offset)
		e.U64(x.Length)
	}
	e.U32(uint32(len(data)))
	return append(e.Data, data...)
}

// A successful reply: SPARSE_READ ops return one extent covering their first
// four bytes; READ ops return their full length.
func sparseReadCompoundReply(ops []Operation) Reply {
	r := Reply{Operations: make([]OperationResult, len(ops))}
	for i, op := range ops {
		got := OperationResult{Code: op.Code, Flags: op.Flags, Offset: op.Offset, Length: op.Length}
		switch op.Code {
		case SparseRead:
			got.Length = 4
			got.Data = sparseReadCompoundData([]Extent{{Offset: op.Offset, Length: 4}}, []byte{0, 255, 'A', '\n'})
		case Read:
			got.Data = bytes.Repeat([]byte{0x80}, int(op.Length))
		}
		got.PayloadLength = uint32(len(got.Data))
		r.Operations[i] = got
	}
	return r
}

func TestSparseReadCompoundReplySuccessPrefixAndAmbiguity(t *testing.T) {
	ops := []Operation{compoundSparse(0, 64), compoundRead(64, 8), compoundSparse(128, 64), compoundRead(0, 4)}
	ok := sparseReadCompoundReply(ops)
	if err := ValidateSparseReadCompoundReply(ops, ok); err != nil || SparseReadCompoundPrefix(ok) != 4 {
		t.Fatal("valid compound reply refused", err)
	}
	// EOF: an empty map/buffer and a short READ are successful outputs.
	eof := sparseReadCompoundReply(ops)
	eof.Operations[2].Length, eof.Operations[2].Data = 0, sparseReadCompoundData(nil, nil)
	eof.Operations[2].PayloadLength = uint32(len(eof.Operations[2].Data))
	eof.Operations[1].Length, eof.Operations[1].Data, eof.Operations[1].PayloadLength = 0, nil, 0
	if err := ValidateSparseReadCompoundReply(ops, eof); err != nil {
		t.Fatal("EOF compound outputs refused", err)
	}
	failed := sparseReadCompoundReply(ops)
	failed.Result, failed.Operations[2].Result = -5, -5
	failed.Operations[2].Length, failed.Operations[2].Data, failed.Operations[2].PayloadLength = 64, nil, 0
	failed.Operations[3].Data, failed.Operations[3].PayloadLength = nil, 0
	if err := ValidateSparseReadCompoundReply(ops, failed); err != nil || SparseReadCompoundPrefix(failed) != 2 {
		t.Fatal("completed prefix before a signed error refused", err)
	}
	copyReply := func(r Reply) Reply { r.Operations = append([]OperationResult(nil), r.Operations...); return r }
	for name, edit := range map[string]func(*Reply){
		"aggregate":        func(r *Reply) { r.Result = -22 },
		"success-negative": func(r *Reply) { r.Result = 0 },
		"suffix-output":    func(r *Reply) { r.Operations[3].Data, r.Operations[3].PayloadLength = []byte{1}, 1 },
		"suffix-result":    func(r *Reply) { r.Operations[3].Result = 1 },
		"suffix-union":     func(r *Reply) { r.Operations[3].Length = 2 },
		"terminal-output": func(r *Reply) {
			r.Operations[2].Data, r.Operations[2].PayloadLength = []byte{0, 0, 0, 0, 0, 0, 0, 0}, 8
		},
		"terminal-length":    func(r *Reply) { r.Operations[2].Length = 4 },
		"prefix-budget":      func(r *Reply) { r.Operations[0].Data = append(r.Operations[0].Data, make([]byte, 4096)...) },
		"prefix-read-long":   func(r *Reply) { r.Operations[1].Data, r.Operations[1].PayloadLength = make([]byte, 9), 9 },
		"prefix-read-length": func(r *Reply) { r.Operations[1].Length = 7 },
		"prefix-sparse-range": func(r *Reply) {
			r.Operations[0].Data = sparseReadCompoundData([]Extent{{Offset: 62, Length: 4}}, []byte{1, 2, 3, 4})
		},
		"prefix-failok": func(r *Reply) {
			r.Operations[0].Flags |= OpFailOK
		},
		"code": func(r *Reply) { r.Operations[1].Code = Stat },
	} {
		t.Run(name, func(t *testing.T) {
			bad := copyReply(failed)
			edit(&bad)
			for i := range bad.Operations {
				if name == "prefix-budget" || name == "prefix-sparse-range" {
					bad.Operations[i].PayloadLength = uint32(len(bad.Operations[i].Data))
				}
			}
			if err := ValidateSparseReadCompoundReply(ops, bad); err == nil {
				t.Fatal("contradictory compound reply accepted")
			}
		})
	}
	// A missing object fails admission before any operation runs: all-zero
	// per-op results and no output stay ambiguous, never a completed prefix.
	missing := copyReply(failed)
	missing.Result = -2
	for i, op := range ops {
		missing.Operations[i] = OperationResult{Code: op.Code, Offset: op.Offset, Length: op.Length}
	}
	if err := ValidateSparseReadCompoundReply(ops, missing); err != nil || SparseReadCompoundPrefix(missing) != 0 {
		t.Fatal("missing-object ambiguity lost", err)
	}
	missing.Operations[0] = ok.Operations[0]
	if err := ValidateSparseReadCompoundReply(ops, missing); err == nil {
		t.Fatal("error without terminal operation claimed processed output")
	}
	// A failed READ zeroes its length but may keep bounded bytes it read.
	readFail := sparseReadCompoundReply(ops)
	readFail.Result, readFail.Operations[1].Result, readFail.Operations[1].Length = -5, -5, 0
	readFail.Operations[2] = OperationResult{Code: SparseRead, Offset: 128, Length: 64}
	readFail.Operations[3] = OperationResult{Code: Read, Offset: 0, Length: 4}
	if err := ValidateSparseReadCompoundReply(ops, readFail); err != nil {
		t.Fatal("bounded failing-read output rejected", err)
	}
	if err := ValidateSparseReadCompoundReply(ops[:1], sparseReadCompoundReply(ops[:1])); !errors.Is(err, ErrOperation) {
		t.Fatal("standalone vector routed through compound validator", err)
	}
}
