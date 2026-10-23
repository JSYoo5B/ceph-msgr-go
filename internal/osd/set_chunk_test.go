package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// setChunkFeatures are the default implemented features of the native vectors;
// SET_CHUNK on a plain pool needs no cache-pool capability.
var setChunkFeatures = map[uint16]uint64{6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038}

func setChunkUnitRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "set-chunk-codec", Hash: 0x11223344,
		Operations: []Operation{{Code: SetChunk, Flags: OpWithReference, SetChunk: SetChunkInput{SourceLength: 8, Target: Locator{Pool: 9, Hash: -1}, TargetObject: "set-chunk-target"}}}}
}

func setChunkEncode(r Request, version uint16, shard int8) error {
	var err error
	switch version {
	case 6:
		_, err = EncodeRequest(r, setChunkFeatures[6], wire.DefaultLimit)
	case 8:
		_, err = EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: shard}, setChunkFeatures[8], wire.DefaultLimit)
	case 9:
		_, err = EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: shard}, setChunkFeatures[9], wire.DefaultLimit)
	}
	return err
}

func TestSetChunkEncodesReadWithReferenceAndIndata(t *testing.T) {
	r := setChunkUnitRequest()
	if err := ValidateSetChunkRequest(r); err != nil {
		t.Fatal(err)
	}
	mode, size, err := ValidateOperations(r.Operations, wire.DefaultLimit)
	want := appendSetChunkData(nil, r.Operations[0].SetChunk)
	if err != nil || mode != FlagRead || size != uint64(len(want)) {
		t.Fatalf("mode %#x size %d, want READ and %d: %v", mode, size, len(want), err)
	}
	m, err := EncodeRequest(r, setChunkFeatures[6], wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:]) != uint32(FlagOnDisk|FlagReturnVec|FlagRead) || !bytes.Equal(m.Data, want) {
		t.Fatal("SET_CHUNK request flags or indata differ")
	}
	// u64 src_offset, u64 src_length, object_locator_t, object_t, u64 tgt_offset.
	if binary.LittleEndian.Uint64(want) != 0 || binary.LittleEndian.Uint64(want[8:]) != 8 || want[16] != 6 || want[17] != 3 ||
		!bytes.HasSuffix(want, append(binary.LittleEndian.AppendUint32(nil, 16), "set-chunk-target\x00\x00\x00\x00\x00\x00\x00\x00"...)) {
		t.Fatalf("SET_CHUNK indata layout differs: %x", want)
	}
	packed := binary.LittleEndian.AppendUint16(nil, SetChunk)
	packed = binary.LittleEndian.AppendUint32(packed, OpWithReference)
	packed = append(packed, make([]byte, 28)...)
	packed = binary.LittleEndian.AppendUint32(packed, uint32(len(want)))
	if bytes.Count(m.Front, packed) != 1 {
		t.Fatal("packed SET_CHUNK operation differs")
	}
	for _, version := range []uint16{8, 9} {
		if err := setChunkEncode(r, version, -1); err != nil {
			t.Fatal(version, err)
		}
	}
}

func TestSetChunkRefusalsBeforeEncoding(t *testing.T) {
	op := func(r *Request) *Operation { return &r.Operations[0] }
	edits := map[string]func(*Request){
		"without reference":      func(r *Request) { op(r).Flags = 0 },
		"reference and failok":   func(r *Request) { op(r).Flags |= OpFailOK },
		"fadvise":                func(r *Request) { op(r).Flags |= OpFadviseNoCache },
		"non-zero source offset": func(r *Request) { op(r).SetChunk.SourceOffset = 16 },
		"zero length":            func(r *Request) { op(r).SetChunk.SourceLength = 0 },
		"empty target":           func(r *Request) { op(r).SetChunk.TargetObject = "" },
		"target with NUL":        func(r *Request) { op(r).SetChunk.TargetObject = "a\x00b" },
		"negative target pool":   func(r *Request) { op(r).SetChunk.Target.Pool = -1 },
		"target hash":            func(r *Request) { op(r).SetChunk.Target.Hash = 5 },
		"self target": func(r *Request) {
			op(r).SetChunk = SetChunkInput{SourceLength: 8, Target: r.Locator, TargetObject: r.Object}
		},
		"self target by named key": func(r *Request) {
			r.Locator.Key = r.Object
			op(r).SetChunk.Target, op(r).SetChunk.TargetObject = Locator{Pool: 7, Namespace: "tenant", Hash: -1}, r.Object
		},
		"extent":          func(r *Request) { op(r).Length = 1 },
		"offset":          func(r *Request) { op(r).Offset = 1 },
		"data":            func(r *Request) { op(r).Data = []byte{1} },
		"version":         func(r *Request) { op(r).Version = 1 },
		"copy-from input": func(r *Request) { op(r).CopyFrom.Object = "x" },
		"compound":        func(r *Request) { r.Operations = append(r.Operations, Operation{Code: Stat}) },
		"leading stat":    func(r *Request) { r.Operations = append([]Operation{{Code: Stat}}, r.Operations...) },
		"mtime":           func(r *Request) { r.MTimeSeconds = 1 },
		"snapshot read":   func(r *Request) { r.ReadSnap = 1 },
		"snap context":    func(r *Request) { r.SnapSeq = 1 },
		"order snap":      func(r *Request) { r.OrderSnap = true },
		"enforce snapc":   func(r *Request) { r.EnforceSnapContext = true },
		"balance reads":   func(r *Request) { r.BalanceReads = true },
		"localize reads":  func(r *Request) { r.LocalizeReads = true },
		"ignore redirect": func(r *Request) { r.IgnoreRedirect = true },
		"rw ordered":      func(r *Request) { r.OrderReadsWrites = true },
		"skip locks":      func(r *Request) { r.SkipRWLocks = true },
		"ignore cache":    func(r *Request) { r.IgnoreCache = true },
		"ignore overlay":  func(r *Request) { r.IgnoreOverlay = true },
		"full try":        func(r *Request) { r.FullTry = true },
		"retry":           func(r *Request) { r.RetryAttempt = 1 },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			for _, version := range []uint16{6, 8, 9} {
				r := setChunkUnitRequest()
				edit(&r)
				if err := setChunkEncode(r, version, -1); err == nil {
					t.Fatal("invalid SET_CHUNK request encoded", version)
				}
			}
		})
	}
	// The hazard refusal is an unsupported operation, not a malformed one.
	r := setChunkUnitRequest()
	r.Operations[0].SetChunk.SourceOffset = 16
	if err := setChunkEncode(r, 6, -1); !errors.Is(err, ErrOperation) {
		t.Fatal("non-zero source offset not refused as an unsupported operation", err)
	}
	if err := setChunkEncode(setChunkUnitRequest(), 8, 1); !errors.Is(err, ErrRequest) {
		t.Fatal("SET_CHUNK encoded for an erasure-code shard", err)
	}
	// WITH_REFERENCE and the chunk input belong to SET_CHUNK alone.
	read := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Hash: -1}, Object: "o", Operations: []Operation{{Code: Read, Length: 1, Flags: OpWithReference}}}
	if _, err := EncodeRequest(read, setChunkFeatures[6], wire.DefaultLimit); !errors.Is(err, ErrOperation) {
		t.Fatal("WITH_REFERENCE admitted outside SET_CHUNK", err)
	}
	read.Operations[0].Flags = 0
	read.Operations[0].SetChunk.SourceLength = 1
	if _, err := EncodeRequest(read, setChunkFeatures[6], wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("chunk input admitted outside SET_CHUNK", err)
	}
}

func TestSetChunkAcceptsDistinctTargets(t *testing.T) {
	for name, edit := range map[string]func(*SetChunkInput){
		"other pool":            func(in *SetChunkInput) { in.Target.Pool = 9 },
		"other namespace":       func(in *SetChunkInput) { in.Target.Namespace = "other" },
		"other key":             func(in *SetChunkInput) { in.Target.Key = "other" },
		"other name":            func(in *SetChunkInput) { in.TargetObject = "other" },
		"located target offset": func(in *SetChunkInput) { in.Target.Key, in.TargetOffset = "chunk-key", 65536 },
	} {
		r := setChunkUnitRequest()
		r.Operations[0].SetChunk = SetChunkInput{SourceLength: 8, Target: r.Locator, TargetObject: r.Object}
		edit(&r.Operations[0].SetChunk)
		if err := setChunkEncode(r, 6, -1); err != nil {
			t.Error("distinct SET_CHUNK target refused:", name, err)
		}
	}
	r := setChunkUnitRequest()
	r.Operations[0].SetChunk.TargetObject = string(make([]byte, wire.DefaultLimit))
	if err := setChunkEncode(r, 6, -1); err == nil {
		t.Fatal("unbounded SET_CHUNK target encoded")
	}
}
