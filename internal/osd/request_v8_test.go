package osd

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const v8OracleFeatures uint64 = oracleFeatures | featureNewOSDOp | featureResendOnSplitMask

func nativeV8Request(kind string) V8Request {
	r := V8Request{Request: Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "v8-codec", Hash: 0x11223344}, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}
	switch kind {
	case "read":
		r.Operations = []Operation{{Code: Stat}, {Code: Read, Offset: 2, Length: 4}}
	case "mutation":
		r.Operations = []Operation{{Code: Create, Flags: OpExclusive}, {Code: Write, Offset: 2, Length: 4, Data: []byte{0, 255, 'A', '\n'}}}
		r.MTimeSeconds = 1700000000
		r.MTimeNanoseconds = 123456789
	case "snapshot":
		r.Operations = []Operation{{Code: WriteFull, Length: 4, Data: []byte("snap")}}
		r.ClientInc = 99
		r.MTimeSeconds = 1700000000
		r.MTimeNanoseconds = 123456789
		r.SnapSeq = 7
		r.Snaps = []uint64{7, 5, 2}
		r.OrderSnap = true
	case "checksum":
		r.Operations = []Operation{{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumInput{Type: ChecksumCRC32C, Seed: ^uint32(0), ChunkSize: 4}}}
	case "shard":
		r.PG.Seed = 0x55
		r.Hash = 0xabcdef01
		r.Shard = 3
		r.ReadSnap = 5
		r.Operations = []Operation{{Code: Stat}}
	default:
		panic("unknown native v8 case")
	}
	return r
}

func TestV8RequestRejectsIncompleteIncarnationAndUnsupportedLayout(t *testing.T) {
	r := nativeV8Request("read")
	for _, features := range []uint64{oracleFeatures, v8OracleFeatures &^ featureNewOSDOp, v8OracleFeatures &^ (1 << 21), v8OracleFeatures &^ (1 << 57), v8OracleFeatures &^ featureObjectLocator, v8OracleFeatures &^ featurePGID64, v8OracleFeatures | featureServerSquidMask, v8OracleFeatures | featureNewOSDReply} {
		if _, err := EncodeRequestV8(r, features, wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
			t.Fatal("unsupported encoding features admitted", features, err)
		}
	}
	if _, err := EncodeRequest(r.Request, v8OracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
		t.Fatal("legacy encoder silently selected another wire layout", err)
	}
}

func TestV8RequestValidatesTargetSnapshotAndWholeFrameBudget(t *testing.T) {
	for _, edit := range []func(*V8Request){func(r *V8Request) { r.PG.Pool++ }, func(r *V8Request) { r.PG.Pool = 1 << 63 }, func(r *V8Request) { r.Shard = -2 }, func(r *V8Request) { r.MapEpoch = 0 }, func(r *V8Request) { r.Locator.Hash = 0 }, func(r *V8Request) { r.Object = "" }, func(r *V8Request) { r.Operations = nil }, func(r *V8Request) { r.MTimeSeconds = 1 }, func(r *V8Request) { r.ReadSnap = SnapDir }, func(r *V8Request) { r.OrderSnap = true }} {
		r := nativeV8Request("read")
		edit(&r)
		if _, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit); err == nil {
			t.Fatal("invalid v8 request admitted", r)
		}
	}
	r := nativeV8Request("read")
	if _, err := EncodeRequestV8(r, v8OracleFeatures, 310); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("v8 prefix escaped frame budget", err)
	}
	m, err := EncodeRequestV8(r, v8OracleFeatures, 311)
	if err != nil || 41+len(m.Front) != 311 {
		t.Fatal("exact v8 request frame budget", len(m.Front), err)
	}
	r.Operations = []Operation{{Code: Read, Length: 600}, {Code: Read, Length: 600}}
	if _, err := EncodeRequestV8(r, v8OracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("v8 aggregate reply escaped limit", err)
	}
	r = nativeV8Request("snapshot")
	r.Snaps = []uint64{5, 7}
	if _, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit); err == nil {
		t.Fatal("invalid v8 snapcontext admitted")
	}
}

func TestV8PrefixKeepsActualPGAndRawHashIndependent(t *testing.T) {
	for _, kind := range []string{"read", "snapshot", "shard"} {
		t.Run(kind, func(t *testing.T) {
			r := nativeV8Request(kind)
			m, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			d := wire.NewDecoder(m.Front)
			version, spg := d.Struct(1)
			pg := decodePG(spg)
			shard := int8(spg.U8())
			if version != 1 || spg.Done() != nil || pg != r.PG || shard != r.Shard || d.U32() != r.Hash || d.U32() != r.MapEpoch {
				t.Fatal("actual PG/raw hash/shard changed", pg, shard)
			}
			flags := uint64(d.U32())
			v, id := d.Struct(2)
			if v != 2 || id.U8() != 0 || id.U64() != 0 || id.U64() != 0 || id.U32() != r.ClientInc || id.Done() != nil {
				t.Fatal("native default reqid lost client incarnation")
			}
			if d.U64() != 0 || d.U64() != 0 || d.U64() != 0 || d.U32() != r.ClientInc || d.U32() != r.MTimeSeconds || d.U32() != r.MTimeNanoseconds {
				t.Fatal("v8 trace/body prefix changed")
			}
			want, _, err := ValidateOperations(r.Operations, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			want |= FlagOnDisk | FlagReturnVec
			if r.OrderSnap {
				want |= FlagOrderSnap
			}
			if flags != want || m.Type != RequestMessage || m.Version != 8 || m.CompatVersion != 3 || m.Transaction != 0 {
				t.Fatal("v8 mode or envelope differs", flags, m)
			}
		})
	}
}

func TestV8RequestsMatchFreshNativeTwoStageEncoder(t *testing.T) {
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum", "shard"} {
		t.Run(kind, func(t *testing.T) {
			r := nativeV8Request(kind)
			got, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			name := "request-v8-native-" + kind
			header := oracleBytes(t, name+".header")
			native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data")}})
			if err != nil {
				t.Fatal(err)
			}
			got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
			if native.Type != RequestMessage || native.Version != 8 || native.CompatVersion != 3 || !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatal("v8 header/front/data differ from fresh native encoder", kind)
			}
		})
	}
}
