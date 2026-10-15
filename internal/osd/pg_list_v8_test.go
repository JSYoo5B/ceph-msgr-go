package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestPGListV8RejectsUnsupportedFeaturesAndInput(t *testing.T) {
	r := pgListNativeRequest()
	for _, features := range []uint64{oracleFeatures, v8OracleFeatures &^ featureObjectLocator, v8OracleFeatures &^ featurePGID64, v8OracleFeatures &^ featureNewOSDOp, v8OracleFeatures &^ (1 << 21), v8OracleFeatures &^ (1 << 57), v8OracleFeatures | featureServerSquidMask} {
		if _, err := EncodePGListRequestV8(r, features, wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
			t.Fatal("unsupported v8 PG list feature admitted", features, err)
		}
	}
	for _, edit := range []func(*PGListRequest){func(r *PGListRequest) { r.Count = 0 }, func(r *PGListRequest) { r.MapEpoch = 0 }, func(r *PGListRequest) { r.PG.Pool = 1 << 63 }, func(r *PGListRequest) { r.Cursor = []byte{0} }, func(r *PGListRequest) { r.MaxOutputBytes = 48 }, func(r *PGListRequest) { r.MaxOutputBytes = 1025 }} {
		bad := r
		edit(&bad)
		if _, err := EncodePGListRequestV8(bad, v8OracleFeatures, 1024); err == nil {
			t.Fatal("invalid v8 PG list admitted", bad)
		}
	}
	r.MaxOutputBytes = MinPGListBytes
	m, err := EncodePGListRequestV8(r, v8OracleFeatures, 295)
	if err != nil || 41+len(m.Front)+len(m.Data) != 295 {
		t.Fatal("exact request frame bound", len(m.Front), len(m.Data), err)
	}
	if _, err := EncodePGListRequestV8(r, v8OracleFeatures, 294); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("v8 prefix/cursor escaped frame budget", err)
	}
	r.MaxOutputBytes = 800
	if _, err := EncodePGListRequestV8(r, v8OracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("reply frame budget ignored", err)
	}
}

func TestPGListV8ExplicitPGHashLocatorIncarnationAndRawCursor(t *testing.T) {
	r := pgListNativeRequest()
	r.ClientInc = math.MaxUint32
	r.PG.Seed = 0xfedcba98
	r.Namespace = "\x01"
	r.Cursor = initialPGCursor()
	m, err := EncodePGListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	d := wire.NewDecoder(m.Front)
	v, spg := d.Struct(1)
	pg := decodePG(spg)
	shard := int8(spg.U8())
	if v != 1 || spg.Done() != nil || pg != r.PG || shard != -1 || d.U32() != r.PG.Seed || d.U32() != r.MapEpoch {
		t.Fatal("actual PG/raw hash mismatched")
	}
	if flags := d.U32(); uint64(flags) != FlagRead|FlagOnDisk|FlagReturnVec|flagPGOp {
		t.Fatal("PGOP/read flags", flags)
	}
	_, id := d.Struct(2)
	if id.U8() != 0 || id.U64() != 0 || id.U64() != 0 || id.U32() != r.ClientInc || id.Done() != nil {
		t.Fatal("v8 reqid incarnation not preserved")
	}
	for i := 0; i < 3; i++ {
		if d.U64() != 0 {
			t.Fatal("trace not empty")
		}
	}
	if d.U32() != r.ClientInc || d.U32() != 0 || d.U32() != 0 {
		t.Fatal("incarnation/read mtime")
	}
	loc := decodeLocator(d)
	if loc.Pool != int64(r.PG.Pool) || loc.Hash != int64(r.PG.Seed) || loc.Key != "" || loc.Namespace != r.Namespace {
		t.Fatal("empty-oid explicit-hash locator variant", loc)
	}
	if d.String() != "" || d.U16() != 1 || d.U16() != PGNLS || d.U32() != 0 {
		t.Fatal("not standalone PGNLS")
	}
	if d.U64() != r.Count || d.U32() != r.StartEpoch || d.U64() != 0 || d.U64() != 0 || d.U32() != uint32(len(r.Cursor)) {
		t.Fatal("PGNLS union/input bounds")
	}
	if d.U64() != NoSnap || d.U64() != 0 || d.U32() != 0 || d.U32() != 0 || d.U64() != v8OracleFeatures || d.Done() != nil {
		t.Fatal("head/attempt/features changed")
	}
	if !bytes.Equal(m.Data, r.Cursor) {
		t.Fatal("raw cursor changed")
	}
	r.Cursor[0] = 255
	if m.Data[0] != 4 {
		t.Fatal("cursor aliases caller memory")
	}
	// A decoded cursor's hash is continuation data; it cannot replace the
	// explicit target PG/raw hash selected by the calling placement layer.
	cursor := PGCursor{Pool: 7, Hash: 0x11223344, Object: "last", Key: "route", Namespace: "tenant", Snap: NoSnap}
	r.Cursor, err = backoffCursorBytes(cursor, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m, err = EncodePGListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil || binary.LittleEndian.Uint32(m.Front[24:28]) != r.PG.Seed || !bytes.Equal(m.Data, r.Cursor) {
		t.Fatal("continuation cursor replaced PG routing", err)
	}
}

func TestPGListV8RequestMatchesFreshNativeEncoder(t *testing.T) {
	r := pgListNativeRequest()
	got, err := EncodePGListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	name := "pg-list-v8-native-request-v8-list"
	native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{oracleBytes(t, name+".header"), oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data")}})
	if err != nil {
		t.Fatal(err)
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	if native.Type != RequestMessage || native.Version != 8 || native.CompatVersion != 3 || !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("native PGNLS v8 header/front/data differ")
	}
	r.MaxOutputBytes = 1024
	changed, err := EncodePGListRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) || !bytes.Equal(changed.Data, native.Data) {
		t.Fatal("local budget changed wire", err)
	}
}

func TestDecodeFreshNativePGListV8OriginV6Replies(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		n    int
	}{{"populated", 0, 2}, {"empty-end", 1, 0}, {"einval", -22, 0}, {"eacces", -13, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, "pg-list-v8-native-reply-v6-"+tc.name)
			reply, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if m.Version != 6 || reply.Result != tc.code || reply.Object != "" || reply.PG != (PG{Pool: 7, Seed: 0x44}) || reply.MapEpoch != 11 || reply.UserVersion != 19 {
				t.Fatal("v8-origin native reply actual PG/identity", reply)
			}
			if err := ValidatePGListReply(pgListNativeRequest(), reply, false); err != nil {
				t.Fatal(err)
			}
			if tc.code < 0 {
				return
			}
			page, err := DecodePGList(reply.RawData, 2)
			if err != nil || len(page.Entries) != tc.n || page.Cursor.Object != "last" || page.Cursor.Hash != 0x11223344 {
				t.Fatal("reply page/cursor lost", page, err)
			}
		})
	}
}
