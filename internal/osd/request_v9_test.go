package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const v9UnitFeatures = v8OracleFeatures | featureNewOSDReply | featureServerSquidMask
const unitOTELStart = 87 // spg24 + rawhash/epoch/flags12 + reqid27 + trace24.

func TestV9RequestPinnedEmptyOTELAndUnchangedOperationBodies(t *testing.T) {
	// This is a structural regression against independent retained v8 bytes,
	// not a newly generated v9 native oracle. Fresh v9 proof is separate.
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum"} {
		t.Run(kind, func(t *testing.T) {
			r := V9Request(nativeV8Request(kind))
			m, err := EncodeRequestV9(r, v9UnitFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			old := oracleBytes(t, "request-v8-native-"+kind+".front")
			if !bytes.Equal(m.Front[:unitOTELStart], old[:unitOTELStart]) || !bytes.Equal(m.Front[unitOTELStart:unitOTELStart+7], []byte{1, 1, 1, 0, 0, 0, 0}) || !bytes.Equal(m.Front[unitOTELStart+7:len(m.Front)-8], old[unitOTELStart:len(old)-8]) || !bytes.Equal(m.Data, oracleBytes(t, "request-v8-native-"+kind+".data")) {
				t.Fatal("v9 changed caller identity/op/snapshot fields beyond its trace envelope")
			}
			if m.Type != RequestMessage || m.Version != 9 || m.CompatVersion != 3 || m.Priority != 127 || m.Transaction != 0 || len(m.Middle) != 0 || binary.LittleEndian.Uint64(m.Front[len(m.Front)-8:]) != v9UnitFeatures {
				t.Fatal("fixed v9 envelope/features", m)
			}
		})
	}
	r := pgListNativeRequest()
	m, err := EncodePGListRequestV9(r, v9UnitFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	old := oracleBytes(t, "pg-list-v8-native-request-v8-list.front")
	if !bytes.Equal(m.Front[:unitOTELStart], old[:unitOTELStart]) || !bytes.Equal(m.Front[unitOTELStart:unitOTELStart+7], []byte{1, 1, 1, 0, 0, 0, 0}) || !bytes.Equal(m.Front[unitOTELStart+7:len(m.Front)-8], old[unitOTELStart:len(old)-8]) || !bytes.Equal(m.Data, oracleBytes(t, "pg-list-v8-native-request-v8-list.data")) {
		t.Fatal("PGNLS v9 changed raw PG/locator/cursor")
	}
	if m.Version != 9 || m.CompatVersion != 3 {
		t.Fatal(m)
	}
}

func TestV9RequestRequiresFullFeaturesAndDoesNotRelaxEarlierProfiles(t *testing.T) {
	r, pg := V9Request(nativeV8Request("read")), pgListNativeRequest()
	for _, missing := range []uint{8, 9, 21, 49, 56, 57} {
		features := v9UnitFeatures &^ (uint64(1) << missing)
		if _, err := EncodeRequestV9(r, features, 4096); !errors.Is(err, ErrFeatures) {
			t.Fatal(missing, err)
		}
		if _, err := EncodePGListRequestV9(pg, features, 4096); !errors.Is(err, ErrFeatures) {
			t.Fatal(missing, err)
		}
	}
	if _, err := EncodeRequestV8(V8Request(r), v9UnitFeatures, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("v8 accepted Squid", err)
	}
	if _, err := EncodePGListRequestV8(pg, v9UnitFeatures, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("v8 PGNLS accepted Squid", err)
	}
	if _, err := EncodeRequest(r.Request, v9UnitFeatures, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("v6 selected another encoding", err)
	}
	if _, err := EncodePGListRequest(pg, v9UnitFeatures, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("v6 PGNLS selected another encoding", err)
	}
	for _, edit := range []func(*V9Request){func(r *V9Request) { r.Shard = 0 }, func(r *V9Request) { r.Shard = -2 }, func(r *V9Request) { r.PG.Pool++ }, func(r *V9Request) { r.MapEpoch = 0 }, func(r *V9Request) { r.Locator.Hash = 0 }, func(r *V9Request) { r.Object = "" }, func(r *V9Request) { r.ReadSnap = SnapDir }, func(r *V9Request) { r.MTimeNanoseconds = 1_000_000_000 }, func(r *V9Request) { r.Operations = []Operation{{Code: Write, Length: 2, Data: []byte{1}}} }} {
		bad := r
		edit(&bad)
		if _, err := EncodeRequestV9(bad, v9UnitFeatures, 4096); err == nil {
			t.Fatal("invalid untraced request admitted", bad)
		}
	}
}

func TestV9RequestAndPGNLSBoundTheAdditionalEnvelopeBeforeEncoding(t *testing.T) {
	r := V9Request(nativeV8Request("read"))
	for _, limit := range []uint32{1, 310, 311, 317} {
		if _, err := EncodeRequestV9(r, v9UnitFeatures, limit); !errors.Is(err, wire.ErrLimit) {
			t.Fatal(limit, err)
		}
	}
	m, err := EncodeRequestV9(r, v9UnitFeatures, 318)
	if err != nil || 41+len(m.Front)+len(m.Data) != 318 {
		t.Fatal("exact v9 frame limit", len(m.Front), err)
	}
	pg := pgListNativeRequest()
	pg.MaxOutputBytes = MinPGListBytes
	if _, err := EncodePGListRequestV9(pg, v9UnitFeatures, 301); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("empty trace escaped PGNLS frame budget", err)
	}
	m, err = EncodePGListRequestV9(pg, v9UnitFeatures, 302)
	if err != nil || 41+len(m.Front)+len(m.Data) != 302 {
		t.Fatal("exact PGNLS v9 frame bound", len(m.Front), len(m.Data), err)
	}
	for _, edit := range []func(*PGListRequest){func(r *PGListRequest) { r.Count = 0 }, func(r *PGListRequest) { r.MapEpoch = 0 }, func(r *PGListRequest) { r.PG.Pool = 1 << 63 }, func(r *PGListRequest) { r.Cursor = []byte{0} }, func(r *PGListRequest) { r.MaxOutputBytes = 48 }, func(r *PGListRequest) { r.MaxOutputBytes = 1025 }} {
		bad := pg
		edit(&bad)
		if _, err := EncodePGListRequestV9(bad, v9UnitFeatures, 1024); err == nil {
			t.Fatal("invalid PGNLS admitted", bad)
		}
	}
	pg.MaxOutputBytes = 800
	if _, err := EncodePGListRequestV9(pg, v9UnitFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("PGNLS reply bound ignored", err)
	}
}

func unitV9AdmissionMessages(t *testing.T) []msgr.MessageData {
	t.Helper()
	var out []msgr.MessageData
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum"} {
		m, err := EncodeRequestV9(V9Request(nativeV8Request(kind)), v9UnitFeatures, 4096)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	m, err := EncodePGListRequestV9(pgListNativeRequest(), v9UnitFeatures, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return append(out, m)
}
