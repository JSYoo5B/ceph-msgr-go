package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Pinned ceph_features.h:13-20,178-180 defines NEW_OSDOPREPLY_ENCODING as
// incarnation 1 (no extra marker). MOSDOp.h:367-396 keeps requests at v8
// without the full SERVER_SQUID incarnation mask, independent of reply bit 58.
func TestReplyV8CapabilityChangesOnlyRequestFeatureTail(t *testing.T) {
	if featureNewOSDReply != uint64(1)<<58 || featureServerSquidMask != uint64(1)<<49|uint64(1)<<57 {
		t.Fatal("pinned reply/SQUID incarnation masks changed")
	}
	features := v8OracleFeatures | featureNewOSDReply
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum"} {
		t.Run(kind, func(t *testing.T) {
			r := nativeV8Request(kind)
			got, err := EncodeRequestV8(r, features, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			assertReplyV8RequestTail(t, got, "request-v8-native-"+kind, features)
		})
	}
	t.Run("pgnls", func(t *testing.T) {
		got, err := EncodePGListRequestV8(pgListNativeRequest(), features, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		assertReplyV8RequestTail(t, got, "pg-list-v8-native-request-v8-list", features)
	})
}

func assertReplyV8RequestTail(t *testing.T, got msgr.MessageData, name string, features uint64) {
	t.Helper()
	// The reference is an independently native-encoded v8 request with bit 58
	// absent. Only its final advertised-feature field may differ; this is not
	// a claim of a newly generated native request vector for the opt-in mask.
	wantFront := bytes.Clone(oracleBytes(t, name+".front"))
	if binary.LittleEndian.Uint64(wantFront[len(wantFront)-8:]) != v8OracleFeatures {
		t.Fatal("native baseline feature set changed")
	}
	binary.LittleEndian.PutUint64(wantFront[len(wantFront)-8:], features)
	if !bytes.Equal(got.Front, wantFront) || !bytes.Equal(got.Data, oracleBytes(t, name+".data")) || got.Version != 8 || got.CompatVersion != 3 {
		t.Fatal("reply capability changed actual PG/hash/shard, operations, cursor or request layout")
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	if !bytes.Equal(got.Frame().Segments[0], oracleBytes(t, name+".header")) {
		t.Fatal("reply capability changed canonical native v8 request envelope")
	}
}

func TestReplyV8CapabilityDoesNotRelaxEncoderContracts(t *testing.T) {
	r, pg := nativeV8Request("read"), pgListNativeRequest()
	features := v8OracleFeatures | featureNewOSDReply
	for _, missing := range []uint64{featureObjectLocator, featurePGID64, featureNewOSDOp, 1 << 21, 1 << 57} {
		if _, err := EncodeRequestV8(r, features&^missing, 4096); !errors.Is(err, ErrFeatures) {
			t.Fatal("reply bit replaced a required request capability", missing, err)
		}
		if _, err := EncodePGListRequestV8(pg, features&^missing, 4096); !errors.Is(err, ErrFeatures) {
			t.Fatal("reply bit replaced a required PG request capability", missing, err)
		}
	}
	if _, err := EncodeRequestV8(r, features|featureServerSquidMask, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("reply profile admitted full SQUID incarnation", err)
	}
	if _, err := EncodePGListRequestV8(pg, features|featureServerSquidMask, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("reply profile admitted SQUID PG request", err)
	}
	// Isolate bit 58 from NEW_OSDOP_ENCODING so a future v6 relaxation cannot
	// hide behind rejection of another unrelated request feature.
	if _, err := EncodeRequest(r.Request, oracleFeatures|featureNewOSDReply, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("default v6 object request accepted reply bit 58", err)
	}
	if _, err := EncodePGListRequest(pg, oracleFeatures|featureNewOSDReply, 4096); !errors.Is(err, ErrFeatures) {
		t.Fatal("default v6 PG request accepted reply bit 58", err)
	}
	r.MTimeNanoseconds = 1_000_000_000
	if _, err := EncodeRequestV8(r, features, 4096); !errors.Is(err, ErrRequest) {
		t.Fatal("reply bit bypassed malformed caller time", err)
	}
	r = nativeV8Request("read")
	r.Operations = []Operation{{Code: Watch, Watch: WatchInput{Cookie: 9, Action: WatchRegister, Version: 1}}}
	if _, err := EncodeRequestV8(r, features, 4096); err == nil {
		t.Fatal("reply bit bypassed inactive WATCH union validation")
	}
	r = nativeV8Request("read")
	if _, err := EncodeRequestV8(r, features, 310); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("reply bit bypassed complete request frame bound", err)
	}
	pg.MaxOutputBytes = MinPGListBytes
	if _, err := EncodePGListRequestV8(pg, features, 294); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("reply bit bypassed PG cursor frame bound", err)
	}
	pg.Cursor = []byte{0}
	if _, err := EncodePGListRequestV8(pg, features, 4096); err == nil {
		t.Fatal("reply bit bypassed malformed PG cursor")
	}
}

func TestReplyV8MixedUnionSignedPerOperationResultsAndPadding(t *testing.T) {
	// Construct an independent heterogeneous reply: aggregate success may
	// coexist with FAILOK's negative per-operation result. The codec retains
	// both; request-specific validation belongs to the public request path.
	e := wire.Encoder{}
	e.String("mixed")
	encodePG(&e, PG{Pool: 7, Seed: 3})
	e.U64(FlagOnDisk | FlagReturnVec)
	e.U32(0)
	e.U64(12)
	e.U32(4)
	e.U32(9)
	e.U32(2)
	e.U16(Watch)
	e.U32(0)
	e.U64(0x1020304050607080)
	e.U64(0)
	e.U8(WatchReconnect)
	e.U32(0xffffffff)
	e.U32(0)
	padding := len(e.Data)
	e.Raw(make([]byte, 3))
	e.U32(0)
	e.U16(Checksum)
	e.U32(OpFailOK)
	e.U64(2)
	e.U64(8)
	e.U32(4)
	e.U8(ChecksumCRC32C)
	e.Raw(make([]byte, 7))
	e.U32(0)
	e.U32(0)
	e.U32(0)
	e.U32(uint32(0xffffcfc7)) // signed -12345, without unsigned normalization.
	e.U64(22)
	e.U32(4)
	e.U64(12)
	e.U8(0) // no redirect
	e.Raw(make([]byte, 24))
	m := msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Front: e.Data}
	r, err := DecodeReplyV8(m, 4096)
	if err != nil || r.Result != 0 || r.Operations[0].Watch.Cookie != 0x1020304050607080 || r.Operations[0].Watch.Action != WatchReconnect || r.Operations[0].Watch.Generation != 0xffffffff || r.Operations[1].Result != -12345 || r.Operations[1].Flags != OpFailOK || r.Operations[1].Checksum.Type != ChecksumCRC32C || r.Operations[1].Checksum.ChunkSize != 4 || r.Operations[1].Offset != 2 || r.Operations[1].Length != 8 {
		t.Fatal("heterogeneous union or signed FAILOK outcome changed", r, err)
	}
	m.Front = bytes.Clone(e.Data)
	m.Front[padding] = 1
	if _, err := DecodeReplyV8(m, 4096); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("malformed WATCH reserved padding accepted by v8 decoder", err)
	}
}
