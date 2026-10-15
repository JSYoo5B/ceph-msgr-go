package osd

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func pgListNativeRequest() PGListRequest {
	return PGListRequest{ClientInc: 1, MapEpoch: 11, PG: PG{Pool: 7, Seed: 0x44}, Namespace: "tenant", Count: 2, StartEpoch: 9, MaxOutputBytes: 512}
}
func pgListTestPage() []byte {
	b := wire.Encoder{}
	b.Data = append(b.Data, initialPGCursor()...)
	b.U32(2)
	for _, e := range []PGListEntry{{"tenant", "one", ""}, {"tenant", "two", "route"}} {
		b.String(e.Namespace)
		b.String(e.Object)
		b.String(e.Key)
	}
	e := wire.Encoder{}
	e.Struct(1, 1, b.Data)
	return e.Data
}

func TestPGListBoundsAndMalformedCursor(t *testing.T) {
	r := pgListNativeRequest()
	if _, err := EncodePGListRequest(r, oracleFeatures, 1024); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*PGListRequest){
		func(r *PGListRequest) { r.Count = 0 }, func(r *PGListRequest) { r.MapEpoch = 0 }, func(r *PGListRequest) { r.PG.Pool = 1 << 63 },
		func(r *PGListRequest) { r.Cursor = []byte{0} }, func(r *PGListRequest) { r.MaxOutputBytes = 48 }, func(r *PGListRequest) { r.MaxOutputBytes = 1025 },
	} {
		bad := r
		edit(&bad)
		if _, err := EncodePGListRequest(bad, oracleFeatures, 1024); err == nil {
			t.Fatal("accepted invalid PG list", bad)
		}
	}
	if _, _, err := ValidateOperations([]Operation{{Code: PGNLS}}, 1024); err == nil {
		t.Fatal("object request admitted PG opcode without PGOP")
	}
	cursor := initialPGCursor()
	for n := 0; n < len(cursor); n++ {
		if _, err := DecodePGCursor(cursor[:n]); err == nil {
			t.Fatal("truncated cursor admitted", n)
		}
	}
	max := bytes.Clone(cursor)
	max[26] = 2 // max byte follows key/oid/snap/hash.
	if _, err := DecodePGCursor(max); err == nil {
		t.Fatal("nonboolean max admitted")
	}
	max = bytes.Clone(cursor)
	max[26] = 1
	max[14] = 1
	if _, err := DecodePGCursor(max); err == nil {
		t.Fatal("noncanonical max admitted")
	}
	c, err := DecodePGCursor(cursor)
	if err != nil || c.Pool != -1<<63 || c.Max || !bytes.Equal(c.Raw, cursor) {
		t.Fatal(c, err)
	}
}

func TestPGListPageMalformedAndReplyIdentity(t *testing.T) {
	data := pgListTestPage()
	page, err := DecodePGList(data, 2)
	if err != nil || len(page.Entries) != 2 || page.Entries[1].Key != "route" || !bytes.Equal(page.Cursor.Raw, initialPGCursor()) {
		t.Fatal(page, err)
	}
	for n := 0; n < len(data); n++ {
		if _, err := DecodePGList(data[:n], 2); err == nil {
			t.Fatal("truncated page admitted", n)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(data), 0), func() []byte { v := bytes.Clone(data); binary.LittleEndian.PutUint32(v[45:], ^uint32(0)); return v }()} {
		if _, err := DecodePGList(bad, 2); err == nil {
			t.Fatal("malformed page admitted")
		}
	}
	if _, err := DecodePGList(data, 1); err == nil {
		t.Fatal("server exceeded requested count")
	}
	r := pgListNativeRequest()
	reply := Reply{PG: r.PG, Result: 1, Operations: []OperationResult{{Code: PGNLS, Offset: r.Count, Length: uint64(r.StartEpoch), PayloadLength: uint32(len(data)), Data: data}}}
	if err := ValidatePGListReply(r, reply, false); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*Reply){func(r *Reply) { r.Result = 2 }, func(r *Reply) { r.Object = "x" }, func(r *Reply) { r.PG.Seed++ }, func(r *Reply) { r.RetryAttempt = 1 }, func(r *Reply) { r.Operations[0].Flags = 1 }, func(r *Reply) { r.Operations[0].Length |= 1 << 32 }, func(r *Reply) { r.Operations[0].TruncateSize = 1 }, func(r *Reply) { r.Operations[0].Result = 1 }, func(r *Reply) { r.Operations[0].PayloadLength-- }} {
		bad := reply
		bad.Operations = append([]OperationResult(nil), reply.Operations...)
		edit(&bad)
		if err := ValidatePGListReply(r, bad, false); err == nil {
			t.Fatal("invalid reply admitted", bad)
		}
	}
	if err := ValidatePGListReply(r, reply, true); err == nil {
		t.Fatal("redirect returned inventory")
	}
}

func TestPGListRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := pgListNativeRequest()
	got, err := EncodePGListRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "pg-list-native-request-v6-list")
	if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("native PGOP request differs")
	}
	r.MaxOutputBytes = 1024
	changed, err := EncodePGListRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) {
		t.Fatal("local budget changed wire", err)
	}
}

func TestDecodeFreshNativePGListReplies(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		n    int
	}{{"populated", 0, 2}, {"empty-end", 1, 0}, {"einval", -22, 0}, {"eacces", -13, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, "pg-list-native-reply-v6-"+tc.name)
			reply, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if reply.Result != tc.code || reply.Object != "" || reply.PG != (PG{Pool: 7, Seed: 0x44}) || reply.MapEpoch != 11 || reply.UserVersion != 19 {
				t.Fatal(reply)
			}
			if err := ValidatePGListReply(pgListNativeRequest(), reply, false); err != nil {
				t.Fatal(err)
			}
			if tc.code < 0 {
				return
			}
			page, err := DecodePGList(reply.RawData, 2)
			if err != nil || len(page.Entries) != tc.n || page.Cursor.Object != "last" || page.Cursor.Key != "route" || page.Cursor.Snap != NoSnap || page.Cursor.Hash != 0x11223344 || page.Cursor.Pool != 7 || page.Cursor.Namespace != "tenant" {
				t.Fatal(page, err)
			}
		})
	}
}
