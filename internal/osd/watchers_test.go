package osd

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func watcherBytes(version uint8, extra bool) []byte {
	item := wire.Encoder{}
	item.U8(8)
	item.U64(42)
	item.U64(^uint64(0))
	item.U32(37)
	msgr.Address{Type: 2, Nonce: 1000, Endpoint: netip.MustParseAddrPort("[2001:db8::1]:6800"), FlowInfo: 9, ScopeID: 3}.Encode(&item)
	if extra {
		item.U8(0xab)
	}
	entry := wire.Encoder{}
	entry.Struct(version, 1, item.Data)
	body := wire.Encoder{}
	body.U32(1)
	body.Raw(entry.Data)
	out := wire.Encoder{}
	out.Struct(1, 1, body.Data)
	return out.Data
}

func TestWatcherDecodeBoundsAndVersioning(t *testing.T) {
	data := watcherBytes(2, false)
	got, err := DecodeWatchers(data)
	if err != nil || len(got) != 1 || got[0].NameType != 8 || got[0].NameNumber != 42 || got[0].Cookie != ^uint64(0) || got[0].TimeoutSeconds != 37 || got[0].Address.FlowInfo != 9 || got[0].Address.ScopeID != 3 || got[0].Address.Endpoint != netip.MustParseAddrPort("[2001:db8::1]:6800") {
		t.Fatal(got, err)
	}
	for i := 0; i < len(data); i++ {
		if _, err := DecodeWatchers(data[:i]); err == nil {
			t.Fatal("accepted truncated listing", i)
		}
	}
	for _, invalid := range [][]byte{append(append([]byte{}, data...), 0), watcherBytes(1, false), watcherBytes(2, true)} {
		if _, err := DecodeWatchers(invalid); err == nil {
			t.Fatal("invalid version/trailer accepted")
		}
	}
	if _, err := DecodeWatchers(watcherBytes(3, true)); err != nil {
		t.Fatal("newer compatible entry rejected", err)
	}
	oversized := append([]byte{}, data...)
	binary.LittleEndian.PutUint32(oversized[6:10], ^uint32(0))
	if _, err := DecodeWatchers(oversized); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("unbounded entry allocation", err)
	}
}

func TestWatcherOperationAdmissionAndReplyBudget(t *testing.T) {
	input := Operation{Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 512}}
	mode, n, err := ValidateOperations([]Operation{input}, 1024)
	if err != nil || mode != FlagRead || n != 0 {
		t.Fatal(mode, n, err)
	}
	for _, edit := range []func(*Operation){
		func(o *Operation) { o.Flags = OpFailOK }, func(o *Operation) { o.Offset = 1 }, func(o *Operation) { o.Data = []byte{1} },
		func(o *Operation) { o.ListWatchers.MaxOutputBytes = 9 }, func(o *Operation) { o.ListWatchers.MaxOutputBytes = 1025 },
		func(o *Operation) { o.Snap = 1 }, func(o *Operation) { o.Omap.MaxOutputBytes = 1 }, func(o *Operation) { o.Xattr.Name = "x" },
		func(o *Operation) { o.AllocHint.Flags = 1 }, func(o *Operation) { o.Class.Class = "hello" }, func(o *Operation) { o.ListSnaps.MaxOutputBytes = 10 },
	} {
		op := input
		edit(&op)
		if _, _, err := ValidateOperations([]Operation{op}, 1024); err == nil {
			t.Fatal("invalid listing admitted", op)
		}
	}
	if _, _, err := ValidateOperations([]Operation{{Code: Stat}, input}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("compound admitted", err)
	}
	if _, _, err := ValidateOperations([]Operation{{Code: Stat, ListWatchers: input.ListWatchers}}, 1024); err == nil {
		t.Fatal("inactive input admitted")
	}
	data := watcherBytes(2, false)
	reply := OperationResult{Code: ListWatchers, PayloadLength: uint32(len(data)), Data: data}
	if err := ValidateListWatchersReply(input, reply, true); err != nil {
		t.Fatal(err)
	}
	small := input
	small.ListWatchers.MaxOutputBytes = uint32(len(data) - 1)
	if err := ValidateListWatchersReply(small, reply, true); err == nil {
		t.Fatal("over-budget output accepted")
	}
	if err := ValidateListWatchersReply(input, reply, false); err == nil {
		t.Fatal("failed listing output accepted")
	}
	reply = OperationResult{Code: ListWatchers}
	if err := ValidateListWatchersReply(input, reply, false); err != nil {
		t.Fatal("signed refusal rejected", err)
	}
	if err := ValidateListWatchersReply(input, reply, true); err == nil {
		t.Fatal("success without listing accepted")
	}
	request := Request{MapEpoch: 11, Object: "object", Locator: Locator{Pool: 7, Hash: -1}, Operations: []Operation{input}, ReadSnap: 3}
	if _, err := EncodeRequest(request, oracleFeatures, 1024); err == nil {
		t.Fatal("listing at a snapshot admitted")
	}
	request.ReadSnap = 0
	request.Operations[0].ListWatchers.MaxOutputBytes = 1000
	if _, err := EncodeRequest(request, oracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("combined envelope budget not enforced", err)
	}
}

func FuzzDecodeWatchers(f *testing.F) {
	f.Add(watcherBytes(2, false))
	f.Add([]byte{1, 1, 4, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, p []byte) {
		if len(p) > 65536 {
			t.Skip()
		}
		DecodeWatchers(p)
	})
}
