package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func backoffRange() Backoff {
	return Backoff{PG: PG{Pool: 7, Seed: 0x12}, Shard: -1, MapEpoch: 11, Op: BackoffBlock, ID: math.MaxUint64,
		Begin: PGCursor{Object: "a", Key: "k", Snap: NoSnap, Hash: 0x11223344, Pool: 7, Namespace: "tenant"},
		End:   PGCursor{Object: "z", Key: "route", Snap: NoSnap, Hash: 0x11223344, Pool: 7, Namespace: "tenant"}}
}

func TestBackoffCanonicalOrderAndRange(t *testing.T) {
	ordered := []PGCursor{
		{Pool: math.MinInt64},
		{Pool: 7, Hash: 0},
		{Pool: 7, Hash: 0x80000000}, // bit reverse 1, before numeric hash 1
		{Pool: 7, Hash: 1, Namespace: "a"},
		{Pool: 7, Hash: 1, Namespace: "\xff"},
		{Max: true, Pool: math.MinInt64},
	}
	for i, a := range ordered {
		for j, b := range ordered {
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got := CompareBackoffObjects(a, b); got != want {
				t.Fatalf("canonical order %d/%d = %d, want %d", i, j, got, want)
			}
		}
	}
	empty := PGCursor{Pool: 7, Object: "object", Snap: NoSnap}
	explicit := empty
	explicit.Key = "object"
	if CompareBackoffObjects(empty, explicit) != 0 {
		t.Fatal("effective key equality lost")
	}
	keyFirst := empty
	keyFirst.Key = "a"
	keyFirst.Object = "z"
	if CompareBackoffObjects(keyFirst, empty) >= 0 {
		t.Fatal("object name sorted ahead of effective locator key")
	}
	older := empty
	older.Snap = 1
	if CompareBackoffObjects(older, empty) >= 0 {
		t.Fatal("snapshot order lost")
	}
	b := backoffRange()
	if !BackoffContains(b, b.Begin) || BackoffContains(b, b.End) {
		t.Fatal("half-open boundaries lost")
	}
	middle := b.Begin
	middle.Key = "m"
	middle.Object = "middle"
	if !BackoffContains(b, middle) {
		t.Fatal("interior missing")
	}
	before := b.Begin
	before.Key = "j"
	if BackoffContains(b, before) {
		t.Fatal("object before start included")
	}
	b.End = b.Begin
	if !BackoffContains(b, b.Begin) || BackoffContains(b, middle) {
		t.Fatal("equal bounds not a single object")
	}
}

func TestBackoffCodecBoundsIdentityAndAck(t *testing.T) {
	b := backoffRange()
	m, err := EncodeBackoff(b, 1024)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBackoff(m, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != math.MaxUint64 || decoded.PG != b.PG || decoded.Shard != -1 || decoded.MapEpoch != 11 || !bytes.Equal(decoded.RawFront, m.Front) {
		t.Fatal("wire identity lost", decoded)
	}
	ack, err := EncodeBackoffAck(decoded, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != 61 || ack.Version != 1 || ack.CompatVersion != 1 || ack.Priority != 127 || ack.Transaction != 0 {
		t.Fatal("ACK envelope", ack)
	}
	expected := bytes.Clone(m.Front)
	expected[28] = BackoffAckBlock
	if !bytes.Equal(ack.Front, expected) {
		t.Fatal("ACK changed original epoch/id/bounds")
	}
	// Caller memory cannot alter the stored identity or the ACK payload.
	saved := bytes.Clone(decoded.RawFront)
	m.Front[0] = 255
	if !bytes.Equal(decoded.RawFront, saved) {
		t.Fatal("decoded control aliases caller bytes")
	}
	decoded.Begin.Raw[6] = 255
	if _, err := EncodeBackoffAck(decoded, 1024); err == nil {
		t.Fatal("raw and typed range disagreement accepted")
	}
	// Full frame budget, including header, is checked identically in both paths.
	fresh, _ := EncodeBackoff(b, 1024)
	exact := uint32(41 + len(fresh.Front))
	if _, err := EncodeBackoff(b, exact); err != nil {
		t.Fatal("exact bound rejected", err)
	}
	if _, err := DecodeBackoff(fresh, exact); err != nil {
		t.Fatal("exact decode bound rejected", err)
	}
	if _, err := EncodeBackoff(b, exact-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("encode limit ignored", err)
	}
	if _, err := DecodeBackoff(fresh, exact-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("decode limit ignored", err)
	}
	b.Op = BackoffUnblock
	if _, err := EncodeBackoffAck(b, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("non-BLOCK acknowledged", err)
	}
}

func TestBackoffMalformedControls(t *testing.T) {
	good, _ := EncodeBackoff(backoffRange(), 1024)
	mutations := []func(*msgr.MessageData){
		func(m *msgr.MessageData) { m.Type = 43 }, func(m *msgr.MessageData) { m.Version = 2 }, func(m *msgr.MessageData) { m.CompatVersion = 0 },
		func(m *msgr.MessageData) { m.Middle = []byte{0} }, func(m *msgr.MessageData) { m.Data = []byte{0} },
		func(m *msgr.MessageData) { m.Front = append(m.Front, 0) },
		func(m *msgr.MessageData) { m.Front[0] = 2 }, func(m *msgr.MessageData) { m.Front[1] = 0 },
		func(m *msgr.MessageData) { m.Front[6] = 0 },  // pg_t version
		func(m *msgr.MessageData) { m.Front[19] = 0 }, // preferred OSD must be -1
		func(m *msgr.MessageData) { m.Front[28] = 0 }, func(m *msgr.MessageData) { m.Front[28] = 4 },
		func(m *msgr.MessageData) { m.Front[37] = 5 }, func(m *msgr.MessageData) { m.Front[38] = 4 },
		func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[39:43], math.MaxUint32) },
	}
	for i, change := range mutations {
		bad := good
		bad.Front = bytes.Clone(good.Front)
		change(&bad)
		if _, err := DecodeBackoff(bad, 1024); err == nil {
			t.Fatalf("accepted malformed control %d", i)
		}
	}
	for cut := 0; cut < len(good.Front); cut++ {
		bad := good
		bad.Front = good.Front[:cut]
		if _, err := DecodeBackoff(bad, 1024); err == nil {
			t.Fatalf("accepted truncated control at %d", cut)
		}
	}
	reversed := backoffRange()
	reversed.Begin, reversed.End = reversed.End, reversed.Begin
	if _, err := EncodeBackoff(reversed, 1024); err == nil {
		t.Fatal("reversed range accepted")
	}
	noncanonical := backoffRange()
	noncanonical.Begin = PGCursor{Pool: -1}
	if _, err := EncodeBackoff(noncanonical, 1024); err == nil {
		t.Fatal("old minimum alias accepted")
	}
	noncanonical.Begin = PGCursor{Max: true, Pool: 7}
	if _, err := EncodeBackoff(noncanonical, 1024); err == nil {
		t.Fatal("noncanonical maximum accepted")
	}
}

func nativeBackoffCases() []struct {
	name  string
	value Backoff
} {
	rangeBlock := backoffRange()
	single := rangeBlock
	single.ID = 17
	single.Begin = PGCursor{Object: "bin\x00\xff", Key: "route-key", Snap: NoSnap, Hash: 0x11223344, Pool: 7, Namespace: "tenant"}
	single.End = single.Begin
	ack := rangeBlock
	ack.Op = BackoffAckBlock
	unblock := rangeBlock
	unblock.Op = BackoffUnblock
	unblock.MapEpoch = 12
	shard := Backoff{PG: PG{Pool: 9, Seed: 2}, Shard: 3, MapEpoch: 19, Op: BackoffBlock, Begin: PGCursor{Pool: math.MinInt64}, End: PGCursor{Pool: math.MinInt64, Max: true}}
	return []struct {
		name  string
		value Backoff
	}{{"block-single", single}, {"block-range", rangeBlock}, {"ack-range", ack}, {"unblock-range", unblock}, {"block-sharded", shard}}
}

func TestBackoffMatchesFreshNativeEncoder(t *testing.T) {
	for _, tc := range nativeBackoffCases() {
		t.Run(tc.name, func(t *testing.T) {
			name := "backoff-native-event-v1-" + tc.name
			native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{oracleBytes(t, name+".header"), oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data")}})
			if err != nil {
				t.Fatal(err)
			}
			if native.Sequence != 1 || native.AckSequence != 3 || native.Transaction != 0 || native.Priority != 127 {
				t.Fatal("native offline header identity", native)
			}
			got, err := EncodeBackoff(tc.value, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			got.Sequence, got.AckSequence = 1, 3
			if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
				t.Fatal("native header/front differ")
			}
			decoded, err := DecodeBackoff(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			decoded.RawFront = nil
			decoded.Begin.Raw = nil
			decoded.End.Raw = nil
			if !reflect.DeepEqual(decoded, tc.value) {
				t.Fatal("native backoff identity differs", decoded, tc.value)
			}
			if tc.name == "block-range" {
				ack, err := EncodeBackoffAck(decoded, wire.DefaultLimit)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(ack.Front, oracleBytes(t, "backoff-native-event-v1-ack-range.front")) {
					t.Fatal("ACK differs from independently encoded native response")
				}
			}
		})
	}
}
