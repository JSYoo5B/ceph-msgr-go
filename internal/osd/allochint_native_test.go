package osd

import (
	"bytes"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The native wrapper received the same logical inputs through the pinned
// public builders: set_alloc_hint(4194304, 65536, SEQUENTIAL_WRITE|COMPRESSIBLE),
// write_full("abc") and set_alloc_hint(0, 0, 0). set_alloc_hint always adds the
// per-op FAILOK flag, so the Go operations request FAILOK explicitly.
var allocHintNativeHint = AllocHintInput{ExpectedObjectSize: 4194304, ExpectedWriteSize: 65536, Flags: 1 | 256}

func allocHintNativeOperation(hint AllocHintInput) Operation {
	return Operation{Code: SetAllocHint, Flags: OpFailOK, AllocHint: hint}
}

var allocHintNativeWriteFull = Operation{Code: WriteFull, Length: 3, Data: []byte("abc")}

func allocHintNativeRequest(ops ...Operation) Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "alloc-hint-codec",
		Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: ops}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestAllocHintRequestMatchesFreshNativeEncoder(t *testing.T) {
	m, err := EncodeRequest(allocHintNativeRequest(allocHintNativeOperation(allocHintNativeHint), allocHintNativeWriteFull, allocHintNativeOperation(AllocHintInput{})), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "alloc-hint-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 293 || len(native.Data) != 3 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatalf("Go SETALLOCHINT request differs from fresh native encoding\nGo     front %x\nnative front %x\nGo     data %x\nnative data %x", m.Front, native.Front, m.Data, native.Data)
	}
	// SETALLOCHINT and WRITEFULL are WR: native op-mode derivation selects
	// WRITE alone; ONDISK and RETURNVEC are always requested.
	if flags := uint64(native.Front[8]) | uint64(native.Front[9])<<8 | uint64(native.Front[10])<<16 | uint64(native.Front[11])<<24; flags != FlagWrite|FlagOnDisk|FlagReturnVec {
		t.Fatalf("native SETALLOCHINT request flags changed: %#x", flags)
	}
	// Native add_alloc_hint: code 0x2223, builder FAILOK, le64 object size,
	// le64 write size, le32 flags, 8 zero union bytes and payload_len 0.
	for _, op := range []string{
		"2322 02000000 0000400000000000 0000010000000000 01010000 0000000000000000 00000000",
		"2322 02000000 0000000000000000 0000000000000000 00000000 0000000000000000 00000000",
	} {
		if !bytes.Contains(native.Front, xattrLiteral(t, op)) {
			t.Fatalf("native SETALLOCHINT op differs from the pinned alloc_hint layout: %x", native.Front)
		}
	}
	// WRITEFULL is the only data owner.
	if !bytes.Contains(native.Front, xattrLiteral(t, "0222 00000000 0000000000000000 0300000000000000 0000000000000000 00000000 03000000")) || string(native.Data) != "abc" {
		t.Fatalf("native WRITEFULL op or data changed: %x %q", native.Front, native.Data)
	}
	// Without the builder FAILOK the Go request is a different wire request.
	plain := allocHintNativeRequest(Operation{Code: SetAllocHint, AllocHint: allocHintNativeHint}, allocHintNativeWriteFull, allocHintNativeOperation(AllocHintInput{}))
	if changed, err := EncodeRequest(plain, oracleFeatures, wire.DefaultLimit); err != nil || bytes.Equal(changed.Front, native.Front) {
		t.Fatal("FAILOK-free allocation hint encoded like the native builder", err)
	}
}

func TestDecodeFreshNativeAllocHintReplies(t *testing.T) {
	max := AllocHintInput{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: AllocHintFlags}
	committed := FlagAck | FlagOnDisk | FlagWrite | FlagReturnVec
	for _, tc := range []struct {
		name    string
		ops     []Operation
		front   int
		code    int32
		results []int32
		replay  Version
		legacy  Version
		user    uint64
	}{
		{name: "alloc-hint-native-reply-v6-success", ops: []Operation{allocHintNativeOperation(allocHintNativeHint), allocHintNativeWriteFull, allocHintNativeOperation(AllocHintInput{})},
			front: 291, results: []int32{0, 0, 0}, replay: Version{Epoch: 11, Version: 20}, legacy: Version{Epoch: 11, Version: 20}, user: 20},
		{name: "alloc-hint-native-reply-v6-failok-refused", ops: []Operation{allocHintNativeOperation(allocHintNativeHint), allocHintNativeWriteFull},
			front: 249, results: []int32{-95, 0}, replay: Version{Epoch: 11, Version: 20}, legacy: Version{Epoch: 11, Version: 20}, user: 20},
		// set_reply_versions(at_version, unchanged user version 19) keeps
		// the replay epoch while the legacy version takes the user version.
		{name: "alloc-hint-native-reply-v6-enospc", ops: []Operation{allocHintNativeOperation(allocHintNativeHint)},
			front: 207, code: -28, results: []int32{-28}, replay: Version{Epoch: 11, Version: 20}, legacy: Version{Epoch: 11, Version: 19}, user: 19},
		{name: "alloc-hint-native-reply-v6-max", ops: []Operation{allocHintNativeOperation(max)},
			front: 207, results: []int32{0}, replay: Version{Epoch: 11, Version: 20}, legacy: Version{Epoch: 11, Version: 20}, user: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 || len(m.Front) != tc.front || len(m.Data) != 0 {
				t.Fatal("native SETALLOCHINT reply envelope changed", len(m.Front), len(m.Data))
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native flags=%#x result=%d legacy=%+v replay=%+v user=%d", r.Flags, r.Result, r.LegacyVersion, r.ReplayVersion, r.UserVersion)
			if r.Object != "alloc-hint-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != tc.user ||
				r.ReplayVersion != tc.replay || r.LegacyVersion != tc.legacy || r.Flags != committed ||
				len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
				t.Fatal("native SETALLOCHINT reply lost identity, flags, versions or raw payload", r)
			}
			// The native constructor keeps the request WRITE bit, which Go
			// admission derives for every SETALLOCHINT vector.
			mode, _, err := ValidateOperations(tc.ops, wire.DefaultLimit)
			if err != nil || mode != FlagWrite || r.Flags&(FlagRead|FlagWrite) != mode {
				t.Fatal("native reply mode bits differ from Go request admission", mode, err)
			}
			if r.Result >= 0 && !r.OnDisk() {
				t.Fatal("native committed SETALLOCHINT lost ONDISK", r.Flags)
			}
			for i, op := range r.Operations {
				requested := tc.ops[i]
				t.Logf("op code=%#x flags=%d result=%d hint=%+v extent=%d~%d payload=%d", op.Code, op.Flags, op.Result, op.AllocHint, op.Offset, op.Length, op.PayloadLength)
				if op.Code != requested.Code || op.Flags != requested.Flags || op.Result != tc.results[i] || op.PayloadLength != 0 || len(op.Data) != 0 || op.Xattr != (XattrResult{}) || op.Class != (ClassResult{}) {
					t.Fatal("native reply changed per-op code, flags, result or output", op)
				}
				if requested.Code == SetAllocHint {
					if op.AllocHint != requested.AllocHint {
						t.Fatal("native union echo differs from the submitted hint", op.AllocHint)
					}
					if err := ValidateAllocHintReply(requested, op); err != nil {
						t.Fatal("native SETALLOCHINT reply rejected by strict wire validation", err)
					}
					continue
				}
				if op.AllocHint != (AllocHintInput{}) || op.Offset != 0 || op.Length != 3 || op.TruncateSize != 0 || op.TruncateSequence != 0 {
					t.Fatal("native WRITEFULL echo changed", op)
				}
			}
		})
	}
	// The max vector pins all eleven flag bits and both maximum sizes. Any
	// change to the echoed union, including its zero tail, is refused.
	m := xattrNativeMessage(t, "alloc-hint-native-reply-v6-max")
	union := xattrLiteral(t, "2322 02000000 ffffffffffffffff ffffffffffffffff ff070000 0000000000000000 00000000")
	at := bytes.Index(m.Front, union)
	if at < 0 || bytes.LastIndex(m.Front, union) != at {
		t.Fatalf("native max SETALLOCHINT union is not the pinned layout: %x", m.Front)
	}
	r, err := DecodeReply(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAllocHintReply(allocHintNativeOperation(AllocHintInput{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: AllocHintFlags &^ 1024}), r.Operations[0]); err == nil {
		t.Fatal("native max hint echo accepted for a different requested flag set")
	}
	tail := m
	tail.Front = append([]byte(nil), m.Front...)
	tail.Front[at+6+20] = 1
	if _, err := DecodeReply(tail, wire.DefaultLimit); err == nil {
		t.Fatal("nonzero native alloc_hint union tail accepted")
	}
}
