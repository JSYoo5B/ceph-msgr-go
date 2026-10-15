package osd

import (
	"bytes"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// The native wrapper received the same logical inputs: a CMPXATTR guard on
// "a" EQ "v" (STRING mode) through ObjectOperation::cmpxattr, then a
// write-mode CALL hello.write_return_data with binary input through the
// three-argument call(const char*, const char*, buffer::list&) overload.
func callNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "call-codec",
		Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: []Operation{
			{Code: CompareXattr, Data: []byte("v"), Xattr: XattrInput{Name: "a", ComparisonOp: XattrCompareEqual, ComparisonMode: XattrCompareString}},
			{Code: Call, Data: []byte("T\x00\xff"), Class: ClassInput{Class: "hello", Method: "write_return_data", Mode: ClassWrite, MaxOutputBytes: 64}},
		}}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestClassCallRequestMatchesFreshNativeEncoder(t *testing.T) {
	m, err := EncodeRequest(callNativeRequest(), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "call-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 249 || len(native.Data) != 27 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatalf("Go CALL request differs from fresh native encoding\nGo     front %x\nnative front %x\nGo     data %x\nnative data %x", m.Front, native.Front, m.Data, native.Data)
	}
	// The explicit write mode plus the CMPXATTR guard select READ|WRITE;
	// ONDISK and RETURNVEC are always requested.
	if flags := uint64(native.Front[8]) | uint64(native.Front[9])<<8 | uint64(native.Front[10])<<16 | uint64(native.Front[11])<<24; flags != FlagRead|FlagWrite|FlagOnDisk|FlagReturnVec {
		t.Fatalf("native CALL request flags changed: %#x", flags)
	}
	// Native add_call: u8 class_len 5, u8 method_len 17, argc 0, u32
	// indata_len 3, 21 zero union bytes and payload_len 5+17+3.
	union := xattrLiteral(t, "0114 00000000 05 11 00 03000000 000000000000000000000000000000000000000000 19000000")
	if !bytes.Contains(native.Front, union) {
		t.Fatalf("native CALL union differs from the pinned cls layout: %x", native.Front)
	}
	// Request data is the CMPXATTR name/value, then class, method and input.
	if want := []byte("av" + "hello" + "write_return_data" + "T\x00\xff"); !bytes.Equal(native.Data, want) {
		t.Fatalf("native CALL data differs from class/method/input framing: %q", native.Data)
	}
	// MaxOutputBytes is a local receipt budget with no wire field.
	r := callNativeRequest()
	r.Operations[1].Class.MaxOutputBytes = 4096
	changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) || !bytes.Equal(changed.Data, native.Data) {
		t.Fatal("local CALL output budget altered the wire request", err)
	}
}

func TestDecodeFreshNativeClassCallReplies(t *testing.T) {
	call := func(method, input string, mode uint8) Operation {
		return Operation{Code: Call, Data: []byte(input), Class: ClassInput{Class: "hello", Method: method, Mode: mode, MaxOutputBytes: 64}}
	}
	for _, tc := range []struct {
		name          string
		op            Operation
		flags         uint64
		code, result  int32
		class         ClassResult
		output        string
		replay        Version
		legacy        Version
		user          uint64
		executedWrite bool
	}{
		{name: "call-native-reply-v6-read-output", op: call("say_hello", "Tester", ClassRead),
			flags: FlagAck | FlagOnDisk | FlagRead | FlagReturnVec, class: ClassResult{ClassLength: 5, MethodLength: 9, InputLength: 6, OutputLength: 14},
			output: "Hello, Tester!", legacy: Version{Version: 19}, user: 19},
		{name: "call-native-reply-v6-positive", op: call("write_return_data", "", ClassWrite),
			flags: FlagAck | FlagOnDisk | FlagWrite | FlagReturnVec, code: 42, result: 42, class: ClassResult{ClassLength: 5, MethodLength: 17, OutputLength: 18},
			output: "you might see this", replay: Version{Epoch: 11, Version: 20}, legacy: Version{Epoch: 11, Version: 20}, user: 20, executedWrite: true},
		{name: "call-native-reply-v6-eoverflow", op: call("write_too_much_return_data", "", ClassWrite),
			flags: FlagAck | FlagOnDisk | FlagWrite | FlagReturnVec, code: -75, result: -75, class: ClassResult{ClassLength: 5, MethodLength: 26, OutputLength: 520}},
		{name: "call-native-reply-v6-eopnotsupp", op: call("nope", "", ClassRead),
			flags: FlagOnDisk | FlagRead | FlagReturnVec, code: -95, class: ClassResult{ClassLength: 5, MethodLength: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 || len(m.Front) != 201 || string(m.Data) != tc.output {
				t.Fatal("native CALL reply envelope changed", len(m.Front), len(m.Data))
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native flags=%#x result=%d legacy=%+v replay=%+v user=%d", r.Flags, r.Result, r.LegacyVersion, r.ReplayVersion, r.UserVersion)
			if r.Object != "call-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != tc.user ||
				r.ReplayVersion != tc.replay || r.LegacyVersion != tc.legacy || r.Flags != tc.flags ||
				len(r.Operations) != 1 || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
				t.Fatal("native CALL reply lost identity, flags, versions or raw payload", r)
			}
			// The native constructor keeps the request mode bit, which is the
			// explicit caller mode; only the completion bits are logical inputs.
			mode, _, err := ValidateOperations([]Operation{tc.op}, wire.DefaultLimit)
			if err != nil || r.Flags&(FlagRead|FlagWrite) != mode {
				t.Fatal("native reply mode bits differ from Go request admission", mode, err)
			}
			if tc.executedWrite && (!r.OnDisk() || r.Result <= 0) {
				t.Fatal("native positive write commit lost ONDISK or its positive aggregate", r.Flags, r.Result)
			}
			op := r.Operations[0]
			t.Logf("op code=%#x flags=%d result=%d class=%+v payload=%d", op.Code, op.Flags, op.Result, op.Class, op.PayloadLength)
			if op.Code != Call || op.Flags != 0 || op.Result != tc.result || op.Class != tc.class || op.Offset != 0 || op.Length != 0 ||
				op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Xattr != (XattrResult{}) || op.PayloadLength != uint32(len(tc.output)) || string(op.Data) != tc.output {
				t.Fatal("native CALL reply changed per-op union, result or output", op)
			}
			if err := ValidateClassReply(tc.op, op); err != nil {
				t.Fatal("native CALL reply rejected by strict wire validation", err)
			}
		})
	}
	// The EOVERFLOW-cleared shape is accepted only for -75: the same union
	// with another negative result keeps its output-length mismatch.
	m := xattrNativeMessage(t, "call-native-reply-v6-eoverflow")
	r, err := DecodeReply(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	op := r.Operations[0]
	op.Result = -22
	if err := ValidateClassReply(call("write_too_much_return_data", "", ClassWrite), op); err == nil {
		t.Fatal("non-EOVERFLOW result accepted a cleared 520-byte output length")
	}
	// A smaller local budget than the native output refuses the receipt.
	read := xattrNativeMessage(t, "call-native-reply-v6-read-output")
	if r, err = DecodeReply(read, wire.DefaultLimit); err != nil {
		t.Fatal(err)
	}
	small := call("say_hello", "Tester", ClassRead)
	small.Class.MaxOutputBytes = 13
	if err := ValidateClassReply(small, r.Operations[0]); err == nil {
		t.Fatal("native 14-byte CALL output accepted above a 13-byte local budget")
	}
}
