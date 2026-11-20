package osd

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The native wrapper built these operations with the fields the CephFS data
// path sets: Objecter::read_trunc, ObjectOperation::write(off, bl,
// truncate_size, truncate_seq) and Objecter::trunc (see
// TestTruncateFieldRequestsMatchFreshNativeEncoder).
func truncateNativeRequest(ops ...Operation) Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "trunc-codec",
		Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: ops}
}

var (
	truncateNativeRead  = Operation{Code: Read, Offset: 4, Length: 12, TruncateSize: 10, TruncateSequence: 3}
	truncateNativeWrite = Operation{Code: Write, Offset: 8, Length: 8, Data: []byte("truncate"), TruncateSize: 10, TruncateSequence: 2}
	truncateNativeTrunc = Operation{Code: Truncate, Offset: 10, TruncateSize: 10, TruncateSequence: 3}
)

func truncateNativeRequests() map[string]Request {
	write := truncateNativeRequest(truncateNativeWrite)
	write.MTimeSeconds, write.MTimeNanoseconds, write.SnapSeq = 1739999999, 5, 1
	trunc := truncateNativeRequest(truncateNativeTrunc)
	trunc.MTimeSeconds, trunc.MTimeNanoseconds, trunc.SnapSeq = 1739999999, 6, 1
	return map[string]Request{
		"truncate-native-request-v6-read":  truncateNativeRequest(truncateNativeRead),
		"truncate-native-request-v6-write": write,
		"truncate-native-request-v6-trunc": trunc,
	}
}

// TestTruncateFieldRequestsMatchFreshNativeEncoder compares the three requests
// of the six truncate-native-* vectors (regenerate with `encode-osd-truncate`,
// see runNativeEncoder in integration/fixture; provenance in
// testdata/truncate-native-provenance.json): READ 4~12, WRITE at 8 and TRUNCATE
// 10, each carrying truncate_size/truncate_seq as the CephFS data path sends them
// (ceph/ceph/blob/v20.2.4/src/client/Client.cc Filer::read_trunc/write_trunc,
// src/osdc/Objecter.h read_trunc, write_trunc and trunc). Fresh native encoders
// built them and separate native decoders checked them; headers use offline
// sequence/ACK inputs. The same requests without truncate fields must differ.
// No v8 vector exists, so EncodeRequestV8's copy of the field order is not
// covered.
func TestTruncateFieldRequestsMatchFreshNativeEncoder(t *testing.T) {
	requests := truncateNativeRequests()
	for _, tc := range []struct {
		name   string
		flags  uint64
		data   string
		union  string
		opcode uint16
	}{
		{"truncate-native-request-v6-read", FlagRead, "", "0400000000000000 0c00000000000000 0a00000000000000 03000000", Read},
		{"truncate-native-request-v6-write", FlagWrite, "truncate", "0800000000000000 0800000000000000 0a00000000000000 02000000", Write},
		{"truncate-native-request-v6-trunc", FlagWrite, "", "0a00000000000000 0000000000000000 0a00000000000000 03000000", Truncate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := requests[tc.name]
			m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
			native := xattrNativeMessage(t, tc.name)
			if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 212 || string(native.Data) != tc.data ||
				!bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
				t.Fatalf("Go truncate-field request differs from fresh native encoding\nGo     front %x\nnative front %x", m.Front, native.Front)
			}
			if flags := uint64(binary.LittleEndian.Uint32(native.Front[8:12])); flags != tc.flags|FlagOnDisk|FlagReturnVec {
				t.Fatalf("native truncate-field request flags changed: %#x", flags)
			}
			// Opcode, op flags 0, the extent union (offset, length,
			// truncate_size, truncate_seq) and payload_len.
			op := binary.LittleEndian.AppendUint16(nil, tc.opcode)
			op = append(op, 0, 0, 0, 0)
			op = append(op, xattrLiteral(t, tc.union)...)
			op = binary.LittleEndian.AppendUint32(op, uint32(len(tc.data)))
			if !bytes.Contains(native.Front, op) {
				t.Fatalf("native extent union with truncate fields differs: %x", native.Front)
			}
			// Without the truncate fields the Go request no longer matches.
			plain := r
			plain.Operations = []Operation{r.Operations[0]}
			plain.Operations[0].TruncateSize, plain.Operations[0].TruncateSequence = 0, 0
			z, err := EncodeRequest(plain, oracleFeatures, wire.DefaultLimit)
			if err != nil || bytes.Equal(z.Front, native.Front) {
				t.Fatal("zero truncate fields encode the native sequenced request", err)
			}
		})
	}
}

// TestDecodeFreshNativeTruncateFieldReplies decodes the three constructed
// truncate replies (not daemon replies). Their shapes follow
// ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc: do_read clips a READ to
// truncate_size when the object truncate_seq is older (extent 6), the WRITE
// handler trims an old-sequence write (extent 2), and a sequenced TRUNCATE no
// newer than the object is a committed no-op. They claim no OSD execution,
// trimming, durability or cap behaviour.
func TestDecodeFreshNativeTruncateFieldReplies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     Operation
		flags  uint64
		length uint64
		data   string
		replay Version
		user   uint64
	}{
		{"truncate-native-reply-v6-read-clipped", truncateNativeRead, FlagRead | FlagAck | FlagOnDisk | FlagReturnVec, 6, "ABCDEF", Version{}, 21},
		{"truncate-native-reply-v6-write-trimmed", truncateNativeWrite, FlagWrite | FlagAck | FlagOnDisk | FlagReturnVec, 2, "", Version{Epoch: 11, Version: 22}, 22},
		{"truncate-native-reply-v6-trunc-noop", truncateNativeTrunc, FlagWrite | FlagAck | FlagOnDisk | FlagReturnVec, 0, "", Version{Epoch: 11, Version: 23}, 23},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Object != "trunc-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != 0 || r.Flags != tc.flags || r.MapEpoch != 11 ||
				r.ReplayVersion != tc.replay || r.UserVersion != tc.user || len(r.Operations) != 1 {
				t.Fatalf("native truncate-field reply identity, flags or versions changed: %+v", r)
			}
			op := r.Operations[0]
			if op.Code != tc.op.Code || op.Flags != 0 || op.Result != 0 || op.Offset != tc.op.Offset || op.Length != tc.length ||
				op.TruncateSize != tc.op.TruncateSize || op.TruncateSequence != tc.op.TruncateSequence || string(op.Data) != tc.data || op.PayloadLength != uint32(len(tc.data)) {
				t.Fatalf("native truncate-field reply operation changed: %+v", op)
			}
		})
	}
}
