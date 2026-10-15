package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func checksumOperation() Operation {
	return Operation{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumInput{ChunkSize: 4, Type: ChecksumCRC32C, Seed: 0xffffffff}}
}

func TestChecksumBoundedAdmissionAndWire(t *testing.T) {
	op := checksumOperation()
	r := oracleRequest()
	r.Operations = []Operation{op}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	union := xattrLiteral(t, "1f12 00000000 0200000000000000 0800000000000000 04000000 02 00000000000000 04000000")
	if !bytes.Contains(m.Front, union) || !bytes.Equal(m.Data, []byte{255, 255, 255, 255}) || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("checksum encoding differs", m)
	}
	for _, edit := range []func(*Operation){
		func(op *Operation) { op.Checksum.Type = 0 }, func(op *Operation) { op.Checksum.Type = 1 }, func(op *Operation) { op.Flags = OpFailOK },
		func(op *Operation) { op.Length = 0 }, func(op *Operation) { op.Length = 7 }, func(op *Operation) { op.Offset = ^uint64(0) },
		func(op *Operation) { op.Data = []byte{1} }, func(op *Operation) { op.Version = 1 }, func(op *Operation) { op.Class = ClassInput{Class: "x"} },
	} {
		bad := op
		edit(&bad)
		if _, _, err := ValidateOperations([]Operation{bad}, 1024); err == nil {
			t.Fatal("invalid checksum request admitted", bad)
		}
	}
	if _, _, err := ValidateOperations([]Operation{op, {Code: Stat}}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("compound checksum admitted", err)
	}
	bad := op
	bad.Length = 1028
	bad.Checksum.ChunkSize = 1
	if _, _, err := ValidateOperations([]Operation{bad}, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("unbounded checksum admitted", err)
	}
	// A small read can have a larger count-prefix/digest result than its data.
	bad = op
	bad.Offset = 0
	bad.Length = 8
	bad.Checksum.ChunkSize = 1
	if _, _, err := ValidateOperations([]Operation{bad}, 32); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("checksum output bound omitted", err)
	}
	for _, snap := range []Request{{ReadSnap: 1}, {SnapSeq: 1, Snaps: []uint64{1}}, {OrderSnap: true}} {
		snap.ClientInc = r.ClientInc
		snap.MapEpoch = r.MapEpoch
		snap.Locator = r.Locator
		snap.Object = r.Object
		snap.Hash = r.Hash
		snap.Operations = r.Operations
		if _, err := EncodeRequest(snap, oracleFeatures, 1024); err == nil {
			t.Fatal("snapshot checksum admitted")
		}
	}
}

func checksumValues(values ...uint32) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(values)))
	for _, v := range values {
		e.U32(v)
	}
	return e.Data
}

func TestChecksumReplyCountAndEOF(t *testing.T) {
	op := checksumOperation()
	valid := OperationResult{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumResult{ChunkSize: 4, Type: 2}, PayloadLength: 12, Data: checksumValues(1, 2)}
	if err := ValidateChecksumReply(op, valid, true); err != nil {
		t.Fatal(err)
	}
	trimmed := valid
	trimmed.Length = 4
	trimmed.Data = checksumValues(9)
	trimmed.PayloadLength = 8
	if err := ValidateChecksumReply(op, trimmed, true); err != nil {
		t.Fatal("trimmed checksum refused", err)
	}
	eof := valid
	eof.Data = nil
	eof.PayloadLength = 0
	if err := ValidateChecksumReply(op, eof, true); err != nil {
		t.Fatal("EOF checksum refused", err)
	}
	refused := eof
	refused.Length = 6
	refused.Result = -22
	if err := ValidateChecksumReply(op, refused, false); err != nil {
		t.Fatal("unaligned trimmed refusal rejected", err)
	}
	for _, edit := range []func(*OperationResult){
		func(r *OperationResult) { r.Checksum.Type = 1 }, func(r *OperationResult) { r.Checksum.ChunkSize = 8 }, func(r *OperationResult) { r.Length = 12 },
		func(r *OperationResult) { r.Length = 7 },
		func(r *OperationResult) { r.Length = 0; r.Data = checksumValues(); r.PayloadLength = 4 }, func(r *OperationResult) { r.Data = checksumValues(1); r.PayloadLength = 8 },
		func(r *OperationResult) { r.Data = []byte{255, 255, 255, 255}; r.PayloadLength = 4 }, func(r *OperationResult) { r.Offset++ },
	} {
		bad := valid
		edit(&bad)
		if err := ValidateChecksumReply(op, bad, true); err == nil {
			t.Fatal("contradictory checksum reply admitted", bad)
		}
	}
	for _, data := range [][]byte{{0}, {255, 255, 255, 255}, append(checksumValues(1), 0)} {
		if _, err := DecodeChecksum(data); err == nil {
			t.Fatal("malformed checksum count admitted", data)
		}
	}
	e := wire.Encoder{}
	encodeChecksumUnion(&e, op)
	d := wire.NewDecoderLimit(e.Data, 28)
	offset, length, result := decodeChecksumUnion(d)
	if err := d.Done(); err != nil || offset != 2 || length != 8 || result != (ChecksumResult{ChunkSize: 4, Type: 2}) {
		t.Fatal(result, err)
	}
	e.Data[27] = 1
	d = wire.NewDecoderLimit(e.Data, 28)
	decodeChecksumUnion(d)
	if d.Done() == nil {
		t.Fatal("nonzero checksum padding accepted")
	}
}

func TestChecksumMatchesFreshNativeEncoder(t *testing.T) {
	op := checksumOperation()
	r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "checksum-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{op}}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "checksum-native-request-v6-crc32c")
	if !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("checksum request differs from independently constructed native request")
	}
	for _, name := range []string{"success", "trimmed", "eof", "unaligned"} {
		message := xattrNativeMessage(t, "checksum-native-reply-v6-"+name)
		reply, err := DecodeReply(message, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateChecksumReply(op, reply.Operations[0], reply.Result >= 0); err != nil {
			t.Fatal(name, err)
		}
		if name == "unaligned" && (reply.Result != -22 || reply.Operations[0].Length != 6) {
			t.Fatal("native trimmed refusal changed", reply)
		}
	}
}
