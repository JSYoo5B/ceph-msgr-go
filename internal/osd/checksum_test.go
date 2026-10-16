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
		func(op *Operation) { op.Checksum.Type = 3 }, func(op *Operation) { op.Checksum.Seed = 1 << 32 }, func(op *Operation) { op.Flags = OpFailOK },
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
	for _, snap := range []Request{{ReadSnap: SnapDir}, {SnapSeq: 1, Snaps: []uint64{1}}, {OrderSnap: true}} {
		snap.ClientInc = r.ClientInc
		snap.MapEpoch = r.MapEpoch
		snap.Locator = r.Locator
		snap.Object = r.Object
		snap.Hash = r.Hash
		snap.Operations = r.Operations
		if _, err := EncodeRequest(snap, oracleFeatures, 1024); err == nil {
			t.Fatal("invalid snapshot checksum admitted")
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
		if _, err := DecodeChecksum(ChecksumCRC32C, data); err == nil {
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

func TestChecksumAlgorithmWidthsAndReplyBudgets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		algorithm uint8
		seed      uint64
		width     uint64
	}{
		{"crc32c", ChecksumCRC32C, 0xffffffff, 4},
		{"xxhash32", ChecksumXXHash32, 0x89abcdef, 4},
		{"xxhash64", ChecksumXXHash64, 0x0123456789abcdef, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := checksumOperation()
			op.Checksum.Type, op.Checksum.Seed = tc.algorithm, tc.seed
			budget := uint32(4 + 2*tc.width)
			if _, bytes, err := ValidateOperations([]Operation{op}, budget); err != nil || bytes != tc.width {
				t.Fatal("wrong seed/reply width", bytes, err)
			}
			if _, _, err := ValidateOperations([]Operation{op}, budget-1); !errors.Is(err, wire.ErrLimit) {
				t.Fatal("reply budget failed to account for algorithm width", err)
			}
			for _, seed := range []uint64{0, 1 << 32, ^uint64(0)} {
				op.Checksum.Seed = seed
				_, _, err := ValidateOperations([]Operation{op}, 1024)
				if (tc.width == 4 && seed > 0xffffffff) != errors.Is(err, ErrRequest) {
					t.Fatal("seed width was truncated or rejected incorrectly", seed, err)
				}
			}
			op.Checksum.Seed = tc.seed
			r := nativeV8Request("checksum")
			r.Operations = []Operation{op}
			message, err := EncodeRequestV8(r, v8OracleFeatures, 1024)
			if err != nil || uint64(len(message.Data)) != tc.width {
				t.Fatal("v8 seed payload differs", message.Data, err)
			}
			encoded := uint64(binary.LittleEndian.Uint32(message.Data))
			if tc.width == 8 {
				encoded = binary.LittleEndian.Uint64(message.Data)
			}
			if encoded != tc.seed {
				t.Fatal("v8 seed truncated", encoded)
			}
			// The union payload-length tail is width-specific in both request encoders.
			e := wire.Encoder{}
			e.U16(Checksum)
			e.U32(0)
			encodeChecksumUnion(&e, op)
			e.U32(uint32(tc.width))
			if !bytes.Contains(message.Front, e.Data) {
				t.Fatal("v8 op payload size differs")
			}
		})
	}
	if _, err := DecodeChecksum(3, nil); !errors.Is(err, ErrOperation) {
		t.Fatal("unknown algorithm was accepted for empty output", err)
	}
}

func TestChecksumXXHashMatchesFreshNativeEncoder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		algorithm uint8
		seed      uint64
	}{
		{"xxhash32", ChecksumXXHash32, 0x89abcdef},
		{"xxhash64", ChecksumXXHash64, 0x0123456789abcdef},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := checksumOperation()
			op.Checksum.Type, op.Checksum.Seed = tc.algorithm, tc.seed
			r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "checksum-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{op}}
			m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
			native := xattrNativeMessage(t, "checksum-native-request-v6-"+tc.name)
			if !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
				t.Fatal("checksum request differs from independently constructed native request")
			}
			for _, name := range []string{"success", "trimmed", "eof", "unaligned"} {
				message := xattrNativeMessage(t, "checksum-native-reply-v6-"+tc.name+"-"+name)
				reply, err := DecodeReply(message, wire.DefaultLimit)
				if err != nil {
					t.Fatal(name, err)
				}
				result := reply.Operations[0]
				if err := ValidateChecksumReply(op, result, reply.Result >= 0); err != nil {
					t.Fatal(name, err)
				}
				if name == "success" {
					values, err := DecodeChecksum(tc.algorithm, result.Data)
					if err != nil || len(values) != 2 {
						t.Fatal(values, err)
					}
					want := []uint64{0x11223344, 0xaabbccdd}
					if tc.algorithm == ChecksumXXHash64 {
						want = []uint64{0x1122334455667788, 0xffeeddccbbaa0099}
					}
					for i := range want {
						if values[i] != want[i] {
							t.Fatal("digest truncated or changed endianness", values)
						}
					}
					for n := 1; n < len(result.Data); n++ {
						if _, err := DecodeChecksum(tc.algorithm, result.Data[:n]); err == nil {
							t.Fatal("truncated native output accepted", n)
						}
					}
					for _, malformed := range [][]byte{append(append([]byte(nil), result.Data...), 0), {255, 255, 255, 255}} {
						if _, err := DecodeChecksum(tc.algorithm, malformed); err == nil {
							t.Fatal("malformed native output accepted")
						}
					}
					bad := result
					bad.Checksum.Type = ChecksumCRC32C
					if err := ValidateChecksumReply(op, bad, true); err == nil {
						t.Fatal("different algorithm reply accepted")
					}
				}
				if name == "unaligned" && (reply.Result != -22 || result.Length != 6) {
					t.Fatal("native trimmed refusal changed", reply)
				}
			}
		})
	}
}
