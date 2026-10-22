package osd

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The retained requests come from fresh ObjectOperation/MOSDOp objects in the
// pinned native SDK. They are codec evidence, not live execution receipts.
func TestCopyFromFreshNativeRequests(t *testing.T) {
	raw, err := os.ReadFile("testdata/copy-from-native.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Fixtures map[string]struct {
			FrontHex     string `json:"front_hex"`
			DataHex      string `json:"data_hex"`
			HeaderHex    string `json:"header_hex"`
			FrontSHA256  string `json:"front_sha256"`
			DataSHA256   string `json:"data_sha256"`
			HeaderSHA256 string `json:"header_sha256"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Fixtures) != 5 {
		t.Fatal("expected five distinct native requests")
	}
	for _, shape := range []string{"copy-head", "copy2-head", "copy2-truncate", "copy-snapshot", "copy2-snapshot"} {
		t.Run(shape, func(t *testing.T) {
			fixture, ok := proof.Fixtures["copy-from-native-request-v6-"+shape]
			if !ok {
				t.Fatal("missing native shape")
			}
			decode := func(text, digest string) []byte {
				t.Helper()
				b, err := hex.DecodeString(text)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(b)
				if hex.EncodeToString(sum[:]) != digest {
					t.Fatal("native segment hash differs")
				}
				return b
			}
			front := decode(fixture.FrontHex, fixture.FrontSHA256)
			data := decode(fixture.DataHex, fixture.DataSHA256)
			header := decode(fixture.HeaderHex, fixture.HeaderSHA256)
			op := Operation{Code: CopyFrom, CopyFrom: CopyFromInput{Object: "src\x01é", Locator: Locator{Pool: 8, Key: "source-key", Namespace: "source-ns", Hash: -1}}}
			if strings.HasPrefix(shape, "copy2-") {
				op.Code = CopyFrom2
			}
			if strings.HasSuffix(shape, "snapshot") {
				op.CopyFrom.Snap, op.CopyFrom.Version, op.CopyFrom.SourceFadvise = 6, 0x0102030405060708, OpFadviseSequential
			}
			if shape == "copy2-truncate" {
				op.CopyFrom.Flags, op.CopyFrom.TruncateSequence, op.CopyFrom.TruncateSize = CopyFromTruncateSequence, 9, 0xfedcba98
			}
			r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "copy-codec",
				Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
				MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: []Operation{op}}
			got, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
			if !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatal("native MessageFrame header differs")
			}
			if op.Code == CopyFrom2 && op.CopyFrom.Flags == 0 {
				// Native Objecter appends the unused fields unconditionally.
				// The Go contract deliberately retains the shorter server input.
				// Assert the entire difference; do not call these bytes identical.
				if len(data) != len(got.Data)+12 || !bytes.Equal(data[:len(got.Data)], got.Data) || !bytes.Equal(data[len(got.Data):], make([]byte, 12)) {
					t.Fatal("flagless native COPY_FROM2 must have exactly twelve extra zero bytes")
				}
				if len(front) != len(got.Front) {
					t.Fatal("native front length differs")
				}
				// The legacy request has 59 trailing bytes after the op vector:
				// snap 8, snap sequence 8, count 4, retry 4, features 8, reqid 27.
				lengthOffset := len(front) - 59 - 4
				if binary.LittleEndian.Uint32(front[lengthOffset:]) != uint32(len(data)) || binary.LittleEndian.Uint32(got.Front[lengthOffset:]) != uint32(len(got.Data)) ||
					!bytes.Equal(front[:lengthOffset], got.Front[:lengthOffset]) || !bytes.Equal(front[lengthOffset+4:], got.Front[lengthOffset+4:]) {
					t.Fatal("difference extends beyond the copy input payload length")
				}
			} else if !bytes.Equal(got.Front, front) || !bytes.Equal(got.Data, data) {
				t.Fatalf("native copy request differs: front=%x data=%x", got.Front, got.Data)
			}
		})
	}
}
