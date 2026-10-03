package osd

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const oracleFeatures uint64 = 3026423347916342070

func oracleBytes(t *testing.T, name string) []byte {
	t.Helper()
	p, err := os.ReadFile("testdata/" + name + ".hex")
	if err != nil {
		t.Fatal(err)
	}
	p, err = hex.DecodeString(strings.TrimSpace(string(p)))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func oracleRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "osd-codec", Hash: 0x11223344,
		Operations: []Operation{{Code: Stat}, {Code: Read, Offset: 2, Length: 4}}}
}

func oracleReply(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	m := msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: oracleBytes(t, name+".front")}
	if name == "reply-v6-success" {
		m.Data = oracleBytes(t, name+".data")
	}
	return m
}

func TestEncodeRequestMatchesNativeCeph(t *testing.T) {
	m, err := EncodeRequest(oracleRequest(), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != RequestMessage || m.Version != 6 || m.CompatVersion != 3 || m.Priority != 127 || len(m.Data) != 0 || len(m.Middle) != 0 || m.Transaction != 0 {
		t.Fatal("incorrect request message metadata", m)
	}
	if !bytes.Equal(m.Front, oracleBytes(t, "request-v6.front")) {
		t.Fatal("stat/read request differs from pinned native Ceph encoder bytes")
	}
}

func TestReadVectorReplyBudget(t *testing.T) {
	r := oracleRequest()
	// The native success fixture uses 41 header + 242 front + 20 data bytes.
	if _, err := EncodeRequest(r, oracleFeatures, 302); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("reply vector must fit the entire message", err)
	}
	if _, err := EncodeRequest(r, oracleFeatures, 303); err != nil {
		t.Fatal("exact reply budget", err)
	}
	r.Operations = []Operation{{Code: Read, Length: 600}, {Code: Read, Length: 600}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("multiple individually bounded reads exceeded aggregate reply limit", err)
	}
	r.Operations = []Operation{{Code: Stat}}
	r.Object = strings.Repeat("x", 1024)
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("request front and metadata exceeded message limit", err)
	}
}

func TestRequestRejectsUnsupportedEncodingAndOperations(t *testing.T) {
	for _, f := range []uint64{oracleFeatures &^ featureObjectLocator, oracleFeatures &^ featurePGID64, oracleFeatures | featureNewOSDOp, oracleFeatures | featureNewOSDReply} {
		if _, err := EncodeRequest(oracleRequest(), f, wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
			t.Fatal("unsupported negotiated encoding", f, err)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*Request)
		want error
	}{
		{"epoch", func(r *Request) { r.MapEpoch = 0 }, ErrRequest},
		{"pool", func(r *Request) { r.Locator.Pool = -1 }, ErrRequest},
		{"object", func(r *Request) { r.Object = "" }, ErrRequest},
		{"locator hash", func(r *Request) { r.Locator.Hash = 0 }, ErrRequest},
		{"empty vector", func(r *Request) { r.Operations = nil }, ErrRequest},
		{"too many ops", func(r *Request) { r.Operations = make([]Operation, MaxOperations+1) }, ErrRequest},
		{"mutation", func(r *Request) { r.Operations[0].Code = 0x2201 }, ErrOperation},
		{"op flags", func(r *Request) { r.Operations[0].Flags = 1 }, ErrOperation},
		{"stat offset", func(r *Request) { r.Operations[0].Offset = 1 }, ErrRequest},
		{"unbounded read", func(r *Request) { r.Operations[1].Length = 0 }, ErrRequest},
		{"overflow", func(r *Request) { r.Operations[1].Offset = math.MaxUint64 }, ErrRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := oracleRequest()
			tc.edit(&r)
			if _, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
}

func TestDecodeNativeCephReplies(t *testing.T) {
	for _, name := range []string{"reply-v6-success", "reply-v6-enoent", "reply-v6-redirect"} {
		t.Run(name, func(t *testing.T) {
			m := oracleReply(t, name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if name == "reply-v6-redirect" {
				if !errors.Is(err, ErrRedirect) || r.Redirect.Locator != (Locator{Pool: 9, Key: "redirect", Namespace: "tenant", Hash: -1}) || r.Redirect.Object != "next-object" {
					t.Fatal("redirect must be preserved and rejected", r.Redirect, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if r.Object != "osd-codec" || r.PG != (PG{Pool: 7, Seed: 0}) || r.Flags != FlagRead|FlagOnDisk|FlagReturnVec || !r.OnDisk() || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.LegacyVersion != (Version{Version: 19, Epoch: 11}) || r.ReplayVersion != (Version{Version: 20, Epoch: 11}) || r.UserVersion != 19 || len(r.Operations) != 2 {
				t.Fatal("lost native reply metadata", r)
			}
			if !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) || r.Operations[0].Code != Stat || r.Operations[1].Code != Read || r.Operations[1].Offset != 2 {
				t.Fatal("lost native operations or raw message")
			}
			if name == "reply-v6-enoent" {
				if r.Result != -2 || r.Operations[0].Result != -2 || r.Operations[1].Result != -2 || len(r.RawData) != 0 {
					t.Fatal("lost aggregate/per-op negative error", r)
				}
			} else if r.Result != 0 {
				t.Fatal("lost aggregate success", r.Result)
			}
			if name == "reply-v6-success" {
				stat := r.Operations[0]
				read := r.Operations[1]
				if stat.Result != 0 || stat.PayloadLength != 16 || len(stat.Data) != 16 || binary.LittleEndian.Uint64(stat.Data[:8]) != 6 || binary.LittleEndian.Uint32(stat.Data[8:12]) != 1000 || binary.LittleEndian.Uint32(stat.Data[12:]) != 55 || read.Result != 4 || read.PayloadLength != 4 || read.Length != 4 || !bytes.Equal(read.Data, []byte{0, 255, 65, 10}) {
					t.Fatal("lost native stat/read data", r.Operations)
				}
				m.Front[0] ^= 1
				m.Data[0] ^= 1
				if bytes.Equal(r.RawFront, m.Front) || r.Operations[0].Data[0] != 6 {
					t.Fatal("reply borrowed mutable input buffers")
				}
			}
		})
	}
}

func TestReplyRejectsTruncationAndMalformedMetadata(t *testing.T) {
	m := oracleReply(t, "reply-v6-success")
	for n := 0; n < len(m.Front); n++ {
		truncated := m
		truncated.Front = m.Front[:n]
		if _, err := DecodeReply(truncated, wire.DefaultLimit); err == nil {
			t.Fatal("accepted truncated native reply", n)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*msgr.MessageData)
		want error
	}{
		{"type", func(m *msgr.MessageData) { m.Type = RequestMessage }, msgr.ErrFrame},
		{"version", func(m *msgr.MessageData) { m.Version = 8 }, wire.ErrVersion},
		{"compat", func(m *msgr.MessageData) { m.CompatVersion = 3 }, wire.ErrVersion},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{0} }, msgr.ErrFrame},
		{"pg version", func(m *msgr.MessageData) { m.Front[13] = 2 }, wire.ErrVersion},
		{"op count", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[58:62], math.MaxUint32) }, wire.ErrLimit},
		{"unknown op", func(m *msgr.MessageData) { binary.LittleEndian.PutUint16(m.Front[62:64], 0x2201) }, ErrOperation},
		{"payload length", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[96:100], math.MaxUint32) }, wire.ErrLimit},
		{"redirect compat", func(m *msgr.MessageData) { m.Front[171] = 2 }, wire.ErrVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := oracleReply(t, "reply-v6-success")
			tc.edit(&input)
			if _, err := DecodeReply(input, wire.DefaultLimit); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	for n := 0; n < len(m.Data); n++ {
		input := m
		input.Data = m.Data[:n]
		if _, err := DecodeReply(input, wire.DefaultLimit); err == nil {
			t.Fatal("accepted truncated operation data", n)
		}
	}
	m.Data = append(m.Data, 0)
	if _, err := DecodeReply(m, wire.DefaultLimit); err == nil {
		t.Fatal("accepted unassigned trailing operation data")
	}
}

func TestReplyHeaderAndDataBudget(t *testing.T) {
	m := oracleReply(t, "reply-v6-success")
	if _, err := DecodeReply(m, 302); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("message header absent from decoded budget", err)
	}
	r, err := DecodeReply(m, 303)
	if err != nil || !reflect.DeepEqual(r.Trace, [3]uint64{}) {
		t.Fatal("exact native reply budget", err, r.Trace)
	}
}

func FuzzReplyDecode(f *testing.F) {
	for _, name := range []string{"reply-v6-success", "reply-v6-enoent", "reply-v6-redirect"} {
		readHex := func(suffix string) []byte {
			p, err := os.ReadFile("testdata/" + name + suffix + ".hex")
			if err != nil {
				f.Fatal(err)
			}
			p, err = hex.DecodeString(strings.TrimSpace(string(p)))
			if err != nil {
				f.Fatal(err)
			}
			return p
		}
		var data []byte
		if name == "reply-v6-success" {
			data = readHex(".data")
		}
		f.Add(readHex(".front"), data, uint16(6), uint16(2))
	}
	f.Fuzz(func(t *testing.T, front, data []byte, version, compat uint16) {
		// Arbitrary inputs must stay bounded before cloning or allocating ops.
		m := msgr.MessageData{Type: ReplyMessage, Version: version, CompatVersion: compat, Front: front, Data: data}
		r, err := DecodeReply(m, 64*1024)
		if err != nil && !errors.Is(err, ErrRedirect) {
			return
		}
		if len(r.Operations) == 0 || len(r.Operations) > MaxOperations || !bytes.Equal(r.RawFront, front) || !bytes.Equal(r.RawData, data) {
			t.Fatal("accepted reply lost bounds or raw bytes")
		}
		var combined []byte
		for _, op := range r.Operations {
			if op.Code != Read && op.Code != Stat || uint64(len(op.Data)) != uint64(op.PayloadLength) {
				t.Fatal("accepted reply lost operation metadata")
			}
			combined = append(combined, op.Data...)
		}
		if !bytes.Equal(combined, data) {
			t.Fatal("accepted reply did not account for all data")
		}
	})
}
