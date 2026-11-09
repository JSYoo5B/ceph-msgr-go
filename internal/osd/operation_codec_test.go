package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// nativeV6OperationRequests pairs retained fresh-native v6 request vectors with
// the logical requests their own tests build. The vectors cover every union
// family and compound requests whose operations carry distinct input lengths.
func nativeV6OperationRequests() map[string]Request {
	base := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}}
	with := func(object string, ops []Operation, seconds, nanoseconds uint32) Request {
		r := base
		r.Object, r.Operations, r.MTimeSeconds, r.MTimeNanoseconds = object, ops, seconds, nanoseconds
		return r
	}
	vectors := map[string]Request{
		"request-v6":                        oracleRequest(),
		"xattr-native-request-v6-all":       xattrNativeRequest(),
		"call-native-request-v6-all":        callNativeRequest(),
		"omap-native-request-v6-read-all":   with("omap-codec", omapNativeOperations(), 0, 0),
		"omap-write-native-request-v6-all":  omapWriteNativeRequest(),
		"omap-cmp-native-request-v6-all":    omapCompareNativeRequest(),
		"omap-range-native-request-v6-all":  omapRangeNativeRequest(),
		"mapext-native-request-v6-map":      mapExtNativeRequest(),
		"sparse-read-native-request-v6-map": sparseReadNativeRequest(),
		"watchers-native-request-v6-list":   watchersNativeRequest(),
		"writesame-native-request-v6-all":   with("writesame-codec", writeSameNativeOperations(), 1700000000, 123456789),
		"mutation-native-request-v6-all":    with("mutation-codec", nativeEncoderOperations(), 1700000000, 123456789),
		"alloc-hint-native-request-v6-all":  allocHintNativeRequest(allocHintNativeOperation(allocHintNativeHint), allocHintNativeWriteFull, allocHintNativeOperation(AllocHintInput{})),
	}
	for _, name := range []string{"register", "unwatch", "ping", "reconnect", "notify", "ack"} {
		vectors["watch-notify-native-request-v6-"+name] = with("watch-notify-codec", []Operation{watchNativeOperation(name)}, 0, 0)
	}
	return vectors
}

// operationVector returns the u16 count and ceph_osd_op entries of a request
// front. start is the encoded size of the version-specific envelope before
// the object name.
func operationVector(t *testing.T, front []byte, start int, r Request) []byte {
	t.Helper()
	begin := start + 4 + len(r.Object)
	end := begin + 2 + len(r.Operations)*opWireSize
	if end > len(front) {
		t.Fatal("request front shorter than its operation vector", len(front), end)
	}
	return front[begin:end]
}

func locatorWireBytes(loc Locator) int {
	return 6 + 8 + 4 + 4 + len(loc.Key) + 4 + len(loc.Namespace) + 8
}

// The shared operation codec must produce one wire vector regardless of the
// envelope version. v6 bytes are anchored to fresh native encodings; v8 and
// v9 must then carry the same vector, the same snapshot/retry tail and the
// same data segment through their own public entry points.
func TestOperationVectorMatchesNativeAcrossRequestVersions(t *testing.T) {
	for name, r := range nativeV6OperationRequests() {
		t.Run(name, func(t *testing.T) {
			v6, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(v6.Front, oracleBytes(t, name+".front")) {
				t.Fatal("v6 front differs from the retained native vector")
			}
			var data []byte
			if _, err := os.Stat("testdata/" + name + ".data.hex"); err == nil {
				data = oracleBytes(t, name+".data")
			}
			if !bytes.Equal(v6.Data, data) {
				t.Fatal("v6 data differs from the retained native vector")
			}
			v8Input := V8Request{Request: r, PG: PG{Pool: uint64(r.Locator.Pool), Seed: 0x44}, Shard: -1}
			v8, err := EncodeRequestV8(v8Input, v8OracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			v9, err := EncodeRequestV9(V9Request(v8Input), v9UnitFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			loc := locatorWireBytes(r.Locator)
			// v6: client_inc, epoch, flags, mtime, reassert version, locator, pg_t.
			v6Start := 4 + 4 + 4 + 8 + 12 + loc + 17
			// v8: spg_t, raw hash, epoch, flags, reqid, trace, client_inc, mtime, locator.
			v8Start := 24 + 12 + 27 + 24 + 4 + 8 + loc
			v9Start := v8Start + int(emptyOTELBytes)
			want := operationVector(t, v6.Front, v6Start, r)
			if !bytes.Equal(operationVector(t, v8.Front, v8Start, r), want) || !bytes.Equal(operationVector(t, v9.Front, v9Start, r), want) {
				t.Fatal("v8/v9 operation vector differs from the native v6 vector")
			}
			// Snapshot, snap context and retry fields follow the vector in all
			// versions; v6 alone appends its reqid after the feature word.
			v6Tail := v6.Front[v6Start+4+len(r.Object)+len(want) : len(v6.Front)-8-27]
			v8Tail := v8.Front[v8Start+4+len(r.Object)+len(want) : len(v8.Front)-8]
			v9Tail := v9.Front[v9Start+4+len(r.Object)+len(want) : len(v9.Front)-8]
			if !bytes.Equal(v8Tail, v6Tail) || !bytes.Equal(v9Tail, v6Tail) {
				t.Fatal("v8/v9 snapshot or retry tail differs from v6")
			}
			if !bytes.Equal(v8.Data, v6.Data) || !bytes.Equal(v9.Data, v6.Data) {
				t.Fatal("v8/v9 operation data differs from the native v6 data")
			}
		})
	}
}

// Every request entry point must apply the same per-operation reply output
// budget on top of its envelope estimate.
func TestOperationReplyBudgetIsSharedByRequestVersions(t *testing.T) {
	r := xattrNativeRequest()
	v8Input := V8Request{Request: r, PG: PG{Pool: uint64(r.Locator.Pool), Seed: 0x44}, Shard: -1}
	encode := map[string]func(uint32) error{
		"v6": func(limit uint32) error { _, err := EncodeRequest(r, oracleFeatures, limit); return err },
		"v8": func(limit uint32) error { _, err := EncodeRequestV8(v8Input, v8OracleFeatures, limit); return err },
		"v9": func(limit uint32) error {
			_, err := EncodeRequestV9(V9Request(v8Input), v9UnitFeatures, limit)
			return err
		},
	}
	// 190 reply envelope bytes, object name, per-op reply metadata, then
	// GETXATTR and GETXATTRS caller output budgets (16 and 64 bytes).
	exact := uint32(190 + len(r.Object) + len(r.Operations)*(opWireSize+4) + 16 + 64)
	for version, call := range encode {
		if err := call(exact); err != nil {
			t.Fatal(version, "exact reply budget refused", err)
		}
		if err := call(exact - 1); !errors.Is(err, wire.ErrLimit) {
			t.Fatal(version, "reply output budget not enforced", err)
		}
	}
}

// v6ReplyAsV8 replaces the v6 always-present empty redirect structure with
// the v8 absent-redirect flag; every other byte is shared by both layouts.
func v6ReplyAsV8(t *testing.T, m msgr.MessageData, r Reply) msgr.MessageData {
	t.Helper()
	body := wire.Encoder{}
	encodeLocator(&body, r.Redirect.Locator)
	body.String(r.Redirect.Object)
	body.Bytes(r.Redirect.LegacyInstructions)
	redirect := wire.Encoder{}
	redirect.Struct(1, 1, body.Data)
	end := len(m.Front) - 24
	start := end - len(redirect.Data)
	if start < 0 || !bytes.Equal(m.Front[start:end], redirect.Data) {
		t.Fatal("native v6 reply does not end with an empty redirect and trace")
	}
	front := append(bytes.Clone(m.Front[:start]), 0)
	m.Front = append(front, m.Front[end:]...)
	m.Version = 8
	return m
}

// Both reply entry points share one operation decoder. Each retained native
// v6 reply, rewritten only at its redirect field, must decode to the same
// operations through the v8 entry point, and every malformed or truncated
// prefix must fail with the same error and the same partial operations.
func TestOperationDecodingMatchesAcrossReplyVersions(t *testing.T) {
	fronts, err := filepath.Glob("testdata/*reply-v6*.front.hex")
	if err != nil || len(fronts) == 0 {
		t.Fatal("no retained native v6 replies", err)
	}
	checked := 0
	for _, path := range fronts {
		name := strings.TrimSuffix(strings.TrimPrefix(path, "testdata/"), ".front.hex")
		m := msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: oracleBytes(t, name+".front")}
		if _, err := os.Stat("testdata/" + name + ".data.hex"); err == nil {
			m.Data = oracleBytes(t, name+".data")
		}
		v6, err := DecodeReply(m, wire.DefaultLimit)
		if errors.Is(err, ErrRedirect) {
			continue // A redirect has no v8 equivalent produced by rewriting one field.
		}
		if err != nil {
			t.Fatal(name, err)
		}
		checked++
		v8Message := v6ReplyAsV8(t, m, v6)
		v8, err := DecodeReplyV8(v8Message, wire.DefaultLimit)
		if err != nil {
			t.Fatal(name, err)
		}
		if !reflect.DeepEqual(v8.Operations, v6.Operations) || v8.Result != v6.Result || v8.Flags != v6.Flags || v8.RetryAttempt != v6.RetryAttempt || v8.UserVersion != v6.UserVersion || v8.Trace != v6.Trace {
			t.Fatal(name, "v8 entry point decoded different operations or reply fields")
		}
		// Bytes before the redirect field are identical in both layouts.
		prefix := len(v8Message.Front) - 25
		for n := 0; n < prefix; n++ {
			a6, err6 := DecodeReply(msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: m.Front[:n], Data: m.Data}, wire.DefaultLimit)
			a8, err8 := DecodeReplyV8(msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Front: v8Message.Front[:n], Data: m.Data}, wire.DefaultLimit)
			if err6 == nil || err8 == nil || err6.Error() != err8.Error() || !reflect.DeepEqual(a6.Operations, a8.Operations) {
				t.Fatal(name, "truncated reply decoded differently by version", n, err6, err8)
			}
		}
		for n := 0; n < len(m.Data); n++ {
			a6, err6 := DecodeReply(msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: m.Front, Data: m.Data[:n]}, wire.DefaultLimit)
			a8, err8 := DecodeReplyV8(msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Front: v8Message.Front, Data: m.Data[:n]}, wire.DefaultLimit)
			if err6 == nil || err8 == nil || !errors.Is(err6, io.ErrUnexpectedEOF) || !errors.Is(err8, io.ErrUnexpectedEOF) || !reflect.DeepEqual(a6.Operations, a8.Operations) {
				t.Fatal(name, "truncated reply data split differently by version", n, err6, err8)
			}
		}
		extra := bytes.Clone(m.Data)
		extra = append(extra, 0)
		_, err6 := DecodeReply(msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: m.Front, Data: extra}, wire.DefaultLimit)
		_, err8 := DecodeReplyV8(msgr.MessageData{Type: ReplyMessage, Version: 8, CompatVersion: 2, Front: v8Message.Front, Data: extra}, wire.DefaultLimit)
		if err6 == nil || err8 == nil || !strings.HasPrefix(err6.Error(), "ceph osd reply operation data: ") || !strings.HasPrefix(err8.Error(), "ceph osd v8 reply operation data: ") {
			t.Fatal(name, "unassigned reply data accepted or lost its version context", err6, err8)
		}
	}
	if checked < 40 {
		t.Fatal("too few native v6 replies exercised", checked)
	}
}

// Malformed operation metadata must fail identically through both reply
// entry points and keep the same partially decoded operations.
func TestMalformedOperationMetadataMatchesAcrossReplyVersions(t *testing.T) {
	native := oracleReply(t, "reply-v6-success")
	decoded, err := DecodeReply(native, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Operations) < 2 {
		t.Fatal("native success reply needs a compound operation vector", len(decoded.Operations))
	}
	const count = 58 // u32 operation count in this native reply front.
	second := count + 4 + opWireSize
	for _, tc := range []struct {
		name    string
		edit    func([]byte)
		want    error
		partial int
	}{
		{"empty vector", func(f []byte) { binary.LittleEndian.PutUint32(f[count:], 0) }, ErrOperation, 0},
		{"vector above cap", func(f []byte) { binary.LittleEndian.PutUint32(f[count:], MaxOperations+1) }, wire.ErrLimit, 0},
		{"first opcode unsupported", func(f []byte) { binary.LittleEndian.PutUint16(f[count+4:], 0x2302) }, ErrOperation, len(decoded.Operations)},
		{"second opcode unsupported", func(f []byte) { binary.LittleEndian.PutUint16(f[second:], 0x2302) }, ErrOperation, len(decoded.Operations)},
		{"payload length beyond limit", func(f []byte) { binary.LittleEndian.PutUint32(f[second-4:], math.MaxUint32) }, wire.ErrLimit, len(decoded.Operations)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v6 := native
			v6.Front = bytes.Clone(native.Front)
			tc.edit(v6.Front)
			v8 := v6ReplyAsV8(t, native, decoded)
			v8.Front = bytes.Clone(v8.Front)
			tc.edit(v8.Front)
			a6, err6 := DecodeReply(v6, wire.DefaultLimit)
			a8, err8 := DecodeReplyV8(v8, wire.DefaultLimit)
			// Only the data-split error carries a version-specific context.
			if !errors.Is(err6, tc.want) || !errors.Is(err8, tc.want) || err6.Error() != strings.Replace(err8.Error(), "ceph osd v8 reply", "ceph osd reply", 1) {
				t.Fatal("malformed metadata error differs by version", err6, err8)
			}
			if len(a6.Operations) != tc.partial || !reflect.DeepEqual(a6.Operations, a8.Operations) {
				t.Fatal("partially decoded operations differ by version", a6.Operations, a8.Operations)
			}
		})
	}
}
