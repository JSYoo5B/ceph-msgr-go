package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func enforceSnapEncode(version int, r V8Request) (msgr.MessageData, error) {
	switch version {
	case 6:
		return EncodeRequest(r.Request, oracleFeatures, 4096)
	case 8:
		return EncodeRequestV8(r, v8OracleFeatures, 4096)
	case 9:
		return EncodeRequestV9(V9Request(r), v9UnitFeatures, 4096)
	default:
		panic("unsupported test version")
	}
}

func TestEnforceSnapContextChangesOnlyRequestFlagEveryLayout(t *testing.T) {
	for _, version := range []int{6, 8, 9} {
		for _, populated := range []bool{false, true} {
			for _, ordered := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/context%t/order%t", version, populated, ordered), func(t *testing.T) {
					r := nativeV8Request("snapshot")
					r.OrderSnap = ordered
					if !populated {
						r.SnapSeq, r.Snaps = 0, nil
					}
					plain, err := enforceSnapEncode(version, r)
					if err != nil {
						t.Fatal(err)
					}
					r.EnforceSnapContext = true
					if err := ValidateEnforceSnapContext(r.Request, 4096); err != nil {
						t.Fatal(err)
					}
					got, err := enforceSnapEncode(version, r)
					if err != nil {
						t.Fatal(err)
					}
					at := 8
					if version != 6 {
						at = 32
					}
					flags := binary.LittleEndian.Uint32(got.Front[at:])
					before := binary.LittleEndian.Uint32(plain.Front[at:])
					if flags != before|0x100000 || flags&uint32(FlagOrderSnap) != before&uint32(FlagOrderSnap) {
						t.Fatal("flags changed beyond ENFORCE_SNAPC", flags, before)
					}
					normalized := bytes.Clone(got.Front)
					binary.LittleEndian.PutUint32(normalized[at:], flags&^0x100000)
					if !bytes.Equal(normalized, plain.Front) || !bytes.Equal(got.Data, plain.Data) || !bytes.Equal(got.Middle, plain.Middle) || got.Version != plain.Version || got.CompatVersion != plain.CompatVersion {
						t.Fatal("flag changed context, payload or layout")
					}
					front, data := bytes.Clone(got.Front), bytes.Clone(got.Data)
					if len(r.Snaps) > 0 {
						r.Snaps[0]--
					}
					r.Operations[0].Data[0] ^= 0xff
					if !bytes.Equal(got.Front, front) || !bytes.Equal(got.Data, data) {
						t.Fatal("encoded message retained caller snapshot/data slices")
					}
				})
			}
		}
	}
}

func TestEnforceSnapContextFalseKeepsRetainedNativeBytes(t *testing.T) {
	v6 := snapNativeWrite(snapNativeRequest(snapNativeFull))
	v6.OrderSnap = true
	got, err := EncodeRequest(v6, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(got.Front, oracleBytes(t, "snap-native-request-v6-write.front")) || !bytes.Equal(got.Data, oracleBytes(t, "snap-native-request-v6-write.data")) {
		t.Fatal("default v6 native bytes changed", err)
	}
	r := nativeV8Request("snapshot")
	got, err = EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(got.Front, oracleBytes(t, "request-v8-native-snapshot.front")) || !bytes.Equal(got.Data, oracleBytes(t, "request-v8-native-snapshot.data")) {
		t.Fatal("default v8 native bytes changed", err)
	}
	r.Object = "v9-codec"
	got, err = EncodeRequestV9(V9Request(r), requestV9NativeFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(got.Front, requestV9NativeSegment(t, "request-v9-native-snapshot", "front")) || !bytes.Equal(got.Data, requestV9NativeSegment(t, "request-v9-native-snapshot", "data")) {
		t.Fatal("default v9 native bytes changed", err)
	}
}

func TestEnforceSnapContextRejectsIgnoredOrInvalidSnapshotInputs(t *testing.T) {
	cases := map[string]func(*V8Request){
		"read": func(r *V8Request) { r.Operations = []Operation{{Code: Read, Length: 1}} },
		"stat": func(r *V8Request) { r.Operations = []Operation{{Code: Stat}} },
		"metadata-read": func(r *V8Request) {
			r.Operations = []Operation{{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 8}}}
		},
		"list-snaps": func(r *V8Request) {
			r.Operations = []Operation{{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 128}}}
		},
		"list-watchers": func(r *V8Request) {
			r.Operations = []Operation{{Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 128}}}
		},
		"read-call": func(r *V8Request) {
			r.Operations = []Operation{{Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 8}}}
		},
		"watch": func(r *V8Request) {
			r.Operations = []Operation{{Code: Watch, Watch: WatchInput{Cookie: 1, Action: WatchRegister}}}
		},
		"notify": func(r *V8Request) {
			r.Operations = []Operation{{Code: Notify, Watch: WatchInput{Cookie: 1, ProtocolVersion: 1}}}
		},
		"notify-ack": func(r *V8Request) {
			r.Operations = []Operation{{Code: NotifyAck, Watch: WatchInput{Cookie: 1, NotifyID: 2}}}
		},
		"write-snapshot": func(r *V8Request) { r.ReadSnap = 3 },
		"ascending":      func(r *V8Request) { r.SnapSeq, r.Snaps = 7, []uint64{5, 7} },
		"above-sequence": func(r *V8Request) { r.SnapSeq, r.Snaps = 5, []uint64{7} },
	}
	for name, edit := range cases {
		for _, version := range []int{6, 8, 9} {
			r := nativeV8Request("mutation")
			r.MTimeSeconds, r.MTimeNanoseconds = 0, 0
			r.EnforceSnapContext = true
			edit(&r)
			if err := ValidateEnforceSnapContext(r.Request, 4096); !errors.Is(err, ErrRequest) {
				t.Fatal(name, "preflight", err)
			}
			if _, err := enforceSnapEncode(version, r); !errors.Is(err, ErrRequest) {
				t.Fatal(name, version, err)
			}
		}
	}
	for _, version := range []int{6, 8, 9} {
		r := cachePoolInput(Operation{Code: WriteFull, Length: 1, Data: []byte{1}})
		r.EnforceSnapContext = true
		if _, err := cachePoolEncode(version, r, true); !errors.Is(err, ErrRequest) {
			t.Fatal("cache combination accepted", version, err)
		}
	}
	r := nativeV8Request("snapshot")
	r.EnforceSnapContext = true
	r.Snaps = make([]uint64, 1024)
	if err := ValidateEnforceSnapContext(r.Request, 4096); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("snapshot preflight ignored frame bound", err)
	}
	if err := ValidateEnforceSnapContext(Request{}, 4096); err != nil {
		t.Fatal("false changed ordinary validation", err)
	}
}
