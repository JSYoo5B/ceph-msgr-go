package osd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func cachePoolEncode(version int, r V8Request, cacheFeature bool) (msgr.MessageData, error) {
	features := oracleFeatures
	if cacheFeature {
		features |= FeatureCachePool
	}
	switch version {
	case 6:
		return EncodeRequest(r.Request, features, 4096)
	case 8:
		return EncodeRequestV8(r, features|featureNewOSDOp|featureResendOnSplitMask, 4096)
	case 9:
		return EncodeRequestV9(V9Request(r), features|featureNewOSDOp|featureResendOnSplitMask|featureServerSquidMask, 4096)
	default:
		panic("unsupported test version")
	}
}

func cachePoolInput(op Operation) V8Request {
	r := nativeV8Request("read")
	r.Operations = []Operation{op}
	r.IgnoreCache = true
	return r
}

func TestCachePoolFlagsRequireCapabilityAndKeepWireProfile(t *testing.T) {
	for _, version := range []int{6, 8, 9} {
		for _, op := range []Operation{{Code: Stat}, {Code: Read, Length: 2}, {Code: WriteFull, Length: 2, Data: []byte{0, 255}}, {Code: Delete}} {
			t.Run(fmt.Sprintf("v%d/op%x", version, op.Code), func(t *testing.T) {
				r := cachePoolInput(op)
				if _, err := cachePoolEncode(version, r, false); !errors.Is(err, ErrFeatures) {
					t.Fatal("flag without negotiated feature", err)
				}
				got, err := cachePoolEncode(version, r, true)
				if err != nil {
					t.Fatal(err)
				}
				r.IgnoreCache = false
				plain, err := cachePoolEncode(version, r, true)
				if err != nil {
					t.Fatal(err)
				}
				old, err := cachePoolEncode(version, r, false)
				if err != nil {
					t.Fatal(err)
				}
				offset := 8
				if version != 6 {
					offset = 32
				}
				flags := binary.LittleEndian.Uint32(got.Front[offset:])
				want := binary.LittleEndian.Uint32(plain.Front[offset:]) | uint32(FlagIgnoreCache)
				if flags != want || got.Version != plain.Version || got.CompatVersion != plain.CompatVersion || string(got.Data) != string(plain.Data) {
					t.Fatal("cache bypass changed envelope/data or unrelated flags")
				}
				// v8/v9 append negotiated features at the end; outside that field,
				// enabling the capability alone must not set IGNORE_CACHE.
				if binary.LittleEndian.Uint32(old.Front[offset:]) != binary.LittleEndian.Uint32(plain.Front[offset:]) {
					t.Fatal("feature opt-in implicitly changed request flags")
				}
			})
		}
	}
}

func TestCachePoolFlagsRejectUnsupportedCombinationsEveryEncoder(t *testing.T) {
	edits := map[string]func(*V8Request){
		"compound": func(r *V8Request) { r.Operations = append(r.Operations, Operation{Code: Stat}) },
		"opcode":   func(r *V8Request) { r.Operations[0].Code = Append },
		"snapshot": func(r *V8Request) { r.ReadSnap = 1 }, "snap-seq": func(r *V8Request) { r.SnapSeq = 1 },
		"snap-context": func(r *V8Request) { r.Snaps = []uint64{1} }, "order-snap": func(r *V8Request) { r.OrderSnap = true },
		"balance": func(r *V8Request) { r.BalanceReads = true }, "localize": func(r *V8Request) { r.LocalizeReads = true },
		"skip-locks": func(r *V8Request) { r.SkipRWLocks = true }, "redirect": func(r *V8Request) { r.IgnoreRedirect = true },
		"rw-order": func(r *V8Request) { r.OrderReadsWrites = true }, "retry": func(r *V8Request) { r.RetryAttempt = 1 },
		"opflags":       func(r *V8Request) { r.Operations[0].Flags = OpFadviseNoCache },
		"truncate-size": func(r *V8Request) { r.Operations[0].TruncateSize = 1 },
		"truncate-seq":  func(r *V8Request) { r.Operations[0].TruncateSequence = 1 },
	}
	for _, version := range []int{6, 8, 9} {
		for name, edit := range edits {
			t.Run(fmt.Sprintf("v%d/%s", version, name), func(t *testing.T) {
				r := cachePoolInput(Operation{Code: Read, Length: 2})
				edit(&r)
				if _, err := cachePoolEncode(version, r, true); err == nil {
					t.Fatal("unsupported cache-bypass combination admitted")
				}
			})
		}
	}
	for _, version := range []int{8, 9} {
		r := cachePoolInput(Operation{Code: Stat})
		r.Shard = 0
		if _, err := cachePoolEncode(version, r, true); !errors.Is(err, ErrRequest) {
			t.Fatal("cache bypass accepted an EC shard", version, err)
		}
	}
	for _, version := range []int{6, 8, 9} {
		r := cachePoolInput(Operation{Code: Stat})
		r.ReadSnap = NoSnap
		if _, err := cachePoolEncode(version, r, true); err != nil {
			t.Fatal("explicit head refused", version, err)
		}
	}
}
