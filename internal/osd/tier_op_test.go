package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// tierOpUnitRequest is the native request shape of one tier operation:
// promote is a write with an mtime; flush and evict are reads with
// IGNORE_CACHE and IGNORE_OVERLAY.
func tierOpUnitRequest(code uint16) Request {
	r := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "tier-op-codec", Hash: 0x11223344,
		Operations: []Operation{{Code: code}}}
	if code == TierPromote {
		r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 123456789
	} else {
		r.IgnoreCache, r.IgnoreOverlay = true, true
	}
	return r
}

// Default features without OSD_CACHEPOOL: tier operations need no cache-pool
// capability.
var tierOpFeatures = map[uint16]uint64{6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038}

func TestTierOperationNativeFlagSetsAndModes(t *testing.T) {
	for _, code := range []uint16{TierPromote, TierFlush, TierEvict} {
		r := tierOpUnitRequest(code)
		if err := ValidateTierRequest(r); err != nil {
			t.Fatalf("%#x native flag set refused: %v", code, err)
		}
		if tierOpFeatures[6]&FeatureCachePool != 0 {
			t.Fatal("test features carry the cache-pool capability")
		}
		if _, err := EncodeRequest(r, tierOpFeatures[6], wire.DefaultLimit); err != nil {
			t.Fatalf("%#x refused by the v6 encoder without the cache-pool capability: %v", code, err)
		}
		for _, version := range []uint16{8, 9} {
			var err error
			if version == 8 {
				_, err = EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, tierOpFeatures[8], wire.DefaultLimit)
			} else {
				_, err = EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, tierOpFeatures[9], wire.DefaultLimit)
			}
			if err != nil {
				t.Fatalf("%#x refused by the v%d encoder: %v", code, version, err)
			}
		}
		mode, input, err := ValidateOperations(r.Operations, wire.DefaultLimit)
		want := FlagRead
		if code == TierPromote {
			want = FlagWrite
		}
		if err != nil || mode != want || input != 0 {
			t.Fatalf("%#x mode %#x input %d, want %#x: %v", code, mode, input, want, err)
		}
		if !RequiresOnDisk(r.Operations, mode) {
			t.Fatalf("%#x success does not require ONDISK", code)
		}
	}
	// A promote may omit the mtime, like any other write.
	r := tierOpUnitRequest(TierPromote)
	r.MTimeSeconds, r.MTimeNanoseconds = 0, 0
	if _, err := EncodeRequest(r, tierOpFeatures[6], wire.DefaultLimit); err != nil {
		t.Fatal("promote without an mtime refused", err)
	}
	if IsTierOperation(CacheFlush) || IsTierOperation(SetChunk) || IsTierRequest([]Operation{{Code: Read}}) {
		t.Fatal("non-tier opcode classified as a tier operation")
	}
}

func TestTierOperationRefusalsBeforeEncoding(t *testing.T) {
	edits := map[string]func(*Request){
		"flush without overlay":       func(r *Request) { r.IgnoreOverlay = false },
		"flush without ignorecache":   func(r *Request) { r.IgnoreCache = false },
		"flush with skiplocks":        func(r *Request) { r.SkipRWLocks = true },
		"flush with mtime":            func(r *Request) { r.MTimeSeconds = 1 },
		"evict without overlay":       func(r *Request) { r.Operations[0].Code = TierEvict; r.IgnoreOverlay = false },
		"evict without ignorecache":   func(r *Request) { r.Operations[0].Code = TierEvict; r.IgnoreCache = false },
		"evict with skiplocks":        func(r *Request) { r.Operations[0].Code = TierEvict; r.SkipRWLocks = true },
		"promote with ignorecache":    func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.IgnoreCache = true },
		"promote with overlay":        func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.IgnoreOverlay = true },
		"promote with skiplocks":      func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.SkipRWLocks = true },
		"promote with full force":     func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.FullForce = true },
		"promote with snap context":   func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.SnapSeq, r.Snaps = 2, []uint64{2} },
		"compound":                    func(r *Request) { r.Operations = append(r.Operations, Operation{Code: Stat}) },
		"second tier op":              func(r *Request) { r.Operations = append(r.Operations, Operation{Code: TierEvict}) },
		"with cache op":               func(r *Request) { r.Operations = append(r.Operations, Operation{Code: CacheEvict}) },
		"per-op flags":                func(r *Request) { r.Operations[0].Flags = OpFailOK },
		"with reference flag":         func(r *Request) { r.Operations[0].Flags = OpWithReference },
		"extent":                      func(r *Request) { r.Operations[0].Length = 1 },
		"input":                       func(r *Request) { r.Operations[0].Data = []byte{1} },
		"version":                     func(r *Request) { r.Operations[0].Version = 1 },
		"chunk input":                 func(r *Request) { r.Operations[0].SetChunk = SetChunkInput{SourceLength: 1, TargetObject: "t"} },
		"copy input":                  func(r *Request) { r.Operations[0].CopyFrom = CopyFromInput{Object: "s"} },
		"snapshot read":               func(r *Request) { r.ReadSnap = 1 },
		"order snap":                  func(r *Request) { r.OrderSnap = true },
		"enforce snapc":               func(r *Request) { r.EnforceSnapContext = true },
		"balance reads":               func(r *Request) { r.BalanceReads = true },
		"localize reads":              func(r *Request) { r.LocalizeReads = true },
		"ignore redirect":             func(r *Request) { r.IgnoreRedirect = true },
		"rw ordered":                  func(r *Request) { r.OrderReadsWrites = true },
		"full try":                    func(r *Request) { r.FullTry = true },
		"retry":                       func(r *Request) { r.RetryAttempt = 1 },
		"promote retry":               func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.RetryAttempt = 1 },
		"promote ignore redirect":     func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.IgnoreRedirect = true },
		"promote with per-op failok":  func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.Operations[0].Flags = OpFailOK },
		"promote with exclusive flag": func(r *Request) { *r = tierOpUnitRequest(TierPromote); r.Operations[0].Flags = OpExclusive },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			r := tierOpUnitRequest(TierFlush)
			edit(&r)
			if _, err := EncodeRequest(r, tierOpFeatures[6], wire.DefaultLimit); !errors.Is(err, ErrRequest) && !errors.Is(err, ErrOperation) {
				t.Fatal("invalid tier request encoded", err)
			}
			if _, err := EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, tierOpFeatures[8], wire.DefaultLimit); !errors.Is(err, ErrRequest) && !errors.Is(err, ErrOperation) {
				t.Fatal("invalid tier request encoded by v8", err)
			}
		})
	}
	// An erasure-coded shard is never targeted.
	for _, code := range []uint16{TierPromote, TierFlush, TierEvict} {
		if _, err := EncodeRequestV8(V8Request{Request: tierOpUnitRequest(code), PG: PG{Pool: 7, Seed: 0x44}, Shard: 1}, tierOpFeatures[8], wire.DefaultLimit); !errors.Is(err, ErrRequest) {
			t.Fatalf("%#x encoded for an erasure-coded shard: %v", code, err)
		}
	}
	// The tier contract neither widens IgnoreOverlay nor IgnoreCache for
	// other requests: a READ still refuses the first and still needs the
	// cache-pool capability for the second.
	read := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Hash: -1}, Object: "o", Operations: []Operation{{Code: Read, Length: 1}}, IgnoreOverlay: true}
	if _, err := EncodeRequest(read, tierOpFeatures[6]|FeatureCachePool, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("IgnoreOverlay admitted outside a cache or tier operation", err)
	}
	read.IgnoreOverlay, read.IgnoreCache = false, true
	if _, err := EncodeRequest(read, tierOpFeatures[6], wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
		t.Fatal("IgnoreCache READ admitted without the cache-pool capability", err)
	}
}
