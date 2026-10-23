package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func cacheOpUnitRequest(code uint16) Request {
	write := code == CachePin || code == CacheUnpin
	flush := code == CacheFlush
	return cacheOpNativeRequest(code, write, !write, !write, !write && !flush)
}

func TestCacheOperationNativeFlagSetsAndModes(t *testing.T) {
	features := ignoreCacheNativeFeatures[6]
	for _, code := range []uint16{CacheFlush, CacheTryFlush, CacheEvict, CachePin, CacheUnpin} {
		r := cacheOpUnitRequest(code)
		if err := ValidateCacheRequest(r, features); err != nil {
			t.Fatalf("%#x native flag set refused: %v", code, err)
		}
		if _, err := EncodeRequest(r, features, wire.DefaultLimit); err != nil {
			t.Fatalf("%#x refused by the v6 encoder: %v", code, err)
		}
		mode, _, err := ValidateOperations(r.Operations, wire.DefaultLimit)
		want := FlagRead
		if code == CachePin || code == CacheUnpin {
			want = FlagWrite
		}
		if err != nil || mode != want {
			t.Fatalf("%#x mode %#x, want %#x: %v", code, mode, want, err)
		}
	}
	// Pin and unpin accept IgnoreCache, which the plain native form omits.
	for _, code := range []uint16{CachePin, CacheUnpin} {
		r := cacheOpUnitRequest(code)
		r.IgnoreCache = true
		if err := ValidateCacheRequest(r, features); err != nil {
			t.Fatal("pin/unpin with IgnoreCache refused", err)
		}
	}
}

func TestCacheOperationRefusalsBeforeEncoding(t *testing.T) {
	features := ignoreCacheNativeFeatures[6]
	edits := map[string]func(*Request){
		"flush without overlay":     func(r *Request) { r.Operations[0].Code = CacheFlush; r.IgnoreOverlay = false },
		"flush without ignorecache": func(r *Request) { r.Operations[0].Code = CacheFlush; r.IgnoreCache = false },
		"flush with skiplocks":      func(r *Request) { r.Operations[0].Code = CacheFlush; r.SkipRWLocks = true },
		"try-flush without locks":   func(r *Request) { r.SkipRWLocks = false },
		"evict without locks":       func(r *Request) { r.Operations[0].Code = CacheEvict; r.SkipRWLocks = false },
		"pin with overlay":          func(r *Request) { *r = cacheOpUnitRequest(CachePin); r.IgnoreOverlay = true },
		"pin with skiplocks":        func(r *Request) { *r = cacheOpUnitRequest(CachePin); r.SkipRWLocks = true },
		"compound":                  func(r *Request) { r.Operations = append(r.Operations, Operation{Code: Stat}) },
		"second cache op":           func(r *Request) { r.Operations = append(r.Operations, Operation{Code: CacheEvict}) },
		"per-op flags":              func(r *Request) { r.Operations[0].Flags = OpFailOK },
		"extent":                    func(r *Request) { r.Operations[0].Length = 1 },
		"input":                     func(r *Request) { r.Operations[0].Data = []byte{1} },
		"snapshot read":             func(r *Request) { r.ReadSnap = 1 },
		"snap context":              func(r *Request) { r.SnapSeq = 1 },
		"order snap":                func(r *Request) { r.OrderSnap = true },
		"enforce snapc":             func(r *Request) { r.EnforceSnapContext = true },
		"balance reads":             func(r *Request) { r.BalanceReads = true },
		"localize reads":            func(r *Request) { r.LocalizeReads = true },
		"ignore redirect":           func(r *Request) { r.IgnoreRedirect = true },
		"rw ordered":                func(r *Request) { r.OrderReadsWrites = true },
		"full try":                  func(r *Request) { r.FullTry = true },
		"retry":                     func(r *Request) { r.RetryAttempt = 1 },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			r := cacheOpUnitRequest(CacheTryFlush)
			edit(&r)
			if _, err := EncodeRequest(r, features, wire.DefaultLimit); !errors.Is(err, ErrRequest) && !errors.Is(err, ErrOperation) {
				t.Fatal("invalid cache request encoded", err)
			}
			if _, err := EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, ignoreCacheNativeFeatures[8], wire.DefaultLimit); !errors.Is(err, ErrRequest) && !errors.Is(err, ErrOperation) {
				t.Fatal("invalid cache request encoded by v8", err)
			}
		})
	}
	if _, err := EncodeRequest(cacheOpUnitRequest(CacheFlush), features&^FeatureCachePool, wire.DefaultLimit); !errors.Is(err, ErrFeatures) {
		t.Fatal("cache operation encoded without the cache-pool capability", err)
	}
	// IgnoreOverlay belongs only to the native flush/try-flush/evict requests.
	read := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Hash: -1}, Object: "o", Operations: []Operation{{Code: Read, Length: 1}}, IgnoreOverlay: true}
	if _, err := EncodeRequest(read, features, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("IgnoreOverlay admitted outside a cache operation", err)
	}
}
