package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// A replica-directed read that names an erasure-coded shard is refused by
// the request codec for every admitted replica-read operation and for either
// flag, whatever the shard; the same vectors with NO_SHARD, and the same
// shard without a flag, encode. The v9 request codec admits no shard at all.
func TestErasureShardRefusesReplicaDirectedReads(t *testing.T) {
	vectors := map[string][]Operation{
		"read":        {{Code: Read, Length: 8}},
		"stat":        {{Code: Stat}},
		"getxattr":    {{Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 64}}},
		"getxattrs":   {{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}}},
		"omap-keys":   {{Code: OmapGetKeys, Omap: OmapInput{MaxEntries: 2, MaxOutputBytes: 64}}},
		"omap-header": {{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 64}}},
		"read-read":   {{Code: Read, Length: 8}, {Code: Read, Offset: 8, Length: 8}},
	}
	flags := map[string]func(*Request){
		"balance":  func(r *Request) { r.BalanceReads = true },
		"localize": func(r *Request) { r.LocalizeReads = true },
		"both":     func(r *Request) { r.BalanceReads, r.LocalizeReads = true, true },
	}
	for name, ops := range vectors {
		for flag, set := range flags {
			r := nativeV8Request("read")
			r.Operations = ops
			set(&r.Request)
			if _, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit); err != nil {
				t.Fatal("a replicated replica-directed read refused", name, flag, err)
			}
			for _, shard := range []int8{0, 1, 3, 127} {
				r.Shard = shard
				if _, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
					t.Fatal("a replica-directed read naming an erasure-coded shard encoded", name, flag, shard, err)
				}
			}
		}
		r := nativeV8Request("read")
		r.Operations, r.Shard = ops, 1
		if _, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit); err != nil {
			t.Fatal("the same shard without a replica-read flag refused", name, err)
		}
	}
	r := V9Request(nativeV8Request("read"))
	r.BalanceReads, r.Shard = true, 1
	if _, err := EncodeRequestV9(r, v9UnitFeatures, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
		t.Fatal("the v9 request codec admitted a shard", err)
	}
}
