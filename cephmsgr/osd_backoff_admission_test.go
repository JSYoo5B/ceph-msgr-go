package cephmsgr

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func backoffUnitRequest(t *testing.T, object osd.PGCursor, pg osd.PG, shard int8, epoch uint32, snap uint64) msgr.MessageData {
	t.Helper()
	m, err := osd.EncodeRequestV8(osd.V8Request{PG: pg, Shard: shard, Request: osd.Request{
		MapEpoch: epoch, Object: object.Object, Hash: object.Hash, ReadSnap: snap,
		Locator:    osd.Locator{Pool: object.Pool, Key: object.Key, Namespace: object.Namespace, Hash: -1},
		Operations: []osd.Operation{{Code: osd.Stat}},
	}}, (1<<8)|(1<<9)|(1<<21)|(1<<30)|(1<<56)|(1<<57), 4096)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func backoffUnitMatchingRequest(t *testing.T) msgr.MessageData {
	t.Helper()
	return backoffUnitRequest(t, osd.PGCursor{Pool: 7, Object: "b"}, osd.PG{Pool: 7, Seed: 3}, -1, 15, 0)
}

func TestOSDBackoffAdmissionMatchesOnlyActualPGAndHeadObjectRange(t *testing.T) {
	base := osd.PGCursor{Pool: 7, Hash: 0x12345678, Namespace: "tenant", Key: "route", Object: "object", Snap: osd.NoSnap}
	for _, tc := range []struct {
		name string
		edit func(*osd.PGCursor, *osd.PG, *int8, *uint32, *uint64)
		want bool
	}{
		{"matching", func(*osd.PGCursor, *osd.PG, *int8, *uint32, *uint64) {}, true},
		{"other object", func(o *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Object = "unrelated" }, false},
		{"other namespace", func(o *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Namespace = "other" }, false},
		{"other key", func(o *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Key = "elsewhere" }, false},
		{"empty effective key", func(o *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Key = "" }, false},
		{"raw hash not PG seed", func(o *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Hash = 3 }, false},
		{"other PG", func(_ *osd.PGCursor, p *osd.PG, _ *int8, _ *uint32, _ *uint64) { p.Seed++ }, false},
		{"other pool", func(o *osd.PGCursor, p *osd.PG, _ *int8, _ *uint32, _ *uint64) { o.Pool++; p.Pool++ }, false},
		{"other shard", func(_ *osd.PGCursor, _ *osd.PG, s *int8, _ *uint32, _ *uint64) { *s = 1 }, false},
		{"newer epoch cannot bypass", func(_ *osd.PGCursor, _ *osd.PG, _ *int8, e *uint32, _ *uint64) { *e = 999 }, true},
		{"older epoch cannot bypass", func(_ *osd.PGCursor, _ *osd.PG, _ *int8, e *uint32, _ *uint64) { *e = 1 }, true},
		{"snapshot read uses head", func(_ *osd.PGCursor, _ *osd.PG, _ *int8, _ *uint32, s *uint64) { *s = 5 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := backoffUnitConnection()
			b := o.backoffState
			b.gateInstalled = true
			block := osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: -1, Op: osd.BackoffBlock, MapEpoch: 15, ID: 1, Begin: base, End: base}
			if err := b.accept(o, block, 11, 7); err != nil {
				t.Fatal(err)
			}
			object, pg, shard, epoch, snap := base, block.PG, int8(-1), uint32(15), uint64(0)
			tc.edit(&object, &pg, &shard, &epoch, &snap)
			err := o.beforeWriteBackoff(backoffUnitRequest(t, object, pg, shard, epoch, snap))
			if tc.want && !errors.Is(err, ErrOSDBackoff) || !tc.want && err != nil {
				t.Fatal("range admission differs", err)
			}
		})
	}
}

// A BLOCK names the OSD's actual shard; an optimized EC request may carry its
// shard in primary-first numbering, so every shard of the PG is gated.
func TestOSDBackoffAdmissionGatesEveryShardOfAnErasurePG(t *testing.T) {
	object := osd.PGCursor{Pool: 7, Object: "b", Snap: osd.NoSnap}
	for _, tc := range []struct {
		shard int8
		want  bool
	}{{2, true}, {1, true}, {0, true}, {-1, false}} {
		o := backoffUnitConnection()
		o.backoffState.gateInstalled = true
		if err := o.backoffState.accept(o, osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: 2, Op: osd.BackoffBlock, ID: 1, Begin: object, End: object}, 11, 7); err != nil {
			t.Fatal(err)
		}
		err := o.beforeWriteBackoff(backoffUnitRequest(t, object, osd.PG{Pool: 7, Seed: 3}, tc.shard, 16, 0))
		if tc.want && !errors.Is(err, ErrOSDBackoff) || !tc.want && err != nil {
			t.Fatal("erasure shard gate differs", tc.shard, err)
		}
	}
}

func TestOSDBackoffAdmissionHalfOpenRangesAndEffectiveKey(t *testing.T) {
	for _, singleton := range []bool{false, true} {
		o := backoffUnitConnection()
		o.backoffState.gateInstalled = true
		start := osd.PGCursor{Pool: 7, Object: "b", Snap: osd.NoSnap}
		end := start
		if !singleton {
			end.Object = "d"
		}
		if err := o.backoffState.accept(o, osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: -1, Op: osd.BackoffBlock, ID: 1, Begin: start, End: end}, 11, 7); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "b", "c", "d", "e"} {
			for _, key := range []string{"", name} {
				object := osd.PGCursor{Pool: 7, Object: name, Key: key}
				err := o.beforeWriteBackoff(backoffUnitRequest(t, object, osd.PG{Pool: 7, Seed: 3}, -1, 16, 0))
				blocked := name == "b" || !singleton && name == "c"
				if blocked && !errors.Is(err, ErrOSDBackoff) || !blocked && err != nil {
					t.Fatal("singleton/half-open range or equivalent locator key differs", singleton, name, key, err)
				}
			}
		}
	}
}

func TestOSDBackoffAdmissionPGNLSUsesEmptyHeadTargetNotCursor(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	b.gateInstalled = true
	pg := osd.PG{Pool: 7, Seed: 3}
	list, err := osd.EncodePGListRequestV8(osd.PGListRequest{MapEpoch: 16, PG: pg, Namespace: "tenant", Count: 1, MaxOutputBytes: 512}, (1<<8)|(1<<9)|(1<<21)|(1<<30)|(1<<56)|(1<<57), 4096)
	if err != nil {
		t.Fatal(err)
	}
	object := osd.PGCursor{Pool: 7, Hash: 3, Namespace: "tenant", Object: "object", Snap: osd.NoSnap}
	block := osd.Backoff{PG: pg, Shard: -1, ID: 1, Op: osd.BackoffBlock, Begin: object, End: object}
	if err := b.accept(o, block, 11, 7); err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(list); err != nil {
		t.Fatal("object BLOCK incorrectly blocked PGNLS's different empty object", err)
	}
	object.Object = ""
	block.ID, block.Begin, block.End = 2, object, object
	if err := b.accept(o, block, 11, 7); err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(list); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("matching empty-object PGNLS target bypassed gate", err)
	}
}

func TestOSDBackoffAdmissionMalformedOutgoingRequestFailsClosed(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	b.gateInstalled = true
	if err := b.accept(o, backoffUnitControl(t, 1, osd.BackoffBlock), 11, 7); err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(msgr.MessageData{Type: osd.RequestMessage, Version: 8, CompatVersion: 3}); !errors.Is(err, msgr.ErrFrame) || errors.Is(err, ErrOSDBackoff) {
		t.Fatal("unidentifiable queued request was admitted or called an object match", err)
	}
}
