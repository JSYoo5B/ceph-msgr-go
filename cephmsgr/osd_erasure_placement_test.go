package cephmsgr

import (
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestOSDErasurePlacementSendsTheShard(t *testing.T) {
	_, o, peer, ctx, _ := replyV8ProfileOpen(t)
	send := func(p OSDPlacement) byte {
		t.Helper()
		input := backoffTransportRequest()
		input.Placement = &p
		input.Operations = []OSDOperation{{Code: OSDStat}}
		go o.Request(ctx, input)
		m := peer.next(t)
		// spg_t opens the v8 front: 6-byte struct header, pg_t (17), then shard.
		return m.Front[23]
	}
	base := *backoffTransportRequest().Placement
	base.Primary = o.target.ID
	if got := send(base); got != 0xff {
		t.Fatal("replicated placement did not send NO_SHARD", got)
	}
	erasure := base
	erasure.Erasure, erasure.Shard = true, 2
	if got := send(erasure); got != 2 {
		t.Fatal("erasure placement did not send its shard", got)
	}
}

func TestOSDErasurePlacementRefusesMeaninglessShards(t *testing.T) {
	_, o, _, ctx, _ := replyV8ProfileOpen(t)
	for name, edit := range map[string]func(*OSDPlacement){
		"shard without erasure": func(p *OSDPlacement) { p.Shard = 1 },
		"shard above int8":      func(p *OSDPlacement) { p.Erasure, p.Shard = true, 128 },
	} {
		input := backoffTransportRequest()
		input.Placement.Primary = o.target.ID
		edit(input.Placement)
		input.Operations = []OSDOperation{{Code: OSDStat}}
		if _, err := o.Request(ctx, input); !errors.Is(err, osd.ErrRequest) {
			t.Fatal("meaningless shard accepted", name, err)
		}
	}
}
