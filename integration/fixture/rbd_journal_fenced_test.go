package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	rbdJournalFencedHoldSelector        = "^TestCephRBDJournalFencedHoldIntegration$"
	rbdJournalErasureFencedHoldSelector = "^TestCephRBDJournalErasureFencedHoldIntegration$"
	rbdJournalRestartBeforeDownSelector = "^TestCephRBDJournalPrimaryRestartBeforeDownIntegration$"

	rbdJournalFencedPendingSelector            = "^TestCephRBDJournalFencedPendingHoldIntegration$"
	rbdJournalErasureFencedPendingSelector     = "^TestCephRBDJournalErasureFencedPendingHoldIntegration$"
	rbdJournalErasureRestartBeforeDownSelector = "^TestCephRBDJournalErasurePrimaryRestartBeforeDownIntegration$"
)

// The fenced-hold, fenced pending and pre-down restart sequences are
// admitted only under their exact selectors on three BlueStore OSDs, each
// with its own actions; only the erasure-coded cells get the erasure-coded
// journal pool, and their fixture-own actions are admitted in no other
// sequence.
func TestRBDJournalFencedSelectorsAreExclusive(t *testing.T) {
	three := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}
	one := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1"}
	token := strings.Repeat("a1", 16)
	for selector, want := range map[string][]string{rbdJournalFencedHoldSelector: rbdJournalFencedHoldActions, rbdJournalErasureFencedHoldSelector: rbdJournalErasureFencedHoldActions,
		rbdJournalRestartBeforeDownSelector: rbdJournalPrimaryRestartBeforeDownActions, rbdJournalFencedPendingSelector: rbdJournalFencedHoldActions,
		rbdJournalErasureFencedPendingSelector: rbdJournalErasureFencedHoldActions, rbdJournalErasureRestartBeforeDownSelector: rbdJournalPrimaryRestartBeforeDownActions} {
		p, err := rbdJournalProfile(t, three, selector)
		if err != nil || p.testRun != selector || p.rbdReplicas() != "3" || p.osdTopology().poolSize != 3 || !slices.Equal(rbdJournalActionsFor(p), want) ||
			!slices.Equal(rbdLockActionsFor(p), rbdLockActions) {
			t.Fatal("selector admission differs", selector, p.testRun, err)
		}
		erasure := selector == rbdJournalErasureFencedHoldSelector || selector == rbdJournalErasureFencedPendingSelector || selector == rbdJournalErasureRestartBeforeDownSelector
		if p.rbdJournalErasure() != erasure || (rbdJournalPlanOf(token, p).JournalPool != "") != erasure {
			t.Fatal("erasure-coded journal pool selection differs", selector)
		}
		beforeDown := selector == rbdJournalRestartBeforeDownSelector || selector == rbdJournalErasureRestartBeforeDownSelector
		if p.rbdJournalFencedSequence() == beforeDown || p.rbdJournalBeforeDownSequence() != beforeDown ||
			p.rbdJournalAlignedSequence() != (erasure && !beforeDown) {
			t.Fatal("fenced-sequence classification differs", selector)
		}
		if _, err := rbdJournalProfile(t, one, selector); err == nil {
			t.Fatal("selector admitted on one OSD", selector)
		}
		if _, err := rbdJournalProfile(t, three, strings.TrimSuffix(selector, "$")); err == nil {
			t.Fatal("widened selector admitted", selector)
		}
		for _, action := range want {
			if !rbdJournalFencedAdmin(action) && !rbdJournalHeldAdmin(action) && !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
				t.Fatal("native journal driver does not admit", action)
			}
		}
	}
	for selector, want := range map[string][]rbdJournalHeldAwait{
		rbdJournalFencedHoldSelector:               {{"journal_data.3.ab.0", true}},
		rbdJournalErasureFencedHoldSelector:        {{"journal_data.3.ab.0", true}},
		rbdJournalRestartBeforeDownSelector:        {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", false}},
		rbdJournalErasureRestartBeforeDownSelector: {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", false}},
		rbdJournalFencedPendingSelector:            {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", true}, {"journal_data.3.ab.2", true}},
		rbdJournalErasureFencedPendingSelector:     {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", true}, {"journal_data.3.ab.2", true}},
	} {
		p, err := rbdJournalProfile(t, three, selector)
		if got := rbdJournalHeldAwaited(p, 3, "ab"); err != nil || !slices.Equal(got, want) {
			t.Fatal("awaited held operations differ", selector, got, err)
		}
	}
	for _, action := range []string{"fence", "align-primary", "keep-up", "observe-primary-dead"} {
		for _, actions := range [][]string{rbdJournalActions, rbdJournalLostAppendActions, rbdJournalForcedBreakActions, rbdJournalReplicaHoldActions, rbdJournalPrimaryRestartActions,
			rbdJournalDeadPrimaryActions, rbdJournalPendingHoldActions} {
			for next := 0; next <= len(actions); next++ {
				if _, _, err := parseRBDJournalRequest(actions, []string{"7", token, action}, token, 6, next); err == nil {
					t.Fatal("fixture-own action admitted in another sequence", action, next)
				}
			}
		}
	}
	if rbdJournalReadOnly("fence") || rbdJournalReadOnly("align-primary") || rbdJournalReadOnly("keep-up") || !rbdJournalReadOnly("observe-primary-dead") ||
		rbdJournalBlocklisted("fence") || !rbdJournalBlocklisted("observe-fenced") {
		t.Fatal("action classification differs")
	}
	plan := rbdJournalPlan{Image: "img", Pool: "pool", JournalPool: "pool2"}
	for action, want := range map[string]string{"fence": "img:osd-blocklist", "align-primary": "pool2:primary-temp,acting-primary", "keep-up": "pool:osd-nodown"} {
		if got := rbdJournalTarget(plan, action); got != want {
			t.Fatal("action target differs", action, got)
		}
	}
}

func TestRBDJournalOSDInfoDecoder(t *testing.T) {
	dump := `{"fsid":"f","epoch":41,"osds":[{"osd":0,"up":1,"state":["exists","up","nodown"],"up_from":38,"down_at":37,` +
		`"public_addrs":{"addrvec":[{"type":"v2","addr":"10.0.0.10:6802","nonce":3400},{"type":"v1","addr":"10.0.0.10:6803","nonce":3400}]}},` +
		`{"osd":1,"up":0,"state":["exists"],"up_from":5,"down_at":40,"public_addrs":{"addrvec":[]}}]}`
	info, err := decodeRBDJournalOSDInfo("f", 0, []byte(dump))
	if err != nil || info.Epoch != 41 || info.OSD != 0 || !info.Up || !slices.Equal(info.State, []string{"exists", "up", "nodown"}) || info.UpFrom != 38 || info.DownAt != 37 ||
		info.Address != "v2:10.0.0.10:6802/3400" || !slices.Equal(info.Down, []int{1}) {
		t.Fatal("OSDMap entry projection differs", info, err)
	}
	for name, data := range map[string]string{"another cluster": strings.Replace(dump, `"fsid":"f"`, `"fsid":"g"`, 1), "no epoch": strings.Replace(dump, `"epoch":41`, `"epoch":0`, 1),
		"not JSON": "{", "no OSDs": `{"fsid":"f","epoch":41,"osds":[]}`} {
		if _, err := decodeRBDJournalOSDInfo("f", 0, []byte(data)); err == nil {
			t.Fatal("OSDMap entry accepted:", name)
		}
	}
	if _, err := decodeRBDJournalOSDInfo("f", 2, []byte(dump)); err == nil {
		t.Fatal("an OSD the map does not list accepted")
	}
}

// fakeRBDJournalFence is a cluster whose MON lists blocks, whose OSDMap is
// at epoch and whose PG query answers the epochs of pg in turn.
type fakeRBDJournalFence struct {
	blocks  []mdsSessionsBlock
	epoch   uint32
	pg      []uint32
	added   []string
	refuse  bool
	ignored bool
}

func (f *fakeRBDJournalFence) ops() rbdJournalFenceOps {
	return rbdJournalFenceOps{
		blocklist: func(context.Context) ([]mdsSessionsBlock, error) { return slices.Clone(f.blocks), nil },
		add: func(_ context.Context, address string) error {
			if f.refuse {
				return errors.New("refused")
			}
			f.added = append(f.added, address)
			if !f.ignored {
				f.blocks = append(f.blocks, mdsSessionsBlock{Address: address, Until: "later"})
				f.epoch++
			}
			return nil
		},
		held: rbdJournalHeldOps{
			down: func(context.Context) (uint32, []int, error) { return f.epoch, []int{}, nil },
			query: func(context.Context, string) (rbdJournalPGQuery, error) {
				e := f.pg[0]
				if len(f.pg) > 1 {
					f.pg = f.pg[1:]
				}
				return rbdJournalPGQuery{State: "active+clean", Epoch: e}, nil
			},
			inFlight: func(_ context.Context, osd int) ([]rbdJournalHeldOp, error) {
				return []rbdJournalHeldOp{{OSD: osd, Client: "client.9", Transaction: 4}}, nil
			},
		},
		bound: 50 * time.Millisecond, pause: time.Millisecond,
	}
}

func TestRBDJournalFenceAddress(t *testing.T) {
	ctx := context.Background()
	f := &fakeRBDJournalFence{epoch: 30, pg: []uint32{30, 30, 31}}
	r, err := rbdJournalFenceAddress(ctx, "10.0.0.11:0/7", 0, "3.0", f.ops())
	if err != nil || !slices.Equal(f.added, []string{"10.0.0.11:0/7"}) || !slices.Equal(r.Commands, []string{"osd blocklist add 10.0.0.11:0/7"}) || len(r.BlocklistBefore) != 0 ||
		len(r.Blocklist) != 1 || r.Epoch != 31 || r.PGEpoch != 31 || r.Polls != 3 || len(r.Ops) != 1 || r.Ops[0].OSD != 0 {
		t.Fatal("fence differs", r, err)
	}
	for name, f := range map[string]*fakeRBDJournalFence{
		"blocklisted before":   {blocks: []mdsSessionsBlock{{Address: "10.0.0.11:0/7"}}, epoch: 30, pg: []uint32{31}},
		"command refused":      {epoch: 30, pg: []uint32{31}, refuse: true},
		"MON lists no entry":   {epoch: 30, pg: []uint32{31}, ignored: true},
		"PG never has the map": {epoch: 30, pg: []uint32{30}},
	} {
		if _, err := rbdJournalFenceAddress(ctx, "10.0.0.11:0/7", 0, "3.0", f.ops()); err == nil {
			t.Fatal("fence accepted:", name)
		}
		if name == "blocklisted before" && len(f.added) != 0 {
			t.Fatal("an address blocklisted before was blocklisted again")
		}
	}
	if _, err := rbdJournalFenceAddress(ctx, "", 0, "3.0", (&fakeRBDJournalFence{}).ops()); err == nil {
		t.Fatal("a fence without an address accepted")
	}
}

func TestRBDJournalAlignPrimary(t *testing.T) {
	ctx := context.Background()
	placement := func(pool, object string, epoch uint32, acting []int, primary int) rbdJournalPlacement {
		return rbdJournalPlacement{Epoch: epoch, Pool: pool, Object: object, PGID: map[string]string{"p": "3.0", "ec": "4.0"}[pool], Up: slices.Clone(acting), UpPrimary: acting[0],
			Acting: slices.Clone(acting), ActingPrimary: primary}
	}
	run := func(header, before rbdJournalPlacement, moves, clean bool) (rbdJournalAlign, []string, error) {
		var commands []string
		current := before
		ops := rbdJournalAlignOps{bound: 50 * time.Millisecond, pause: time.Millisecond,
			place: func(_ context.Context, object string) (rbdJournalPlacement, error) {
				if object == "rbd_header.ab" {
					return header, nil
				}
				return current, nil
			},
			query: func(context.Context, string) (rbdJournalPGQuery, error) {
				state := "peering"
				if clean {
					state = "active+clean"
				}
				return rbdJournalPGQuery{State: state, Acting: slices.Clone(before.Acting), Up: slices.Clone(before.Up)}, nil
			},
			primaryTemp: func(_ context.Context, pgid string, osd int) error {
				commands = append(commands, pgid)
				if moves {
					current = placement("ec", before.Object, before.Epoch+1, before.Acting, osd)
				}
				return nil
			}}
		r, err := rbdJournalAlignPrimary(ctx, "p", "ec", "rbd_header.ab", "journal_data.3.ab.0", ops)
		return r, commands, err
	}
	header := placement("p", "rbd_header.ab", 30, []int{0, 1, 2}, 0)
	// Already led by the header's primary: no command.
	r, commands, err := run(header, placement("ec", "journal_data.3.ab.0", 30, []int{0, 2, 1}, 0), false, true)
	if err != nil || len(commands) != 0 || len(r.Commands) != 0 || r.Query == nil || r.After.ActingPrimary != 0 {
		t.Fatal("aligned PG differs", r, commands, err)
	}
	// Led by another member: one override to the header's primary, shards
	// in place.
	r, commands, err = run(header, placement("ec", "journal_data.3.ab.0", 30, []int{1, 0, 2}, 1), true, true)
	if err != nil || !slices.Equal(commands, []string{"4.0"}) || !slices.Equal(r.Commands, []string{"osd primary-temp 4.0 0"}) || r.After.ActingPrimary != 0 ||
		!slices.Equal(r.After.Acting, []int{1, 0, 2}) || r.Query == nil {
		t.Fatal("overridden PG differs", r, commands, err)
	}
	for name, c := range map[string]struct {
		before       rbdJournalPlacement
		moves, clean bool
	}{
		"override not applied": {placement("ec", "journal_data.3.ab.0", 30, []int{1, 0, 2}, 1), false, true},
		"PG never clean":       {placement("ec", "journal_data.3.ab.0", 30, []int{1, 0, 2}, 1), true, false},
		"PG not settled":       {placement("ec", "journal_data.3.ab.0", 30, []int{1, 1, 2}, 1), true, true},
	} {
		if _, _, err := run(header, c.before, c.moves, c.clean); err == nil {
			t.Fatal("alignment accepted:", name)
		}
	}
}

func TestRBDJournalObservePrimaryDead(t *testing.T) {
	ctx := context.Background()
	reads := 0
	info := func(down int) func(context.Context, int) (rbdJournalOSDInfo, error) {
		return func(_ context.Context, osd int) (rbdJournalOSDInfo, error) {
			reads++
			return rbdJournalOSDInfo{Epoch: uint32(30 + reads), OSD: osd, Up: reads != down, Address: "v2:10.0.0.10:6802/1200", State: []string{"exists", "up", "nodown"}, Down: []int{}}, nil
		}
	}
	place := func(_ context.Context, object string) (rbdJournalPlacement, error) {
		return rbdJournalPlacement{Object: object, Acting: []int{0, 1, 2}, ActingPrimary: 0}, nil
	}
	objects := []string{"rbd_header.ab", "journal_data.3.ab.0", "journal.ab"}
	r, err := rbdJournalObservePrimaryDead(ctx, 0, "v2:10.0.0.10:6802/1200", objects, info(-1), place, 20*time.Millisecond, time.Millisecond)
	if err != nil || len(r.Samples) < 2 || r.Samples[0].Info.Epoch != 31 {
		t.Fatal("primary-dead window differs", r, err)
	}
	for i, sample := range r.Samples {
		if sample.Info.Epoch != uint32(31+i) || len(sample.Objects) != 3 || sample.UTC == "" {
			t.Fatal("a sample of the window was not kept as read", i, sample)
		}
	}
	// A sample that differs and recovers stays in the receipt.
	reads = 0
	r, err = rbdJournalObservePrimaryDead(ctx, 0, "v2:10.0.0.10:6802/1200", objects, info(2), place, 20*time.Millisecond, time.Millisecond)
	if err != nil || len(r.Samples) < 3 || !r.Samples[0].Info.Up || r.Samples[1].Info.Up || !r.Samples[len(r.Samples)-1].Info.Up {
		t.Fatal("an intermediate down sample was not kept", r, err)
	}
	// The sample bound is an error, not a truncation.
	reads = 0
	if r, err := rbdJournalObservePrimaryDead(ctx, 0, "v2:10.0.0.10:6802/1200", objects, info(-1), place, time.Hour, 0); err == nil || len(r.Samples) != rbdJournalPrimaryDeadSamples {
		t.Fatal("a window past its sample bound accepted", len(r.Samples), err)
	}
	if _, err := rbdJournalObservePrimaryDead(ctx, -1, "a", objects, info(-1), place, time.Millisecond, time.Millisecond); err == nil {
		t.Fatal("a window without a primary accepted")
	}
}
