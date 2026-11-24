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
	rbdJournalReplicaHoldSelector       = "^TestCephRBDJournalReplicaHoldIntegration$"
	rbdJournalHeldPrimaryChangeSelector = "^TestCephRBDJournalHeldPrimaryChangeIntegration$"
	rbdJournalDeadPrimarySelector       = "^TestCephRBDJournalDeadPrimaryIntegration$"
	rbdJournalPendingHoldSelector       = "^TestCephRBDJournalPendingHoldIntegration$"
	rbdJournalPrimaryRestartSelector    = "^TestCephRBDJournalPrimaryRestartIntegration$"
	rbdJournalErasureHoldSelector       = "^TestCephRBDJournalErasureHoldIntegration$"
)

// rbdJournalProfile loads the profile of one selector with its catalog
// capabilities on the given base environment.
func rbdJournalProfile(t *testing.T, base map[string]string, selector string) (profile, error) {
	t.Helper()
	return loadProfile(func(key string) string {
		switch key {
		case "CEPH_MSGR_TEST_RUN":
			return selector
		case "CEPH_MSGR_TEST_CAPABILITIES":
			return catalogCapabilities(t, selector)
		}
		return base[key]
	})
}

// The held-request sequences are admitted only under their exact selectors
// and only on three BlueStore OSDs. Each selects its own sequence and changes
// no other.
func TestRBDJournalHeldSelectorsAreExclusive(t *testing.T) {
	three := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}
	one := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1"}
	for selector, want := range map[string][]string{rbdJournalReplicaHoldSelector: rbdJournalReplicaHoldActions, rbdJournalHeldPrimaryChangeSelector: rbdJournalHeldPrimaryChangeActions,
		rbdJournalDeadPrimarySelector: rbdJournalDeadPrimaryActions, rbdJournalPendingHoldSelector: rbdJournalPendingHoldActions,
		rbdJournalPrimaryRestartSelector: rbdJournalPrimaryRestartActions, rbdJournalErasureHoldSelector: rbdJournalErasureHoldActions} {
		p, err := rbdJournalProfile(t, three, selector)
		if err != nil || p.testRun != selector || p.rbdReplicas() != "3" || p.osdTopology().poolSize != 3 || p.osdTopology().poolMinSize != 2 ||
			!slices.Equal(rbdJournalActionsFor(p), want) || !slices.Equal(rbdLockActionsFor(p), rbdLockActions) {
			t.Fatal("held-request selector admission differs", selector, p.testRun, err)
		}
		if _, err := rbdJournalProfile(t, one, selector); err == nil {
			t.Fatal("held-request selector admitted on one OSD", selector)
		}
		for _, widened := range []string{strings.TrimSuffix(selector, "$"), strings.TrimPrefix(selector, "^"), "^TestCephRBDJournal(ReplicaHold|DeadPrimary)Integration$"} {
			if _, err := rbdJournalProfile(t, three, widened); err == nil {
				t.Fatal("widened held-request selector admitted", widened)
			}
		}
		for _, action := range want {
			if !rbdJournalHeldAdmin(action) && action != "change-primary" && action != "advance-map" && !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
				t.Fatal("native journal driver does not admit", action)
			}
		}
	}
	// No single-OSD selection gets a held sequence.
	for _, selector := range []string{rbdJournalTrimSelector, rbdJournalLostAppendSelector} {
		if rbdSelectionProfile(t, selector).rbdJournalThreeOSD() {
			t.Fatal("another selection gets the three-OSD topology", selector)
		}
	}
	// The sequences' own actions are admitted nowhere else.
	token := strings.Repeat("a1", 16)
	for _, action := range []string{"observe-placed", "observe-held", "observe-settled", "observe-primary-lost", "observe-answered", "observe-primary-back", "observe-restarted"} {
		for _, actions := range [][]string{rbdJournalActions, rbdJournalLostAppendActions, rbdJournalPrimaryChangeActions, rbdJournalTrimActions} {
			for next := 0; next <= len(actions); next++ {
				if _, _, err := parseRBDJournalRequest(actions, []string{"7", token, action}, token, 6, next); err == nil {
					t.Fatal("held-request action admitted in another sequence", action, next)
				}
			}
		}
	}
	if !rbdJournalReadOnly("observe-held") || !rbdJournalReadOnly("observe-primary-lost") || !rbdJournalReadOnly("observe-primary-back") || !rbdJournalReadOnly("observe-restarted") ||
		rbdJournalInspected("observe-held") || rbdJournalBlocklisted("observe-held") || rbdJournalInspected("observe-restarted") || rbdJournalBlocklisted("observe-restarted") {
		t.Fatal("held-request action classification differs")
	}
	// observe-held waits for the appends of the selected sequence.
	for selector, want := range map[string][]rbdJournalHeldAwait{
		rbdJournalReplicaHoldSelector:       {{"journal_data.3.ab.0", true}},
		rbdJournalDeadPrimarySelector:       {{"journal_data.3.ab.0", true}},
		rbdJournalHeldPrimaryChangeSelector: {{"journal_data.3.ab.0", false}},
		rbdJournalPendingHoldSelector:       {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", true}, {"journal_data.3.ab.2", true}},
		rbdJournalPrimaryRestartSelector:    {{"journal_data.3.ab.0", true}, {"journal_data.3.ab.1", false}},
		rbdJournalErasureHoldSelector:       {{"journal_data.3.ab.0", true}},
	} {
		p, err := rbdJournalProfile(t, three, selector)
		if got := rbdJournalHeldAwaited(p, 3, "ab"); err != nil || !slices.Equal(got, want) {
			t.Fatal("awaited held operations differ", selector, got, err)
		}
	}
}

func TestRBDJournalHeldDecoders(t *testing.T) {
	query := `{"state":"active+clean","epoch":30,"up":[0,1,2],"acting":[0,1,2],"info":{"pgid":"3.0","last_update":"29'51","history":{"last_epoch_started":22,` +
		`"same_up_since":21,"same_interval_since":21,"same_primary_since":21}},"peer_info":[{"peer":"1","last_update":"29'51"},{"peer":"2","last_update":"29'51"}]}`
	q, err := decodeRBDJournalPGQuery("3.0", []byte(query))
	if err != nil || q.State != "active+clean" || q.SameIntervalSince != 21 || q.LastEpochStarted != 22 || len(q.Peers) != 2 || q.Peers[1] != (rbdJournalPGPeer{2, "29'51"}) {
		t.Fatal("PG query projection differs", q, err)
	}
	// An erasure-coded PG's primary names itself with its shard and each
	// peer with its shard.
	ec := strings.NewReplacer(`"pgid":"3.0"`, `"pgid":"3.0s0"`, `"peer":"1"`, `"peer":"1(1)"`, `"peer":"2"`, `"peer":"2(2)"`).Replace(query)
	if q, err := decodeRBDJournalPGQuery("3.0", []byte(ec)); err != nil || len(q.Peers) != 2 || q.Peers[0] != (rbdJournalPGPeer{1, "29'51"}) || q.Peers[1] != (rbdJournalPGPeer{2, "29'51"}) {
		t.Fatal("erasure-coded PG query projection differs", q, err)
	}
	for _, bad := range []string{strings.Replace(query, `"pgid":"3.0"`, `"pgid":"3.1"`, 1), strings.Replace(query, `"peer":"1"`, `"peer":"osd"`, 1),
		strings.Replace(query, `"pgid":"3.0"`, `"pgid":"3.0s"`, 1), strings.Replace(query, `"pgid":"3.0"`, `"pgid":"3.0sx"`, 1),
		strings.Replace(query, `"pgid":"3.0"`, `"pgid":"3.1s0"`, 1), strings.Replace(query, `"peer":"1"`, `"peer":"1(x)"`, 1),
		strings.Replace(query, `"peer":"1"`, `"peer":"1(1"`, 1), strings.Replace(query, `"peer":"1"`, `"peer":"(1)"`, 1),
		strings.Replace(query, `,"history":{"last_epoch_started":22,"same_up_since":21,"same_interval_since":21,"same_primary_since":21}`, "", 1), `{}`} {
		if _, err := decodeRBDJournalPGQuery("3.0", []byte(bad)); err == nil {
			t.Fatal("malformed PG query accepted", bad)
		}
	}
	ops := `{"ops":[{"description":"osd_op(client.4866.0:10 3.e3 3:c7:::journal_data.3.ab.0:head [call journal.append in=4198b] e30)","type_data":{"flag_point":"waiting for sub ops",` +
		`"client_info":{"client":"client.4866","tid":10},"events":[{"event":"initiated"},{"event":"sub_op_commit_rec"}]}},{"description":"pg scrub","type_data":{"events":[]}}],"num_ops":2}`
	got, err := decodeRBDJournalHeldOps(0, []byte(ops))
	if err != nil || len(got) != 1 || got[0].Client != "client.4866" || got[0].Transaction != 10 || !slices.Equal(got[0].Events, []string{"initiated", "sub_op_commit_rec"}) {
		t.Fatal("operations in flight projection differs", got, err)
	}
	for _, bad := range []string{`{"num_ops":0}`, `{"ops":[],"num_ops":1}`, `{"ops":null,"num_ops":0}`} {
		if _, err := decodeRBDJournalHeldOps(0, []byte(bad)); err == nil {
			t.Fatal("malformed operations in flight accepted", bad)
		}
	}
}

// rbdJournalHeldFake is a scripted cluster for rbdJournalHeldObserve: each
// look returns the next state of its script, the last one repeatedly.
type rbdJournalHeldFake struct {
	placements [][]rbdJournalPlacement
	ops        [][]rbdJournalHeldOp
	down       [][]int
	queries    []rbdJournalPGQuery
	queryErr   error
	looks      int
	opReads    int
}

func (f *rbdJournalHeldFake) reads() rbdJournalHeldOps {
	at := func(n, i int) int { return min(i, n-1) }
	return rbdJournalHeldOps{
		place: func(_ context.Context, object string) (rbdJournalPlacement, error) {
			set := f.placements[at(len(f.placements), f.looks)]
			for _, p := range set {
				if p.Object == object {
					return p, nil
				}
			}
			return rbdJournalPlacement{}, errors.New("no placement")
		},
		query: func(context.Context, string) (rbdJournalPGQuery, error) {
			return f.queries[at(len(f.queries), f.looks-1)], f.queryErr
		},
		inFlight: func(context.Context, int) ([]rbdJournalHeldOp, error) {
			f.opReads++
			return f.ops[at(len(f.ops), f.opReads-1)], nil
		},
		down: func(context.Context) (uint32, []int, error) {
			f.looks++
			return 30, f.down[at(len(f.down), f.looks-1)], nil
		},
		held: 200 * time.Millisecond, lost: 200 * time.Millisecond, back: 200 * time.Millisecond, pause: time.Millisecond,
	}
}

func TestRBDJournalHeldObserveWaits(t *testing.T) {
	objects := rbdJournalPrimaryObjects(3, "ab")
	placed := func(pgid string, acting []int) []rbdJournalPlacement {
		var set []rbdJournalPlacement
		for _, o := range objects {
			set = append(set, rbdJournalPlacement{Epoch: 30, Pool: "p", Object: o, PGID: pgid, Up: acting, Acting: acting, UpPrimary: acting[0], ActingPrimary: acting[0]})
		}
		return set
	}
	query := rbdJournalPGQuery{State: "active+clean", Epoch: 30, Up: []int{0, 1, 2}, Acting: []int{0, 1, 2}}
	waiting := rbdJournalHeldOp{OSD: 0, Description: "osd_op(client.1.0:5 3:c7:::" + objects[1] + ":head [call journal.append])", Client: "client.1", Transaction: 5,
		Events: []string{"initiated", "waiting for subops from 1,2"}}
	committed := waiting
	committed.Events = append(slices.Clone(waiting.Events), "op_commit", "sub_op_commit_rec")
	ctx := context.Background()
	wait := rbdJournalHeldWait{osd: 0, ops: []rbdJournalHeldAwait{{objects[1], true}}}

	// observe-held waits for the replica commit; a bound without it is
	// reported as not settled, not as an error.
	f := &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, ops: [][]rbdJournalHeldOp{{}, {waiting}, {committed}}, down: [][]int{{}},
		queries: []rbdJournalPGQuery{query}}
	r, err := rbdJournalHeldObserve(ctx, "observe-held", objects, wait, f.reads())
	if err != nil || !r.Settled || f.opReads != 3 || r.OSD != 0 || len(r.Ops) != 1 || r.Query == nil || r.PGID != "3.0" || len(r.Objects) != 3 {
		t.Fatal("observe-held did not wait for the replica commit", r, err)
	}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, ops: [][]rbdJournalHeldOp{{waiting}}, down: [][]int{{}}, queries: []rbdJournalPGQuery{query}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-held", objects, wait, f.reads()); err != nil || r.Settled {
		t.Fatal("observe-held settled without a replica commit", r, err)
	}
	// Without the commit requirement the listing alone settles it.
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, ops: [][]rbdJournalHeldOp{{waiting}}, down: [][]int{{}}, queries: []rbdJournalPGQuery{query}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-held", objects, rbdJournalHeldWait{osd: 0, ops: []rbdJournalHeldAwait{{objects[1], false}}}, f.reads()); err != nil || !r.Settled {
		t.Fatal("observe-held did not settle on the listed operation", r, err)
	}
	// Several awaited operations settle only once all are listed, each as
	// required; an object whose name extends another's does not stand in.
	second, longer := committed, waiting
	second.Description = strings.Replace(committed.Description, objects[1], "journal_data.3.ab.1", 1)
	longer.Description = strings.Replace(waiting.Description, objects[1], "journal_data.3.ab.10", 1)
	several := rbdJournalHeldWait{osd: 0, ops: []rbdJournalHeldAwait{{objects[1], true}, {"journal_data.3.ab.1", false}}}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, ops: [][]rbdJournalHeldOp{{committed}, {committed, longer}, {committed, second}},
		down: [][]int{{}}, queries: []rbdJournalPGQuery{query}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-held", objects, several, f.reads()); err != nil || !r.Settled || f.opReads != 3 || len(r.Ops) != 2 {
		t.Fatal("observe-held did not wait for every awaited operation", r, err)
	}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, ops: [][]rbdJournalHeldOp{{waiting, second}}, down: [][]int{{}}, queries: []rbdJournalPGQuery{query}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-held", objects, several, f.reads()); err != nil || r.Settled {
		t.Fatal("observe-held settled with one awaited operation lacking its replica commit", r, err)
	}
	if _, err = rbdJournalHeldObserve(ctx, "observe-held", objects, rbdJournalHeldWait{osd: 0, ops: []rbdJournalHeldAwait{{"", false}}}, f.reads()); err == nil {
		t.Fatal("an awaited operation without an object accepted")
	}
	// observe-primary-lost waits for the old primary down and the PG active
	// on the others, as the query agrees.
	lost := rbdJournalPGQuery{State: "active+undersized+degraded", Epoch: 32, Up: []int{1, 2}, Acting: []int{1, 2}}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2}), placed("3.0", []int{0, 1, 2}), placed("3.0", []int{1, 2})},
		down: [][]int{{}, {0}, {0}}, queries: []rbdJournalPGQuery{lost}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-primary-lost", objects, rbdJournalHeldWait{osd: 0}, f.reads()); err != nil || !r.Settled || r.Polls != 3 || r.Query == nil ||
		r.Objects[0].ActingPrimary != 1 || f.opReads != 0 {
		t.Fatal("observe-primary-lost did not wait for the new primary", r, err)
	}
	peering := lost
	peering.State = "peering"
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{1, 2})}, down: [][]int{{0}}, queries: []rbdJournalPGQuery{peering}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-primary-lost", objects, rbdJournalHeldWait{osd: 0}, f.reads()); err != nil || r.Settled {
		t.Fatal("observe-primary-lost settled on a PG that is not active", r, err)
	}
	// observe-primary-back waits for the restarted OSD up and the PG clean
	// on the original acting set; a clean PG on other members, or one still
	// recovering, does not settle it.
	recovering := query
	recovering.State = "active+recovering"
	back := rbdJournalHeldWait{osd: 0, acting: []int{0, 1, 2}}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{1, 2}), placed("3.0", []int{0, 1, 2}), placed("3.0", []int{0, 1, 2}), placed("3.0", []int{0, 1, 2})},
		down: [][]int{{0}, {}, {}, {}}, queries: []rbdJournalPGQuery{recovering, recovering, recovering, query}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-primary-back", objects, back, f.reads()); err != nil || !r.Settled || r.Polls != 4 || r.Query == nil || r.Query.State != "active+clean" ||
		f.opReads != 0 {
		t.Fatal("observe-primary-back did not wait for the clean PG on its original members", r, err)
	}
	moved := query
	moved.Up, moved.Acting = []int{1, 2, 0}, []int{1, 2, 0}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{1, 2, 0})}, down: [][]int{{}}, queries: []rbdJournalPGQuery{moved}}
	if r, err = rbdJournalHeldObserve(ctx, "observe-primary-back", objects, back, f.reads()); err != nil || r.Settled {
		t.Fatal("observe-primary-back settled on other members", r, err)
	}
	for _, bad := range []rbdJournalHeldWait{{osd: -1, acting: []int{0, 1, 2}}, {osd: 0}, {osd: 3, acting: []int{0, 1, 2}}} {
		if _, err = rbdJournalHeldObserve(ctx, "observe-primary-back", objects, bad, f.reads()); err == nil {
			t.Fatal("observe-primary-back accepted without its primary in its acting set", bad)
		}
	}
	// Objects in different PGs, an action that is not an observation and a
	// failed query are errors.
	split := placed("3.0", []int{0, 1, 2})
	split[2].PGID = "3.1"
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{split}, down: [][]int{{}}, queries: []rbdJournalPGQuery{query}}
	if _, err = rbdJournalHeldObserve(ctx, "observe-placed", objects, rbdJournalHeldWait{osd: -1}, f.reads()); err == nil {
		t.Fatal("objects in two PGs accepted")
	}
	if _, err = rbdJournalHeldObserve(ctx, "change-primary", objects, rbdJournalHeldWait{osd: -1}, f.reads()); err == nil {
		t.Fatal("an action that is not a held-request observation accepted")
	}
	f = &rbdJournalHeldFake{placements: [][]rbdJournalPlacement{placed("3.0", []int{0, 1, 2})}, down: [][]int{{}}, queries: []rbdJournalPGQuery{query}, queryErr: errors.New("down")}
	if _, err = rbdJournalHeldObserve(ctx, "observe-settled", objects, rbdJournalHeldWait{osd: -1}, f.reads()); err == nil {
		t.Fatal("a failed PG query accepted")
	}
}
