package integration_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// An interval witness for the multi-chunk copy barriers. Their observations
// (the destination primary's op tracker and Objecter, and the staging
// listings) are separate reads, each bracketed by the destination's
// placement alone, and a placement that comes back to the same members
// after an interval change compares equal. Such a change cancels the copy
// and requeues its client op (on_change, src/osd/PrimaryLogPG.cc:13125-13216)
// while the listings may still show the staged object, so a barrier or a
// held final transaction would be judged from records of two intervals. A
// copyinterval observation (tools/osd_pool_mode_copyinterval_oracle.py)
// taken after the reads closes that: the primary's same_interval_since only
// grows, and it must not be newer than the epoch the copy was sent at.

// copyIntervalQuery is the part of the primary's `pg query` reply the
// witness reads.
type copyIntervalQuery struct {
	State             string  `json:"state"`
	Epoch             uint32  `json:"epoch"`
	Up                []int32 `json:"up"`
	Acting            []int32 `json:"acting"`
	PGID              string  `json:"pgid"`
	SameIntervalSince uint32  `json:"same_interval_since"`
}

// copyIntervalObservation is two placement records of the destination
// around one `pg query` of the primary the first names. A record that could
// not be read is nil.
type copyIntervalObservation struct {
	Before           *osdFixtureOracle  `json:"before"`
	After            *osdFixtureOracle  `json:"after"`
	PlacementFailure string             `json:"placement_failure"`
	Code             *int               `json:"code"`
	Query            *copyIntervalQuery `json:"query"`
	QueryFailure     string             `json:"query_failure"`
}

// copyInterval reads one observation of the PG of an owned object.
func copyInterval(t *testing.T, ctx context.Context, slot int, object string) copyIntervalObservation {
	t.Helper()
	var result struct {
		Interval copyIntervalObservation `json:"copy_interval"`
	}
	nativePoolResult(t, ctx, "copyinterval", map[string]any{"slot": slot, "object": object}, &result)
	return result.Interval
}

// copyRouteSPG is the spg_t a primary's info names for route: the PG of a
// replicated route, the primary's shard of an erasure-coded one.
func copyRouteSPG(route osdFixtureOracle) string {
	if route.Shard < 0 {
		return route.PGID
	}
	return fmt.Sprintf("%ss%d", route.PGID, route.Shard)
}

// errCopyIntervalChanged marks a complete observation that shows another
// interval than the one the copy was sent in.
var errCopyIntervalChanged = errors.New("the destination PG's interval changed since the copy was sent")

// copyIntervalHeld judges one observation taken after a barrier's reads:
// nil when the interval in which the copy was sent at route's epoch still
// holds. Both placement records are identical and route's placement at an
// equal or newer epoch; route's primary answered for its own spg_t with
// route's up and acting sets, at a map epoch not behind the records, in an
// interval that started at or before route's epoch. A complete observation
// that shows anything else wraps errCopyIntervalChanged; an incomplete one
// says nothing about the interval and is a plain error.
func copyIntervalHeld(o copyIntervalObservation, route osdFixtureOracle) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	q := o.Query
	if q == nil || o.Code == nil || *o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: %s", o.QueryFailure)
	}
	if q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, o.Before.Epoch)
	}
	switch {
	case !samePlacement(*o.Before, route) || o.Before.Epoch < route.Epoch:
		return fmt.Errorf("%w: the placement is now %+v, not %+v", errCopyIntervalChanged, *o.Before, route)
	case q.PGID != copyRouteSPG(route):
		return fmt.Errorf("%w: the query was answered for %s, not %s", errCopyIntervalChanged, q.PGID, copyRouteSPG(route))
	case !slices.Equal(q.Up, route.Up) || !slices.Equal(q.Acting, route.Acting):
		return fmt.Errorf("%w: the primary reports up %v and acting %v; the copy was sent with %v and %v", errCopyIntervalChanged, q.Up, q.Acting, route.Up, route.Acting)
	case q.SameIntervalSince == 0 || q.SameIntervalSince > route.Epoch:
		return fmt.Errorf("%w: the interval started at epoch %d, after the copy's epoch %d", errCopyIntervalChanged, q.SameIntervalSince, route.Epoch)
	}
	return nil
}

// requireIntervalHeld observes the destination's PG after a barrier's reads
// and fails unless the interval the copy was sent in at route's epoch still
// holds. It returns the interval's start for a receipt.
func (r *copyPrimaryChangeRun) requireIntervalHeld(ctx context.Context, route osdFixtureOracle, stage string) uint32 {
	r.t.Helper()
	o := copyInterval(r.t, ctx, 2, r.dest)
	if err := copyIntervalHeld(o, route); err != nil {
		r.t.Fatalf("%s: %v", stage, err)
	}
	return o.Query.SameIntervalSince
}

// copyBudgetSpent says whether too little of work's deadline is left for one
// more round of native observations, so a polling loop reports its last
// verdict instead of an observation cut off by the deadline.
func copyBudgetSpent(work context.Context) bool {
	deadline, ok := work.Deadline()
	return work.Err() != nil || ok && time.Until(deadline) < 2*time.Second
}

func TestCopyFromIntervalJudgements(t *testing.T) {
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	ec := route
	ec.Up, ec.Acting, ec.Replicas, ec.Shard, ec.PoolType = []int32{1, 0, 2, 3}, []int32{1, 0, 2, 3}, 4, 0, 3
	observe := func(r osdFixtureOracle, epoch, since uint32) copyIntervalObservation {
		at := r
		at.Epoch = epoch
		before, after, code := at, at, 0
		return copyIntervalObservation{Before: &before, After: &after, Code: &code,
			Query: &copyIntervalQuery{State: "active+clean", Epoch: epoch, Up: r.Up, Acting: r.Acting, PGID: copyRouteSPG(r), SameIntervalSince: since}}
	}
	for name, c := range map[string]struct {
		r            osdFixtureOracle
		epoch, since uint32
	}{"replicated, same epoch": {route, 30, 28}, "replicated, a later map": {route, 34, 30}, "erasure-coded": {ec, 31, 25}} {
		if err := copyIntervalHeld(observe(c.r, c.epoch, c.since), c.r); err != nil {
			t.Errorf("held interval refused (%s): %v", name, err)
		}
	}
	for name, c := range map[string]struct {
		edit    func(*copyIntervalObservation)
		changed bool
	}{
		// The same members after a repeer: only the interval start tells.
		"same members, later interval": {func(o *copyIntervalObservation) { o.Query.SameIntervalSince = 32 }, true},
		"another primary": {func(o *copyIntervalObservation) {
			b := *o.Before
			b.ID, b.PrimaryID = 0, 0
			o.Before, o.After = &b, &b
		}, true},
		"older placement":    {func(o *copyIntervalObservation) { b := *o.Before; b.Epoch = 29; o.Before, o.After = &b, &b }, true},
		"another acting set": {func(o *copyIntervalObservation) { o.Query.Acting = []int32{1, 2, 0} }, true},
		"another spg":        {func(o *copyIntervalObservation) { o.Query.PGID = "4.3s0" }, true},
		"no interval start":  {func(o *copyIntervalObservation) { o.Query.SameIntervalSince = 0 }, true},
		"records differ":     {func(o *copyIntervalObservation) { a := *o.After; a.Epoch++; o.After = &a }, false},
		"no record":          {func(o *copyIntervalObservation) { o.After = nil }, false},
		"no query":           {func(o *copyIntervalObservation) { o.Query, o.QueryFailure = nil, "pg query returned -2" }, false},
		"refused query":      {func(o *copyIntervalObservation) { c := -2; o.Code = &c }, false},
		"primary behind":     {func(o *copyIntervalObservation) { o.Query.Epoch = 33 }, false},
	} {
		o := observe(route, 34, 30)
		c.edit(&o)
		err := copyIntervalHeld(o, route)
		if err == nil || errors.Is(err, errCopyIntervalChanged) != c.changed {
			t.Errorf("interval misjudged (%s): %v", name, err)
		}
	}

	// The placement-only predicates accept records of a later interval with
	// the same members; the witness refuses that interval.
	const gid = 4641
	at := ec
	at.Epoch = 34
	temp := "#-6:c7a5d36b:::temp_4.3s0_0_4123_1:head#"
	ops := opsInFlightSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.3s0", Epoch: 31, FlagPoint: "started", Events: []string{"queued_for_pg", "reached_pg", "started"}}}}
	sent := ec
	sent.Epoch = 31
	// The copy was canceled at the change: the Objecter is empty, while the
	// primary's listing still shows the staged object.
	empty := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3"}
	staging := copyStagingSnapshot{OSD: 1, PGID: "4.3", Shard: 0, Collection: "4.3s0_head", Max: 4096, Objects: 3, Complete: true, Temp: []string{"0" + temp}}
	if _, err := checkCommitHeld(ops, empty, staging, sent, gid, 5, temp); err != nil {
		t.Fatal("the placement-only held-commit judgement refused the later interval's records:", err)
	}
	if err := copyIntervalHeld(observe(sent, 34, 33), sent); !errors.Is(err, errCopyIntervalChanged) {
		t.Error("an interval after the copy's epoch was accepted with an empty Objecter and the staged object:", err)
	}
	// A barrier read in a later interval: the requeued op keeps its epoch,
	// and a pinned copy's restarted first copy-get prints like a later one.
	held := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3",
		Ops: []objecterRequestsOp{{TID: 10, PG: "3.5", OSD: 0, Paused: true, OSDOps: []string{"assert-version v5", "copy-get max 32768 in=29b"}, CopyGetMax: []uint64{32768}}}}
	if _, _, err := checkChunkHeld(ops, held, sent, gid, 33, multiChunkLimit, 5, 0, []uint64{9}); err != nil {
		t.Fatal("the placement-only barrier judgement refused the later interval's records:", err)
	}
	if err := copyIntervalHeld(observe(sent, 34, 32), sent); !errors.Is(err, errCopyIntervalChanged) {
		t.Error("a barrier in a later interval with the same members was accepted:", err)
	}

	if copyRouteSPG(route) != "4.3" || copyRouteSPG(ec) != "4.3s0" {
		t.Fatal("spg misjudged")
	}
	expired, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if !copyBudgetSpent(expired) || copyBudgetSpent(context.Background()) {
		t.Fatal("budget misjudged")
	}
	roomy, stopRoomy := context.WithTimeout(context.Background(), time.Minute)
	defer stopRoomy()
	if copyBudgetSpent(roomy) {
		t.Fatal("a roomy budget judged spent")
	}
}
