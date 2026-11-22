package integration_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

// The EC scenarios judge what a request showed from the placement and the
// interval that the request actually used, not from the pool's nominal width
// or from the first map after a fault. These helpers bind one set of
// requests to one native interval: a judged read-only observation of the PG
// (oracle mode pgstate: two placement records around the primary's
// `pg query`) gives the placement the requests carry, and a second
// observation after them must show that interval still holding. Test-only;
// nothing is exported.

// erasurePGObservations bounds the observations of one awaited PG state.
const erasurePGObservations = 60

// erasurePGState reads one native observation of the PG of an object of the
// owned pool in slot 1.
func erasurePGState(t *testing.T, ctx context.Context, object string) pgIntervalObservation {
	t.Helper()
	return nativePool(t, ctx, "pgstate", map[string]any{"slot": 1, "object": object}).PGState
}

// erasureAwaitPG observes the object's PG, within erasurePGObservations
// observations two seconds apart, until judge accepts one, and returns it.
func erasureAwaitPG(t *testing.T, ctx context.Context, object, label string, judge func(pgIntervalObservation) error) (pgIntervalObservation, error) {
	t.Helper()
	err := errors.New("no observation")
	for attempt := 1; attempt <= erasurePGObservations; attempt++ {
		o := erasurePGState(t, ctx, object)
		if err = judge(o); err == nil {
			t.Logf("%s: epoch %d pg %s up %v acting %v primary osd.%d shard %d pg_temp %v, state %s, interval since %d (observation %d)",
				label, o.Before.Epoch, o.Before.PGID, o.Before.Up, o.Before.Acting, o.Before.PrimaryID, o.Before.Shard, o.Before.PGTemp, o.Query.State, o.Query.SameIntervalSince, attempt)
			return o, nil
		}
		if attempt < erasurePGObservations {
			time.Sleep(2 * time.Second)
		}
	}
	return pgIntervalObservation{}, fmt.Errorf("%s: no accepted observation within %d observations; the last refusal: %w", label, erasurePGObservations, err)
}

// erasureCleanPG awaits the object's PG active+clean on its placement and
// returns that placement and the epoch at which its interval started.
func erasureCleanPG(t *testing.T, ctx context.Context, object string, facts erasureFacts) (osdFixtureOracle, uint32) {
	t.Helper()
	o, err := erasureAwaitPG(t, ctx, object, "clean PG of "+object, func(o pgIntervalObservation) error { return o.cleanOn() })
	if err != nil {
		t.Fatal(err)
	}
	facts.checkPlacement(t, *o.Before)
	return *o.Before, o.Query.SameIntervalSince
}

// heldSince requires one observation taken after requests that carried
// route to show that the interval in which route was read still holds: two
// identical placement records around a query that route's primary answered
// for its own shard; the placement is route's in everything but an equal or
// newer epoch; the primary reports route's up and acting sets, at a map epoch
// not behind the placement's, in an interval that started at or before
// route's epoch. An interval start only grows, so any interval change after
// route's epoch, also one that came back to the same sets, leaves a later
// start.
func (o pgIntervalObservation) heldSince(route osdFixtureOracle) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	if !samePlacement(*o.Before, route) || o.Before.Epoch < route.Epoch {
		return fmt.Errorf("the placement is now %+v, not %+v", *o.Before, route)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if q.Info.PGID != route.PGID+"s"+strconv.Itoa(int(route.Shard)) || q.Info.Shard != int(route.Shard) {
		return fmt.Errorf("the query was answered for %s shard %d, not for shard %d of %s", q.Info.PGID, q.Info.Shard, route.Shard, route.PGID)
	}
	if !slices.Equal(q.Up, route.Up) || !slices.Equal(q.Acting, route.Acting) {
		return fmt.Errorf("the primary reports up %v and acting %v; the requests carried %v and %v", q.Up, q.Acting, route.Up, route.Acting)
	}
	if q.SameIntervalSince == 0 || q.SameIntervalSince > route.Epoch {
		return fmt.Errorf("the interval started at epoch %d, after the requests' epoch %d", q.SameIntervalSince, route.Epoch)
	}
	if q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, o.Before.Epoch)
	}
	return nil
}

// failedOverOn judges one observation after the loss of origin's primary
// and returns the shard of the primary it accepts. A pool with nonprimary
// shards is judged by replacedOn: the first member of the primary-first
// order leads under a pg_temp. A pool without them claims no lead: the
// placement is origin's at a newer epoch with the primary's position empty,
// led by another member on its own shard under another address, and that
// primary reports the PG active and undersized, optionally degraded, with
// those sets, in an interval that started after since and not after the
// placement's epoch, in which the PG has started, at a map epoch not behind
// the placement's.
func (o pgIntervalObservation) failedOverOn(origin osdFixtureOracle, nonprimary []int, since uint32) (int, error) {
	if len(nonprimary) > 0 {
		return o.replacedOn(origin, nonprimary, since)
	}
	gone := int(origin.Shard)
	if origin.PGTemp || slices.Contains(origin.Acting, crushItemNone) || !slices.Equal(origin.Up, origin.Acting) || gone < 0 || gone >= len(origin.Acting) ||
		origin.Acting[gone] != origin.PrimaryID || origin.ID != origin.PrimaryID || origin.Nonprimary != 0 || since == 0 || since > origin.Epoch {
		return 0, fmt.Errorf("%+v, with an interval since epoch %d, is not a complete acting set without nonprimary shards led by its own primary", origin, since)
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return 0, fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	p := *o.Before
	holed := holedSet(origin.Acting, gone)
	shard := int(p.Shard)
	if p.ID != p.PrimaryID || shard < 0 || shard >= len(holed) || shard == gone || holed[shard] != p.ID {
		return 0, fmt.Errorf("the placement %+v is not led by another member of %v on its own shard", p, holed)
	}
	want := origin
	want.Up, want.Acting, want.Epoch = holed, slices.Clone(holed), p.Epoch
	want.ID, want.PrimaryID, want.Shard, want.Address, want.PGTemp = p.ID, p.PrimaryID, p.Shard, p.Address, p.PGTemp
	if !reflect.DeepEqual(p, want) || p.Epoch <= origin.Epoch || p.Address == "" || p.Address == origin.Address {
		return 0, fmt.Errorf("the placement %+v is not the recorded one at a newer epoch without its primary: %+v", p, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return 0, fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"active", "undersized"}) && !slices.Equal(states, []string{"active", "degraded", "undersized"}) {
		return 0, fmt.Errorf("the primary reports %q, not an active undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(shard) || q.Info.Shard != shard {
		return 0, fmt.Errorf("the primary reports up %v and acting %v for %s; want %v for shard %d", q.Up, q.Acting, q.Info.PGID, holed, shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > p.Epoch || q.LastEpochStarted < q.SameIntervalSince || q.Epoch < p.Epoch {
		return 0, fmt.Errorf("the interval started at epoch %d and the PG at %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, q.Epoch, since, p.Epoch)
	}
	return shard, nil
}

// erasureHeldRequests sends one set of requests on the placement of a judged
// observation and accepts it only when an observation after it shows that
// interval still holding (heldSince). An interval that changed around the
// requests leaves their outcome unattributed either way: the attempt is
// recorded in changed, and the next attempt waits for a newly judged
// placement. Requests that fail in an interval that held are a failure and
// are not resent. judged returns one accepted placement, send the requests'
// verdict on it and after one observation of the PG. It returns the
// placement the accepted requests carried.
func erasureHeldRequests(attempts int, judged func() (osdFixtureOracle, error), send func(osdFixtureOracle) error,
	after func() pgIntervalObservation) (route osdFixtureOracle, changed []string, err error) {
	for attempt := 1; attempt <= attempts; attempt++ {
		route, err = judged()
		if err != nil {
			return osdFixtureOracle{}, changed, fmt.Errorf("attempt %d: %w", attempt, err)
		}
		sent := send(route)
		held := after().heldSince(route)
		if held == nil {
			if sent != nil {
				return route, changed, fmt.Errorf("attempt %d at epoch %d on osd.%d shard %d, in an interval that held: %w", attempt, route.Epoch, route.ID, route.Shard, sent)
			}
			return route, changed, nil
		}
		changed = append(changed, fmt.Sprintf("attempt %d at epoch %d on osd.%d shard %d: %v (requests: %v)", attempt, route.Epoch, route.ID, route.Shard, held, sent))
	}
	return osdFixtureOracle{}, changed, fmt.Errorf("no requests within %d attempts were followed by their own interval: %q", attempts, changed)
}

// erasureReadShard is the data shard other than the primary's whose chunk a
// full read of an object of size bytes from the start of its first stripe
// needs first, or -1: the lowest shard s below k with s*chunk < size.
func erasureReadShard(route osdFixtureOracle, k, chunk, size int) int {
	for shard := range k {
		if shard != int(route.Shard) && chunk > 0 && shard*chunk < size {
			return shard
		}
	}
	return -1
}

// erasureOutageWitness checks the two observations around a native read made
// during the outage of other's OSD, after the clean recovery of victim's:
// origin is the clean placement after the recovery and since the epoch at
// which its interval started. The observation before the read must show
// the PG active under origin's primary with other's position alone empty
// (activeWithout), the one after it that interval still holding
// (heldSince). It returns the live acting members of that interval: the
// distinct owned OSDs of origin but other, victim among them.
func erasureOutageWitness(facts erasureFacts, origin osdFixtureOracle, since uint32, victim, other int32, before, after pgIntervalObservation) ([]int32, error) {
	vi, oi := slices.Index(origin.Acting, victim), slices.Index(origin.Acting, other)
	if vi < 0 || oi < 0 || vi == oi || victim == origin.ID || other == origin.ID {
		return nil, fmt.Errorf("osd.%d and osd.%d are not two members of %v other than the primary osd.%d", victim, other, origin.Acting, origin.ID)
	}
	if err := before.activeWithout(origin, []int{oi}, since); err != nil {
		return nil, fmt.Errorf("before the read: %w", err)
	}
	if err := after.heldSince(*before.Before); err != nil {
		return nil, fmt.Errorf("after the read: %w", err)
	}
	var live []int32
	for _, osd := range before.Before.Acting {
		if osd == crushItemNone {
			continue
		}
		if slices.Contains(live, osd) || !slices.Contains(facts.owned, osd) {
			return nil, fmt.Errorf("the acting set %v is not distinct owned OSDs %v", before.Before.Acting, facts.owned)
		}
		live = append(live, osd)
	}
	if len(live) != len(origin.Acting)-1 || slices.Contains(live, other) || !slices.Contains(live, victim) {
		return nil, fmt.Errorf("the live members %v are not those of %v without osd.%d and with osd.%d", live, origin.Acting, other, victim)
	}
	return live, nil
}

// erasureReadNeedsEveryShard reports whether a full read of an object of
// size bytes needs every one of live shards of a k-data-shard code: exactly
// k shards are live, and the object reaches the last data chunk of its
// single stripe, so every data shard holds part of it and any missing data
// chunk must be decoded from all the others.
func erasureReadNeedsEveryShard(live, k, chunk, size int) bool {
	return live == k && chunk > 0 && size > (k-1)*chunk && size <= k*chunk
}

// heldAt is a copy of the observation at the given epoch, with the
// primary's map at that epoch and the given interval start. No daemon
// produced it.
func heldAt(o pgIntervalObservation, epoch, since uint32) pgIntervalObservation {
	before, after, query := *o.Before, *o.After, *o.Query
	before.Up, before.Acting, after.Up, after.Acting = slices.Clone(before.Up), slices.Clone(before.Acting), slices.Clone(after.Up), slices.Clone(after.Acting)
	query.Up, query.Acting = slices.Clone(query.Up), slices.Clone(query.Acting)
	before.Epoch, after.Epoch, query.Epoch, query.SameIntervalSince = epoch, epoch, epoch, since
	return pgIntervalObservation{Before: &before, After: &after, Query: &query}
}

func TestErasureIntervalHeldSince(t *testing.T) {
	receipt, route := pgIntervalChange()
	// Recorded at epoch 25 in the interval since 24, observed again at 28.
	held := heldAt(receipt.OriginObservation, 28, 24)
	if err := held.heldSince(route); err != nil {
		t.Fatal("an interval that held since the placement's epoch refused:", err)
	}
	if err := heldAt(receipt.OriginObservation, 25, 25).heldSince(route); err != nil {
		t.Fatal("an interval that started at the placement's own epoch refused:", err)
	}
	both := func(edit func(*osdFixtureOracle)) func(*pgIntervalObservation) {
		return func(o *pgIntervalObservation) { edit(o.Before); edit(o.After) }
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		// The same sets after an interval change and back (pg repeer, an
		// OSD that left and returned): only the interval start shows it.
		"interval changed and came back": func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 27 },
		"another primary": both(func(p *osdFixtureOracle) {
			p.ID, p.PrimaryID, p.Shard, p.Address = 2, 2, 1, "v2:192.0.2.8:6800/3"
		}),
		"another address":   both(func(p *osdFixtureOracle) { p.Address = "v2:192.0.2.7:6800/9" }),
		"one position gone": both(func(p *osdFixtureOracle) { p.Up[2], p.Acting[2] = crushItemNone, crushItemNone }),
		"a pg_temp":         both(func(p *osdFixtureOracle) { p.PGTemp = true }),
		"older placement":   both(func(p *osdFixtureOracle) { p.Epoch = 24 }),
		"records differ":    func(o *pgIntervalObservation) { o.After.Epoch = 29 },
		"no record after":   func(o *pgIntervalObservation) { o.After = nil },
		"no query":          func(o *pgIntervalObservation) { o.Query, o.QueryFailure = nil, "pg query returned -110" },
		"query of shard 1":  func(o *pgIntervalObservation) { o.Query.Info.PGID, o.Query.Info.Shard = "3.7s1", 1 },
		"primary's sets":    func(o *pgIntervalObservation) { o.Query.Acting = []int32{3, 2, crushItemNone, 0} },
		"primary behind":    func(o *pgIntervalObservation) { o.Query.Epoch = 27 },
		"no interval start": func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 0 },
	} {
		o := heldAt(receipt.OriginObservation, 28, 24)
		edit(&o)
		if err := o.heldSince(route); err == nil {
			t.Fatal("interval accepted as held:", name)
		}
	}
}

// failedOverObservation builds one synthetic observation of PG 3.7 of a
// pool without nonprimary shards after the loss of its primary osd.3 on
// shard 0: active on [NONE 2 1 0] under osd.2 on shard 1 at epoch 28 in the
// interval that started at 27. No daemon produced it.
func failedOverObservation() (pgIntervalObservation, osdFixtureOracle) {
	_, origin := pgIntervalChange()
	origin.Nonprimary = 0
	holed := []int32{crushItemNone, 2, 1, 0}
	before := origin
	before.Up, before.Acting, before.Epoch = slices.Clone(holed), slices.Clone(holed), 28
	before.ID, before.PrimaryID, before.Shard, before.Address = 2, 2, 1, "v2:192.0.2.9:6800/4"
	after := before
	after.Up, after.Acting = slices.Clone(holed), slices.Clone(holed)
	return pgIntervalObservation{Before: &before, After: &after, Query: &pgIntervalQuery{State: "active+undersized+degraded", Epoch: 28, Up: slices.Clone(holed),
		Acting: slices.Clone(holed), SameIntervalSince: 27, LastEpochStarted: 28, Info: pgIntervalInfo{PGID: "3.7s1", Shard: 1}}}, origin
}

func TestErasureFailedOverOn(t *testing.T) {
	seen, origin := failedOverObservation()
	if shard, err := seen.failedOverOn(origin, nil, 24); err != nil || shard != 1 {
		t.Fatal("active PG under another member refused:", shard, err)
	}
	both := func(edit func(*osdFixtureOracle)) func(*pgIntervalObservation) {
		return func(o *pgIntervalObservation) { edit(o.Before); edit(o.After) }
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"old primary": both(func(p *osdFixtureOracle) {
			p.ID, p.PrimaryID, p.Shard, p.Address = 3, 3, 0, origin.Address
		}),
		"primary off its shard": both(func(p *osdFixtureOracle) { p.Shard = 2 }),
		"same address":          both(func(p *osdFixtureOracle) { p.Address = origin.Address }),
		"no empty position":     both(func(p *osdFixtureOracle) { p.Up, p.Acting = []int32{3, 2, 1, 0}, []int32{3, 2, 1, 0} }),
		"another member gone":   both(func(p *osdFixtureOracle) { p.Up[2], p.Acting[2] = crushItemNone, crushItemNone }),
		"recorded epoch":        both(func(p *osdFixtureOracle) { p.Epoch = 25 }),
		"another PG":            both(func(p *osdFixtureOracle) { p.PGID = "3.6" }),
		"records differ":        func(o *pgIntervalObservation) { o.After.Epoch = 29 },
		"recorded interval":     func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 24 },
		"interval after map":    func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 29 },
		"not started":           func(o *pgIntervalObservation) { o.Query.LastEpochStarted = 26 },
		"peered":                func(o *pgIntervalObservation) { o.Query.State = "undersized+degraded+peered" },
		"query of shard 0":      func(o *pgIntervalObservation) { o.Query.Info.PGID, o.Query.Info.Shard = "3.7s0", 0 },
		"primary behind":        func(o *pgIntervalObservation) { o.Query.Epoch = 27 },
		"no query":              func(o *pgIntervalObservation) { o.Query = nil },
	} {
		o := heldAt(seen, 28, 27)
		o.Query.LastEpochStarted = 28
		edit(&o)
		if _, err := o.failedOverOn(origin, nil, 24); err == nil {
			t.Fatal("failover accepted:", name)
		}
	}
	if _, err := heldAt(seen, 28, 27).failedOverOn(origin, nil, 24); err != nil {
		t.Fatal("the unedited copy of the failover refused")
	}
	if _, err := seen.failedOverOn(origin, nil, 26); err == nil {
		t.Fatal("an interval start after the recorded placement's epoch accepted")
	}
	// With nonprimary shards the lead is judged: the first parity shard
	// under a pg_temp. A later map that leads with another shard or without
	// the pg_temp is not the expected placement.
	receipt, route := pgReplacedObservation()
	if shard, err := receipt.Observation.failedOverOn(route, []int{1}, 24); err != nil || shard != 2 {
		t.Fatal("the first parity shard under a pg_temp refused:", shard, err)
	}
	later := heldAt(receipt.Observation, 31, 30)
	later.Query.LastEpochStarted = 31
	if shard, err := later.failedOverOn(route, []int{1}, 24); err != nil || shard != 2 {
		t.Fatal("a later map of the same lead refused:", shard, err)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"no pg_temp": func(p *osdFixtureOracle) { p.PGTemp = false },
		"parity shard 3": func(p *osdFixtureOracle) {
			p.ID, p.PrimaryID, p.Shard = 0, 0, 3
		},
		"nonprimary shard 1": func(p *osdFixtureOracle) {
			p.ID, p.PrimaryID, p.Shard = 2, 2, 1
		},
	} {
		o := heldAt(receipt.Observation, 31, 30)
		o.Query.LastEpochStarted = 31
		edit(o.Before)
		edit(o.After)
		o.Query.Info.PGID, o.Query.Info.Shard = "3.7s"+strconv.Itoa(int(o.Before.Shard)), int(o.Before.Shard)
		if _, err := o.failedOverOn(route, []int{1}, 24); err == nil {
			t.Fatal("a later placement accepted as the expected lead:", name)
		}
	}
}

func TestErasureHeldRequests(t *testing.T) {
	receipt, _ := pgReplacedObservation()
	first := *receipt.Observation.Before // epoch 28, interval since 27
	second := first
	second.Epoch = 31
	held := func(route osdFixtureOracle, since uint32) pgIntervalObservation {
		return heldAt(receipt.Observation, route.Epoch+1, since)
	}
	// The interval changes after the first placement (a newer start), and
	// the second placement holds: only the second is accepted, and the
	// first attempt is reported.
	var judged, sent []osdFixtureOracle
	routes := []osdFixtureOracle{first, second}
	afters := []pgIntervalObservation{held(first, 30), held(second, 30)}
	route, changed, err := erasureHeldRequests(3, func() (osdFixtureOracle, error) {
		r := routes[len(judged)]
		judged = append(judged, r)
		return r, nil
	}, func(r osdFixtureOracle) error {
		sent = append(sent, r)
		return nil
	}, func() pgIntervalObservation { return afters[len(sent)-1] })
	if err != nil || route.Epoch != 31 || len(changed) != 1 || len(judged) != 2 || !slices.EqualFunc(judged, sent, samePlacement) || sent[1].Epoch != 31 {
		t.Fatal("the held second attempt was not the one accepted:", route.Epoch, changed, err, len(judged))
	}
	// A stale placement whose requests succeed is not accepted when its
	// interval changed around them, and no attempt is left.
	if _, changed, err := erasureHeldRequests(1, func() (osdFixtureOracle, error) { return first, nil }, func(osdFixtureOracle) error { return nil },
		func() pgIntervalObservation { return held(first, 30) }); err == nil || len(changed) != 1 {
		t.Fatal("requests in a changed interval accepted:", changed, err)
	}
	// Nor when the placement itself moved after them.
	moved := held(first, 27)
	moved.Before.ID, moved.After.ID, moved.Before.PrimaryID, moved.After.PrimaryID = 0, 0, 0, 0
	if _, _, err := erasureHeldRequests(1, func() (osdFixtureOracle, error) { return first, nil }, func(osdFixtureOracle) error { return nil },
		func() pgIntervalObservation { return moved }); err == nil {
		t.Fatal("requests on a placement that moved accepted")
	}
	// Requests that fail in an interval that held are a failure and are
	// not resent.
	calls := 0
	if _, changed, err := erasureHeldRequests(3, func() (osdFixtureOracle, error) { calls++; return first, nil },
		func(osdFixtureOracle) error { return errors.New("no reply") }, func() pgIntervalObservation { return held(first, 27) }); err == nil || calls != 1 || len(changed) != 0 {
		t.Fatal("a failure in a held interval was resent or accepted:", calls, changed, err)
	}
	// A judgement that never accepts a placement ends the attempts.
	if _, _, err := erasureHeldRequests(3, func() (osdFixtureOracle, error) { return osdFixtureOracle{}, errors.New("not replaced") },
		func(osdFixtureOracle) error { t.Fatal("requests sent without a judged placement"); return nil }, func() pgIntervalObservation { return held(first, 27) }); err == nil {
		t.Fatal("no judged placement accepted")
	}
}

func TestErasureOutageWitness(t *testing.T) {
	_, origin := pgIntervalChange() // acting [3 2 1 0], interval since 24
	facts := erasureFacts{size: 4, owned: []int32{0, 1, 2, 3}, chunk: 4096}
	const victim, other = 2, 1 // positions 1 and 2
	before, _ := pgStateObservation(30, "active+undersized+degraded", 2)
	after := heldAt(before, 31, 29)
	live, err := erasureOutageWitness(facts, origin, 24, victim, other, before, after)
	if err != nil || !slices.Equal(live, []int32{3, 2, 0}) {
		t.Fatal("the outage of osd.1 refused:", live, err)
	}
	if !erasureReadNeedsEveryShard(len(live), 3, facts.chunk, 12000) {
		t.Fatal("three live shards of k=3 do not need every shard")
	}
	victimOut, _ := pgStateObservation(30, "active+undersized+degraded", 1)
	stillLive, _ := pgStateObservation(30, "active+clean")
	twoOut, _ := pgStateObservation(30, "active+undersized+degraded", 2, 3)
	moved := heldAt(before, 30, 29)
	moved.Before.ID, moved.After.ID, moved.Before.PrimaryID, moved.After.PrimaryID = 0, 0, 0, 0
	if _, err := erasureOutageWitness(facts, origin, 24, victim, other, heldAt(before, 30, 29), heldAt(before, 31, 29)); err != nil {
		t.Fatal("the unedited copies of the outage refused:", err)
	}
	for name, c := range map[string]struct {
		facts         erasureFacts
		victim, other int32
		before, after pgIntervalObservation
	}{
		"victim out instead":     {facts, victim, other, victimOut, heldAt(victimOut, 31, 29)},
		"other still live":       {facts, victim, other, stillLive, heldAt(stillLive, 31, 29)},
		"two members out":        {facts, victim, other, twoOut, heldAt(twoOut, 31, 29)},
		"interval changed after": {facts, victim, other, before, heldAt(before, 31, 31)},
		"records after differ":   {facts, victim, other, before, func() pgIntervalObservation { o := heldAt(before, 31, 29); o.After.Epoch = 32; return o }()},
		"primary moved":          {facts, victim, other, moved, heldAt(moved, 31, 29)},
		"victim not a member":    {facts, 7, other, before, after},
		"victim is other":        {facts, other, other, before, after},
		"other is the primary":   {facts, victim, 3, before, after},
		"unowned member":         {erasureFacts{size: 4, owned: []int32{1, 2, 3}, chunk: 4096}, victim, other, before, after},
	} {
		if live, err := erasureOutageWitness(c.facts, origin, 24, c.victim, c.other, c.before, c.after); err == nil {
			t.Fatal("outage witness accepted:", name, live)
		}
	}
	if _, err := erasureOutageWitness(facts, origin, 30, victim, other, before, after); err == nil {
		t.Fatal("an interval that did not start after the clean one accepted")
	}
}

func TestErasureReadShard(t *testing.T) {
	route := osdFixtureOracle{Shard: 0}
	for _, c := range []struct {
		shard          int8
		k, chunk, size int
		want           int
	}{
		{0, 2, 4096, 8192, 1}, {0, 3, 4096, 8192, 1}, {0, 4, 4096, 8192, 1},
		{0, 3, 4096, 4096, -1}, {0, 3, 0, 8192, -1}, {1, 3, 4096, 100, 0}, {0, 1, 4096, 8192, -1},
	} {
		route.Shard = c.shard
		if got := erasureReadShard(route, c.k, c.chunk, c.size); got != c.want {
			t.Fatal("read shard", c, got)
		}
	}
}
