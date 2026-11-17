package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Watch, snapshot and backoff state of an optimized isa k=2,m=2 pool across
// the epoch changes that a lost OSD causes, on four BlueStore OSDs. The
// client neither follows the map nor reconnects or resends on its own: in
// each control the test, as the caller, reads the native placement and
// decides what to send where. The pinned source (Ceph v20.2.4) gives the
// expected native behaviour:
//
//   - A watch is part of the object's state (object_info_t::watchers). A new
//     primary loads it as a disconnected watch with the registration's
//     timeout (src/osd/PrimaryLogPG.cc populate_obc_watchers), and the caller
//     RECONNECTs the same cookie to it (src/osd/PrimaryLogPG.cc do_osd_ops,
//     CEPH_OSD_WATCH_OP_RECONNECT).
//   - Clones and the snapset are object state too. A write with a newer
//     SnapContext through a primary that leads under a primary-first
//     pg_temp clones the head as through any primary
//     (src/osd/PrimaryLogPG.cc make_writeable).
//   - On a connection that negotiated RADOS_BACKOFF, a PG that is peered
//     but not active (an EC PG with fewer than min_size shards) answers a
//     client op with a PG-wide BLOCK and drops the op
//     (src/osd/PrimaryLogPG.cc:1868-1901). The primary of a new interval
//     releases its PG backoffs with UNBLOCK before anything else
//     (src/osd/PeeringState.cc:684-830 start_peering_interval ->
//     clear_primary_state -> src/osd/PG.cc:351-361 release_pg_backoffs ->
//     :596-657), so an OSD that returns and ends the interval releases it.
//
// OSDs leave through the fixture's forced kill of an owned OSD and return
// through its start, as in the existing EC outage tests. These consumer
// helpers are test-only and nothing is exported.

// pgStates is the sorted set of state tokens the primary reported.
func pgStates(state string) []string {
	tokens := strings.Split(state, "+")
	slices.Sort(tokens)
	return tokens
}

// holedSet is set with the given positions empty.
func holedSet(set []int32, positions ...int) []int32 {
	holed := slices.Clone(set)
	for _, position := range positions {
		holed[position] = crushItemNone
	}
	return holed
}

// peeredOn requires one read-only observation of the PG to show it peered
// and not active without the members of the given acting positions: two
// identical placement records around a query that the unchanged primary
// answered for its own shard; the placement is the recorded one except for
// a newer epoch and exactly those positions empty in the up and acting sets,
// without a pg_temp; the primary reports peered and undersized, optionally
// degraded, and nothing else, with those sets, in an interval that started
// after since and not after the placement's epoch, and its own map epoch is
// not behind the placement's. Too few shards for min_size keep the PG from
// activating (src/osd/PeeringState.cc:6919-6933, :7117).
func (o pgIntervalObservation) peeredOn(origin osdFixtureOracle, holes []int, since uint32) error {
	if slices.Contains(origin.Acting, crushItemNone) || origin.PGTemp || !slices.Equal(origin.Up, origin.Acting) || origin.ID != origin.PrimaryID ||
		origin.Shard < 0 || int(origin.Shard) >= len(origin.Acting) || origin.Acting[origin.Shard] != origin.PrimaryID || since == 0 || since > origin.Epoch {
		return fmt.Errorf("%+v, with an interval since epoch %d, is not a complete acting set led by its own primary", origin, since)
	}
	if len(holes) == 0 || len(origin.Acting)-len(holes) >= origin.MinSize {
		return fmt.Errorf("%d empty positions of %d leave the PG at or above min_size %d", len(holes), len(origin.Acting), origin.MinSize)
	}
	for i, position := range holes {
		if position < 0 || position >= len(origin.Acting) || position == int(origin.Shard) || slices.Contains(holes[:i], position) {
			return fmt.Errorf("the empty positions %v are not distinct positions other than the primary's %d", holes, origin.Shard)
		}
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	holed := holedSet(origin.Acting, holes...)
	want := origin
	want.Up, want.Acting, want.Epoch = holed, slices.Clone(holed), o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not the recorded one at a newer epoch with positions %v empty: %+v", *o.Before, holes, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"peered", "undersized"}) && !slices.Equal(states, []string{"degraded", "peered", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not a peered undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) {
		return fmt.Errorf("the primary reports up %v and acting %v; want %v", q.Up, q.Acting, holed)
	}
	if q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the query was answered for %s shard %d, not for shard %d of %s", q.Info.PGID, q.Info.Shard, origin.Shard, origin.PGID)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d; the recorded interval started at %d and the placement is at epoch %d", q.SameIntervalSince, since, o.Before.Epoch)
	}
	if q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, o.Before.Epoch)
	}
	return nil
}

// activeWithout requires one read-only observation to show the PG active
// and undersized, optionally degraded, without the members of exactly the
// given positions, under the unchanged primary and without a pg_temp, in an
// interval that started after since: checkPGAbsent's judgement for any
// number of empty positions that leaves min_size shards.
func (o pgIntervalObservation) activeWithout(origin osdFixtureOracle, holes []int, since uint32) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	if len(origin.Acting)-len(holes) < origin.MinSize || slices.Contains(holes, int(origin.Shard)) || origin.PGTemp || since == 0 {
		return fmt.Errorf("positions %v of %+v, since epoch %d, cannot leave an active PG under its own primary", holes, origin, since)
	}
	holed := holedSet(origin.Acting, holes...)
	want := origin
	want.Up, want.Acting, want.Epoch = holed, slices.Clone(holed), o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not the recorded one at a newer epoch with positions %v empty: %+v", *o.Before, holes, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"active", "undersized"}) && !slices.Equal(states, []string{"active", "degraded", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not an active undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the primary reports up %v and acting %v for %s; want %v for shard %d", q.Up, q.Acting, q.Info.PGID, holed, origin.Shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince || q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d and the PG at %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, q.Epoch, since, o.Before.Epoch)
	}
	return nil
}

// cleanAgainOn requires one read-only observation to show the PG
// active+clean on the recorded placement (cleanOn), with the primary under
// the address the placement now names, in an interval that started after
// since.
func (o pgIntervalObservation) cleanAgainOn(origin osdFixtureOracle, since uint32) (osdFixtureOracle, error) {
	if err := o.cleanOn(); err != nil {
		return osdFixtureOracle{}, err
	}
	back := origin
	back.Address, back.Epoch = o.Before.Address, o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, back) || o.Before.Epoch <= origin.Epoch {
		return osdFixtureOracle{}, fmt.Errorf("the placement %+v is not the recorded one at a newer epoch: %+v", *o.Before, origin)
	}
	if q := o.Query; q.SameIntervalSince <= since {
		return osdFixtureOracle{}, fmt.Errorf("the interval started at epoch %d, not after epoch %d", q.SameIntervalSince, since)
	}
	return back, nil
}

// pgBackoffBounds is the object range of a PG-wide backoff: pinned
// pg_t::get_hobj_start and get_hobj_end (src/osd/osd_types.cc:879-901) for
// a pool of pgNum PGs, a power of two. Begin carries the PG seed and
// snapshot 0; end the next seed in bit-reversed order with CEPH_NOSNAP, or
// the maximum object for the last PG of that order.
func pgBackoffBounds(seed uint32, pgNum int) (begin uint32, end uint32, endMax bool, err error) {
	if pgNum < 1 || pgNum&(pgNum-1) != 0 || seed >= uint32(pgNum) {
		return 0, 0, false, fmt.Errorf("seed %d of %d PGs is not a PG of a power-of-two pool", seed, pgNum)
	}
	split := bits.Len(uint(pgNum - 1))
	revEnd := (uint64(bits.Reverse32(seed)) | uint64(0xffffffff>>split)) + 1
	if revEnd >= 1<<32 {
		return seed, 0, true, nil
	}
	return seed, bits.Reverse32(uint32(revEnd)), false, nil
}

// checkPGBackoff requires a native backoff event to be the PG-wide one of
// route's PG on its primary's actual shard: its pool, seed, shard, source,
// session and range bounds.
func checkPGBackoff(event cephmsgr.OSDBackoffEvent, op uint8, route osdFixtureOracle, pgNum int, globalID uint64) error {
	begin, end, endMax, err := pgBackoffBounds(route.PG, pgNum)
	if err != nil {
		return err
	}
	if event.Op != op || event.Pool != uint64(route.Pool) || event.Seed != route.PG || event.Shard != route.Shard || event.ID == 0 ||
		event.Source.ID != route.ID || event.GlobalID != globalID || event.SessionGeneration == 0 || len(event.RawFront) == 0 {
		return fmt.Errorf("event op %d pg %d.%x shard %d id %d from osd.%d gid %d is not op %d for %s shard %d from osd.%d gid %d",
			event.Op, event.Pool, event.Seed, event.Shard, event.ID, event.Source.ID, event.GlobalID, op, route.PGID, route.Shard, route.ID, globalID)
	}
	b, e := event.Begin, event.End
	if b.Max || b.Pool != route.Pool || b.Hash != begin || b.Object != "" || b.Key != "" || b.Namespace != "" || b.Snapshot != 0 {
		return fmt.Errorf("the range begins at %+v, not at the start of %s", b, route.PGID)
	}
	if endMax {
		if !e.Max {
			return fmt.Errorf("the range ends at %+v, not at the maximum object", e)
		}
	} else if e.Max || e.Pool != route.Pool || e.Hash != end || e.Object != "" || e.Key != "" || e.Namespace != "" || e.Snapshot != cephmsgr.OSDHeadSnapshot {
		return fmt.Errorf("the range ends at %+v, not at the start of the next PG (hash %#x)", e, end)
	}
	return nil
}

// checkOSDDials requires the dials since snapshot to include exactly one
// attempt at endpoint at and none at any other OSD endpoint: one of osds or
// one the client had not dialed before. A MON redial is allowed. A restarted
// OSD may listen on an endpoint the client dialed before.
func checkOSDDials(snapshot, now map[string]int, osds []string, at string) error {
	var changed []dialChange
	for endpoint, n := range now {
		before, known := snapshot[endpoint]
		if n == before || known && endpoint != at && !slices.Contains(osds, endpoint) {
			continue
		}
		changed = append(changed, dialChange{endpoint, before, n})
	}
	slices.SortFunc(changed, func(a, b dialChange) int { return strings.Compare(a.endpoint, b.endpoint) })
	if len(changed) != 1 || changed[0].endpoint != at || changed[0].now-changed[0].before != 1 {
		return fmt.Errorf("OSD dials since the snapshot %v; want exactly one new attempt at %s (%d before)", changed, at, snapshot[at])
	}
	return nil
}

// ecEpochRun is one control on its own owned optimized isa k=2,m=2 pool.
type ecEpochRun struct {
	t       *testing.T
	ctx     context.Context
	c       *cephmsgr.Client
	base    osdFixtureOracle
	hash    bool
	pgNum   int
	request func(osdFixtureOracle, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
	facts   erasureFacts
	// control runs one fixture daemon control (action, daemon, name).
	control func(ctx context.Context, action, daemon, name string) error
	// outages lists every OSD the run killed, in kill order.
	outages []*ecOutage
}

// fixtureControl runs one daemon control through the native fixture's
// control directory.
func fixtureControl(ctx context.Context, action, daemon, name string) error {
	return testcluster.ControlDaemon(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), action, daemon, name)
}

func startECEpochRun(t *testing.T, timeout time.Duration) *ecEpochRun {
	width := erasureK2M2
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	r := &ecEpochRun{t: t, request: osdObjectRequest, pgNum: 1, control: fixtureControl}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture, r.request, r.hash, r.pgNum = replicatedPGFixture, replicatedPGRequest, true, 8
	}
	if !r.hash {
		t.Skip("the epoch-state controls address actual shards and need a replicated-PG request profile")
	}
	r.c, _, r.base, _ = fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	r.ctx = ctx
	r.teardown(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		removed := nativePool(t, cleanup, "cleanup", nil)
		if !removed.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
		if slices.Contains(removed.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": r.pgNum, "hashpspool": r.hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	r.facts = erasurePoolMapFacts(t, ctx, r.c, created, width, true)
	t.Logf("EC pool %d %s optimized size=%d min_size=%d nonprimary shards %v owned OSDs %v", created.PoolID, width, r.facts.size, r.facts.minSize, r.facts.nonprimary, r.facts.owned)
	return r
}

// place reads the object's native placement.
func (r *ecEpochRun) place(object string) osdFixtureOracle {
	return poolPlace(r.t, r.ctx, 1, object, "")
}

// state reads one native observation of the object's PG (oracle mode
// pgstate): two placement records around the primary's pg query.
func (r *ecEpochRun) state(object string) pgIntervalObservation {
	r.t.Helper()
	return nativePool(r.t, r.ctx, "pgstate", map[string]any{"slot": 1, "object": object}).PGState
}

// healthy reads the object's placement and one observation of its PG and
// requires the PG active+clean under a primary on its own primary-eligible
// shard without a pg_temp. It returns the placement and the epoch at which
// the PG's interval started.
func (r *ecEpochRun) healthy(object string) (osdFixtureOracle, uint32) {
	r.t.Helper()
	seen := r.state(object)
	if err := seen.cleanOn(); err != nil {
		r.t.Fatalf("the PG of %s is not active+clean on its placement: %v", object, err)
	}
	route := *seen.Before
	r.facts.checkPlacement(r.t, route)
	if route.Name != object || route.PGTemp {
		r.t.Fatalf("the placement %+v is not %s's without a pg_temp", route, object)
	}
	return route, seen.Query.SameIntervalSince
}

// waitState observes the object's PG, within a bounded number of
// observations, until judge accepts one, and returns it.
func (r *ecEpochRun) waitState(label, object string, judge func(pgIntervalObservation) error) pgIntervalObservation {
	r.t.Helper()
	err := errors.New("no observation")
	var seen []string
	for attempt := 1; attempt <= shardOutageObservations; attempt++ {
		o := r.state(object)
		if err = judge(o); err == nil {
			r.t.Logf("%s: epoch %d pg %s up %v acting %v primary osd.%d shard %d pg_temp %v, state %s, interval since %d (observation %d, seen before %v)",
				label, o.Before.Epoch, o.Before.PGID, o.Before.Up, o.Before.Acting, o.Before.PrimaryID, o.Before.Shard, o.Before.PGTemp, o.Query.State, o.Query.SameIntervalSince, attempt, seen)
			return o
		}
		if o.Before != nil && o.Query != nil {
			step := fmt.Sprintf("epoch %d osd.%d(%d) %v %s", o.Before.Epoch, o.Before.PrimaryID, o.Before.Shard, o.Before.Acting, o.Query.State)
			if len(seen) == 0 || seen[len(seen)-1] != step {
				seen = append(seen, step)
			}
		}
		time.Sleep(shardOutagePause)
	}
	r.t.Fatalf("%s was not observed: %v (seen %v)", label, err, seen)
	return pgIntervalObservation{}
}

// ecOutage is one owned forced kill of an OSD that the run restores.
type ecOutage struct {
	osd     int32
	started bool
}

// teardown registers the run's two cleanups: removePool, then one recovery
// of every OSD the run killed and did not start. Go runs cleanups last in,
// first out, so the recovery runs first on every exit, also a fatal one
// with several OSDs down, and the pool is removed only after it.
func (r *ecEpochRun) teardown(removePool func()) {
	r.t.Cleanup(removePool)
	r.t.Cleanup(r.recover)
}

// recover starts every OSD the run killed and has not started, then waits
// once for every OSD up and every PG clean (restoreOSDOutages). A per-kill
// cleanup that waited for clean PGs after its own start would ask for that
// state while another killed OSD is still down.
func (r *ecEpochRun) recover() {
	var down []int32
	for _, o := range r.outages {
		if !o.started {
			down = append(down, o.osd)
		}
	}
	if len(down) == 0 {
		return
	}
	restore, stop := context.WithTimeout(context.Background(), 300*time.Second)
	defer stop()
	errs := restoreOSDOutages(down,
		func(id int32) error { return r.control(restore, "start", "osd", strconv.FormatInt(int64(id), 10)) },
		func() error { return r.control(restore, "verify", "osd-ready", "clean") })
	for _, err := range errs {
		r.t.Error(err)
	}
	if len(errs) == 0 {
		r.t.Logf("restored: started osd %v; every OSD up and every PG clean", down)
	}
}

// restoreOSDOutages starts every OSD in down, in order, and attempts each
// start even after an earlier one failed. Only once every start succeeded
// does it run clean, exactly once; with any OSD still down the clean state
// cannot be reached, so the check is not asked for and that is reported.
// It returns every failure in order.
func restoreOSDOutages(down []int32, start func(int32) error, clean func() error) []error {
	var errs []error
	for _, id := range down {
		if err := start(id); err != nil {
			errs = append(errs, fmt.Errorf("restart the killed osd.%d: %w", id, err))
		}
	}
	if len(errs) != 0 {
		return append(errs, fmt.Errorf("every OSD up and every PG clean was not checked: %d of the killed OSDs %v did not start", len(errs), down))
	}
	if err := clean(); err != nil {
		return []error{fmt.Errorf("the fixture did not return to every OSD up and every PG clean: %w", err)}
	}
	return nil
}

// kill SIGKILLs one owned OSD through the fixture. The OSD is recorded as
// down before the kill is requested, because a control error cannot prove
// that the daemon still runs; the run's single recovery starts it on exit
// if the test did not. Waiting for clean PGs is left to the caller, which
// may have taken further OSDs out.
func (r *ecEpochRun) kill(id int32) *ecOutage {
	r.t.Helper()
	dir, name := os.Getenv("CEPH_MSGR_CONTROL_DIR"), strconv.FormatInt(int64(id), 10)
	o := &ecOutage{osd: id}
	r.outages = append(r.outages, o)
	if err := r.control(r.ctx, "kill", "osd", name); err != nil {
		r.t.Fatal("kill", id, err)
	}
	var state struct {
		Daemon   string `json:"daemon"`
		ExitCode *int   `json:"exit_code"`
	}
	if data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json")); err != nil || json.Unmarshal(data, &state) != nil || state.Daemon != "osd."+name || state.ExitCode == nil || *state.ExitCode != 137 {
		r.t.Fatal("no SIGKILL receipt of the owned OSD", name, err)
	}
	return o
}

// start starts the killed OSD again.
func (r *ecEpochRun) start(o *ecOutage) {
	r.t.Helper()
	if err := r.control(r.ctx, "start", "osd", strconv.FormatInt(int64(o.osd), 10)); err != nil {
		r.t.Fatal("start", o.osd, err)
	}
	o.started = true
}

// clean waits until every OSD is up and every PG is clean.
func (r *ecEpochRun) clean() {
	r.t.Helper()
	if err := r.control(r.ctx, "verify", "osd-ready", "clean"); err != nil {
		r.t.Fatal("the fixture did not return to every OSD up and every PG clean:", err)
	}
}

// replacePrimary kills the OSD of the healthy route's primary and waits until
// the PG is active under the member the map makes primary, with the
// primary's position empty under a primary-first pg_temp, in an interval
// after since (replacedOn). It returns that placement and the outage.
func (r *ecEpochRun) replacePrimary(object string, route osdFixtureOracle, since uint32) (osdFixtureOracle, *ecOutage) {
	r.t.Helper()
	outage := r.kill(route.ID)
	lead := -1
	seen := r.waitState("under the replacement primary", object, func(o pgIntervalObservation) error {
		var err error
		lead, err = o.replacedOn(route, r.facts.nonprimary, since)
		// A nonprimary shard may be named first by the map, but its OSD
		// must never report the PG active.
		if err != nil && o.Before != nil && o.Query != nil && slices.Contains(r.facts.nonprimary, int(o.Before.Shard)) && slices.Contains(pgStates(o.Query.State), "active") {
			r.t.Fatalf("osd.%d on nonprimary shard %d reports the PG %s at epoch %d", o.Before.PrimaryID, o.Before.Shard, o.Query.State, o.Before.Epoch)
		}
		return err
	})
	fresh := r.place(object)
	if !samePlacement(fresh, *seen.After) || fresh.Epoch < seen.After.Epoch || routePrimaryFirst(fresh) == nil || int(fresh.Shard) != lead {
		r.t.Fatalf("placement moved after the PG became active under the replacement, or gives no primary-first numbering: %+v vs %+v", fresh, *seen.After)
	}
	return fresh, outage
}

// primaryBack starts the outage's OSD, waits for every PG to be clean and for
// one observation of the PG active+clean on route's placement under the
// returned primary's new address, in an interval after since.
func (r *ecEpochRun) primaryBack(object string, route osdFixtureOracle, outage *ecOutage, since uint32) (osdFixtureOracle, uint32) {
	r.t.Helper()
	r.start(outage)
	r.clean()
	var back osdFixtureOracle
	seen := r.waitState("primary back", object, func(o pgIntervalObservation) error {
		var err error
		back, err = o.cleanAgainOn(route, since)
		return err
	})
	if back.Address == route.Address {
		r.t.Fatalf("the restarted primary osd.%d kept its address %s", back.ID, back.Address)
	}
	return back, seen.Query.SameIntervalSince
}

// write sends one WRITEFULL through route's primary and requires ONDISK.
func (r *ecEpochRun) write(label string, route osdFixtureOracle, data []byte, seconds uint32, snapc cephmsgr.OSDSnapContext) cephmsgr.OSDReply {
	r.t.Helper()
	o := mutationConnection(r.t, r.c, r.ctx, route)
	defer o.Close()
	request := r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
	request.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
	request.SnapContext = snapc
	reply, err := o.Request(r.ctx, request)
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		r.t.Fatal(label, err, reply.Code, reply.Flags)
	}
	if !reflect.DeepEqual(reply.Submission.Placement, request.Placement) {
		r.t.Fatalf("%s: the submission placement %+v differs from the request's %+v", label, reply.Submission.Placement, request.Placement)
	}
	return reply
}

// TestCephOSDErasureOptimizedK2M2WatchFailoverIntegration: a Go watch on an
// object of the optimized pool, registered at the shard-0 primary, follows
// two primary changes only through the caller's explicit RECONNECT. The
// primary's OSD is killed; once the PG is active under the first parity
// shard's OSD with a primary-first pg_temp, the caller RECONNECTs the same
// cookie there with primary-first numbering. A notification from a second
// client through that primary must reach the reconnected handle and complete
// with its ACK. After the original primary returns and every PG is clean,
// the caller RECONNECTs once more, to it, without primary-first numbering,
// and a notification completes again before the UNWATCH. The native watcher
// list must hold exactly that registration throughout and nothing after the
// UNWATCH, and the client must open no OSD connection the caller did not ask
// for.
//
//fixture:isolated kills the primary OSD, restart waits osd-ready clean; EC pool CRUSH rule and mon_allow_pool_delete config left
func TestCephOSDErasureOptimizedK2M2WatchFailoverIntegration(t *testing.T) {
	r := startECEpochRun(t, 900*time.Second)
	const (
		object      = "ec-watch-failover"
		incarnation = 31
		// Long enough for each primary to see the RECONNECT before a
		// disconnected registration expires.
		watchTimeout = 120
	)
	route, since := r.healthy(object)
	r.write("seed", route, bytes.Repeat([]byte{0x77, 0, 0xff, 'W'}, 1024), 1700007000, cephmsgr.OSDSnapContext{})
	route, since = r.healthy(object)

	watcher := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer watcher.Close()
	gid := watcher.Snapshot().GlobalID
	o := mutationConnection(t, watcher.Client, r.ctx, route)
	request := r.request(route)
	request.ClientIncarnation = incarnation
	watch, initial, err := o.WatchObject(r.ctx, request, cephmsgr.OSDWatchOptions{TimeoutSeconds: watchTimeout, RegistrationGeneration: 3})
	if err != nil {
		t.Fatal("watch at the original primary", err)
	}
	cookie := watch.Cookie()
	listed := func(label string, want int) {
		t.Helper()
		got := nativePool(t, r.ctx, "watchers", map[string]any{"slot": 1, "object": object}).Watchers
		if got == nil || got.Object != object || len(got.Entries) != want || want == 1 && (got.Entries[0].GlobalID != gid || got.Entries[0].Cookie != cookie) {
			t.Fatalf("native watcher list %s: %+v; want %d entry of client.%d cookie %d", label, got, want, gid, cookie)
		}
		t.Logf("native watcher list %s: %+v", label, got.Entries)
	}
	listed("at the original primary", 1)
	notify := func(label string, at osdFixtureOracle, w *cephmsgr.OSDWatch, payload, ack []byte) {
		t.Helper()
		n := mutationConnection(t, r.c, r.ctx, at)
		defer n.Close()
		pending, reply, err := n.NotifyObject(r.ctx, r.request(at), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 15})
		if err != nil {
			t.Fatal("notify", label, err)
		}
		defer pending.Close()
		event, err := w.Next(r.ctx)
		if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.Cookie != cookie || event.Source.ID != at.ID ||
			event.NotifierGlobalID != r.c.Snapshot().GlobalID {
			t.Fatal("notification", label, event, err)
		}
		if _, err := w.Acknowledge(r.ctx, at.Epoch, event, ack); err != nil {
			t.Fatal("acknowledge", label, err)
		}
		done, err := pending.Next(r.ctx)
		if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || done.Replies[0].GlobalID != gid || done.Replies[0].Cookie != cookie ||
			!bytes.Equal(done.Replies[0].Data, ack) || len(done.Missed) != 0 {
			t.Fatal("notify completion", label, done, err)
		}
		t.Logf("notify %s through osd.%d (shard %d, primary-first %+v): notify reply tid %d, one ACK from client.%d cookie %d", label, at.ID, at.Shard, routePrimaryFirst(at), reply.Transaction, gid, cookie)
	}
	notify("at the original primary", route, watch, []byte{0, 0xff, 'o'}, []byte{1, 'o'})
	dialed := watcher.dialSnapshot()
	t.Logf("watch cookie %d of client.%d at osd.%d shard %d, epoch %d pg %s, generation %d", cookie, gid, route.ID, route.Shard, route.Epoch, route.PGID, initial.Operations[0].Watch.Generation)

	fresh, outage := r.replacePrimary(object, route, since)
	o.Close()
	if extra := newDials(dialed, watcher.dialSnapshot(), osdEndpoint(t, route.Address)); len(extra) != 0 {
		t.Fatal("the client connected on its own after the primary was lost:", extra)
	}
	// The registration is object state (object_info_t::watchers), which
	// LIST_WATCHERS reads: the replacement lists it before any RECONNECT.
	listed("under the replacement before the RECONNECT", 1)
	ro := mutationConnection(t, watcher.Client, r.ctx, fresh)
	again := r.request(fresh)
	again.ClientIncarnation = incarnation
	moved, reconnect, err := ro.ReconnectWatch(r.ctx, watch, again)
	if err != nil {
		t.Fatal("RECONNECT to the replacement primary", reconnect, err)
	}
	if moved.Cookie() != cookie || reconnect.Submission.GlobalID != gid || reconnect.Operations[0].Watch.Generation != initial.Operations[0].Watch.Generation+1 ||
		reconnect.Submission.Placement == nil || reconnect.Submission.Placement.PrimaryFirst == nil || int8(reconnect.Submission.Placement.Shard) != fresh.Shard {
		t.Fatal("RECONNECT changed the cookie, kept the generation or lost the primary-first placement", reconnect)
	}
	if err := checkOSDDials(dialed, watcher.dialSnapshot(), []string{osdEndpoint(t, route.Address)}, osdEndpoint(t, fresh.Address)); err != nil {
		t.Fatal("the RECONNECT did not open exactly its one connection:", err)
	}
	listed("after the RECONNECT to the replacement", 1)
	notify("through the replacement", fresh, moved, []byte{0, 0xff, 'r'}, []byte{1, 'r'})
	t.Logf("cookie %d moved osd.%d (shard %d) -> osd.%d (shard %d, wire shard under %+v), epoch %d -> %d, generation %d",
		cookie, route.ID, route.Shard, fresh.ID, fresh.Shard, routePrimaryFirst(fresh), route.Epoch, fresh.Epoch, reconnect.Operations[0].Watch.Generation)

	replaced := r.state(object).Query
	if replaced == nil {
		t.Fatal("no pg query under the replacement before the return")
	}
	dialed = watcher.dialSnapshot()
	back, _ := r.primaryBack(object, route, outage, replaced.SameIntervalSince)
	ro.Close()
	if extra := newDials(dialed, watcher.dialSnapshot(), osdEndpoint(t, fresh.Address)); len(extra) != 0 {
		t.Fatal("the client connected on its own after the primary returned:", extra)
	}
	listed("at the returned primary before the RECONNECT", 1)
	bo := mutationConnection(t, watcher.Client, r.ctx, back)
	defer bo.Close()
	home := r.request(back)
	home.ClientIncarnation = incarnation
	returned, rereconnect, err := bo.ReconnectWatch(r.ctx, moved, home)
	if err != nil {
		t.Fatal("RECONNECT to the returned primary", rereconnect, err)
	}
	if returned.Cookie() != cookie || rereconnect.Operations[0].Watch.Generation != initial.Operations[0].Watch.Generation+2 ||
		rereconnect.Submission.Placement == nil || rereconnect.Submission.Placement.PrimaryFirst != nil || int8(rereconnect.Submission.Placement.Shard) != back.Shard {
		t.Fatal("RECONNECT to the returned primary changed the cookie, kept the generation or used primary-first numbering", rereconnect)
	}
	if err := checkOSDDials(dialed, watcher.dialSnapshot(), []string{osdEndpoint(t, route.Address), osdEndpoint(t, fresh.Address)}, osdEndpoint(t, back.Address)); err != nil {
		t.Fatal("the second RECONNECT did not open exactly its one connection:", err)
	}
	listed("after the RECONNECT to the returned primary", 1)
	notify("at the returned primary", back, returned, []byte{0, 0xff, 'b'}, []byte{1, 'b'})
	if _, err := returned.Unwatch(r.ctx, back.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	listed("after the UNWATCH", 0)
	t.Logf("cookie %d back at osd.%d (shard %d) at %s (was %s), epoch %d, generation %d; unwatched", cookie, back.ID, back.Shard, back.Address, route.Address, back.Epoch,
		rereconnect.Operations[0].Watch.Generation)
}

// TestCephOSDErasureOptimizedK2M2SnapshotFailoverIntegration: self-managed
// snapshot state of an object of the optimized pool across a primary
// replacement and its return. A first clone is made through the shard-0
// primary. The primary's OSD is killed, and with the PG active under the
// first parity shard's OSD with a primary-first pg_temp, the object is read
// at the snapshot and at the head through that primary. A second snapshot id
// is allocated and one overwrite with the newer SnapContext goes through the
// replacement while shard 0 is absent; it must clone the head again. Go reads
// at both ids and at the head and LIST_SNAPS through the replacement must
// match the native reads. After the original primary returns and every PG is
// clean, the same reads and LIST_SNAPS through it must give the same bytes,
// clones and user versions.
//
//fixture:isolated kills the primary OSD, restart waits osd-ready clean; EC pool CRUSH rule and mon_allow_pool_delete config left
func TestCephOSDErasureOptimizedK2M2SnapshotFailoverIntegration(t *testing.T) {
	r := startECEpochRun(t, 900*time.Second)
	const object = "ec-snapshot-failover"
	first := bytes.Repeat([]byte("ec-snap-first\x00\xff"), 400)
	second := bytes.Repeat([]byte("ec-snap-second\x01"), 300)
	third := bytes.Repeat([]byte("ec-snap-third\xfe\x00"), 500)
	route, since := r.healthy(object)
	seeded := r.write("first write", route, first, 1700008000, cephmsgr.OSDSnapContext{})
	allocate := func() uint64 {
		t.Helper()
		allocated, err := r.c.PoolOp(r.ctx, cephmsgr.PoolOpRequest{Pool: uint32(r.facts.pool), Op: cephmsgr.SelfManagedSnapshotCreate})
		if err != nil {
			t.Fatal("allocate a self-managed snapshot id", err)
		}
		id, err := allocated.SnapshotID()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	s1 := allocate()
	cloned := r.write("overwrite under the first snapshot", route, second, 1700008001, cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}})
	route, since = r.healthy(object)
	if cloned.UserVersion <= seeded.UserVersion {
		t.Fatal("the overwrite did not advance the user version", seeded.UserVersion, cloned.UserVersion)
	}

	type view struct {
		data    map[uint64][]byte
		version uint64
		list    cephmsgr.OSDSnapshotList
	}
	// read reads the head and every snapshot id through route's primary,
	// with LIST_SNAPS, and compares each with the native read.
	read := func(label string, at osdFixtureOracle, want map[uint64][]byte, headVersion uint64) view {
		t.Helper()
		o := mutationConnection(t, r.c, r.ctx, at)
		defer o.Close()
		got := view{data: map[uint64][]byte{}}
		for _, snap := range slices.Sorted(maps.Keys(want)) {
			request := r.request(at, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 1 << 16})
			if snap != cephmsgr.OSDHeadSnapshot {
				request.ReadSnapshot = snap
			}
			reply, err := o.Request(r.ctx, request)
			if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || !bytes.Equal(reply.Operations[0].Data, want[snap]) {
				t.Fatalf("%s: Go read at snapshot %#x through osd.%d: %v, code %d, %d operations; want %d bytes", label, snap, at.ID, err, reply.Code, len(reply.Operations), len(want[snap]))
			}
			native := poolObserve(t, r.ctx, 1, object, "", "")
			if snap != cephmsgr.OSDHeadSnapshot {
				native = poolObserveSnap(t, r.ctx, 1, object, "", "", snap)
			}
			if native.Code != 0 || !bytes.Equal(native.Data, want[snap]) {
				t.Fatalf("%s: native read at snapshot %#x differs (%d bytes, code %d)", label, snap, len(native.Data), native.Code)
			}
			if snap == cephmsgr.OSDHeadSnapshot {
				if reply.UserVersion != headVersion || native.Version != headVersion {
					t.Fatalf("%s: head user version Go %d native %d; want %d", label, reply.UserVersion, native.Version, headVersion)
				}
				got.version = reply.UserVersion
			}
			got.data[snap] = reply.Operations[0].Data
		}
		listed, err := o.Request(r.ctx, r.request(at, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}))
		if err != nil || listed.Code != 0 || len(listed.Operations) != 1 {
			t.Fatal(label, "LIST_SNAPS", err, listed.Code)
		}
		got.list = listed.Operations[0].Snapshots
		t.Logf("%s through osd.%d (shard %d, primary-first %+v) at epoch %d: head version %d, snapset sequence %d, clones %+v",
			label, at.ID, at.Shard, routePrimaryFirst(at), at.Epoch, got.version, got.list.Sequence, got.list.Clones)
		return got
	}
	// clones requires the listed clones, in order, to be one per snapshot
	// id with the bytes it preserved, then the head.
	clones := func(label string, list cephmsgr.OSDSnapshotList, sizes map[uint64]int, sequence uint64) {
		t.Helper()
		ids := slices.Sorted(maps.Keys(sizes))
		if list.Sequence != sequence || len(list.Clones) != len(ids)+1 {
			t.Fatalf("%s: snapset sequence %d with %d entries; want %d with %d clones and the head", label, list.Sequence, len(list.Clones), sequence, len(ids))
		}
		for i, id := range ids {
			c := list.Clones[i]
			if c.ID != id || !slices.Equal(c.Snapshots, []uint64{id}) || c.Size != uint64(sizes[id]) {
				t.Fatalf("%s: clone %d is %+v; want id %d for snapshot %d of %d bytes", label, i, c, id, id, sizes[id])
			}
		}
		if head := list.Clones[len(ids)]; head.ID != cephmsgr.OSDHeadSnapshot || len(head.Snapshots) != 0 {
			t.Fatalf("%s: the last entry %+v is not the head", label, head)
		}
	}
	before := read("before the outage", route, map[uint64][]byte{s1: first, cephmsgr.OSDHeadSnapshot: second}, cloned.UserVersion)
	clones("before the outage", before.list, map[uint64]int{s1: len(first)}, s1)

	fresh, outage := r.replacePrimary(object, route, since)
	away := read("under the replacement", fresh, map[uint64][]byte{s1: first, cephmsgr.OSDHeadSnapshot: second}, cloned.UserVersion)
	if !reflect.DeepEqual(away.list, before.list) {
		t.Fatalf("LIST_SNAPS under the replacement %+v differs from before the outage %+v", away.list, before.list)
	}
	s2 := allocate()
	if s2 <= s1 {
		t.Fatal("the second snapshot id is not newer", s1, s2)
	}
	degraded := r.write("overwrite under the second snapshot through the replacement", fresh, third, 1700008002, cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}})
	if degraded.UserVersion <= cloned.UserVersion || degraded.Submission.Placement.PrimaryFirst == nil {
		t.Fatal("the write through the replacement did not advance the version or was not sent with primary-first numbering", cloned.UserVersion, degraded.UserVersion, degraded.Submission.Placement)
	}
	want := map[uint64][]byte{s1: first, s2: second, cephmsgr.OSDHeadSnapshot: third}
	sizes := map[uint64]int{s1: len(first), s2: len(second)}
	written := read("after the write through the replacement", fresh, want, degraded.UserVersion)
	clones("after the write through the replacement", written.list, sizes, s2)

	replaced := r.state(object).Query
	if replaced == nil {
		t.Fatal("no pg query under the replacement before the return")
	}
	back, _ := r.primaryBack(object, route, outage, replaced.SameIntervalSince)
	returned := read("after the return", back, want, degraded.UserVersion)
	if !reflect.DeepEqual(returned.list, written.list) {
		t.Fatalf("LIST_SNAPS after the return %+v differs from the one under the replacement %+v", returned.list, written.list)
	}
	t.Logf("snapshots %d and %d: clones of %d and %d bytes, head %d bytes at version %d, unchanged across the primary's outage and return", s1, s2, len(first), len(second), len(third), degraded.UserVersion)
}

// TestCephOSDErasureOptimizedK2M2PGBackoffIntegration: a PG-wide backoff of
// an optimized EC PG, released by the interval change of a returning OSD.
// The OSDs of the nonprimary data shard and of the last parity shard are
// killed, which leaves two of four shards, below min_size 3: the PG must be
// peered and not active under the unchanged shard-0 primary. One Go
// APPEND+SETXATTR on a connection to that primary must draw a native BLOCK
// for the whole PG on its actual shard; the caller ACKs it. Further requests
// for the PG on that connection, also one that names another shard as an
// optimized pool's primary-first numbering would, must then be refused
// locally without being sent. Starting the parity shard's OSD ends the
// interval: the primary must send the matching UNBLOCK and the PG must be
// active again without the nonprimary shard. The dropped APPEND keeps its
// unknown outcome; the native object is unchanged, and one explicit
// resubmission under the original identity applies it once. After the last
// OSD returns and every PG is clean, the native state is unchanged.
//
//fixture:isolated kills two OSDs, restart waits osd-ready clean; EC pool CRUSH rule and mon_allow_pool_delete config left
func TestCephOSDErasureOptimizedK2M2PGBackoffIntegration(t *testing.T) {
	r := startECEpochRun(t, 900*time.Second)
	const (
		object          = "ec-pg-backoff"
		attr            = "ec-backoff"
		seedIncarnation = 40
		heldIncarnation = 41
		readIncarnation = 42
		seeded          = 1700009000
		mutated         = 1700009001
	)
	route, _ := r.healthy(object)
	seed := bytes.Repeat([]byte{0x42, 0, 0xff, 'B'}, 1500)
	r.write("seed", route, seed, seeded, cephmsgr.OSDSnapContext{})
	route, since := r.healthy(object)
	pre := poolObserve(t, r.ctx, 1, object, "", "")
	if pre.Code != 0 || !bytes.Equal(pre.Data, seed) || pre.Seconds != seeded || xattrInventory(pre)[attr] != "" {
		t.Fatal("native pre-state differs", pre.Code, pre.Version, pre.Seconds)
	}
	// Keep the primary's shard and the first parity shard: k=2 shards
	// remain, enough to decode, one fewer than min_size.
	parity := r.facts.size - (erasureK2M2.m)
	var holes []int
	for shard := range r.facts.size {
		if shard != int(route.Shard) && shard != parity {
			holes = append(holes, shard)
		}
	}
	if route.Shard != 0 || len(holes) != 2 || !slices.Contains(holes, r.facts.nonprimary[0]) {
		t.Fatalf("the placement %+v with nonprimary shards %v does not leave shards 0 and %d", route, r.facts.nonprimary, parity)
	}
	t.Logf("pre-state version %d; placement epoch %d pg %s acting %v primary osd.%d shard %d; taking out shards %v (osd %d, %d)",
		pre.Version, route.Epoch, route.PGID, route.Acting, route.ID, route.Shard, holes, route.Acting[holes[0]], route.Acting[holes[1]])

	client := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer client.Close()
	gid := client.Snapshot().GlobalID
	o := mutationConnection(t, client.Client, r.ctx, route)
	defer o.Close()
	// One read on the connection before the outage.
	probe := r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	probe.ClientIncarnation = readIncarnation
	if reply, err := o.Request(r.ctx, probe); err != nil || reply.Code != 0 {
		t.Fatal("STAT before the outage", err, reply.Code)
	}
	endpoint := osdEndpoint(t, route.Address)
	dialed := client.dialSnapshot()

	// The last parity shard first, then the nonprimary data shard.
	outages := []*ecOutage{r.kill(route.Acting[holes[1]]), r.kill(route.Acting[holes[0]])}
	peered := r.waitState("peered without two shards", object, func(o pgIntervalObservation) error { return o.peeredOn(route, holes, since) })

	tail := []byte("backoff\x00\xff" + object)
	value := []byte{0, 0xff, 'b'}
	ops := []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
	}
	held := r.request(route, ops...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	callCtx, cancelCall := context.WithCancel(r.ctx)
	defer cancelCall()
	go func() {
		reply, err := o.Request(callCtx, held)
		finished <- result{reply, err}
	}()
	eventCtx, stopEvent := context.WithTimeout(r.ctx, 30*time.Second)
	block, err := o.NextBackoff(eventCtx)
	stopEvent()
	if err != nil {
		select {
		case early := <-finished:
			t.Fatal("no native BLOCK; the APPEND ended", early.reply.Code, early.err, err)
		default:
			t.Fatal("no native BLOCK", err)
		}
	}
	if err := checkPGBackoff(block, cephmsgr.OSDBackoffBlock, route, r.pgNum, gid); err != nil {
		t.Fatal("native BLOCK:", err)
	}
	if block.MapEpoch < peered.Query.Epoch || block.Matched {
		t.Fatalf("BLOCK at epoch %d (matched %v); the peered primary was at epoch %d", block.MapEpoch, block.Matched, peered.Query.Epoch)
	}
	t.Logf("BLOCK id %d for pg %d.%x shard %d at epoch %d, range [%+v, %+v)", block.ID, block.Pool, block.Seed, block.Shard, block.MapEpoch, block.Begin, block.End)
	if err := o.AckBackoff(r.ctx, block); err != nil {
		t.Fatal("explicit ACK of the BLOCK", err)
	}
	select {
	case early := <-finished:
		t.Fatal("the APPEND that drew the BLOCK was answered", early.reply.Code, early.err)
	default:
	}
	// Refused locally, never sent: the same object, and the same object
	// addressed as primary-first numbering would name another shard.
	var unknown *cephmsgr.OutcomeUnknownError
	refused := func(label string, request cephmsgr.OSDRequest) {
		t.Helper()
		reply, err := o.Request(r.ctx, request)
		if !errors.Is(err, cephmsgr.ErrOSDBackoff) || errors.As(err, &unknown) || reply.Transaction != 0 || len(reply.RawFront) != 0 ||
			reply.Submission.Transaction == 0 || !reflect.DeepEqual(reply.Submission.Placement, request.Placement) {
			t.Fatal(label, "was not refused locally as an unsent request", reply.Transaction, reply.Submission, err)
		}
		p := reply.Submission.Placement
		t.Logf("%s refused locally: tid %d, pg %d.%x shard %d primary-first %+v", label, reply.Submission.Transaction, p.PG.Pool, p.PG.Seed, p.Shard, p.PrimaryFirst)
	}
	read := r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
	read.ClientIncarnation = readIncarnation
	refused("READ of the blocked object", read)
	renumbered := r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	renumbered.ClientIncarnation = readIncarnation
	renumbered.Placement.Shard = uint8(parity)
	renumbered.Placement.PrimaryFirst = &cephmsgr.OSDPrimaryFirst{PoolSize: uint8(r.facts.size), NonprimaryShards: uint8(len(r.facts.nonprimary))}
	refused("STAT naming the parity shard with primary-first numbering", renumbered)

	// The parity shard's OSD returns: a new interval with three shards.
	r.start(outages[0])
	releaseCtx, stopRelease := context.WithTimeout(r.ctx, 180*time.Second)
	unblock, err := o.NextBackoff(releaseCtx)
	stopRelease()
	if err != nil {
		t.Fatal("no native UNBLOCK after the interval change", err)
	}
	if err := checkPGBackoff(unblock, cephmsgr.OSDBackoffUnblock, route, r.pgNum, gid); err != nil || !unblock.Matched || unblock.ID != block.ID ||
		unblock.SessionGeneration != block.SessionGeneration || unblock.MapEpoch <= block.MapEpoch ||
		!bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) || !bytes.Equal(unblock.End.Raw, block.End.Raw) {
		t.Fatalf("native UNBLOCK %+v does not release BLOCK %d of epoch %d: %v", unblock, block.ID, block.MapEpoch, err)
	}
	t.Logf("UNBLOCK id %d at epoch %d (BLOCK at %d), matched", unblock.ID, unblock.MapEpoch, block.MapEpoch)
	active := r.waitState("active again without the nonprimary shard", object, func(o pgIntervalObservation) error {
		return o.activeWithout(route, holes[:1], peered.Query.SameIntervalSince)
	})
	if active.Query.SameIntervalSince > unblock.MapEpoch {
		t.Fatalf("the PG's interval started at epoch %d, after the UNBLOCK of epoch %d", active.Query.SameIntervalSince, unblock.MapEpoch)
	}

	// The APPEND that drew the BLOCK was dropped by the primary; nothing
	// answers it. The caller ends its local wait.
	cancelCall()
	lost := <-finished
	sub := lost.reply.Submission
	if !errors.As(lost.err, &unknown) || !errors.Is(lost.err, context.Canceled) || sub.Transaction == 0 || sub.GlobalID != gid || sub.ClientIncarnation != heldIncarnation ||
		sub.MapEpoch != route.Epoch || sub.Placement == nil || sub.Placement.Primary != route.ID || int8(sub.Placement.Shard) != route.Shard {
		t.Fatal("the dropped APPEND did not keep its unknown outcome and original identity", lost.err, sub)
	}
	n := poolObserve(t, r.ctx, 1, object, "", "")
	if n.Code != 0 || n.Version != pre.Version || !bytes.Equal(n.Data, seed) || n.Seconds != seeded || n.Nanos != pre.Nanos || !maps.Equal(xattrInventory(n), xattrInventory(pre)) {
		t.Fatal("native state after the UNBLOCK differs from the pre-state", n.Version, pre.Version, len(n.Data), n.Seconds)
	}
	t.Logf("APPEND tid %d (incarnation %d, epoch %d) ended unknown: %v; native object unchanged at version %d", sub.Transaction, sub.ClientIncarnation, sub.MapEpoch, lost.err, n.Version)

	// The caller's explicit decision: resubmit the same operations once
	// under the original identity, routed by the current placement.
	now := *active.After
	retry := r.request(now, ops...)
	retry.ClientIncarnation = heldIncarnation
	retry.ModificationTime = held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	retryCtx, stopRetry := context.WithTimeout(r.ctx, 30*time.Second)
	reply, err := o.Request(retryCtx, retry)
	stopRetry()
	if err != nil {
		t.Fatal("explicit resubmission after the UNBLOCK", err)
	}
	if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= pre.Version ||
		reply.Submission.MapEpoch != now.Epoch || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != heldIncarnation {
		t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	want := append(bytes.Clone(seed), tail...)
	final := poolObserve(t, r.ctx, 1, object, "", "")
	if final.Code != 0 || !bytes.Equal(final.Data, want) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion || final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	if extra := newDials(dialed, client.dialSnapshot(), endpoint); len(extra) != 0 {
		t.Fatal("the client opened another connection:", extra)
	}
	t.Logf("resubmission tid %d at epoch %d applied once: version %d -> %d, replay %s", reply.Transaction, reply.MapEpoch, pre.Version, final.Version, replayVersion(reply))

	// The last OSD returns; the PG is clean on its original placement.
	r.start(outages[1])
	r.clean()
	back := r.waitState("clean again", object, func(o pgIntervalObservation) error {
		_, err := o.cleanAgainOn(route, active.Query.SameIntervalSince)
		return err
	})
	if back.Before.Address != route.Address {
		t.Fatalf("the primary osd.%d moved from %s to %s although it never stopped", route.ID, route.Address, back.Before.Address)
	}
	recovered := poolObserve(t, r.ctx, 1, object, "", "")
	if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || recovered.Seconds != final.Seconds || recovered.Nanos != final.Nanos ||
		!maps.Equal(xattrInventory(recovered), xattrInventory(final)) {
		t.Fatal("native state after recovery differs", recovered.Version, final.Version)
	}
	if !errors.As(lost.err, &unknown) {
		t.Fatal("the resubmission changed the original outcome")
	}
}

// pgStateObservation builds one synthetic read-only observation of PG 3.7 on
// acting [3 2 1 0] under osd.3 (shard 0), recorded at epoch 25 in the
// interval since 24, at the given epoch with the given positions empty and
// state, in an interval that started one epoch earlier. No daemon produced
// it.
func pgStateObservation(epoch uint32, state string, holes ...int) (pgIntervalObservation, osdFixtureOracle) {
	_, route := pgIntervalChange()
	holed := holedSet(route.Acting, holes...)
	before := route
	before.Up, before.Acting, before.Epoch = slices.Clone(holed), slices.Clone(holed), epoch
	after := before
	after.Up, after.Acting = slices.Clone(holed), slices.Clone(holed)
	return pgIntervalObservation{Before: &before, After: &after, Query: &pgIntervalQuery{State: state, Epoch: epoch, Up: slices.Clone(holed), Acting: slices.Clone(holed),
		SameIntervalSince: epoch - 1, LastEpochStarted: epoch, Info: pgIntervalInfo{PGID: "3.7s0", Shard: 0, LastUpdate: "25'2", LastComplete: "25'2"}}}, route
}

func TestPGPeeredOn(t *testing.T) {
	seen, route := pgStateObservation(30, "undersized+degraded+peered", 1, 3)
	if err := seen.peeredOn(route, []int{1, 3}, 24); err != nil {
		t.Fatal("peered PG without shards 1 and 3 refused:", err)
	}
	if err := seen.peeredOn(route, []int{3, 1}, 24); err != nil {
		t.Fatal("the same positions in another order refused:", err)
	}
	if other, _ := pgStateObservation(30, "peered+undersized", 1, 3); other.peeredOn(route, []int{1, 3}, 24) != nil {
		t.Fatal("peered PG without degraded objects refused")
	}
	both := func(edit func(*osdFixtureOracle)) func(*pgIntervalObservation) {
		return func(o *pgIntervalObservation) {
			edit(o.Before)
			edit(o.After)
		}
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"active":          func(o *pgIntervalObservation) { o.Query.State = "active+undersized+degraded" },
		"peering":         func(o *pgIntervalObservation) { o.Query.State = "peering" },
		"down":            func(o *pgIntervalObservation) { o.Query.State = "down" },
		"incomplete":      func(o *pgIntervalObservation) { o.Query.State = "incomplete" },
		"remapped":        func(o *pgIntervalObservation) { o.Query.State = "undersized+peered+remapped" },
		"placement moved": func(o *pgIntervalObservation) { o.After.Epoch = 31 },
		"no placement":    func(o *pgIntervalObservation) { o.Before = nil },
		"no query":        func(o *pgIntervalObservation) { o.Query = nil },
		"query refused":   func(o *pgIntervalObservation) { o.Code = -11 },
		"one position empty": both(func(p *osdFixtureOracle) {
			p.Up, p.Acting = holedSet([]int32{3, 2, 1, 0}, 1), holedSet([]int32{3, 2, 1, 0}, 1)
		}),
		"other positions empty": both(func(p *osdFixtureOracle) {
			p.Up, p.Acting = holedSet([]int32{3, 2, 1, 0}, 1, 2), holedSet([]int32{3, 2, 1, 0}, 1, 2)
		}),
		"a pg_temp":                  both(func(p *osdFixtureOracle) { p.PGTemp = true }),
		"another primary":            both(func(p *osdFixtureOracle) { p.ID, p.PrimaryID, p.Shard = 1, 1, 2 }),
		"another address":            both(func(p *osdFixtureOracle) { p.Address = "v2:192.0.2.9:6800/9" }),
		"the recorded epoch":         both(func(p *osdFixtureOracle) { p.Epoch = 25 }),
		"query on other sets":        func(o *pgIntervalObservation) { o.Query.Acting = []int32{3, 2, 1, 0} },
		"answered for another shard": func(o *pgIntervalObservation) { o.Query.Info.PGID, o.Query.Info.Shard = "3.7s2", 2 },
		"the recorded interval":      func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 24 },
		"interval after placement":   func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 31 },
		"primary behind":             func(o *pgIntervalObservation) { o.Query.Epoch = 29 },
	} {
		changed, _ := pgStateObservation(30, "undersized+degraded+peered", 1, 3)
		edit(&changed)
		if err := changed.peeredOn(route, []int{1, 3}, 24); err == nil {
			t.Error("observation accepted:", name)
		}
	}
	for name, c := range map[string]struct {
		holes []int
		since uint32
	}{
		"no positions":           {nil, 24},
		"one position":           {[]int{1}, 24},
		"the primary's position": {[]int{0, 1}, 24},
		"a repeated position":    {[]int{1, 1}, 24},
		"out of range":           {[]int{1, 4}, 24},
		"no recorded interval":   {[]int{1, 3}, 0},
		"interval after origin":  {[]int{1, 3}, 26},
	} {
		if err := seen.peeredOn(route, c.holes, c.since); err == nil {
			t.Error("request accepted:", name)
		}
	}
}

func TestPGActiveWithoutAndCleanAgain(t *testing.T) {
	seen, route := pgStateObservation(32, "active+undersized+degraded", 1)
	if err := seen.activeWithout(route, []int{1}, 30); err != nil {
		t.Fatal("active PG without shard 1 refused:", err)
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"peered":           func(o *pgIntervalObservation) { o.Query.State = "undersized+degraded+peered" },
		"clean":            func(o *pgIntervalObservation) { o.Query.State = "active+clean" },
		"the old interval": func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 30 },
		"not started":      func(o *pgIntervalObservation) { o.Query.LastEpochStarted = 25 },
		"primary behind":   func(o *pgIntervalObservation) { o.Query.Epoch = 31 },
		"placement moved":  func(o *pgIntervalObservation) { o.After.Epoch = 33 },
	} {
		changed, _ := pgStateObservation(32, "active+undersized+degraded", 1)
		edit(&changed)
		if err := changed.activeWithout(route, []int{1}, 30); err == nil {
			t.Error("observation accepted:", name)
		}
	}
	if err := seen.activeWithout(route, []int{1, 3}, 30); err == nil {
		t.Error("two empty positions below min_size accepted")
	}

	clean, _ := pgStateObservation(34, "active+clean")
	clean.Before.Address, clean.After.Address = "v2:192.0.2.7:6800/9", "v2:192.0.2.7:6800/9"
	back, err := clean.cleanAgainOn(route, 31)
	if err != nil || back.Address != "v2:192.0.2.7:6800/9" || back.Epoch != 34 {
		t.Fatal("clean PG on the recorded placement refused:", back, err)
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"the recorded interval": func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 31 },
		"a hole":                func(o *pgIntervalObservation) { o.Before.Acting[1], o.After.Acting[1] = crushItemNone, crushItemNone },
		"another primary":       func(o *pgIntervalObservation) { o.Before.PrimaryID, o.After.PrimaryID = 2, 2 },
		"recovering":            func(o *pgIntervalObservation) { o.Query.State = "active+recovering" },
	} {
		changed, _ := pgStateObservation(34, "active+clean")
		edit(&changed)
		if _, err := changed.cleanAgainOn(route, 31); err == nil {
			t.Error("observation accepted:", name)
		}
	}
}

func TestPGBackoffBounds(t *testing.T) {
	for _, c := range []struct {
		seed   uint32
		pgNum  int
		end    uint32
		endMax bool
	}{
		// Eight PGs: the seeds in bit-reversed order are 0 4 2 6 1 5 3 7.
		{0, 8, 4, false}, {4, 8, 2, false}, {6, 8, 1, false}, {3, 8, 7, false}, {7, 8, 0, true},
		{0, 1, 0, true}, {1, 2, 0, true}, {0, 2, 1, false},
	} {
		begin, end, endMax, err := pgBackoffBounds(c.seed, c.pgNum)
		if err != nil || begin != c.seed || end != c.end || endMax != c.endMax {
			t.Errorf("seed %d of %d PGs: [%d, %d max %v) %v; want end %d max %v", c.seed, c.pgNum, begin, end, endMax, err, c.end, c.endMax)
		}
	}
	for _, c := range [][2]int{{8, 8}, {0, 6}, {0, 0}} {
		if _, _, _, err := pgBackoffBounds(uint32(c[0]), c[1]); err == nil {
			t.Error("bounds of seed", c[0], "of", c[1], "PGs accepted")
		}
	}
}

func TestCheckOSDDials(t *testing.T) {
	snapshot := map[string]int{"192.0.2.1:3300": 1, "192.0.2.7:6800": 1, "192.0.2.8:6800": 1}
	osds := []string{"192.0.2.7:6800", "192.0.2.8:6800"}
	with := func(changes map[string]int) map[string]int {
		now := maps.Clone(snapshot)
		maps.Copy(now, changes)
		return now
	}
	for name, now := range map[string]map[string]int{
		"a new endpoint":                  with(map[string]int{"192.0.2.9:6800": 1}),
		"an endpoint dialed before":       with(map[string]int{"192.0.2.7:6800": 2}),
		"a new endpoint and a MON redial": with(map[string]int{"192.0.2.9:6800": 1, "192.0.2.1:3300": 2}),
	} {
		at := "192.0.2.9:6800"
		if name == "an endpoint dialed before" {
			at = "192.0.2.7:6800"
		}
		if err := checkOSDDials(snapshot, now, osds, at); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	for name, now := range map[string]map[string]int{
		"no dial":                   snapshot,
		"two attempts":              with(map[string]int{"192.0.2.9:6800": 2}),
		"another OSD as well":       with(map[string]int{"192.0.2.9:6800": 1, "192.0.2.8:6800": 2}),
		"an unknown endpoint":       with(map[string]int{"192.0.2.9:6800": 1, "192.0.2.10:6800": 1}),
		"only the old OSD":          with(map[string]int{"192.0.2.8:6800": 2}),
		"elsewhere than the target": with(map[string]int{"192.0.2.10:6800": 1}),
	} {
		if err := checkOSDDials(snapshot, now, osds, "192.0.2.9:6800"); err == nil {
			t.Error("dials accepted:", name)
		}
	}
}

func TestRestoreOSDOutages(t *testing.T) {
	run := func(failStart map[int32]bool, cleanErr error) ([]string, []error) {
		var calls []string
		errs := restoreOSDOutages([]int32{3, 1, 2}, func(id int32) error {
			calls = append(calls, "start "+strconv.Itoa(int(id)))
			if failStart[id] {
				return errors.New("start refused")
			}
			return nil
		}, func() error {
			calls = append(calls, "clean")
			return cleanErr
		})
		return calls, errs
	}
	if calls, errs := run(nil, nil); len(errs) != 0 || !slices.Equal(calls, []string{"start 3", "start 1", "start 2", "clean"}) {
		t.Fatal("every start succeeded:", calls, errs)
	}
	// A failed start does not stop the later ones, and the clean state is
	// not asked for while an OSD is down.
	if calls, errs := run(map[int32]bool{3: true}, nil); len(errs) != 2 || !slices.Equal(calls, []string{"start 3", "start 1", "start 2"}) ||
		!strings.Contains(errs[0].Error(), "osd.3") || !strings.Contains(errs[1].Error(), "not checked") {
		t.Fatal("the first start failed:", calls, errs)
	}
	if calls, errs := run(map[int32]bool{1: true, 2: true}, nil); len(errs) != 3 || !slices.Equal(calls, []string{"start 3", "start 1", "start 2"}) {
		t.Fatal("two later starts failed:", calls, errs)
	}
	if calls, errs := run(nil, errors.New("not clean")); len(errs) != 1 || !slices.Equal(calls, []string{"start 3", "start 1", "start 2", "clean"}) {
		t.Fatal("the clean check failed:", calls, errs)
	}
}

// TestECEpochRunRecoversEveryOutageOnce ends a run early with OSDs killed
// and not started, as a fatal check after the kills would, against a fake
// fixture control that refuses the clean check while any OSD it killed is
// down. The run's cleanups must start every outstanding OSD, ask for clean
// PGs once after the last start and remove the pool only after that.
func TestECEpochRunRecoversEveryOutageOnce(t *testing.T) {
	t.Setenv("CEPH_MSGR_CONTROL_DIR", t.TempDir())
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	for name, c := range map[string]struct {
		body func(r *ecEpochRun)
		want []string
	}{
		"two OSDs down": {func(r *ecEpochRun) { r.kill(3); r.kill(1) },
			[]string{"kill osd 3", "kill osd 1", "start osd 3", "start osd 1", "verify osd-ready clean", "remove pool"}},
		"one of two started": {func(r *ecEpochRun) { first := r.kill(3); r.kill(1); r.start(first) },
			[]string{"kill osd 3", "kill osd 1", "start osd 3", "start osd 1", "verify osd-ready clean", "remove pool"}},
		"both started": {func(r *ecEpochRun) { a, b := r.kill(3), r.kill(1); r.start(b); r.start(a) },
			[]string{"kill osd 3", "kill osd 1", "start osd 1", "start osd 3", "remove pool"}},
		"no outage": {func(*ecEpochRun) {}, []string{"remove pool"}},
	} {
		var calls []string
		down := map[string]bool{}
		var refused error
		t.Run(name, func(t *testing.T) {
			control := func(_ context.Context, action, daemon, id string) error {
				calls = append(calls, strings.Join([]string{action, daemon, id}, " "))
				switch {
				case action == "kill" && daemon == "osd":
					down[id] = true
					receipt, _ := json.Marshal(map[string]any{"daemon": "osd." + id, "exit_code": 137})
					return testcluster.WritePublicEvidence(filepath.Join(dir, "osd-crash-state.json"), receipt)
				case action == "start" && daemon == "osd":
					delete(down, id)
				case action == "verify" && daemon == "osd-ready" && len(down) != 0:
					refused = fmt.Errorf("clean PGs asked for with osd %v down", slices.Sorted(maps.Keys(down)))
					return refused
				}
				return nil
			}
			r := &ecEpochRun{t: t, ctx: context.Background(), control: control}
			r.teardown(func() { calls = append(calls, "remove pool") })
			// The test ends here, before any explicit restart or clean check.
			c.body(r)
		})
		if refused != nil || len(down) != 0 || !slices.Equal(calls, c.want) {
			t.Errorf("%s: fixture controls %q (down %v, %v); want %q", name, calls, down, refused, c.want)
		}
	}
}
